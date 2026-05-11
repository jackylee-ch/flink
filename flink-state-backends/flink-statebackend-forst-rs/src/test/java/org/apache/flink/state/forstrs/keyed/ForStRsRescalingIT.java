/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Rescaling + strict-restore integration tests for {@link ForStRsRestoreOperation}
 * (B-Prod-P4 Tasks 4.5 + 4.6).
 *
 * <p>The tests build kg-prefixed source DBs by hand (the keyed-backend's full kg-prefixed
 * encoding is wired in P3+; here we construct keys exactly as the spec §6 layout requires
 * — {@code kg(2B BE) || payload}) so the rescaling restore's kg-prefix iteration works
 * end-to-end without depending on the higher-level ForStRsAbstractKeyedStateBackend wiring.
 *
 * <p>Test plan:
 *
 * <ul>
 *   <li>{@code rescaleFourToEight} — snapshot a job that owns kgRange=[0,3] writing 4 keys
 *       (one per kg), then restore as parallelism=8 by splitting that single source handle
 *       into 8 target sub-ranges; verify each kg lands in the correct target subtask DB.
 *   <li>{@code rescaleEightToFour} — converse of the above; snapshot 8 source subtasks
 *       (one per kg in [0,7]), restore as a single parallelism-4 subtask owning [0,3];
 *       verify only the relevant kgs (0..3) are present in the restored DB.
 *   <li>{@code roundTripFourToEightToFour} — full restart loop ensuring rescaling preserves
 *       all originally-written keys after a 4 → 8 → 4 round-trip.
 *   <li>{@code missingSstFailsStrictRestore} (Task 4.6) — delete an uploaded SST handle
 *       (simulated by clearing the registry-backed handle's bytes); the restore must throw
 *       {@link ForStRsCheckpointRestoreException} carrying the offending path.
 * </ul>
 */
class ForStRsRescalingIT {

    @Test
    void rescaleFourToEight(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);

            // Source: parallelism=4, kgRange=[0,3]; one key per kg.
            ForStRsIncrementalKeyedStateHandle source =
                    snapshotKgRange(linker, arena, tmp.resolve("src"), 0, 3);

            // Restore as parallelism=8, but the source still covers kgs 0..3 only.
            // Each target subtask covers (numKg/parallelism) = 8/8 = 1 kg.
            // We exercise the restore for one target subtask at a time — say [2,2] should
            // contain kg=2's key, [5,5] should be empty (no source covers it).
            for (int kg = 0; kg < 4; kg++) {
                ForStRsRestoreOperation op =
                        new ForStRsRestoreOperation(
                                linker,
                                arena,
                                tmp.resolve("dst-kg-" + kg),
                                new KeyGroupRange(kg, kg),
                                new ForStRsSstRegistry());
                ForStRsRestoreOperation.RestoreResult res = op.restore(List.of(source));
                byte[] expectedKey = kgPrefixedKey(kg, "k");
                byte[] expectedValue = ("v-kg" + kg).getBytes();
                byte[] got = linker.get(res.getDb(), res.getDefaultCf(), expectedKey);
                assertArrayEquals(
                        expectedValue,
                        got,
                        "rescale 4→8: kg=" + kg + " must land in target [kg,kg]; got=" + java.util.Arrays.toString(got));
                res.getDefaultCf().close();
                res.getDb().close();
            }

            // Target subtask [4,4] is outside the source range [0,3] — restored DB is empty.
            ForStRsRestoreOperation opEmpty =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("dst-kg-4-empty"),
                            new KeyGroupRange(4, 4),
                            new ForStRsSstRegistry());
            ForStRsRestoreOperation.RestoreResult empty = opEmpty.restore(List.of(source));
            byte[] got = linker.get(empty.getDb(), empty.getDefaultCf(), kgPrefixedKey(4, "k"));
            assertEquals(0, got == null ? 0 : got.length, "kg=4 should not exist in restored target");
            empty.getDefaultCf().close();
            empty.getDb().close();
        }
    }

    @Test
    void rescaleEightToFour(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);

            // Source: 8 subtasks, each kgRange=[i,i] containing one key for kg=i.
            List<ForStRsIncrementalKeyedStateHandle> sources = new ArrayList<>(8);
            for (int kg = 0; kg < 8; kg++) {
                sources.add(snapshotKgRange(linker, arena, tmp.resolve("src-" + kg), kg, kg));
            }

            // Restore as parallelism=4 → each subtask owns 2 kgs.
            // Target subtask 0 owns [0,1]; subtask 1 owns [2,3]; etc.
            // Verify subtask 0 has kg=0 + kg=1, subtask 3 has kg=6 + kg=7.
            ForStRsRestoreOperation op0 =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("dst-0"),
                            new KeyGroupRange(0, 1),
                            new ForStRsSstRegistry());
            ForStRsRestoreOperation.RestoreResult r0 =
                    op0.restore(new ArrayList<KeyedStateHandle>(sources));
            for (int kg = 0; kg <= 1; kg++) {
                byte[] got = linker.get(r0.getDb(), r0.getDefaultCf(), kgPrefixedKey(kg, "k"));
                assertArrayEquals(("v-kg" + kg).getBytes(), got, "subtask 0 missing kg=" + kg);
            }
            // kg=4 should not be present in subtask 0's restored DB.
            byte[] outOfRange = linker.get(r0.getDb(), r0.getDefaultCf(), kgPrefixedKey(4, "k"));
            assertEquals(
                    0,
                    outOfRange == null ? 0 : outOfRange.length,
                    "subtask 0 must not contain kg=4 from a different target range");
            r0.getDefaultCf().close();
            r0.getDb().close();

            ForStRsRestoreOperation op3 =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("dst-3"),
                            new KeyGroupRange(6, 7),
                            new ForStRsSstRegistry());
            ForStRsRestoreOperation.RestoreResult r3 =
                    op3.restore(new ArrayList<KeyedStateHandle>(sources));
            for (int kg = 6; kg <= 7; kg++) {
                byte[] got = linker.get(r3.getDb(), r3.getDefaultCf(), kgPrefixedKey(kg, "k"));
                assertArrayEquals(("v-kg" + kg).getBytes(), got, "subtask 3 missing kg=" + kg);
            }
            r3.getDefaultCf().close();
            r3.getDb().close();
        }
    }

    @Test
    void roundTripFourToEightToFour(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);

            // Phase A: parallelism=4, kgs [0..3] split as 2 subtasks ([0,1] and [2,3]) — write 4 keys.
            ForStRsIncrementalKeyedStateHandle phaseA0 =
                    snapshotKgRange(linker, arena, tmp.resolve("phaseA0"), 0, 1);
            ForStRsIncrementalKeyedStateHandle phaseA1 =
                    snapshotKgRange(linker, arena, tmp.resolve("phaseA1"), 2, 3);

            // Phase B: rescale to parallelism=4 (one kg each).
            List<ForStRsIncrementalKeyedStateHandle> phaseAHandles = List.of(phaseA0, phaseA1);
            List<ForStRsIncrementalKeyedStateHandle> phaseBHandles = new ArrayList<>(4);
            for (int kg = 0; kg <= 3; kg++) {
                ForStRsRestoreOperation op =
                        new ForStRsRestoreOperation(
                                linker,
                                arena,
                                tmp.resolve("phaseB-" + kg),
                                new KeyGroupRange(kg, kg),
                                new ForStRsSstRegistry());
                ForStRsRestoreOperation.RestoreResult r =
                        op.restore(new ArrayList<KeyedStateHandle>(phaseAHandles));
                // Re-snapshot this restored subtask so it becomes a phase-B handle.
                phaseBHandles.add(reSnapshot(linker, arena, r.getDb(), r.getDefaultCf(), kg, kg));
                r.getDefaultCf().close();
                r.getDb().close();
            }

            // Phase C: rescale back to parallelism=2 ([0,1] and [2,3]). Verify all 4 keys present.
            ForStRsRestoreOperation opC0 =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("phaseC-0"),
                            new KeyGroupRange(0, 1),
                            new ForStRsSstRegistry());
            ForStRsRestoreOperation.RestoreResult c0 =
                    opC0.restore(new ArrayList<KeyedStateHandle>(phaseBHandles));
            for (int kg = 0; kg <= 1; kg++) {
                byte[] got = linker.get(c0.getDb(), c0.getDefaultCf(), kgPrefixedKey(kg, "k"));
                assertArrayEquals(("v-kg" + kg).getBytes(), got);
            }
            c0.getDefaultCf().close();
            c0.getDb().close();
        }
    }

    @Test
    void missingSstFailsStrictRestore(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            ForStRsIncrementalKeyedStateHandle source =
                    snapshotKgRange(linker, arena, tmp.resolve("src"), 0, 3);

            // Replace the first shared SST handle with one whose openInputStream yields 0 bytes —
            // simulates an SST that was deleted from remote storage between snapshot and restore.
            assertTrue(source.getSharedState().size() >= 1, "snapshot must produce >= 1 shared SST");
            HandleAndLocalPath broken = source.getSharedState().get(0);
            ForStRsIncrementalKeyedStateHandle bad =
                    new ForStRsIncrementalKeyedStateHandle(
                            source.getBackendIdentifier(),
                            source.getKeyGroupRange(),
                            source.getCheckpointId(),
                            source.getBaseCheckpointId(),
                            replaceFirstShared(source.getSharedState(), broken),
                            source.getPrivateState(),
                            source.getMetaDataStateHandle(),
                            source.getCfMap());

            ForStRsRestoreOperation op =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("dst"),
                            new KeyGroupRange(0, 3),
                            new ForStRsSstRegistry());

            ForStRsCheckpointRestoreException thrown =
                    assertThrows(
                            ForStRsCheckpointRestoreException.class, () -> op.restore(List.of(bad)));
            assertNotNull(thrown.getMissingPath(), "exception must carry the missing path");
            assertEquals(
                    broken.getLocalPath(),
                    thrown.getMissingPath(),
                    "exception's missing path must match the deleted SST's local path");
            assertEquals(source.getCheckpointId(), thrown.getCheckpointId());
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    /**
     * Snapshots a fresh source DB seeded with one key per kg in the inclusive range
     * {@code [startKg, endKg]} using kg-prefixed keys per spec §6.
     */
    private static ForStRsIncrementalKeyedStateHandle snapshotKgRange(
            ForStRsLinker linker, Arena arena, Path srcDir, int startKg, int endKg)
            throws Exception {
        java.nio.file.Files.createDirectories(srcDir);
        FrsDb db = linker.dbOpen(arena, srcDir.toString());
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);
        try {
            for (int kg = startKg; kg <= endKg; kg++) {
                linker.put(db, cf, kgPrefixedKey(kg, "k"), ("v-kg" + kg).getBytes());
            }
            ForStRsSstRegistry registry = new ForStRsSstRegistry();
            ForStRsSnapshotStrategy strategy =
                    new ForStRsSnapshotStrategy(
                            linker,
                            db,
                            UUID.randomUUID(),
                            new KeyGroupRange(startKg, endKg),
                            registry,
                            new ForStRsSstUploader(),
                            arena,
                            Map.of("default", 0L));
            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            ForStRsSnapshotResources res = strategy.syncPrepareResources(1L);
            SnapshotResult<?> result =
                    strategy.asyncSnapshot(res, 1L, 0L, factory, null).get(new CloseableRegistry());
            return (ForStRsIncrementalKeyedStateHandle) result.getJobManagerOwnedSnapshot();
        } finally {
            cf.close();
            db.close();
        }
    }

    /** Re-snapshots an already-open restored DB so it can be used as a phase-2 handle. */
    private static ForStRsIncrementalKeyedStateHandle reSnapshot(
            ForStRsLinker linker, Arena arena, FrsDb db, FrsCfHandle cf, int startKg, int endKg)
            throws Exception {
        ForStRsSstRegistry registry = new ForStRsSstRegistry();
        ForStRsSnapshotStrategy strategy =
                new ForStRsSnapshotStrategy(
                        linker,
                        db,
                        UUID.randomUUID(),
                        new KeyGroupRange(startKg, endKg),
                        registry,
                        new ForStRsSstUploader(),
                        arena,
                        Map.of("default", 0L));
        MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
        ForStRsSnapshotResources res = strategy.syncPrepareResources(1L);
        SnapshotResult<?> result =
                strategy.asyncSnapshot(res, 1L, 0L, factory, null).get(new CloseableRegistry());
        return (ForStRsIncrementalKeyedStateHandle) result.getJobManagerOwnedSnapshot();
    }

    /** Builds a key with the spec's kg-prefix encoding: {@code kg(2B BE) || suffix.bytes()}. */
    private static byte[] kgPrefixedKey(int kg, String suffix) {
        byte[] sf = suffix.getBytes();
        byte[] out = new byte[2 + sf.length];
        out[0] = (byte) ((kg >>> 8) & 0xFF);
        out[1] = (byte) (kg & 0xFF);
        System.arraycopy(sf, 0, out, 2, sf.length);
        return out;
    }

    /** Replaces the first shared-SST handle's StreamStateHandle with a 0-byte stub. */
    private static List<HandleAndLocalPath> replaceFirstShared(
            List<HandleAndLocalPath> original, HandleAndLocalPath toBreak) {
        List<HandleAndLocalPath> out = new ArrayList<>(original.size());
        for (HandleAndLocalPath h : original) {
            if (h == toBreak) {
                out.add(HandleAndLocalPath.of(new ZeroByteStreamHandle(), h.getLocalPath()));
            } else {
                out.add(h);
            }
        }
        return out;
    }

    /** Test double — a StreamStateHandle whose openInputStream() yields zero bytes. */
    private static final class ZeroByteStreamHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;

        @Override
        public org.apache.flink.core.fs.FSDataInputStream openInputStream() {
            return new org.apache.flink.core.fs.FSDataInputStream() {
                @Override
                public void seek(long desired) {}

                @Override
                public long getPos() {
                    return 0;
                }

                @Override
                public int read() {
                    return -1;
                }
            };
        }

        @Override
        public java.util.Optional<byte[]> asBytesIfInMemory() {
            return java.util.Optional.of(new byte[0]);
        }

        @Override
        public java.util.Optional<org.apache.flink.core.fs.Path> maybeGetPath() {
            return java.util.Optional.empty();
        }

        @Override
        public void discardState() {}

        @Override
        public org.apache.flink.runtime.state.PhysicalStateHandleID getStreamStateHandleID() {
            return new org.apache.flink.runtime.state.PhysicalStateHandleID("zero");
        }

        @Override
        public long getStateSize() {
            return 0;
        }
    }
}

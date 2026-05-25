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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip test for {@link ForStRsRestoreOperation} no-rescaling fast path (B-Prod-P4 Task 4.4).
 *
 * <p>Snapshots a small DB (parallelism = 1, kgRange = [0,0]) and restores it under the same
 * key-group range, then verifies every seeded key is reachable on the restored engine.
 */
class ForStRsRestoreOperationTest {

    @Test
    void snapshotThenRestoreRoundTrip(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            ForStRsIncrementalKeyedStateHandle handle =
                    takeSnapshot(linker, arena, tmp.resolve("src"), 8);

            // Restore into a fresh target directory using the same kgRange (no rescaling).
            ForStRsSstRegistry restoredRegistry = new ForStRsSstRegistry();
            ForStRsRestoreOperation op =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("restored"),
                            new KeyGroupRange(0, 0),
                            restoredRegistry);
            ForStRsRestoreOperation.RestoreResult res = op.restore(List.of(handle));

            assertNotNull(res.getDb());
            assertNotNull(res.getDefaultCf());
            assertEquals(handle.getCheckpointId(), res.getRestoredCheckpointId());

            // Every seeded key reads back identical bytes.
            for (int i = 0; i < 8; i++) {
                byte[] k = ("k-" + i).getBytes();
                byte[] expected = ("v-" + i).getBytes();
                byte[] got = linker.get(res.getDb(), res.getDefaultCf(), k);
                assertArrayEquals(
                        expected,
                        got,
                        "restored DB must round-trip k-"
                                + i
                                + " (was="
                                + java.util.Arrays.toString(got)
                                + ")");
            }

            // SST registry should contain at least one entry now — enables incremental ckpts.
            assertTrue(
                    restoredRegistry.size() >= 1,
                    "restored backend must repopulate SST registry from shared handles");

            res.getDefaultCf().close();
            res.getDb().close();
        }
    }

    @Test
    void strictRestoreMissingMetadataThrows(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            ForStRsIncrementalKeyedStateHandle handle =
                    takeSnapshot(linker, arena, tmp.resolve("src"), 4);

            // Build a corrupted handle by replacing the metadata handle with a 0-byte stub.
            ForStRsIncrementalKeyedStateHandle bad =
                    new ForStRsIncrementalKeyedStateHandle(
                            handle.getBackendIdentifier(),
                            handle.getKeyGroupRange(),
                            handle.getCheckpointId(),
                            handle.getBaseCheckpointId(),
                            handle.getSharedState(),
                            handle.getPrivateState(),
                            new EmptyStreamHandle(),
                            handle.getCfMap());

            ForStRsRestoreOperation op =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("restored"),
                            new KeyGroupRange(0, 0),
                            new ForStRsSstRegistry());
            ForStRsCheckpointRestoreException thrown =
                    org.junit.jupiter.api.Assertions.assertThrows(
                            ForStRsCheckpointRestoreException.class,
                            () -> op.restore(List.of(bad)));
            assertNotNull(thrown.getMissingPath());
            assertEquals(handle.getCheckpointId(), thrown.getCheckpointId());
        }
    }

    @Test
    void restoreWithNoHandlesOpensEmptyDb(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            ForStRsRestoreOperation op =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("empty"),
                            new KeyGroupRange(0, 3),
                            new ForStRsSstRegistry());
            ForStRsRestoreOperation.RestoreResult res =
                    op.restore(java.util.Collections.<KeyedStateHandle>emptyList());
            assertNotNull(res.getDb());
            assertNotNull(res.getDefaultCf());
            assertEquals(0L, res.getRestoredCheckpointId());
            // Empty DB — nothing to read.
            byte[] missing = linker.get(res.getDb(), res.getDefaultCf(), "absent".getBytes());
            assertEquals(0, missing == null ? 0 : missing.length);
            res.getDefaultCf().close();
            res.getDb().close();
        }
    }

    @Test
    void restoreRejectsEscapingSstLocalPath(@TempDir Path tmp) {
        ForStRsIncrementalKeyedStateHandle owner = emptyOwner(44L);
        ForStRsCheckpointRestoreException thrown =
                assertThrows(
                        ForStRsCheckpointRestoreException.class,
                        () ->
                                ForStRsRestoreOperation.resolveSafeRestoreLocalPath(
                                        tmp.resolve("dl"), "../escape.sst", owner));
        assertEquals(44L, thrown.getCheckpointId());
    }

    @Test
    void restoreAcceptsSingleSstFileName(@TempDir Path tmp) throws Exception {
        ForStRsIncrementalKeyedStateHandle owner = emptyOwner(45L);
        Path downloadDir = tmp.resolve("dl");
        Path resolved =
                ForStRsRestoreOperation.resolveSafeRestoreLocalPath(
                        downloadDir, "000123.sst", owner);
        assertEquals(downloadDir.toAbsolutePath().normalize().resolve("000123.sst"), resolved);
    }

    @Test
    void rescalingRestoreRejectsTimerPrefixCollisionKeyGroup(@TempDir Path tmp) {
        ForStRsIncrementalKeyedStateHandle owner =
                new ForStRsIncrementalKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0x712F, 0x712F),
                        46L,
                        /* baseCheckpointId= */ 0L,
                        List.of(),
                        List.of(),
                        new EmptyStreamHandle(),
                        Map.of("default", 0L));
        ForStRsRestoreOperation op =
                new ForStRsRestoreOperation(
                        null,
                        null,
                        tmp.resolve("restore"),
                        new KeyGroupRange(0, 0),
                        new ForStRsSstRegistry());

        ForStRsCheckpointRestoreException thrown =
                assertThrows(
                        ForStRsCheckpointRestoreException.class,
                        () -> op.restore(List.of(owner)));
        assertEquals(46L, thrown.getCheckpointId());
        assertTrue(thrown.getMessage().contains("0x71/0x2f"));
    }

    @Test
    void rescalingRestoreRejectsAsyncV2PrefixCollisionKeyGroup(@TempDir Path tmp) {
        ForStRsIncrementalKeyedStateHandle owner =
                new ForStRsIncrementalKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0x6B2F, 0x6B2F),
                        47L,
                        /* baseCheckpointId= */ 0L,
                        List.of(),
                        List.of(),
                        new EmptyStreamHandle(),
                        Map.of("default", 0L));
        ForStRsRestoreOperation op =
                new ForStRsRestoreOperation(
                        null,
                        null,
                        tmp.resolve("restore"),
                        new KeyGroupRange(0, 0),
                        new ForStRsSstRegistry());

        ForStRsCheckpointRestoreException thrown =
                assertThrows(
                        ForStRsCheckpointRestoreException.class,
                        () -> op.restore(List.of(owner)));
        assertEquals(47L, thrown.getCheckpointId());
        assertTrue(thrown.getMessage().contains("0x6b/0x2f"));
    }

    @Test
    void timerRowKeyParserFailsFastOnMalformedQueueRows() {
        byte[] marker =
                org.apache.flink.state.forstrs.timer.ForStRsKeyGroupedInternalPriorityQueue
                        .QUEUE_NS_MARKER;
        byte[] malformed = "q/state-without-separator".getBytes(StandardCharsets.UTF_8);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ForStRsRestoreOperation.extractTimerRowKeyGroupOrThrow(
                                malformed, marker, -1));
    }

    @Test
    void timerRowKeyParserReadsEmbeddedKeyGroup() {
        byte[] marker =
                org.apache.flink.state.forstrs.timer.ForStRsKeyGroupedInternalPriorityQueue
                        .QUEUE_NS_MARKER;
        byte[] stateName = "timer-state".getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[marker.length + stateName.length + 1 + 2 + Long.BYTES];
        int pos = 0;
        System.arraycopy(marker, 0, key, pos, marker.length);
        pos += marker.length;
        System.arraycopy(stateName, 0, key, pos, stateName.length);
        pos += stateName.length;
        int sepIdx = pos;
        key[pos++] = (byte) '/';
        key[pos++] = 0;
        key[pos++] = 7;

        assertEquals(
                7,
                ForStRsRestoreOperation.extractTimerRowKeyGroupOrThrow(key, marker, sepIdx));
    }

    /** Helper: opens a DB at {@code srcDir}, writes N keys, takes an incremental snapshot. */
    static ForStRsIncrementalKeyedStateHandle takeSnapshot(
            ForStRsLinker linker, Arena arena, Path srcDir, int n) throws Exception {
        java.nio.file.Files.createDirectories(srcDir);
        FrsDb db = linker.dbOpen(arena, srcDir.toString());
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);
        try {
            for (int i = 0; i < n; i++) {
                linker.put(db, cf, ("k-" + i).getBytes(), ("v-" + i).getBytes());
            }
            ForStRsSstRegistry registry = new ForStRsSstRegistry();
            ForStRsSstUploader uploader = new ForStRsSstUploader();
            ForStRsSnapshotStrategy strategy =
                    new ForStRsSnapshotStrategy(
                            linker,
                            db,
                            UUID.randomUUID(),
                            new KeyGroupRange(0, 0),
                            registry,
                            uploader,
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

    private static ForStRsIncrementalKeyedStateHandle emptyOwner(long checkpointId) {
        return new ForStRsIncrementalKeyedStateHandle(
                UUID.randomUUID(),
                new KeyGroupRange(0, 0),
                checkpointId,
                /* baseCheckpointId= */ 0L,
                List.of(),
                List.of(),
                new EmptyStreamHandle(),
                Map.of("default", 0L));
    }

    /** Test double — a StreamStateHandle whose openInputStream() yields zero bytes. */
    private static final class EmptyStreamHandle
            implements org.apache.flink.runtime.state.StreamStateHandle {
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
            return new org.apache.flink.runtime.state.PhysicalStateHandleID("empty");
        }

        @Override
        public long getStateSize() {
            return 0;
        }
    }
}

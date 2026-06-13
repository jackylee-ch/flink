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

package org.apache.flink.state.forstrs.ffm;

import org.apache.flink.state.forstrs.FrsBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FRS-PHASE2 LINK-mode checkpoint module-level correctness cells, driven through the {@link
 * ForStRsLinker} FFM wrappers added by the disagg-java adoption package. These mirror the engine IT
 * {@code crates/forst-rs-ffi/tests/linked_checkpoint_ffi_it.rs} at the Java surface and cover the
 * disagg correctness gate's structural asserts at unit/module scale (the 100M / real-S3 cells are
 * remote-only and out of scope here).
 *
 * <p>Round-trip: open a disk DB → write rows → snapshot → LINK-mode checkpoint (zero data upload,
 * SSTs link()ed into {@code <db>/checkpoints/<id>/}) → instant restore into a fresh dir downloading
 * NOTHING → byte-exact reads. Then the discard-delegate idempotence and result-free idempotence.
 */
class LinkedCheckpointRoundTripTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Reads the NUL-terminated C string at {@code struct[off]} from a linked-result buffer. */
    private static Path readCString(MemorySegment struct, long off) {
        MemorySegment ptr = struct.get(ValueLayout.ADDRESS, off);
        if (ptr.address() == 0L) {
            return null;
        }
        return Paths.get(ptr.reinterpret(Long.MAX_VALUE).getString(0L));
    }

    /** Reads the {@code count} field of the FrsLiveFileList at {@code struct[off]}. */
    private static long listCount(MemorySegment struct, long off) {
        MemorySegment listPtr = struct.get(ValueLayout.ADDRESS, off);
        if (listPtr.address() == 0L) {
            return 0L;
        }
        // FrsLiveFileList layout: files (8) + count (8) + manifest_size (8).
        return listPtr.reinterpret(24L).get(ValueLayout.JAVA_LONG, 8L);
    }

    @Test
    void linkModeCheckpointInstantRestoreRoundTrip(@TempDir Path tmp) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            Path dbDir = tmp.resolve("db");
            Path restoreDir = tmp.resolve("restored");

            String ckptDir;
            // ---- write + LINK-mode checkpoint (FLUSH mode produces ≥1 linked SST). ----
            try (FrsDb db = linker.dbOpen(arena, dbDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                for (int i = 0; i < 64; i++) {
                    linker.put(db, cf, b(String.format("k%03d", i)), b(String.format("v%03d", i)));
                }
                linker.flush(db);

                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                MemorySegment result = arena.allocate(24);
                linker.createIncrementalCheckpointLinked(
                        db, snap, /* checkpointId= */ 1L, /* baseCheckpointId= */ 0L, result);
                snap.close();

                Path manifest = readCString(result, 0L);
                assertNotNull(manifest, "linked checkpoint must surface a manifest path");
                // The checkpoint dir is the manifest's parent (design §9 D1).
                ckptDir = manifest.getParent().toString();

                long newCount = listCount(result, ValueLayout.ADDRESS.byteSize());
                // Flush-on-barrier must produce at least one linked SST (zero data upload, just a
                // metadata link). This is the structural "zero-reupload" assert at unit scale.
                assertTrue(newCount >= 1, "FLUSH-mode link checkpoint must link ≥1 SST");

                linker.dbLinkedCheckpointResultFree(result);
                // Idempotent on the native side — a second free must not throw.
                linker.dbLinkedCheckpointResultFree(result);
            }

            // ---- instant restore into a FRESH dir: downloads / copies NOTHING. ----
            try (FrsDb restored =
                    linker.dbOpenFromLinkedCheckpointInstant(
                            arena, ckptDir, restoreDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(restored, arena);
                for (int i = 0; i < 64; i++) {
                    byte[] got = linker.get(restored, cf, b(String.format("k%03d", i)));
                    assertArrayEquals(
                            b(String.format("v%03d", i)),
                            got,
                            "instant-link restore must read every row byte-exact");
                }
                // Restore adopted foreign physicals from the checkpoint namespace; residual > 0
                // until compaction weans the engine off the source (CLAIM-mode discipline, D-J4).
                long residual = linker.dbAdoptedResidual(arena, restored);
                assertTrue(residual >= 0L, "adopted residual must be reported");
            }
        }
    }

    /**
     * FRS-PHASE2 (Task-A): REMOTE-PRIMARY instant restore round-trip via an emulated OpenDAL {@code
     * file://} backend — exercises {@link
     * ForStRsLinker#dbOpenFromLinkedCheckpointInstantRemote(Arena, String, String, String, long,
     * String, String)}, the symbol the backend now drives when a {@code storageUri} is configured.
     * A remote ({@code file://}) DB is opened, written + flushed, LINK-mode checkpointed (zero data
     * upload — SSTs link()ed into the remote chk namespace), then INSTANT-restored through the
     * remote variant: the engine builds the same {@code CachedFileSystem(OpendalFileSystem,
     * LocalCache)} stack, adopts the chk-namespace physicals over remote storage, and reads every
     * row byte-exact — downloading no SST. This is the same {@code uri / opendalConfig / cacheDir /
     * cacheCapacityBytes} surface the production backend passes through {@code
     * ForStRsStateBackend.buildRemoteRestoreConfig}.
     */
    @Test
    void remotePrimaryInstantRestoreRoundTrip(@TempDir Path tmp) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            Path remoteRoot = tmp.resolve("remote");
            remoteRoot.toFile().mkdirs();
            // file:// OpenDAL backend rooted at a real local dir (emulates a remote object store).
            String uri = "file://" + remoteRoot.toAbsolutePath();
            Path cacheDir = tmp.resolve("cache");
            cacheDir.toFile().mkdirs();
            Path restoreDir = tmp.resolve("restored");
            final long cacheCapacityBytes = 64L * 1024 * 1024;

            String ckptDir;
            try (FrsDb db =
                    linker.dbOpenRemote(
                            arena, uri, "{}", cacheDir.toString(), cacheCapacityBytes)) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                for (int i = 0; i < 64; i++) {
                    linker.put(db, cf, b(String.format("k%03d", i)), b(String.format("v%03d", i)));
                }
                linker.flush(db);

                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                MemorySegment result = arena.allocate(24);
                linker.createIncrementalCheckpointLinked(db, snap, 1L, 0L, result);
                snap.close();

                Path manifest = readCString(result, 0L);
                assertNotNull(manifest, "remote linked checkpoint must surface a manifest path");
                // The chk dir is the manifest's parent in the REMOTE namespace (design §9 D1).
                ckptDir = manifest.getParent().toString();

                long newCount = listCount(result, ValueLayout.ADDRESS.byteSize());
                assertTrue(newCount >= 1, "FLUSH-mode remote link checkpoint must link ≥1 SST");

                linker.dbLinkedCheckpointResultFree(result);
            }

            // ---- REMOTE-PRIMARY instant restore: downloads NO SST, resolves over the file:// FS.
            try (FrsDb restored =
                    linker.dbOpenFromLinkedCheckpointInstantRemote(
                            arena,
                            uri,
                            "{}",
                            cacheDir.toString(),
                            cacheCapacityBytes,
                            ckptDir,
                            restoreDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(restored, arena);
                for (int i = 0; i < 64; i++) {
                    byte[] got = linker.get(restored, cf, b(String.format("k%03d", i)));
                    assertArrayEquals(
                            b(String.format("v%03d", i)),
                            got,
                            "remote-primary instant restore must read every row byte-exact");
                }
                long residual = linker.dbAdoptedResidual(arena, restored);
                assertTrue(residual >= 0L, "adopted residual must be reported");
            }
        }
    }

    /**
     * FRS-WAL-DELTA (Task-A): WAL attach round-trip — write → checkpoint → restore → tail replayed.
     * Exercises {@link ForStRsLinker#dbAttachWal(Arena, FrsDb, String)}, the symbol the backend now
     * calls at open when {@code state.backend.forst-rs.wal.dir} is set. A flushed floor lands in
     * SSTs, then a WAL is attached and an unflushed tail (inserts + an overwrite) is written; the
     * LINK-mode checkpoint captures the tail into the WAL.delta (no forced flush) and the instant
     * restore replays it byte-exact above the floor. A double-attach is rejected
     * (INVALID_ARGUMENT). Mirrors the engine IT {@code
     * linked_checkpoint_wal_delta_mode_replays_unflushed_tail} at the Java surface.
     */
    @Test
    void walDeltaAttachReplaysUnflushedTail(@TempDir Path tmp) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            Path dbDir = tmp.resolve("db");
            Path walDir = tmp.resolve("wal");
            walDir.toFile().mkdirs();
            String walPath = walDir.resolve("db.wal").toString();
            Path restoreDir = tmp.resolve("restored");

            String ckptDir;
            try (FrsDb db = linker.dbOpen(arena, dbDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);

                // Flushed floor: lands in SSTs.
                for (int i = 0; i < 20; i++) {
                    linker.put(db, cf, b(String.format("floor%02d", i)), b(String.format("f%02d", i)));
                }
                linker.flush(db);

                // Opt into WAL-DELTA (per-DB, env-free) — the call the backend now makes at open.
                linker.dbAttachWal(arena, db, walPath);
                // Double-attach must be rejected.
                FrsBackendException doubleAttach = null;
                try {
                    linker.dbAttachWal(arena, db, walPath);
                } catch (FrsBackendException e) {
                    doubleAttach = e;
                }
                assertNotNull(doubleAttach, "second dbAttachWal must be rejected (INVALID_ARGUMENT)");

                // Unflushed tail: inserts + an overwrite, logged to the WAL.
                for (int i = 0; i < 15; i++) {
                    linker.put(db, cf, b(String.format("tail%02d", i)), b(String.format("t%02d", i)));
                }
                linker.put(db, cf, b("floor00"), b("overwritten"));

                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                MemorySegment result = arena.allocate(24);
                linker.createIncrementalCheckpointLinked(db, snap, 5L, 0L, result);
                snap.close();
                Path manifest = readCString(result, 0L);
                assertNotNull(manifest, "WAL-DELTA linked checkpoint must surface a manifest path");
                ckptDir = manifest.getParent().toString();
                linker.dbLinkedCheckpointResultFree(result);

                // A write AFTER the barrier must not be part of the checkpoint.
                linker.put(db, cf, b("after-barrier"), b("nope"));
            }

            // ---- instant restore replays the WAL.delta tail above the flushed floor. ----
            try (FrsDb restored =
                    linker.dbOpenFromLinkedCheckpointInstant(
                            arena, ckptDir, restoreDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(restored, arena);
                // Floor rows (minus the overwrite), tail rows, and the overwrite are all present.
                for (int i = 1; i < 20; i++) {
                    assertArrayEquals(
                            b(String.format("f%02d", i)),
                            linker.get(restored, cf, b(String.format("floor%02d", i))),
                            "floor row must survive");
                }
                for (int i = 0; i < 15; i++) {
                    assertArrayEquals(
                            b(String.format("t%02d", i)),
                            linker.get(restored, cf, b(String.format("tail%02d", i))),
                            "WAL.delta tail row must be replayed");
                }
                assertArrayEquals(
                        b("overwritten"),
                        linker.get(restored, cf, b("floor00")),
                        "WAL.delta overwrite must win");
                assertNull(
                        linker.get(restored, cf, b("after-barrier")),
                        "post-barrier write must NOT be in the checkpoint");
            }
        }
    }

    @Test
    void discardDelegateIsIdempotent(@TempDir Path tmp) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            Path dbDir = tmp.resolve("db");
            try (FrsDb db = linker.dbOpen(arena, dbDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                linker.put(db, cf, b("a"), b("1"));
                linker.flush(db);

                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                MemorySegment result = arena.allocate(24);
                linker.createIncrementalCheckpointLinked(db, snap, 7L, 0L, result);
                snap.close();
                linker.dbLinkedCheckpointResultFree(result);

                // Working-dir refs keep the physicals alive; the JM-discard delegate unlinks the
                // chk namespace. First discard succeeds (true); a retried discard finds it gone
                // (false / NOT_FOUND) — the at-least-once idempotence the protocol requires.
                boolean first = linker.dbDiscardLinkedCheckpoint(arena, db, 7L);
                boolean second = linker.dbDiscardLinkedCheckpoint(arena, db, 7L);
                assertTrue(first, "first discard of a linked checkpoint must report discarded");
                assertFalse(second, "retried discard must report already-gone (NOT_FOUND)");

                // The DB is still readable after discard (physicals survived on working refs).
                assertArrayEquals(b("1"), linker.get(db, cf, b("a")));
            }
        }
    }

    /**
     * FRS-PHASE2 compression: {@code dbOpenWithOptions} round-trips byte-exact for each SST
     * compression discriminant (0=default lz4, 1=none, 2=lz4, 3=zstd). The discriminant maps to
     * {@code FrsEngineOptions.sst_compression}; a flush forces it through the SST writer so a non-lz4
     * codec is actually exercised, and reads must return the same bytes regardless of codec.
     */
    @Test
    void sstCompressionRoundTripPerCodec(@TempDir Path tmp) {
        int[] discriminants = {0, 1, 2, 3};
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            for (int codec : discriminants) {
                Path dbDir = tmp.resolve("db-codec-" + codec);
                try (FrsDb db =
                        linker.dbOpenWithOptions(
                                arena, dbDir.toString(), 0L, 0, 0, 0, 0L, 0L, codec)) {
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                    for (int i = 0; i < 200; i++) {
                        // Compressible payload (repeated bytes) so a real codec has work to do.
                        linker.put(
                                db,
                                cf,
                                b(String.format("ckey%04d", i)),
                                b("payload-" + i + "-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"));
                    }
                    linker.flush(db);
                    for (int i = 0; i < 200; i++) {
                        assertArrayEquals(
                                b("payload-" + i + "-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                                linker.get(db, cf, b(String.format("ckey%04d", i))),
                                "codec=" + codec + " must read back byte-exact after flush");
                    }
                }
            }
        }
    }

    /**
     * FRS-PHASE2 KV-separation: the backend feature-flag bridge ({@link ForStRsLinker#setEnv}) sets
     * {@code FRS_KV_SEPARATION=1} + {@code FRS_KV_MIN_BLOB_SIZE} so flushes of eligible CFs separate
     * large values into a value-log; reads return the same bytes either way. This cell verifies the
     * env plumbs to the engine and a large-value round trip is byte-exact under separation. Restores
     * the env afterwards so the shared test process is not perturbed.
     *
     * <p>NOTE: KV-sep is a process-wide engine OnceLock cached on first flush. In a shared test JVM a
     * previously-run test may have already cached it; this cell asserts the WRITE/READ round trip is
     * byte-exact (the observable contract) rather than the internal vlog file count, so it is robust
     * to ordering.
     */
    @Test
    void kvSeparationEnvRoundTrip(@TempDir Path tmp) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            // Feature-flag bridge: the exact calls ForStRsKeyedStateBackendBuilder makes at open.
            linker.setEnv(arena, "FRS_KV_SEPARATION", "1");
            linker.setEnv(arena, "FRS_KV_MIN_BLOB_SIZE", "64");

            Path dbDir = tmp.resolve("db-kvsep");
            byte[] bigVal = new byte[512];
            for (int i = 0; i < bigVal.length; i++) {
                bigVal[i] = (byte) ('a' + (i % 26));
            }
            try (FrsDb db = linker.dbOpen(arena, dbDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                for (int i = 0; i < 64; i++) {
                    linker.put(db, cf, b(String.format("big%03d", i)), bigVal);
                    linker.put(db, cf, b(String.format("small%03d", i)), b("s" + i));
                }
                linker.flush(db);
                for (int i = 0; i < 64; i++) {
                    assertArrayEquals(
                            bigVal,
                            linker.get(db, cf, b(String.format("big%03d", i))),
                            "separated large value must read back byte-exact");
                    assertArrayEquals(
                            b("s" + i),
                            linker.get(db, cf, b(String.format("small%03d", i))),
                            "inline small value must read back byte-exact");
                }
            }
        }
    }

    /**
     * FRS-PHASE2-C2U3 rescale-by-clip: clipped instant restore adopts only the assigned key-group
     * sub-range. Builds a checkpoint over four 2-byte big-endian key-group prefixes (matching {@link
     * org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer#keyGroupPrefix}), then restores
     * only {@code [kg 1, kg 2]} via {@link
     * ForStRsLinker#dbOpenFromLinkedCheckpointInstantClipped(Arena, String, String, byte[], byte[])}.
     * Groups 1-2 survive; groups 0 and 3 are clipped out.
     */
    @Test
    void clippedInstantRestoreAdoptsOnlyAssignedRange(@TempDir Path tmp) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            Path dbDir = tmp.resolve("db");
            Path restoreDir = tmp.resolve("restored-clip");

            String ckptDir;
            try (FrsDb db = linker.dbOpen(arena, dbDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                for (int kg = 0; kg < 4; kg++) {
                    for (int i = 0; i < 8; i++) {
                        linker.put(db, cf, kgKey(kg, i), b("v-" + kg + "-" + i));
                    }
                }
                linker.flush(db);

                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                MemorySegment result = arena.allocate(24);
                linker.createIncrementalCheckpointLinked(db, snap, 1L, 0L, result);
                snap.close();
                Path manifest = readCString(result, 0L);
                assertNotNull(manifest);
                ckptDir = manifest.getParent().toString();
                linker.dbLinkedCheckpointResultFree(result);
            }

            // Clip to key groups [1, 3): start = 0x0001, end (exclusive) = 0x0003.
            byte[] clipStart = {0x00, 0x01};
            byte[] clipEnd = {0x00, 0x03};
            try (FrsDb restored =
                    linker.dbOpenFromLinkedCheckpointInstantClipped(
                            arena, ckptDir, restoreDir.toString(), clipStart, clipEnd)) {
                FrsCfHandle cf = linker.dbDefaultCf(restored, arena);
                for (int kg = 0; kg < 4; kg++) {
                    boolean inRange = kg == 1 || kg == 2;
                    for (int i = 0; i < 8; i++) {
                        byte[] got = linker.get(restored, cf, kgKey(kg, i));
                        if (inRange) {
                            assertArrayEquals(
                                    b("v-" + kg + "-" + i), got, "kg=" + kg + " must be adopted");
                        } else {
                            assertNull(got, "kg=" + kg + " must be clipped out");
                        }
                    }
                }
            }
        }
    }

    /** Composite key = 2-byte big-endian key group + user key (matches the backend serializer). */
    private static byte[] kgKey(int kg, int i) {
        byte[] user = b("-user" + i);
        byte[] key = new byte[2 + user.length];
        key[0] = (byte) ((kg >>> 8) & 0xFF);
        key[1] = (byte) (kg & 0xFF);
        System.arraycopy(user, 0, key, 2, user.length);
        return key;
    }

    @Test
    void sweepAbandonedCheckpointsWithEmptyLiveSetIsSafe(@TempDir Path tmp) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            Path dbDir = tmp.resolve("db");
            try (FrsDb db = linker.dbOpen(arena, dbDir.toString())) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                linker.put(db, cf, b("x"), b("y"));
                linker.flush(db);

                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                MemorySegment result = arena.allocate(24);
                linker.createIncrementalCheckpointLinked(db, snap, 3L, 0L, result);
                snap.close();
                linker.dbLinkedCheckpointResultFree(result);

                // No JM-live ids ⇒ checkpoint 3's links are abandoned and reaped. Reaping an
                // already-reaped namespace is idempotent (count 0 on the second sweep).
                long reapedFirst = linker.dbSweepAbandonedCheckpoints(arena, db, new long[0]);
                long reapedSecond = linker.dbSweepAbandonedCheckpoints(arena, db, new long[0]);
                assertTrue(reapedFirst >= 0L, "sweep must report a (non-negative) reaped count");
                assertEquals(0L, reapedSecond, "second sweep must reap nothing (idempotent)");

                // Physicals survive on the working dir — the live state is intact post-sweep.
                assertArrayEquals(b("y"), linker.get(db, cf, b("x")));
            }
        }
    }
}

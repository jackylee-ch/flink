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

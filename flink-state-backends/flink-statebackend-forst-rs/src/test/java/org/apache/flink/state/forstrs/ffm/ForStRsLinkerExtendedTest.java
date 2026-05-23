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
import org.apache.flink.state.forstrs.ffm.ForStRsLinker.IteratorEntry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the extended FFM surface: lookup_kv, iterator_open / next / close, prefix_lookup_open,
 * delete, flush, checkpoint, sequence_number, and CF create / open. These supplement the basic
 * round-trip in {@link ForStRsRoundTripTest}.
 */
class ForStRsLinkerExtendedTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void lookupKvRoundTripAndDelete() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = utf8("alpha");
                byte[] value = utf8("one");

                assertNull(linker.lookupKv(db, cf, key));

                linker.put(db, cf, key, value);
                assertArrayEquals(value, linker.lookupKv(db, cf, key));

                linker.delete(db, cf, key);
                assertNull(linker.lookupKv(db, cf, key));
            }
        }
    }

    /**
     * Phase A.1 (audit-design §3 V4): batched merge-append — 3 distinct keys with 1 operand each
     * in a single FFI call.
     */
    @Test
    void frsVecMergeAppendBatch_threeDistinctKeys() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                // 3 keys: "k1", "k2", "k3"; 3 operands: "A", "B", "C"
                byte[] keys = "k1k2k3".getBytes(StandardCharsets.UTF_8);
                int[] keysOff = {0, 2, 4, 6};
                byte[] ops = "ABC".getBytes(StandardCharsets.UTF_8);
                int[] opsOff = {0, 1, 2, 3};

                java.lang.foreign.MemorySegment keysOffSeg =
                        arena.allocate(java.lang.foreign.ValueLayout.JAVA_INT, keysOff.length);
                for (int i = 0; i < keysOff.length; i++) {
                    keysOffSeg.setAtIndex(java.lang.foreign.ValueLayout.JAVA_INT, i, keysOff[i]);
                }
                java.lang.foreign.MemorySegment keysDataSeg = arena.allocate(keys.length);
                java.lang.foreign.MemorySegment.copy(
                        keys, 0, keysDataSeg, java.lang.foreign.ValueLayout.JAVA_BYTE, 0,
                        keys.length);
                java.lang.foreign.MemorySegment opsOffSeg =
                        arena.allocate(java.lang.foreign.ValueLayout.JAVA_INT, opsOff.length);
                for (int i = 0; i < opsOff.length; i++) {
                    opsOffSeg.setAtIndex(java.lang.foreign.ValueLayout.JAVA_INT, i, opsOff[i]);
                }
                java.lang.foreign.MemorySegment opsDataSeg = arena.allocate(ops.length);
                java.lang.foreign.MemorySegment.copy(
                        ops, 0, opsDataSeg, java.lang.foreign.ValueLayout.JAVA_BYTE, 0,
                        ops.length);

                int rc =
                        linker.frsVecMergeAppendBatch(
                                db.handle(),
                                cf.handle(),
                                keysOffSeg,
                                keysDataSeg,
                                opsOffSeg,
                                opsDataSeg,
                                3);
                assertEquals(0, rc, "frsVecMergeAppendBatch returned rc=" + rc);

                assertArrayEquals(utf8("A"), linker.lookupKv(db, cf, utf8("k1")));
                assertArrayEquals(utf8("B"), linker.lookupKv(db, cf, utf8("k2")));
                assertArrayEquals(utf8("C"), linker.lookupKv(db, cf, utf8("k3")));
            }
        }
    }

    /**
     * Phase A.1: batched merge-append — same key repeated; operands concatenate into a single
     * stored value (engine's read-combine-write per distinct key).
     */
    @Test
    void frsVecMergeAppendBatch_sameKeyConcatenates() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] keys = "k1k1k1".getBytes(StandardCharsets.UTF_8);
                int[] keysOff = {0, 2, 4, 6};
                byte[] ops = "XYZ".getBytes(StandardCharsets.UTF_8);
                int[] opsOff = {0, 1, 2, 3};

                java.lang.foreign.MemorySegment keysOffSeg =
                        arena.allocate(java.lang.foreign.ValueLayout.JAVA_INT, keysOff.length);
                for (int i = 0; i < keysOff.length; i++) {
                    keysOffSeg.setAtIndex(java.lang.foreign.ValueLayout.JAVA_INT, i, keysOff[i]);
                }
                java.lang.foreign.MemorySegment keysDataSeg = arena.allocate(keys.length);
                java.lang.foreign.MemorySegment.copy(
                        keys, 0, keysDataSeg, java.lang.foreign.ValueLayout.JAVA_BYTE, 0,
                        keys.length);
                java.lang.foreign.MemorySegment opsOffSeg =
                        arena.allocate(java.lang.foreign.ValueLayout.JAVA_INT, opsOff.length);
                for (int i = 0; i < opsOff.length; i++) {
                    opsOffSeg.setAtIndex(java.lang.foreign.ValueLayout.JAVA_INT, i, opsOff[i]);
                }
                java.lang.foreign.MemorySegment opsDataSeg = arena.allocate(ops.length);
                java.lang.foreign.MemorySegment.copy(
                        ops, 0, opsDataSeg, java.lang.foreign.ValueLayout.JAVA_BYTE, 0,
                        ops.length);

                int rc =
                        linker.frsVecMergeAppendBatch(
                                db.handle(),
                                cf.handle(),
                                keysOffSeg,
                                keysDataSeg,
                                opsOffSeg,
                                opsDataSeg,
                                3);
                assertEquals(0, rc);

                // All 3 ops concatenated into one stored value.
                assertArrayEquals(utf8("XYZ"), linker.lookupKv(db, cf, utf8("k1")));
            }
        }
    }

    /** Phase A.1: n=0 is a no-op (no crash, returns OK). */
    @Test
    void frsVecMergeAppendBatch_zeroRowsIsNoop() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                // Valid empty buffers (callee must not deref when n=0).
                java.lang.foreign.MemorySegment keysOffSeg =
                        arena.allocate(java.lang.foreign.ValueLayout.JAVA_INT, 1);
                java.lang.foreign.MemorySegment keysDataSeg = arena.allocate(1);
                java.lang.foreign.MemorySegment opsOffSeg =
                        arena.allocate(java.lang.foreign.ValueLayout.JAVA_INT, 1);
                java.lang.foreign.MemorySegment opsDataSeg = arena.allocate(1);

                int rc =
                        linker.frsVecMergeAppendBatch(
                                db.handle(),
                                cf.handle(),
                                keysOffSeg,
                                keysDataSeg,
                                opsOffSeg,
                                opsDataSeg,
                                0);
                assertEquals(0, rc);
            }
        }
    }

    @Test
    void iteratorReturnsAllRowsInSortedOrder() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                // Write keys out of order; engine should yield them sorted.
                linker.put(db, cf, utf8("b"), utf8("2"));
                linker.put(db, cf, utf8("a"), utf8("1"));
                linker.put(db, cf, utf8("c"), utf8("3"));

                try (FrsIterator iter = linker.iteratorOpen(db, cf, arena)) {
                    List<String> keys = new ArrayList<>();
                    List<String> values = new ArrayList<>();
                    IteratorEntry entry;
                    while ((entry = linker.iteratorNext(iter)) != null) {
                        keys.add(new String(entry.key(), StandardCharsets.UTF_8));
                        values.add(new String(entry.value(), StandardCharsets.UTF_8));
                    }
                    assertEquals(List.of("a", "b", "c"), keys);
                    assertEquals(List.of("1", "2", "3"), values);
                }
            }
        }
    }

    @Test
    void prefixLookupReturnsOnlyMatches() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                linker.put(db, cf, utf8("user:1"), utf8("alice"));
                linker.put(db, cf, utf8("user:2"), utf8("bob"));
                linker.put(db, cf, utf8("user:3"), utf8("carol"));
                linker.put(db, cf, utf8("admin:1"), utf8("dan"));

                try (FrsIterator iter = linker.prefixLookupOpen(db, cf, utf8("user:"), arena)) {
                    List<String> keys = new ArrayList<>();
                    IteratorEntry entry;
                    while ((entry = linker.iteratorNext(iter)) != null) {
                        keys.add(new String(entry.key(), StandardCharsets.UTF_8));
                    }
                    assertEquals(List.of("user:1", "user:2", "user:3"), keys);
                }
            }
        }
    }

    @Test
    void iteratorSeekRepositionsCursor() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                linker.put(db, cf, utf8("a"), utf8("1"));
                linker.put(db, cf, utf8("b"), utf8("2"));
                linker.put(db, cf, utf8("c"), utf8("3"));

                try (FrsIterator iter = linker.iteratorOpen(db, cf, arena)) {
                    linker.iteratorSeek(iter, utf8("b"));
                    IteratorEntry first = linker.iteratorNext(iter);
                    assertNotNull(first);
                    assertArrayEquals(utf8("b"), first.key());

                    IteratorEntry second = linker.iteratorNext(iter);
                    assertNotNull(second);
                    assertArrayEquals(utf8("c"), second.key());

                    assertNull(linker.iteratorNext(iter));
                }
            }
        }
    }

    @Test
    void createAndOpenNamedColumnFamily() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                try (FrsCfHandle created = linker.dbCreateCf(db, arena, "myCf")) {
                    linker.put(db, created, utf8("k"), utf8("v"));
                }
                try (FrsCfHandle reopened = linker.dbOpenCf(db, arena, "myCf")) {
                    assertArrayEquals(utf8("v"), linker.get(db, reopened, utf8("k")));
                }
            }
        }
    }

    @Test
    void openMissingColumnFamilyFails() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                assertThrows(FrsBackendException.class, () -> linker.dbOpenCf(db, arena, "nope"));
            }
        }
    }

    @Test
    void flushAndSequenceNumberIncreaseAfterWrites() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                long initial = linker.sequenceNumber(db);
                linker.put(db, cf, utf8("k"), utf8("v"));
                long afterPut = linker.sequenceNumber(db);
                assertTrue(
                        afterPut > initial,
                        "sequence number should advance after put: " + initial + " -> " + afterPut);
                assertDoesNotThrow(() -> linker.flush(db));
            }
        }
    }

    @Test
    void createCheckpointWritesToTargetDir(@TempDir Path tmp) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            // Use a real on-disk db so checkpoint hits the local FS.
            Path dbPath = tmp.resolve("db");
            try (FrsDb db = linker.dbOpen(arena, dbPath.toString());
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                linker.put(db, cf, utf8("k"), utf8("v"));
                linker.flush(db);
                Path checkpointDir = tmp.resolve("ckpt");
                linker.createCheckpoint(db, checkpointDir.toString());
                File ckpt = checkpointDir.toFile();
                assertTrue(ckpt.exists(), "checkpoint dir was not created: " + ckpt);
                assertTrue(ckpt.isDirectory(), "checkpoint path not a directory: " + ckpt);
            }
        }
    }

    /**
     * Round-trips put/get through the tuned-config in-memory engine. Mirrors Preset A from the JMH
     * bench: 256 MiB memtable budget × 8 buffers, default 4/4 background threads. Proves the new
     * {@link ForStRsLinker#dbOpenMemoryTuned(Arena, long, long, long, long)} method is wired
     * end-to-end through {@code frs_db_open_memory_tuned}.
     */
    @Test
    void dbOpenMemoryTunedPresetARoundTrip() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db =
                            linker.dbOpenMemoryTuned(
                                    arena,
                                    256L * 1024L * 1024L, // write_buffer_size = 256 MiB
                                    8L, // max_write_buffer_number
                                    4L, // max_background_compactions
                                    4L); // max_background_flushes
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = utf8("tuned-key");
                byte[] value = utf8("tuned-value");
                assertNull(linker.get(db, cf, key));
                linker.put(db, cf, key, value);
                assertArrayEquals(value, linker.get(db, cf, key));
            }
        }
    }

    /**
     * Passing {@code 0} for every knob falls back to the engine defaults; the resulting handle
     * still survives a basic put/get round-trip and a clean close. Covers the per-knob "skip setter
     * when zero" branch in {@code frs_db_open_memory_tuned}.
     */
    @Test
    void dbOpenMemoryTunedZeroKnobsUsesDefaults() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemoryTuned(arena, 0L, 0L, 0L, 0L);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = utf8("zero-key");
                byte[] value = utf8("zero-value");
                linker.put(db, cf, key, value);
                assertArrayEquals(value, linker.get(db, cf, key));
            }
        }
    }

    /**
     * An out-of-range memtable size (1 byte, far below the 4 KiB {@code MIN_WRITE_BUFFER_SIZE}
     * floor enforced by {@code EngineOptionsBuilder::try_build}) must surface as a clean {@link
     * FrsBackendException} carrying the {@code INVALID_ARGUMENT} status, not a JVM panic / crash.
     */
    @Test
    void dbOpenMemoryTunedRejectsUndersizedBuffer() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            assertThrows(
                    FrsBackendException.class,
                    () -> linker.dbOpenMemoryTuned(arena, 1L, 0L, 0L, 0L));
        }
    }

    /**
     * TTL compaction filter — engine-side enforcement. The native binding accepts (db, cf, ttl_ms,
     * state_type, timestamp_offset) and installs a {@code FlinkTtlCompactionFilter} on the CF. The
     * filter activates at the next flush + compaction; this test only verifies the handle plumbing
     * path (no-throw on valid inputs, IAE on invalid). Engine-level expiry semantics are covered by
     * Rust-side tests in {@code crates/forst-rs-engine/src/compaction_filter.rs}.
     */
    @Test
    void testSetCompactionFilterTtl() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                // Typical Flink keyed-state TTL request: 60s, Value state, no offset.
                assertDoesNotThrow(
                        () ->
                                linker.setCompactionFilterTtl(
                                        db, cf, 60_000L, ForStRsLinker.STATE_TYPE_VALUE, 0L));

                // Re-applying a different filter on the same CF replaces the prior one
                // (engine-side semantics).
                assertDoesNotThrow(
                        () ->
                                linker.setCompactionFilterTtl(
                                        db, cf, 300_000L, ForStRsLinker.STATE_TYPE_LIST, 0L));

                // Disabled state-type is a no-op pass-through (kept for API symmetry with
                // FlinkCompactionFilter.StateType.Disabled).
                assertDoesNotThrow(
                        () ->
                                linker.setCompactionFilterTtl(
                                        db, cf, 0L, ForStRsLinker.STATE_TYPE_DISABLED, 0L));

                // Out-of-range stateType is rejected at the Java boundary, before the JNI hop.
                assertThrows(
                        IllegalArgumentException.class,
                        () -> linker.setCompactionFilterTtl(db, cf, 60_000L, 99, 0L));

                // Negative ttl rejected (FFI signature is u64; negative would underflow).
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                linker.setCompactionFilterTtl(
                                        db, cf, -1L, ForStRsLinker.STATE_TYPE_VALUE, 0L));

                // Negative offset rejected (FFI signature is usize).
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                linker.setCompactionFilterTtl(
                                        db, cf, 60_000L, ForStRsLinker.STATE_TYPE_VALUE, -1L));
            }
        }
    }

    /**
     * MVCC end-to-end across the FFM hop: write v1 → snapshot → write v2; getAt(snap, k) sees v1
     * while normal get sees v2. Covers Task 2.6 (linker bindings for getAt) plus the snapshot
     * lifetime contract in {@link FrsSnapshot}.
     */
    @Test
    void snapshotIsolationAcrossFfmHop() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = utf8("k");
                linker.put(db, cf, key, utf8("v1"));

                try (FrsSnapshot snap = linker.dbSnapshot(db, arena)) {
                    // Write v2 AFTER the snapshot — normal get sees v2.
                    linker.put(db, cf, key, utf8("v2"));
                    assertArrayEquals(utf8("v2"), linker.get(db, cf, key));

                    // getAt at the captured snapshot still sees v1.
                    assertArrayEquals(utf8("v1"), linker.getAt(db, cf, snap, key));
                }
            }
        }
    }

    /**
     * iteratorOpenAt across the FFM hop: 3 keys before snapshot + 1 after. The snapshot iterator
     * must yield exactly the 3 pre-snapshot keys (post-snapshot key filtered by snapshot.seq).
     */
    @Test
    void iteratorOpenAtFiltersBySnapshotSeq() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                linker.put(db, cf, utf8("a"), utf8("1"));
                linker.put(db, cf, utf8("b"), utf8("2"));
                linker.put(db, cf, utf8("c"), utf8("3"));

                try (FrsSnapshot snap = linker.dbSnapshot(db, arena)) {
                    // Add post-snapshot key — must NOT appear in the iter.
                    linker.put(db, cf, utf8("d"), utf8("4"));

                    try (FrsIterator iter = linker.iteratorOpenAt(db, cf, snap, arena)) {
                        List<String> keys = new ArrayList<>();
                        IteratorEntry entry;
                        while ((entry = linker.iteratorNext(iter)) != null) {
                            keys.add(new String(entry.key(), StandardCharsets.UTF_8));
                        }
                        assertEquals(List.of("a", "b", "c"), keys);
                    }
                }
            }
        }
    }

    @Test
    void batchGetReturnsCorrectValuesAndNulls() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                linker.put(db, cf, utf8("k1"), utf8("v1"));
                linker.put(db, cf, utf8("k2"), utf8("v2"));
                linker.put(db, cf, utf8("k3"), utf8("v3"));

                byte[][] keys = {utf8("k1"), utf8("k2"), utf8("missing"), utf8("k3")};
                byte[][] results = linker.batchGet(db, cf, keys);

                assertEquals(4, results.length);
                assertArrayEquals(utf8("v1"), results[0]);
                assertArrayEquals(utf8("v2"), results[1]);
                assertNull(results[2]);
                assertArrayEquals(utf8("v3"), results[3]);
            }
        }
    }

    @Test
    void batchGetEmptyKeysReturnsEmptyArray() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[][] results = linker.batchGet(db, cf, new byte[0][]);
                assertEquals(0, results.length);
            }
        }
    }

    @Test
    void getPinnedReturnsInlineValue() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                linker.put(db, cf, utf8("k"), utf8("small-value"));
                byte[] pinned = linker.getPinned(db, cf, utf8("k"));
                assertNotNull(pinned);
                assertArrayEquals(utf8("small-value"), pinned);
            }
        }
    }

    @Test
    void getPinnedReturnsNullForMissingKey() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                assertNull(linker.getPinned(db, cf, utf8("missing")));
            }
        }
    }

    /**
     * A11-H1 / D11-H2 regression: a value with {@code valueOut.length() == 0} (i.e. a legitimately
     * empty serialized form) must PUT-with-empty-value through the batch path, not be silently
     * transformed into a DELETE tombstone.
     *
     * <p>The Rust FFI {@code frs_batch_put} interprets a NULL value pointer as DELETE
     * (lib.rs:1343-1348). The Java batch staging path (both the convenience overload here and the
     * {@code ForStRsKeyedStateBackend.flushWriteBuffer} chunk loop) must therefore stage a non-NULL
     * pointer even for empty payloads — the 1-byte sentinel allocation guarantees this while the
     * paired length slot stays at 0, so the engine sees an empty PUT slice.
     *
     * <p>Witness: open a full iterator after the batchPut. If the empty-value key is present, it
     * shows up in the iterator key set; if D10-M3's NULL-as-empty-value transformation tombstoned
     * it, the key is absent. We use the iterator rather than {@link ForStRsLinker#get} because
     * {@code FrsBytes::from_vec} of an empty {@code Vec} returns {@code data=NULL}, which the
     * Java {@code getInternal} reports as a miss — so {@code get} alone cannot distinguish a
     * present-but-empty value from a tombstone. The iterator iterates the engine's actual key
     * set and is unambiguous.
     */
    @Test
    void emptyValueRoundTripsAsPutNotDelete() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] emptyKey = utf8("empty-val-key");
                byte[] nonEmptyKey = utf8("non-empty-val-key");
                byte[] emptyValue = new byte[0];
                byte[] nonEmptyValue = utf8("payload");

                // Batch put: one row has an empty value, the other a normal payload.
                byte[][] keys = new byte[][] {emptyKey, nonEmptyKey};
                byte[][] values = new byte[][] {emptyValue, nonEmptyValue};
                linker.batchPut(db, cf, keys, values);

                // Witness via iterator: BOTH keys must be present. A NULL-as-DELETE transformation
                // would tombstone the empty-value row and the iterator would only see one key.
                List<String> observedKeys = new ArrayList<>();
                List<Integer> observedValueLens = new ArrayList<>();
                try (FrsIterator iter = linker.iteratorOpen(db, cf, arena)) {
                    IteratorEntry entry;
                    while ((entry = linker.iteratorNext(iter)) != null) {
                        observedKeys.add(new String(entry.key(), StandardCharsets.UTF_8));
                        observedValueLens.add(entry.value().length);
                    }
                }

                assertTrue(
                        observedKeys.contains("empty-val-key"),
                        "key with empty value must be PRESENT after batchPut (FFI null-as-DELETE"
                                + " collision would tombstone it)");
                assertTrue(
                        observedKeys.contains("non-empty-val-key"),
                        "non-empty companion in same batchPut must be present");

                // The empty-value key's value length must be 0 (not absent / not corrupted).
                int idxEmpty = observedKeys.indexOf("empty-val-key");
                assertEquals(
                        0,
                        observedValueLens.get(idxEmpty).intValue(),
                        "empty-value key must retrieve a zero-length payload");

                int idxNonEmpty = observedKeys.indexOf("non-empty-val-key");
                assertEquals(
                        nonEmptyValue.length,
                        observedValueLens.get(idxNonEmpty).intValue(),
                        "non-empty companion payload length must be preserved");
            }
        }
    }
}

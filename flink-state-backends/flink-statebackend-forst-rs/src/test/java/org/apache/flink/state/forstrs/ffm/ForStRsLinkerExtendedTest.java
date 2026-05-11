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
}

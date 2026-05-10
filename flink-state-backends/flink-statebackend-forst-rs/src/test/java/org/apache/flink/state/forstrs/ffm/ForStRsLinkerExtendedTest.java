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
                assertThrows(
                        FrsBackendException.class, () -> linker.dbOpenCf(db, arena, "nope"));
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
                assertTrue(afterPut > initial,
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
     * Round-trips put/get through the tuned-config in-memory engine. Mirrors
     * Preset A from the JMH bench: 256 MiB memtable budget × 8 buffers, default
     * 4/4 background threads. Proves the new {@link
     * ForStRsLinker#dbOpenMemoryTuned(Arena, long, long, long, long)} method
     * is wired end-to-end through {@code frs_db_open_memory_tuned}.
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
     * Passing {@code 0} for every knob falls back to the engine defaults; the
     * resulting handle still survives a basic put/get round-trip and a clean
     * close. Covers the per-knob "skip setter when zero" branch in
     * {@code frs_db_open_memory_tuned}.
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
     * An out-of-range memtable size (1 byte, far below the 4 KiB
     * {@code MIN_WRITE_BUFFER_SIZE} floor enforced by
     * {@code EngineOptionsBuilder::try_build}) must surface as a clean
     * {@link FrsBackendException} carrying the {@code INVALID_ARGUMENT}
     * status, not a JVM panic / crash.
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
     * P5 — TTL compaction filter factory stub: open returns a non-zero handle, dispose accepts it
     * silently, and a successive open hands out a different handle (the per-process counter must
     * advance).
     *
     * <p>The factory is a placeholder — TTL expirations are not actually enforced. See the doc
     * comment on {@link ForStRsLinker#newCompactionFilterFactory(long, long, int)}.
     */
    @Test
    void testCompactionFilterFactoryStub() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);

            // Typical Flink keyed-state TTL request: 60s, query-time-after-1000-entries,
            // Value state.
            long h1 =
                    linker.newCompactionFilterFactory(
                            60_000L, 1000L, ForStRsLinker.STATE_TYPE_VALUE);
            assertTrue(h1 != 0L, "factory handle must be non-zero (Java would NPE on 0)");

            // Second factory: distinct handle so a backend instantiating one factory per CF
            // can keep them apart.
            long h2 =
                    linker.newCompactionFilterFactory(
                            300_000L, 0L, ForStRsLinker.STATE_TYPE_LIST);
            assertTrue(h2 != 0L);
            assertTrue(h2 != h1, "successive handles must be distinct: " + h1 + " == " + h2);

            // Dispose path is a no-op but must not throw on a freshly-issued handle.
            assertDoesNotThrow(() -> linker.disposeCompactionFilterFactory(h1));
            assertDoesNotThrow(() -> linker.disposeCompactionFilterFactory(h2));

            // Idempotent on 0.
            assertDoesNotThrow(() -> linker.disposeCompactionFilterFactory(0L));

            // Negative handles are rejected (caller bug, not silently swallowed).
            assertThrows(
                    IllegalArgumentException.class,
                    () -> linker.disposeCompactionFilterFactory(-1L));

            // Out-of-range stateType is rejected at construction time.
            assertThrows(
                    IllegalArgumentException.class,
                    () -> linker.newCompactionFilterFactory(60_000L, 1000L, 99));
        }
    }
}

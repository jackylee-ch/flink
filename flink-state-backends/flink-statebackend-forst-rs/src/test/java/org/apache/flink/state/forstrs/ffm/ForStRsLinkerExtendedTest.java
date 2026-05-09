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
}

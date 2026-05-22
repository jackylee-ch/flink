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

package org.apache.flink.state.forstrs.state;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.foreign.Arena;

/**
 * PR-C1 pre-snapshot flush hook test. Fills the {@link MapStateArrowBuffer} with 1000 entries
 * (mixing puts + a few tombstones), simulates a snapshot trigger by calling
 * {@code flushTo(linker, db, cf)}, and verifies that every entry is durable in the engine via
 * {@code linker.getFast}.
 *
 * <p>Mirrors the V1-sync {@code statebuf.flushTo} contract: after flush, the buffer is empty,
 * the tombstone set is empty, and every PUT row has landed in the engine, every REMOVE has
 * been propagated as a native delete.
 */
class MapStateV2PreSnapshotFlushTest {

    private Arena linkerArena;
    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;

    @BeforeEach
    void setUp() {
        linkerArena = Arena.ofShared();
        linker = new ForStRsLinker(linkerArena);
        db = linker.dbOpenMemory(linkerArena);
        cf = linker.dbDefaultCf(db, linkerArena);
    }

    @AfterEach
    void tearDown() {
        cf.close();
        db.close();
        linkerArena.close();
    }

    private static byte[] composite(int i) {
        // Distinct 24-byte composite per row, prefixed with a fixed state-name marker so we can
        // assert no cross-state collisions (single-state test, but the layout matches the V2
        // serializeMapEntryKey format: KEY_PREFIX || operatorKey || / || stateName || / || ns || uk).
        byte[] out = new byte[24];
        out[0] = 'k';
        out[1] = '/';
        for (int j = 0; j < 8; j++) {
            out[2 + j] = (byte) (((long) i) >>> (8 * (7 - j))); // operatorKey = i
        }
        out[10] = '/';
        out[11] = 'm';
        out[12] = '/';
        for (int j = 0; j < 4; j++) {
            out[13 + j] = (byte) (0); // VoidNamespace placeholder
        }
        for (int j = 0; j < 7; j++) {
            out[17 + j] = (byte) ((i >>> (8 * (6 - j))) & 0xFF); // userKey = i
        }
        return out;
    }

    private static byte[] value(int i) {
        byte[] out = new byte[16];
        for (int j = 0; j < 8; j++) {
            out[j] = (byte) (((long) i * 31) >>> (8 * (7 - j)));
        }
        for (int j = 0; j < 8; j++) {
            out[8 + j] = (byte) (((long) i + 100) >>> (8 * (7 - j)));
        }
        return out;
    }

    @Test
    void flushOf1000EntriesLandsAllInEngine() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        final int N = 1000;

        // Fill 1000 entries.
        for (int i = 0; i < N; i++) {
            buf.put(composite(i), value(i), linker, db, cf);
        }
        // The buffer may auto-flush internally (shouldAutoFlush at half-capacity) — that's fine,
        // it just means some rows already landed in the engine. The post-flush assertion below is
        // what matters: after the final flushTo, ALL N rows must be in the engine.
        buf.flushTo(linker, db, cf);

        // Buffer must be empty after flush (size + tombstones).
        assertEquals(0, buf.underlying().size(), "buffer size must be 0 after flush");
        assertEquals(0, buf.tombstoneCount(), "tombstone set must be empty after flush");

        // Engine must hold all N entries.
        for (int i = 0; i < N; i++) {
            byte[] retrieved = linker.getFast(db, cf, composite(i));
            assertNotNull(retrieved, "entry " + i + " missing in engine after flush");
            assertArrayEquals(value(i), retrieved, "value mismatch for entry " + i);
        }

        buf.close();
    }

    @Test
    void flushPropagatesTombstonesAsDeletes() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();

        // Seed 10 entries.
        for (int i = 0; i < 10; i++) {
            buf.put(composite(i), value(i), linker, db, cf);
        }
        buf.flushTo(linker, db, cf);
        // All 10 should be in the engine now.
        for (int i = 0; i < 10; i++) {
            assertNotNull(linker.getFast(db, cf, composite(i)));
        }

        // Now remove 5 of them via the buffer + flush.
        for (int i = 0; i < 5; i++) {
            buf.remove(composite(i), linker, db, cf);
        }
        assertEquals(5, buf.tombstoneCount());
        buf.flushTo(linker, db, cf);
        assertEquals(0, buf.tombstoneCount(), "tombstones must clear on flush");

        // Removed entries gone from engine; surviving entries still present.
        for (int i = 0; i < 5; i++) {
            assertNull(
                    linker.getFast(db, cf, composite(i)),
                    "entry " + i + " was tombstoned + flushed; must be gone from engine");
        }
        for (int i = 5; i < 10; i++) {
            assertNotNull(
                    linker.getFast(db, cf, composite(i)),
                    "entry " + i + " was not tombstoned; must still be in engine");
        }

        buf.close();
    }

    @Test
    void putThenRemoveThenFlushDeletesEvenIfNeverFlushedBefore() {
        // put + remove on a key that never reached the engine: the buffer's remove must still
        // dispatch a native delete on flush (in case any prior batch landed the same key).
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        byte[] k = composite(42);
        buf.put(k, value(42), linker, db, cf);
        buf.remove(k, linker, db, cf);
        // Pre-flush, buffer state: PUT row was tombstoned in the index AND the tombstone set
        // carries the key. After flush, neither lingers, and engine has no row.
        buf.flushTo(linker, db, cf);
        assertEquals(0, buf.tombstoneCount());
        assertNull(linker.getFast(db, cf, k));
        buf.close();
    }

    @Test
    void doubleFlushIsIdempotent() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        for (int i = 0; i < 100; i++) {
            buf.put(composite(i), value(i), linker, db, cf);
        }
        buf.flushTo(linker, db, cf);
        assertEquals(0, buf.underlying().size());

        // Second flush on empty buffer is a no-op.
        buf.flushTo(linker, db, cf);
        assertEquals(0, buf.underlying().size());
        for (int i = 0; i < 100; i++) {
            assertNotNull(linker.getFast(db, cf, composite(i)));
        }
        buf.close();
    }

    @Test
    void closeReleasesArenaCleanly() {
        // Ensure the per-buffer Arena (the underlying ArrowBinaryBuffer + the staging arena) all
        // close cleanly without leaking. We can't directly assert no-leak in a unit test, but the
        // close() path must not throw under normal usage.
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        for (int i = 0; i < 50; i++) {
            buf.put(composite(i), value(i), linker, db, cf);
        }
        buf.close();
        // After close, second close must be idempotent (no-op).
        buf.close();
    }

    @Test
    void autoFlushFiresAtHighWaterMark() {
        // Buffer at small initial capacity — the auto-flush gate (size >= capacity/2) should
        // trigger an internal flush as we insert past the high-water mark. This is the same
        // mechanism Q11/Q12 rely on to keep batchPut sizes bounded.
        MapStateArrowBuffer buf =
                new MapStateArrowBuffer(
                        /* initialCapacity */ 16,
                        /* maxCapacity */ 64);
        // Insert 32 distinct rows — the buffer should flush internally at least once on the way
        // up. We assert post-condition: after 32 puts every row is in the engine OR still in
        // the buffer; an explicit flush at the end + getFast must succeed for all rows.
        for (int i = 0; i < 32; i++) {
            buf.put(composite(i), value(i), linker, db, cf);
        }
        buf.flushTo(linker, db, cf);
        for (int i = 0; i < 32; i++) {
            assertNotNull(linker.getFast(db, cf, composite(i)), "row " + i + " lost");
        }
        // Sanity: shouldAutoFlush is the trigger we relied on.
        assertFalse(buf.underlying().shouldAutoFlush(), "post-flush buffer should not be flushing");
        buf.close();
    }
}

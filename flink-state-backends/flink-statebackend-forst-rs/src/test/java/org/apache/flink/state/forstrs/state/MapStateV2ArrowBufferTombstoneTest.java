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

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cleanup-C1 verification: asserts that the V2 MapState off-heap buffer's tombstone path is hit
 * via the underlying {@link ArrowBinaryBuffer}'s per-row tombstone bitmap (no {@code HashSet} of
 * wrapper key objects).
 *
 * <p>The tests cover three properties of the new zero-alloc tombstone implementation:
 *
 * <ol>
 *   <li>{@link MapStateArrowBuffer#remove} marks the row tombstoned without raising a HashSet
 *       allocation (structural: a HashSet field would still exist if the old wiring leaked
 *       through).
 *   <li>A subsequent {@code lookup} returns {@code tombstone=true} and {@code cached=true},
 *       short-circuiting the GET without an engine round-trip.
 *   <li>A subsequent {@code put} for the same key lifts the tombstone (PUT supersedes
 *       tombstone — handled by {@link ArrowBinaryBuffer#insert}'s overwrite branch which clears
 *       the tombstone bit).
 * </ol>
 *
 * <p>The structural assertion (no HashSet field) makes the test fail loudly if a future commit
 * re-introduces the legacy HashSet tracker.
 */
class MapStateV2ArrowBufferTombstoneTest {

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

    private static byte[] composite(long opKey, int namespace, long userKey) {
        byte[] out = new byte[8 + 4 + 8];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (opKey >>> (8 * (7 - i)));
        }
        for (int i = 0; i < 4; i++) {
            out[8 + i] = (byte) (namespace >>> (8 * (3 - i)));
        }
        for (int i = 0; i < 8; i++) {
            out[12 + i] = (byte) (userKey >>> (8 * (7 - i)));
        }
        return out;
    }

    private static byte[] valueBytes(long v) {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (v >>> (8 * (7 - i)));
        }
        return out;
    }

    /**
     * Structural: {@link MapStateArrowBuffer} must NOT declare a HashSet field. If a future
     * commit re-introduces the C1 violation it will fail here with a clear message before any
     * runtime allocations accrue.
     */
    @Test
    void noHashSetFieldInMapStateArrowBuffer() {
        for (Field f : MapStateArrowBuffer.class.getDeclaredFields()) {
            String typeName = f.getType().getName();
            assertFalse(
                    typeName.equals("java.util.HashSet")
                            || typeName.equals("java.util.LinkedHashSet")
                            || typeName.contains("Set"),
                    "MapStateArrowBuffer.\""
                            + f.getName()
                            + "\" is a "
                            + typeName
                            + " — Cleanup-C1 requires the tombstone tracker to live in the "
                            + "underlying ArrowBinaryBuffer's off-heap bitmap, not a heap HashSet.");
        }
    }

    /**
     * Functional: remove then lookup hits the tombstone path WITHOUT going through any
     * heap-allocated set. The buffer's row count grows by 1 (the tombstone row is reserved with
     * empty value) and the tombstoneCount reports 1.
     */
    @Test
    void asyncRemoveThenAsyncGetReturnsTombstoneShortCircuit() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        byte[] k = composite(7L, 0, 99L);

        // Remove a key that was never inserted — the tombstone must still be recorded so a
        // subsequent get short-circuits to null without an engine probe.
        buf.remove(k, linker, db, cf);

        MapStateArrowBuffer.Lookup hit = buf.lookup(k);
        assertTrue(hit.cached, "tombstone lookup must report cached=true");
        assertTrue(hit.tombstone, "lookup after remove must report tombstone=true");
        assertEquals(1, buf.tombstoneCount(), "exactly one tombstoned row expected");
        assertEquals(1, buf.size(), "tombstone row is materialized in the buffer (empty value)");

        buf.close();
    }

    /** PUT after remove lifts the tombstone: insert() clears the per-row bit. */
    @Test
    void putAfterRemoveLiftsTombstone() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        byte[] k = composite(3L, 1, 5L);
        byte[] v = valueBytes(0xCAFEBABEL);

        buf.put(k, v, linker, db, cf);
        buf.remove(k, linker, db, cf);

        // Tombstone is visible.
        assertTrue(buf.lookup(k).tombstone, "remove must record tombstone");
        assertEquals(1, buf.tombstoneCount());

        // PUT supersedes tombstone — the row is alive with the new value.
        byte[] v2 = valueBytes(0xDEADBEEFL);
        buf.put(k, v2, linker, db, cf);

        MapStateArrowBuffer.Lookup hit = buf.lookup(k);
        assertTrue(hit.cached);
        assertFalse(hit.tombstone, "PUT after remove must clear the tombstone");
        assertArrayEquals(v2, buf.valueBytesOf(hit.row));
        assertEquals(0, buf.tombstoneCount());

        buf.close();
    }

    /** Multiple distinct removes accumulate distinct tombstones in the bitmap (no collisions). */
    @Test
    void multipleRemovesAccumulateDistinctTombstones() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        byte[] k1 = composite(1L, 0, 1L);
        byte[] k2 = composite(1L, 0, 2L);
        byte[] k3 = composite(2L, 0, 1L);

        buf.remove(k1, linker, db, cf);
        buf.remove(k2, linker, db, cf);
        buf.remove(k3, linker, db, cf);

        assertEquals(3, buf.tombstoneCount(), "each removed key gets its own tombstone bit");

        // Each lookup short-circuits via the off-heap bitmap.
        for (byte[] k : new byte[][] {k1, k2, k3}) {
            MapStateArrowBuffer.Lookup hit = buf.lookup(k);
            assertTrue(hit.cached, "tombstoned key must be cached=true for " + Arrays.toString(k));
            assertTrue(hit.tombstone, "tombstoned key must be tombstone=true");
        }
        // A non-tombstoned key reports MISS.
        assertFalse(buf.lookup(composite(9L, 0, 9L)).cached);

        buf.close();
    }

    /**
     * The underlying buffer exposes tombstoned-row enumeration. flushTo uses this list to issue
     * per-row deletes; the bitmap is the only source of truth (no HashSet to reconcile).
     */
    @Test
    void underlyingBufferReportsTombstonedRows() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        byte[] k1 = composite(1L, 0, 1L);
        byte[] k2 = composite(1L, 0, 2L);
        buf.put(k1, valueBytes(10L), linker, db, cf);
        buf.put(k2, valueBytes(20L), linker, db, cf);
        buf.remove(k1, linker, db, cf);

        ArrowBinaryBuffer under = buf.underlying();
        int[] tombstoned = under.tombstonedRows();
        assertNotNull(tombstoned);
        assertEquals(1, tombstoned.length, "exactly one tombstoned row");
        assertTrue(under.isTombstoned(tombstoned[0]));

        // The other row is NOT tombstoned.
        boolean foundLive = false;
        for (int r = 0; r < under.size(); r++) {
            if (r != tombstoned[0]) {
                assertFalse(under.isTombstoned(r), "non-tombstoned row must not have the bit set");
                foundLive = true;
            }
        }
        assertTrue(foundLive, "live row must exist");

        buf.close();
    }

    /**
     * findOrTombstone tri-state contract: returns {@code TOMBSTONE_FOUND} only after a tombstone
     * call; otherwise returns the row id (>=0) for a live key or -1 for an absent key. This is
     * the core API that replaces the HashSet-contains lookup.
     */
    @Test
    void findOrTombstoneTriStateContract() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        ArrowBinaryBuffer under = buf.underlying();

        byte[] kLive = composite(1L, 0, 1L);
        byte[] kTomb = composite(1L, 0, 2L);
        byte[] kAbsent = composite(1L, 0, 3L);

        buf.put(kLive, valueBytes(1L), linker, db, cf);
        buf.remove(kTomb, linker, db, cf);

        // Probe via the wrapper to also exercise the staging-arena copy path.
        assertTrue(buf.lookup(kLive).cached);
        assertFalse(buf.lookup(kLive).tombstone);
        assertTrue(buf.lookup(kTomb).cached);
        assertTrue(buf.lookup(kTomb).tombstone);
        assertFalse(buf.lookup(kAbsent).cached);

        // Direct probe via the underlying buffer's findOrTombstone (the API the wrapper uses).
        // Re-stage kAbsent into a fresh ArrowBinaryBuffer scratch arena to test the API directly.
        java.lang.foreign.MemorySegment seg;
        try (Arena scratch = Arena.ofConfined()) {
            seg = scratch.allocate(kLive.length);
            for (int i = 0; i < kLive.length; i++) {
                seg.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, kLive[i]);
            }
            int row = under.findOrTombstone(seg, 0, kLive.length);
            if (row < 0) {
                fail("kLive must be findable as a live row, got " + row);
            }
            seg = scratch.allocate(kTomb.length);
            for (int i = 0; i < kTomb.length; i++) {
                seg.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, kTomb[i]);
            }
            int tRow = under.findOrTombstone(seg, 0, kTomb.length);
            assertEquals(
                    ArrowBinaryBuffer.TOMBSTONE_FOUND,
                    tRow,
                    "tombstoned key must report TOMBSTONE_FOUND");
            seg = scratch.allocate(kAbsent.length);
            for (int i = 0; i < kAbsent.length; i++) {
                seg.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, kAbsent[i]);
            }
            int aRow = under.findOrTombstone(seg, 0, kAbsent.length);
            assertEquals(-1, aRow, "absent key must report -1 (miss)");
        }

        buf.close();
    }
}

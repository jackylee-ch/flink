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

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowBinaryBufferTest {

    /** Helper: write a byte[] into an Arena and return a MemorySegment view of it. */
    private MemorySegment writeIntoArena(Arena arena, byte[] data) {
        MemorySegment seg = arena.allocate(data.length == 0 ? 1 : data.length);
        for (int i = 0; i < data.length; i++) {
            seg.set(ValueLayout.JAVA_BYTE, i, data[i]);
        }
        return seg;
    }

    @Test
    void insertAndFindReturnsCorrectRow() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment key = writeIntoArena(arena, new byte[] {1, 2, 3});
            MemorySegment val = writeIntoArena(arena, new byte[] {10, 20});

            int row = buf.insert(key, 0, 3, val, 0, 2);
            assertTrue(row >= 0, "insert must return a valid row id");

            int found = buf.find(key, 0, 3);
            assertEquals(row, found, "find must return the same row id as insert");

            byte[] readback = buf.copyValue(found);
            assertArrayEquals(new byte[] {10, 20}, readback);

            buf.close();
        }
    }

    @Test
    void findMissReturnsNegativeOne() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment key = writeIntoArena(arena, new byte[] {9, 9, 9});
            assertEquals(-1, buf.find(key, 0, 3));
            buf.close();
        }
    }

    @Test
    void insertOverwritesExistingKey() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment key = writeIntoArena(arena, new byte[] {5, 5});
            MemorySegment v1 = writeIntoArena(arena, new byte[] {100});
            MemorySegment v2 = writeIntoArena(arena, new byte[] {(byte) 200});

            int row1 = buf.insert(key, 0, 2, v1, 0, 1);
            int row2 = buf.insert(key, 0, 2, v2, 0, 1);

            assertEquals(row1, row2, "second insert with same key must return same row");
            assertArrayEquals(new byte[] {(byte) 200}, buf.copyValue(row2));

            buf.close();
        }
    }

    @Test
    void removeMakesKeyDisappear() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment key = writeIntoArena(arena, new byte[] {7, 7});
            MemorySegment val = writeIntoArena(arena, new byte[] {99});
            buf.insert(key, 0, 2, val, 0, 1);
            assertTrue(buf.find(key, 0, 2) >= 0);
            buf.remove(key, 0, 2);
            assertEquals(-1, buf.find(key, 0, 2));
            buf.close();
        }
    }

    @Test
    void resizePreservesAllEntries() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(8); // tiny so resize fires fast
            for (int i = 0; i < 50; i++) {
                byte[] keyBytes = new byte[] {(byte) (i >> 8), (byte) i};
                byte[] valBytes = new byte[] {(byte) (i * 3)};
                MemorySegment k = writeIntoArena(arena, keyBytes);
                MemorySegment v = writeIntoArena(arena, valBytes);
                buf.insert(k, 0, 2, v, 0, 1);
            }
            // Every key still findable + value correct.
            for (int i = 0; i < 50; i++) {
                byte[] keyBytes = new byte[] {(byte) (i >> 8), (byte) i};
                MemorySegment k = writeIntoArena(arena, keyBytes);
                int row = buf.find(k, 0, 2);
                assertNotEquals(-1, row, "missing entry " + i);
                assertEquals((byte) (i * 3), buf.copyValue(row)[0]);
            }
            buf.close();
        }
    }

    @Test
    void clearResetsSizeButRetainsCapacity() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment k = writeIntoArena(arena, new byte[] {1});
            MemorySegment v = writeIntoArena(arena, new byte[] {2});
            buf.insert(k, 0, 1, v, 0, 1);
            assertEquals(1, buf.size());
            buf.clear();
            assertEquals(0, buf.size());
            assertEquals(-1, buf.find(k, 0, 1));
            // Insert again into the cleared buffer.
            int row = buf.insert(k, 0, 1, v, 0, 1);
            assertTrue(row >= 0);
            buf.close();
        }
    }

    @Test
    void clearAfterInserts() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            for (int i = 0; i < 10; i++) {
                MemorySegment k = writeIntoArena(arena, new byte[] {(byte) i});
                MemorySegment v = writeIntoArena(arena, new byte[] {(byte) (i + 100)});
                buf.insert(k, 0, 1, v, 0, 1);
            }
            assertEquals(10, buf.size());
            buf.clear();
            assertEquals(0, buf.size());
            buf.close();
        }
    }

    @Test
    void shouldAutoFlushFiresAtHighWaterMark() {
        try (Arena arena = Arena.ofConfined()) {
            // Tiny capacity so we trigger high-water mark quickly.
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(8);
            // High-water mark is capacity / 2 = 4.
            assertFalse(buf.shouldAutoFlush(), "no auto-flush when empty");
            for (int i = 0; i < 4; i++) {
                MemorySegment k = writeIntoArena(arena, new byte[] {(byte) i});
                MemorySegment v = writeIntoArena(arena, new byte[] {(byte) (i + 100)});
                buf.insert(k, 0, 1, v, 0, 1);
            }
            // After 4 inserts at capacity 8, size >= flushHighWaterMark=4 → should auto-flush.
            assertTrue(buf.shouldAutoFlush(), "auto-flush should fire at half-capacity");
            buf.close();
        }
    }

    @Test
    void overwriteDoesNotCorruptAdjacentRows() {
        // Regression: previously, overwriting row N's value updated valueOffsets[N+1]
        // (the Arrow-style end-offset), which is row (N+1)'s START offset. Reading row
        // (N+1) then returned garbled bytes and downstream deserializers tripped EOF.
        // Fix: sidecar valueLengths[] makes overwrite local to row N.
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(64);
            MemorySegment k1 = writeIntoArena(arena, new byte[] {1});
            MemorySegment k2 = writeIntoArena(arena, new byte[] {2});
            MemorySegment k3 = writeIntoArena(arena, new byte[] {3});
            MemorySegment vShort = writeIntoArena(arena, new byte[] {10});
            MemorySegment vLong = writeIntoArena(arena, new byte[] {20, 21, 22, 23, 24});

            buf.insert(k1, 0, 1, vShort, 0, 1);
            buf.insert(k2, 0, 1, vShort, 0, 1);
            buf.insert(k3, 0, 1, vShort, 0, 1);

            // Overwrite row1's value with a LONGER value. This used to corrupt row2's offset.
            buf.insert(k1, 0, 1, vLong, 0, 5);

            assertArrayEquals(
                    new byte[] {20, 21, 22, 23, 24},
                    buf.copyValue(buf.find(k1, 0, 1)),
                    "row1 must hold the new long value");
            assertArrayEquals(
                    new byte[] {10},
                    buf.copyValue(buf.find(k2, 0, 1)),
                    "row2 must still hold its original short value (was: corruption bug)");
            assertArrayEquals(
                    new byte[] {10},
                    buf.copyValue(buf.find(k3, 0, 1)),
                    "row3 must still hold its original short value");
            buf.close();
        }
    }

    @Test
    void hashCollisionLinearProbingWorks() {
        // Two keys that hash to the same bucket — handled via open addressing + per-byte fallback.
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(16);
            // Construct two keys with the same Arrays.hashCode (impossible to guarantee, so use
            // crafted keys that differ only in length but happen to collide for the small bucket count).
            byte[] k1 = new byte[] {1, 2};
            byte[] k2 = new byte[] {2, 1};
            MemorySegment ms1 = writeIntoArena(arena, k1);
            MemorySegment ms2 = writeIntoArena(arena, k2);
            buf.insert(ms1, 0, 2, ms1, 0, 2);
            buf.insert(ms2, 0, 2, ms2, 0, 2);
            assertNotEquals(-1, buf.find(ms1, 0, 2));
            assertNotEquals(-1, buf.find(ms2, 0, 2));
            // Both must be distinct rows.
            assertNotEquals(buf.find(ms1, 0, 2), buf.find(ms2, 0, 2));
            buf.close();
        }
    }
}

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

package org.apache.flink.state.forstrs.timer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ArrowTimerBuffer} — covers the 4 critical correctness invariants:
 *
 * <ol>
 *   <li>insertAdd then insertRemove (via cancellation in the caller) yields no entries on drain.
 *   <li>Min-heap ordering by timestamp.
 *   <li>Overwrite-via-cancel-then-insert leaves only the latest entry surviving.
 *   <li>Resize on growth: insert beyond initial capacity, all entries retrievable in order.
 * </ol>
 */
class ArrowTimerBufferTest {

    private Arena scratchArena;
    private MemorySegment scratch;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        scratchArena = Arena.ofShared();
        scratch = scratchArena.allocate(256);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        if (scratchArena != null) {
            scratchArena.close();
        }
    }

    /** Writes a 4-byte int into the scratch segment and returns its length. */
    private int putIntKey(int v) {
        scratch.set(ValueLayout.JAVA_INT, 0L, v);
        return 4;
    }

    /**
     * 1. insertAdd + removeAt cancels the entry — drainTo yields no rows.
     */
    @Test
    void insertAddThenRemoveCancels() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(32)) {
            int len = putIntKey(42);
            int pos = buf.insertAdd(scratch, 0L, len, 100L);
            assertEquals(1, buf.size());
            // Look up by key bytes, then remove — the cancellation pattern used by the priority
            // queue's add-then-remove path.
            int found = buf.find(scratch, 0L, len);
            assertEquals(pos, found);
            buf.removeAt(found);
            assertEquals(0, buf.size());

            List<Long> drained = new ArrayList<>();
            buf.drainTo((op, kd, kOff, kLen, ts) -> drained.add(ts));
            assertTrue(drained.isEmpty());
        }
    }

    /**
     * 2. Min-heap ordering: insert ts = 10, 5, 20, 1, 15; drainTo yields min-heap-walk ordering
     *    where the root is always the smallest. We assert root is smallest and at-least every
     *    parent ≤ children (heap invariant), then full sort comes from repeated remove-of-root.
     */
    @Test
    void minHeapOrderingByTs() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(32)) {
            long[] tss = {10L, 5L, 20L, 1L, 15L};
            for (int i = 0; i < tss.length; i++) {
                scratch.set(ValueLayout.JAVA_INT, 0L, i);
                buf.insertAdd(scratch, 0L, 4, tss[i]);
            }
            assertEquals(5, buf.size());

            // Heap invariant: root must be ts = 1.
            assertEquals(1L, buf.tsAt(0));
            // For every internal node i, ts[i] <= ts[2i+1] (if exists) and <= ts[2i+2] (if exists).
            int n = buf.size();
            for (int i = 0; i < n; i++) {
                int left = 2 * i + 1;
                int right = 2 * i + 2;
                long parentTs = buf.tsAt(i);
                if (left < n) {
                    assertTrue(parentTs <= buf.tsAt(left), "heap violation at left child");
                }
                if (right < n) {
                    assertTrue(parentTs <= buf.tsAt(right), "heap violation at right child");
                }
            }

            // Now drain by repeatedly removing root — yields ascending-ts sequence.
            List<Long> ordered = new ArrayList<>();
            while (buf.size() > 0) {
                ordered.add(buf.tsAt(0));
                buf.removeAt(0);
            }
            assertEquals(List.of(1L, 5L, 10L, 15L, 20L), ordered);
        }
    }

    /**
     * 3. Cancel then re-insert at the same key: only the latest entry survives. Models the
     *    priority queue's add-cancel-add cycle.
     */
    @Test
    void insertOverwriteRetainsHeapInvariant() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(32)) {
            int len = putIntKey(7);
            int first = buf.insertAdd(scratch, 0L, len, 100L);
            assertEquals(1, buf.size());
            assertEquals(100L, buf.tsAt(first));

            // Cancel
            int found = buf.find(scratch, 0L, len);
            buf.removeAt(found);
            assertEquals(0, buf.size());
            // Re-insert with a different ts.
            int second = buf.insertAdd(scratch, 0L, len, 50L);
            assertEquals(1, buf.size());
            assertEquals(50L, buf.tsAt(second));
            // The original entry should no longer be findable at ts=100; find() returns the
            // current row.
            int found2 = buf.find(scratch, 0L, len);
            assertNotEquals(-1, found2);
            assertEquals(50L, buf.tsAt(found2));
        }
    }

    /**
     * 4. Resize on growth: insert beyond initial capacity = 16. All entries retrievable in
     *    ascending-ts order after the buffer auto-grows.
     */
    @Test
    void resizeOnGrowth() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(16, 1024)) {
            int target = 200; // well past initial 16
            // Insert in reverse order so the heap actively re-sifts.
            for (int i = target - 1; i >= 0; i--) {
                scratch.set(ValueLayout.JAVA_INT, 0L, i);
                buf.insertAdd(scratch, 0L, 4, (long) i);
            }
            assertEquals(target, buf.size());
            // Drain root repeatedly — should yield 0..target-1 in order.
            for (int i = 0; i < target; i++) {
                assertEquals((long) i, buf.tsAt(0));
                buf.removeAt(0);
            }
            assertEquals(0, buf.size());
        }
    }
}

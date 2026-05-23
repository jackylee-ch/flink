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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
     * D8-H4: resize() must roll back cleanly when an allocate() throws on the new arena. Verifies:
     * <ul>
     *   <li>the failed newArena was closed (no native leak),
     *   <li>the buffer's old arena is still alive (instance state pristine),
     *   <li>the buffer remains fully functional — subsequent insert / drain succeed.
     * </ul>
     */
    @Test
    void resizeRollsBackOnAllocFailure() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(16, 1024)) {
            // Pre-populate up to initial capacity so the NEXT insertAdd triggers a resize.
            int initialCap = buf.capacity();
            for (int i = 0; i < initialCap; i++) {
                scratch.set(ValueLayout.JAVA_INT, 0L, i);
                buf.insertAdd(scratch, 0L, 4, 100L + i);
            }
            assertEquals(initialCap, buf.size());
            Arena oldArena = buf.arenaForTest();
            int snapshotSize = buf.size();

            // Inject a faulty Arena supplier — second allocate() on the new arena (i.e. keyData
            // allocate inside resize) throws. Track the produced arena so we can verify it gets
            // closed by the rollback path.
            List<Arena> producedArenas = new ArrayList<>();
            buf.setArenaSupplierForTest(
                    () -> {
                        Arena failing = new FailingArena(Arena.ofShared(), 2);
                        producedArenas.add(failing);
                        return failing;
                    });

            // The very next insert sees size >= capacity → resize → throws.
            scratch.set(ValueLayout.JAVA_INT, 0L, 9000);
            Throwable observed = null;
            try {
                buf.insertAdd(scratch, 0L, 4, 200L);
            } catch (Throwable t) {
                observed = t;
            }
            assertNotEquals(null, observed, "resize() should have propagated the alloc failure");
            assertTrue(
                    observed instanceof OutOfMemoryError,
                    "expected OutOfMemoryError, got " + observed);

            // The faulty arena must have been closed exactly once by the rollback path.
            assertEquals(1, producedArenas.size(), "supplier should fire exactly once");
            assertFalse(
                    producedArenas.get(0).scope().isAlive(),
                    "failed newArena must be closed by the rollback path");

            // The buffer's instance arena must still point at the original (old) arena and be
            // alive — the rollback path must not have swapped any field.
            assertTrue(oldArena.scope().isAlive(), "old arena must remain alive after rollback");
            assertEquals(oldArena, buf.arenaForTest(), "arena field must not have been swapped");
            assertEquals(snapshotSize, buf.size(), "size must not have been touched");
            assertEquals(initialCap, buf.capacity(), "capacity must not have been touched");

            // Restore the default supplier so a real grow can happen on the next insert.
            buf.setArenaSupplierForTest(Arena::ofShared);

            // The buffer must still be fully functional — insert succeeds (now triggers a real
            // resize on the same insertAdd path).
            scratch.set(ValueLayout.JAVA_INT, 0L, 9999);
            buf.insertAdd(scratch, 0L, 4, 50L);
            assertEquals(snapshotSize + 1, buf.size(), "post-rollback insert should succeed");
            // Min-heap root is now the new entry with ts=50.
            assertEquals(50L, buf.tsAt(0));
        }
    }

    /**
     * Wrapper {@link Arena} whose {@link #allocate(long, long)} throws an OutOfMemoryError on the
     * N-th call (1-indexed). Used by {@link #resizeRollsBackOnAllocFailure} to simulate mid-resize
     * allocation failure.
     */
    private static final class FailingArena implements Arena {
        private final Arena delegate;
        private final AtomicInteger calls = new AtomicInteger(0);
        private final int failOnCall;

        FailingArena(Arena delegate, int failOnCall) {
            this.delegate = delegate;
            this.failOnCall = failOnCall;
        }

        @Override
        public MemorySegment allocate(long byteSize, long byteAlignment) {
            int n = calls.incrementAndGet();
            if (n == failOnCall) {
                throw new OutOfMemoryError("FailingArena: forced failure on call " + n);
            }
            return delegate.allocate(byteSize, byteAlignment);
        }

        @Override
        public MemorySegment.Scope scope() {
            return delegate.scope();
        }

        @Override
        public void close() {
            delegate.close();
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

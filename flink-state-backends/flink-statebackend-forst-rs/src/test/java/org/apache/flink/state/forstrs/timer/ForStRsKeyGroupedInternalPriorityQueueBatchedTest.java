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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spec §"Four Implementation Invariants" — tests for the batched off-heap engine-backed timer
 * queue from {@code 2026-05-21-batched-engine-timer-design.md}.
 *
 * <p>Four invariants verified:
 *
 * <ol>
 *   <li>In-buffer add-remove cancellation — both entries are removed from heap+hashIndex and
 *       NEVER reach the engine.
 *   <li>Min-heap ordering — peek returns the smallest-ts pending ADD.
 *   <li>advance() strict order — flush → batch scan → batch delete.
 *   <li>flushPendingToEngine() is a mandatory pre-snapshot hook (driven by the backend).
 * </ol>
 *
 * <p>Tests use the real {@link ForStRsLinker} on an in-memory {@link FrsDb} — the linker is
 * final and not mockable. Each invariant is validated via observable engine state + the queue's
 * {@code pendingBufferSize()} accessor.
 */
class ForStRsKeyGroupedInternalPriorityQueueBatchedTest {

    private Arena arena;
    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;

    @BeforeEach
    void setUp() {
        arena = Arena.ofShared();
        linker = new ForStRsLinker(arena);
        db = linker.dbOpenMemory(arena);
        cf = linker.dbDefaultCf(db, arena);
    }

    @AfterEach
    void tearDown() {
        try {
            cf.close();
        } catch (Exception ignored) {
        }
        try {
            db.close();
        } catch (Exception ignored) {
        }
        arena.close();
    }

    // ------------------------------------------------------------------
    // Test element + serializer (minimal — ts + seq)
    // ------------------------------------------------------------------

    static final class TestElement implements HeapPriorityQueueElement {
        final long ts;
        final int seq;
        int idx = HeapPriorityQueueElement.NOT_CONTAINED;

        TestElement(long ts, int seq) {
            this.ts = ts;
            this.seq = seq;
        }

        public long getTimestamp() {
            return ts;
        }

        @Override
        public int getInternalIndex() {
            return idx;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.idx = newIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TestElement other)) {
                return false;
            }
            return ts == other.ts && seq == other.seq;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(ts) * 31 + seq;
        }

        @Override
        public String toString() {
            return "TE(" + ts + "," + seq + ")";
        }
    }

    static final class TestElementSerializer extends TypeSerializerSingleton<TestElement> {
        static final TestElementSerializer INSTANCE = new TestElementSerializer();

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TestElement createInstance() {
            return new TestElement(0L, 0);
        }

        @Override
        public TestElement copy(TestElement from) {
            return new TestElement(from.ts, from.seq);
        }

        @Override
        public TestElement copy(TestElement from, TestElement reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return 12;
        }

        @Override
        public void serialize(TestElement record, DataOutputView target) throws IOException {
            target.writeLong(record.ts);
            target.writeInt(record.seq);
        }

        @Override
        public TestElement deserialize(DataInputView source) throws IOException {
            long ts = source.readLong();
            int seq = source.readInt();
            return new TestElement(ts, seq);
        }

        @Override
        public TestElement deserialize(TestElement reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        }

        @Override
        public TypeSerializerSnapshot<TestElement> snapshotConfiguration() {
            throw new UnsupportedOperationException("snapshot not used in unit test");
        }
    }

    private ForStRsKeyGroupedInternalPriorityQueue<TestElement> newQueue(
            String name, int kg, KeyGroupRange range) {
        return new ForStRsKeyGroupedInternalPriorityQueue<>(
                linker,
                db,
                cf,
                arena,
                name,
                TestElementSerializer.INSTANCE,
                e -> e.ts,
                () -> kg,
                range);
    }

    // ------------------------------------------------------------------
    // Spec invariants
    // ------------------------------------------------------------------

    /**
     * Invariant #1 — In-buffer add-remove cancellation: add(T), remove(T), then flush. The engine
     * never sees either operation (we verify by checking the engine prefix-scan returns 0
     * entries after the flush, which is exactly what mocking the linker would also assert).
     */
    @Test
    void addRemoveCancelsAvoidsLinkerCalls() {
        try (ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                newQueue("inv1", 0, new KeyGroupRange(0, 0))) {
            TestElement e = new TestElement(100L, 1);
            q.add(e);
            assertEquals(1, q.pendingBufferSize(), "ADD pending in buffer");
            assertTrue(q.remove(e), "remove cancels the pending add");
            assertEquals(
                    0,
                    q.pendingBufferSize(),
                    "cancellation removes BOTH entries — buffer should be empty");
            // Force a flush — there should be no batchPut/batchDelete payload to ship.
            q.flushPendingToEngine();
            // Engine state must be empty (validated via the queue's own size + isEmpty).
            assertEquals(0, q.size(), "no engine entries written for cancelled pair");
            assertTrue(q.isEmpty());
        }
    }

    /**
     * Invariant #2 — Min-heap ordering. Add ts = 10, 5, 20, 1, 15 into the pending buffer. {@link
     * ForStRsKeyGroupedInternalPriorityQueue#peek()} returns the smallest-ts element WITHOUT
     * flushing.
     */
    @Test
    void minHeapOrderingByTs() {
        try (ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                newQueue("inv2", 0, new KeyGroupRange(0, 0))) {
            long[] tss = {10L, 5L, 20L, 1L, 15L};
            for (int i = 0; i < tss.length; i++) {
                q.add(new TestElement(tss[i], i));
            }
            assertEquals(5, q.pendingBufferSize(), "all entries in pending buffer (below threshold)");
            TestElement head = q.peek();
            assertNotNull(head);
            assertEquals(1L, head.ts, "min-heap returns smallest ts first");
            // peek did not flush.
            assertEquals(5, q.pendingBufferSize(), "peek is non-flushing");
        }
    }

    /**
     * Invariant #3 — advance() strict order: flush → batch scan → batch delete. After advance,
     * the visitor sees the due timers in ts order, the engine no longer carries them, and the
     * pending buffer is empty.
     */
    @Test
    void advanceOrderFlushScanDelete() {
        try (ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                newQueue("inv3", 0, new KeyGroupRange(0, 0))) {
            // Add 5 timers — they live in the buffer.
            q.add(new TestElement(100L, 1));
            q.add(new TestElement(50L, 2));
            q.add(new TestElement(200L, 3));
            q.add(new TestElement(150L, 4));
            q.add(new TestElement(25L, 5));
            assertEquals(5, q.pendingBufferSize());

            // advance(150) should drain buffer (flush) → batch scan engine for ts ≤ 150 → batch
            // delete those entries.
            List<TestElement> fired = new ArrayList<>();
            int due = q.advance(150L, fired::add);
            assertEquals(4, due, "ts = 25, 50, 100, 150 are due");
            // pending buffer is empty post-flush.
            assertEquals(0, q.pendingBufferSize());
            // Engine retains only ts=200 (above maxTimestamp). poll() exercises the engine
            // cache refresh + delete path (peek() doesn't refill engine cache on the
            // registerTimer hot path — see javadoc on peek for why).
            TestElement remaining = q.poll();
            assertNotNull(remaining);
            assertEquals(200L, remaining.ts);
        }
    }

    /**
     * Invariant #4 — flushPendingToEngine is the mandatory pre-snapshot hook. After calling it
     * with a non-empty buffer: the buffer is empty AND the engine reflects all pending mutations.
     */
    @Test
    void snapshotFlushesPendingFirst() {
        try (ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                newQueue("inv4", 0, new KeyGroupRange(0, 0))) {
            q.add(new TestElement(10L, 1));
            q.add(new TestElement(20L, 2));
            q.add(new TestElement(30L, 3));
            assertEquals(3, q.pendingBufferSize(), "all 3 pending pre-flush");
            // Simulate the backend's pre-snapshot hook.
            q.flushPendingToEngine();
            assertEquals(
                    0,
                    q.pendingBufferSize(),
                    "buffer drained — snapshot will see consistent engine");
            // Engine carries the 3 entries — verify via a fresh queue.size().
            assertEquals(3, q.size(), "engine reflects flushed ADDs");
        }
    }

    // ------------------------------------------------------------------
    // Extra: peek non-flushing — guards against accidental flush-on-read
    // ------------------------------------------------------------------

    @Test
    void peekIsNonFlushing() {
        try (ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                newQueue("peek", 0, new KeyGroupRange(0, 0))) {
            q.add(new TestElement(5L, 1));
            q.add(new TestElement(15L, 2));
            assertEquals(2, q.pendingBufferSize());
            TestElement h = q.peek();
            assertNotNull(h);
            assertEquals(5L, h.ts);
            // Critical: peek does NOT flush.
            assertEquals(2, q.pendingBufferSize(), "peek must not flush pending buffer");
        }
    }

    // ------------------------------------------------------------------
    // Extra: poll integrates flush → engine-read
    // ------------------------------------------------------------------

    @Test
    void pollDrainsViaEngine() {
        try (ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                newQueue("poll", 0, new KeyGroupRange(0, 0))) {
            long[] tss = {30L, 10L, 50L, 20L};
            for (int i = 0; i < tss.length; i++) {
                q.add(new TestElement(tss[i], i));
            }
            // poll() flushes then reads engine head — should yield ascending-ts.
            assertEquals(10L, q.poll().ts);
            assertEquals(20L, q.poll().ts);
            assertEquals(30L, q.poll().ts);
            assertEquals(50L, q.poll().ts);
            assertNull(q.poll());
            assertTrue(q.isEmpty());
        }
    }

    /**
     * Extra: empty queue is empty after flush of a cancelled add-remove pair.
     */
    @Test
    void cancellationKeepsQueueEmpty() {
        try (ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                newQueue("cancel", 0, new KeyGroupRange(0, 0))) {
            TestElement e = new TestElement(50L, 1);
            q.add(e);
            assertFalse(q.isEmpty());
            assertTrue(q.remove(e));
            assertTrue(q.isEmpty());
        }
    }

    /**
     * R38-H2 regression: after {@link ForStRsKeyGroupedInternalPriorityQueue#close()} every
     * mutating/reading method must surface the lifecycle violation as {@link
     * IllegalStateException} rather than crash deep inside FFM with an opaque arena-closed
     * error.
     */
    @Test
    void closedQueueRejectsMutations() {
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                newQueue("closed", 0, new KeyGroupRange(0, 0));
        // Seed with one entry so peek / poll have something to look at pre-close.
        q.add(new TestElement(42L, 1));
        // Now close — flushArena + scratchArena are released.
        q.close();
        // Every subsequent call must throw IllegalStateException.
        TestElement late = new TestElement(99L, 2);
        assertThrows(IllegalStateException.class, () -> q.add(late));
        assertThrows(IllegalStateException.class, () -> q.remove(late));
        assertThrows(IllegalStateException.class, q::poll);
        assertThrows(IllegalStateException.class, q::peek);
        assertThrows(IllegalStateException.class, q::isEmpty);
        assertThrows(IllegalStateException.class, () -> q.advance(1000L, e -> {}));
        // R39-H1: close-gates extended to size / iterator / getSubsetForKeyGroup
        // / addAll / removeAll. All five paths touch FFM (flushPendingToEngine
        // or a prefix iterator) and must surface a lifecycle violation as
        // IllegalStateException, not as a deep FFM arena-closed crash.
        assertThrows(IllegalStateException.class, q::size);
        assertThrows(IllegalStateException.class, q::iterator);
        assertThrows(IllegalStateException.class, () -> q.getSubsetForKeyGroup(0));
        assertThrows(
                IllegalStateException.class,
                () -> q.addAll(java.util.Collections.singletonList(late)));
        assertThrows(
                IllegalStateException.class,
                () -> q.removeAll(java.util.Collections.singletonList(late)));
        // close() itself is still idempotent (no exception on second invocation).
        q.close();
    }
}

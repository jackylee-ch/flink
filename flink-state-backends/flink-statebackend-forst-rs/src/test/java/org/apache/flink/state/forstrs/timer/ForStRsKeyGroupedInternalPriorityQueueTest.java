/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the elementCountOTICE file
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

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ForStRsKeyGroupedInternalPriorityQueue}.
 *
 * <p>Covers (10 cases): basic add/poll, peek-without-pop, FIFO at same timestamp, min-heap order
 * across timestamps, sign-flipped negative timestamps, per-key-group isolation, removeAll, size +
 * isEmpty, getSubsetForKeyGroup, and full-range iterator.
 */
class ForStRsKeyGroupedInternalPriorityQueueTest {

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
    // Test helpers
    // ------------------------------------------------------------------

    /** Minimal queue element: timestamp + sequence (FIFO disambiguator at same ts). */
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

    /** TypeSerializer for TestElement: 8B ts + 4B seq. */
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
        public org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<TestElement>
                snapshotConfiguration() {
            throw new UnsupportedOperationException("snapshot not used in unit test");
        }
    }

    private ForStRsKeyGroupedInternalPriorityQueue<TestElement> newQueue(
            String name, int currentKg, KeyGroupRange range) {
        return new ForStRsKeyGroupedInternalPriorityQueue<>(
                linker,
                db,
                cf,
                arena,
                name,
                TestElementSerializer.INSTANCE,
                e -> e.ts,
                () -> currentKg,
                range);
    }

    private ForStRsKeyGroupedInternalPriorityQueue<TestElement> newQueueWithSwitchableKg(
            String name, AtomicInteger currentKg, KeyGroupRange range) {
        return new ForStRsKeyGroupedInternalPriorityQueue<>(
                linker,
                db,
                cf,
                arena,
                name,
                TestElementSerializer.INSTANCE,
                e -> e.ts,
                currentKg::get,
                range);
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /**
     * Micro-bench (NOT a regression test — measurement only). Adds elementCount timers into a single
     * key-group then drains them via {@code poll()}. Prints throughput so SP3's poll-ahead cache
     * gain can be compared against a baseline build (stash the cache changes, rerun this test).
     */
    @Test
    void pollThroughputMicrobench() {
        final int elementCount = 50_000;
        var q = newQueue("bench", 0, new KeyGroupRange(0, 0));
        long t0 = System.nanoTime();
        for (int i = 0; i < elementCount; i++) {
            q.add(new TestElement(i, i));
        }
        long t1 = System.nanoTime();
        int polled = 0;
        TestElement e;
        while ((e = q.poll()) != null) {
            polled++;
        }
        long t2 = System.nanoTime();
        long addNs = t1 - t0;
        long pollNs = t2 - t1;
        double addThru = (double) elementCount * 1e9 / addNs;
        double pollThru = (double) polled * 1e9 / pollNs;
        System.out.printf(
                "MICROBENCH elementCount=%d add_throughput=%.0f ops/s poll_throughput=%.0f ops/s "
                        + "add_ns=%d poll_ns=%d%n",
                elementCount, addThru, pollThru, addNs, pollNs);
        assertEquals(elementCount, polled);
    }

    /** 1. Add then poll returns the same element. */
    @Test
    void addThenPollReturnsElement() {
        var q = newQueue("q1", 0, new KeyGroupRange(0, 0));
        TestElement e = new TestElement(100L, 1);
        assertTrue(q.add(e));
        assertEquals(1, q.size());
        TestElement out = q.poll();
        assertEquals(e, out);
        assertNull(q.poll());
        assertTrue(q.isEmpty());
    }

    /** 2. Peek does not remove. */
    @Test
    void peekDoesNotRemove() {
        var q = newQueue("q2", 0, new KeyGroupRange(0, 0));
        q.add(new TestElement(50L, 1));
        TestElement p1 = q.peek();
        TestElement p2 = q.peek();
        assertEquals(p1, p2);
        assertEquals(1, q.size());
    }

    /** 3. FIFO order at the same timestamp via the seq disambiguator. */
    @Test
    void fifoOrderAtSameTimestamp() {
        var q = newQueue("q3", 0, new KeyGroupRange(0, 0));
        for (int i = 0; i < 5; i++) {
            q.add(new TestElement(42L, i));
        }
        for (int i = 0; i < 5; i++) {
            TestElement out = q.poll();
            assertNotNull(out);
            assertEquals(42L, out.ts);
            assertEquals(i, out.seq);
        }
        assertNull(q.poll());
    }

    /** 4. Min-heap order across distinct timestamps. */
    @Test
    void minHeapOrderAcrossTimestamps() {
        var q = newQueue("q4", 0, new KeyGroupRange(0, 0));
        long[] tss = {500L, 100L, 300L, 50L, 1000L};
        for (long ts : tss) {
            q.add(new TestElement(ts, 0));
        }
        long[] sorted = tss.clone();
        Arrays.sort(sorted);
        for (long expected : sorted) {
            TestElement out = q.poll();
            assertNotNull(out);
            assertEquals(expected, out.ts);
        }
    }

    /** 5. elementCountegative timestamps are ordered correctly via sign-flip. */
    @Test
    void negativeTimestampsHandledViaSignFlip() {
        var q = newQueue("q5", 0, new KeyGroupRange(0, 0));
        long[] tss = {Long.MAX_VALUE, -1L, 0L, Long.MIN_VALUE, 100L, -100L};
        for (long ts : tss) {
            q.add(new TestElement(ts, 0));
        }
        long[] sorted = tss.clone();
        Arrays.sort(sorted);
        for (long expected : sorted) {
            TestElement out = q.poll();
            assertNotNull(out);
            assertEquals(expected, out.ts);
        }
        assertNull(q.poll());
    }

    /** 6. Per-key-group isolation: writes under kg=A are not visible to a kg=B head scan. */
    @Test
    void perKeyGroupIsolation() {
        AtomicInteger kg = new AtomicInteger(7);
        var q = newQueueWithSwitchableKg("q6", kg, new KeyGroupRange(0, 15));
        // Write 3 entries into kg=7
        kg.set(7);
        q.add(new TestElement(10L, 0));
        q.add(new TestElement(20L, 0));
        q.add(new TestElement(30L, 0));
        // Switch current kg to 9; head should be empty for that kg
        kg.set(9);
        assertNull(q.peek());
        q.add(new TestElement(5L, 0));
        // From kg=9, the only entry is ts=5
        TestElement out9 = q.poll();
        assertNotNull(out9);
        assertEquals(5L, out9.ts);
        // Switch back to kg=7; should still see the original 3
        kg.set(7);
        assertEquals(10L, q.poll().ts);
        assertEquals(20L, q.poll().ts);
        assertEquals(30L, q.poll().ts);
    }

    /** 7. removeAll deletes only the supplied elements and reports the actual deletion count. */
    @Test
    void removeAllDeletesOnlyRequestedElements() {
        var q = newQueue("q7", 0, new KeyGroupRange(0, 0));
        TestElement a = new TestElement(10L, 1);
        TestElement b = new TestElement(20L, 2);
        TestElement c = new TestElement(30L, 3);
        TestElement notPresent = new TestElement(99L, 99);
        q.add(a);
        q.add(b);
        q.add(c);
        int removed = q.removeAll(Arrays.asList(b, notPresent));
        assertEquals(1, removed);
        // a and c should remain in min-order
        assertEquals(a, q.poll());
        assertEquals(c, q.poll());
        assertNull(q.poll());
    }

    /** 8. size() and isEmpty() report accurate counts across the configured range. */
    @Test
    void sizeAndIsEmptyReportAccurately() {
        AtomicInteger kg = new AtomicInteger(0);
        var q = newQueueWithSwitchableKg("q8", kg, new KeyGroupRange(0, 3));
        assertTrue(q.isEmpty());
        assertEquals(0, q.size());
        kg.set(0);
        q.add(new TestElement(1L, 0));
        kg.set(2);
        q.add(new TestElement(2L, 0));
        q.add(new TestElement(3L, 0));
        assertFalse(q.isEmpty());
        assertEquals(3, q.size());
    }

    /** 9. getSubsetForKeyGroup returns elements scoped to the requested key-group only. */
    @Test
    void getSubsetForKeyGroupIsScoped() {
        AtomicInteger kg = new AtomicInteger(0);
        var q = newQueueWithSwitchableKg("q9", kg, new KeyGroupRange(0, 3));
        kg.set(0);
        q.add(new TestElement(10L, 0));
        q.add(new TestElement(20L, 0));
        kg.set(1);
        q.add(new TestElement(30L, 0));
        q.add(new TestElement(40L, 0));
        Set<TestElement> kg0 = q.getSubsetForKeyGroup(0);
        assertEquals(2, kg0.size());
        for (TestElement e : kg0) {
            assertTrue(e.ts == 10L || e.ts == 20L);
        }
        Set<TestElement> kg1 = q.getSubsetForKeyGroup(1);
        assertEquals(2, kg1.size());
        for (TestElement e : kg1) {
            assertTrue(e.ts == 30L || e.ts == 40L);
        }
        // Empty kg returns empty set
        assertTrue(q.getSubsetForKeyGroup(2).isEmpty());
    }

    /** 10. Iterator walks every element across the configured key-group range. */
    @Test
    void iteratorWalksEveryKeyGroup() throws Exception {
        AtomicInteger kg = new AtomicInteger(0);
        var q = newQueueWithSwitchableKg("q10", kg, new KeyGroupRange(0, 2));
        kg.set(0);
        q.add(new TestElement(1L, 0));
        kg.set(1);
        q.add(new TestElement(2L, 0));
        kg.set(2);
        q.add(new TestElement(3L, 0));
        q.add(new TestElement(4L, 0));

        List<TestElement> seen = new ArrayList<>();
        try (CloseableIterator<TestElement> it = q.iterator()) {
            while (it.hasNext()) {
                seen.add(it.next());
            }
        }
        assertEquals(4, seen.size());
        // ts values should appear at least once each
        long[] tss = seen.stream().mapToLong(e -> e.ts).sorted().toArray();
        assertEquals(1L, tss[0]);
        assertEquals(2L, tss[1]);
        assertEquals(3L, tss[2]);
        assertEquals(4L, tss[3]);

        // addAll covers Collections branch
        kg.set(0);
        q.addAll(Collections.singletonList(new TestElement(99L, 0)));
        assertEquals(5, q.size());
    }
}

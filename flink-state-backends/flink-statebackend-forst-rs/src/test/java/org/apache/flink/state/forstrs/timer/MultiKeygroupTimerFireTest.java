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

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-A4 / S1-5 regression test for {@code ForStRsKeyGroupedInternalPriorityQueue} —
 * verifies that {@code peek()/poll()/advance()} route to the <em>current key</em>'s key group
 * (instead of a constant {@code keyGroupRange.getStartKeyGroup()}), so timers registered under
 * multiple keys all fire when the operator advances its watermark.
 *
 * <p>Setup mirrors a parallelism-2 deployment: total key groups = 128, this shard owns
 * key groups [0, 63]. We pick 3 keys that hash to 3 distinct key groups inside that range,
 * register one timer per key, advance the queue under each key, and verify all 3 timers fire.
 *
 * <p>Without PR-A4 (constant supplier returning {@code 0}), only the timer whose key hashes
 * to key group 0 would fire — the other two would be invisible to peek/poll/advance.
 */
class MultiKeygroupTimerFireTest {

    private static final int TOTAL_KEY_GROUPS = 128;
    private static final int SHARD_START = 0;
    private static final int SHARD_END = 63;

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

    /**
     * Picks {@code n} distinct integer keys whose hash assigns each to a distinct key group
     * inside {@code [SHARD_START, SHARD_END]}.
     */
    private static List<Integer> pickKeysInDistinctKeyGroups(int n) {
        List<Integer> keys = new ArrayList<>();
        Set<Integer> usedKgs = new HashSet<>();
        int candidate = 1;
        while (keys.size() < n && candidate < 10_000_000) {
            int kg = KeyGroupRangeAssignment.assignToKeyGroup(candidate, TOTAL_KEY_GROUPS);
            if (kg >= SHARD_START && kg <= SHARD_END && usedKgs.add(kg)) {
                keys.add(candidate);
            }
            candidate++;
        }
        if (keys.size() < n) {
            throw new IllegalStateException(
                    "Could not find " + n + " keys in distinct key groups within shard");
        }
        return keys;
    }

    /**
     * The fix in action: 3 keys hash to 3 distinct key groups in the shard range; one timer is
     * registered per key; the queue is advanced once per key and the visitor receives all 3
     * timers. Validates PR-A4 — without the fix the queue would only see the kg-0 timer.
     */
    @Test
    void timersFireAcrossMultipleKeygroupsWithinShard() {
        List<Integer> keys = pickKeysInDistinctKeyGroups(3);
        // Sanity check: keys are in 3 distinct kgs inside the shard range.
        Set<Integer> kgs = new HashSet<>();
        for (Integer k : keys) {
            kgs.add(KeyGroupRangeAssignment.assignToKeyGroup(k, TOTAL_KEY_GROUPS));
        }
        assertEquals(3, kgs.size(), "test setup: 3 keys must produce 3 distinct kgs");
        for (Integer kg : kgs) {
            assertTrue(kg >= SHARD_START && kg <= SHARD_END);
        }

        MutableKeyContext<Integer> ctx = new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                new ForStRsKeyGroupedInternalPriorityQueue<>(
                        linker,
                        db,
                        cf,
                        arena,
                        "mkg",
                        TestElementSerializer.INSTANCE,
                        e -> e.ts,
                        ctx,
                        TOTAL_KEY_GROUPS,
                        new KeyGroupRange(SHARD_START, SHARD_END));

        // Register one timer per key under that key's current-context.
        for (int i = 0; i < keys.size(); i++) {
            Integer k = keys.get(i);
            ctx.setCurrentKey(k);
            assertTrue(q.add(new TestElement(100L + i, i)));
        }
        // Force the buffer to flush so subsequent advance() pulls from engine state.
        q.flushPendingToEngine();

        // Advance with a watermark covering all timers, one key group at a time.
        Set<Long> firedTs = new HashSet<>();
        for (Integer k : keys) {
            ctx.setCurrentKey(k);
            int fired = q.advance(Long.MAX_VALUE, t -> firedTs.add(t.ts));
            assertNotEquals(0, fired, "kg " + KeyGroupRangeAssignment.assignToKeyGroup(k, TOTAL_KEY_GROUPS)
                    + " (key=" + k + ") should fire >0 timers");
        }
        assertEquals(3, firedTs.size(), "all 3 timers across 3 distinct kgs must fire");
        // After draining, the queue should be empty.
        assertTrue(q.isEmpty());
        q.close();
    }

    /**
     * Null-currentKey edge case: before any record arrives, peek/poll fall back to {@code
     * keyGroupRange.getStartKeyGroup()} (same as legacy constant supplier). The queue does not
     * NPE and returns {@code null}/no-op cleanly.
     */
    @Test
    void nullCurrentKeyFallsBackToStartKeyGroup() {
        MutableKeyContext<Integer> ctx = new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        // Leave currentKey == null.
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                new ForStRsKeyGroupedInternalPriorityQueue<>(
                        linker,
                        db,
                        cf,
                        arena,
                        "null-kg",
                        TestElementSerializer.INSTANCE,
                        e -> e.ts,
                        ctx,
                        TOTAL_KEY_GROUPS,
                        new KeyGroupRange(SHARD_START, SHARD_END));
        // peek/poll on empty queue with null currentKey must not throw.
        assertEquals(null, q.peek());
        assertEquals(null, q.poll());
        q.close();
    }

    /**
     * Backward-compat smoke test for the deprecated IntSupplier ctor: a constant supplier still
     * works (used by the legacy test suite). This guards against an accidental ABI break.
     */
    @Test
    void deprecatedIntSupplierCtorStillCompiles() {
        @SuppressWarnings("deprecation")
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                new ForStRsKeyGroupedInternalPriorityQueue<>(
                        linker,
                        db,
                        cf,
                        arena,
                        "legacy",
                        TestElementSerializer.INSTANCE,
                        e -> e.ts,
                        () -> SHARD_START,
                        new KeyGroupRange(SHARD_START, SHARD_END));
        // Constant supplier only sees kg=SHARD_START; this is the legacy (buggy) behaviour we
        // preserve for backward compat.
        assertTrue(q.add(new TestElement(1L, 0)));
        assertFalse(q.isEmpty());
        q.close();
    }

    // ------------------------------------------------------------------
    // Test helpers (mirror ForStRsKeyGroupedInternalPriorityQueueTest)
    // ------------------------------------------------------------------

    static final class TestElement implements HeapPriorityQueueElement {
        final long ts;
        final int seq;
        int idx = HeapPriorityQueueElement.NOT_CONTAINED;

        TestElement(long ts, int seq) {
            this.ts = ts;
            this.seq = seq;
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

    /** Minimal {@link InternalKeyContext} that lets the test set the current key directly. */
    static final class MutableKeyContext<K> implements InternalKeyContext<K> {
        private final KeyGroupRange range;
        private final int totalKeyGroups;
        private K currentKey;
        private int currentKeyGroup = -1;

        MutableKeyContext(KeyGroupRange range, int totalKeyGroups) {
            this.range = range;
            this.totalKeyGroups = totalKeyGroups;
        }

        @Override
        public K getCurrentKey() {
            return currentKey;
        }

        @Override
        public int getCurrentKeyGroupIndex() {
            return currentKeyGroup;
        }

        @Override
        public int getNumberOfKeyGroups() {
            return totalKeyGroups;
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return range;
        }

        @Override
        public void setCurrentKey(@Nonnull K newKey) {
            this.currentKey = newKey;
            this.currentKeyGroup =
                    KeyGroupRangeAssignment.assignToKeyGroup(newKey, totalKeyGroups);
        }

        @Override
        public void setCurrentKeyGroupIndex(int newKeyGroupIndex) {
            this.currentKeyGroup = newKeyGroupIndex;
        }
    }
}

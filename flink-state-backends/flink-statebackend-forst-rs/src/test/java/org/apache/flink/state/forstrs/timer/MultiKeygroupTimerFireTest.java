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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    /**
     * STAGE-0 regression (2026-06-11, q8 timer loss): the post-flush one-shot refill FLOOR
     * must only ever LOWER the effective refill start. Pre-fix, a second flush touching the
     * same key group before its next refill OVERWROTE a lower outstanding floor with a higher
     * one (cache empty → headTs=MAX → floor=minAdd2), permanently skipping every engine timer
     * in [floor1, floor2): registered-but-never-polled timers = unfired windows = the q8
     * GlobalWindowAggregate under-emit (g7Add=2,000,000 vs g7Poll=1,055,800).
     *
     * <p>Sequence: 3 timers flushed; one poll caches the rest and positions the resume cursor;
     * an add INSIDE the cached window flushes (floor=150, cache dropped); an add BEYOND the old
     * tail flushes again (pre-fix: floor overwritten to 400 → 150/200/300 lost). All four
     * remaining timers must poll in timestamp order.
     */
    @Test
    void refillFloorNeverRisesAcrossConsecutiveFlushes() {
        List<Integer> keys = pickKeysInDistinctKeyGroups(1);
        Integer k = keys.get(0);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q =
                new ForStRsKeyGroupedInternalPriorityQueue<>(
                        linker,
                        db,
                        cf,
                        arena,
                        "floorRegression",
                        TestElementSerializer.INSTANCE,
                        e -> e.ts,
                        ctx,
                        TOTAL_KEY_GROUPS,
                        new KeyGroupRange(SHARD_START, SHARD_END));
        ctx.setCurrentKey(k);
        assertTrue(q.add(new TestElement(100L, 0)));
        assertTrue(q.add(new TestElement(200L, 1)));
        assertTrue(q.add(new TestElement(300L, 2)));
        q.flushPendingToEngine();
        // Poll once: fires ts=100, caches [200, 300], resume cursor at the batch tail.
        TestElement first = q.poll();
        assertNotNull(first);
        assertEquals(100L, first.ts);
        // Flush #1: add INSIDE the cached window → cache dropped, floor := min(200, 150) = 150.
        assertTrue(q.add(new TestElement(150L, 3)));
        q.flushPendingToEngine();
        // Flush #2: add BEYOND the old tail with the cache now empty. Pre-fix this OVERWROTE
        // the 150 floor with 400, orphaning 150/200/300 in the engine forever.
        assertTrue(q.add(new TestElement(400L, 4)));
        q.flushPendingToEngine();
        // Drain: every remaining timer must surface, in ts order.
        List<Long> polled = new ArrayList<>();
        TestElement e;
        while ((e = q.poll()) != null) {
            polled.add(e.ts);
        }
        assertEquals(Arrays.asList(150L, 200L, 300L, 400L), polled);
    }

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

    private ForStRsKeyGroupedInternalPriorityQueue<TestElement> newMkgQueue(
            MutableKeyContext<Integer> ctx, String name) {
        return new ForStRsKeyGroupedInternalPriorityQueue<>(
                linker, db, cf, arena, name,
                TestElementSerializer.INSTANCE, e -> e.ts, ctx,
                TOTAL_KEY_GROUPS, new KeyGroupRange(SHARD_START, SHARD_END));
    }

    /**
     * E-PERF regression: with timers in ONE of the 64 owned key groups, poll() must NOT re-open
     * a prefix iterator for the 63 EMPTY key groups on every call. Pre-fix every poll re-opened
     * all empty kgs (≈ polls × 63 ≈ 9450 iterator opens) — the CPU spin that stalled q4/q5/q7.
     * After the fix, empty kgs are detected once and skipped → refills bounded by ≈ numKgs.
     */
    @Test
    void pollDoesNotReopenEmptyKeyGroupsEveryCall() {
        Integer key = pickKeysInDistinctKeyGroups(1).get(0);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q = newMkgQueue(ctx, "perf");
        ctx.setCurrentKey(key);
        final int numTimers = 200;
        for (int i = 0; i < numTimers; i++) {
            assertTrue(q.add(new TestElement(1L + i, i)));
        }
        q.flushPendingToEngine();
        q.multiKgRefillCount = 0; // isolate the poll-loop cost from setup

        final int polls = 150;
        long prevTs = Long.MIN_VALUE;
        for (int i = 0; i < polls; i++) {
            TestElement e = q.poll();
            assertNotEquals(null, e, "poll " + i + " must return a timer");
            assertTrue(e.ts > prevTs, "timers must fire in ascending ts order");
            prevTs = e.ts;
        }
        int numKgs = SHARD_END - SHARD_START + 1; // 64
        assertTrue(
                q.multiKgRefillCount < 2 * numKgs,
                "poll() must NOT re-open empty-kg prefix iterators every call; observed refills="
                        + q.multiKgRefillCount
                        + " (fixed bound < "
                        + (2 * numKgs)
                        + "; pre-fix would be ≈ "
                        + (polls * (numKgs - 1))
                        + ")");
        q.close();
    }

    /**
     * E-PERF correctness: a timer ADDED to a previously-empty (exhausted) key group MUST still
     * fire — the exhaustion-skip must be cleared on add, else the new timer is silently skipped.
     */
    @Test
    void addToPreviouslyExhaustedKeyGroupStillFires() {
        List<Integer> keys = pickKeysInDistinctKeyGroups(2);
        Integer keyA = keys.get(0);
        Integer keyB = keys.get(1);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q = newMkgQueue(ctx, "unexhaust");
        // Put a few timers in kg(A), drain them ALL — now every kg in range is exhausted.
        ctx.setCurrentKey(keyA);
        for (int i = 0; i < 3; i++) {
            assertTrue(q.add(new TestElement(10L + i, i)));
        }
        q.flushPendingToEngine();
        while (q.poll() != null) {
            // drain
        }
        assertTrue(q.isEmpty(), "all kg(A) timers drained");
        // Now add a timer to a DIFFERENT (previously-empty/exhausted) kg(B).
        ctx.setCurrentKey(keyB);
        assertTrue(q.add(new TestElement(999L, 0)));
        q.flushPendingToEngine();
        // The fix must un-exhaust kg(B) so poll sees it.
        TestElement fired = q.poll();
        assertNotEquals(null, fired, "timer added to a previously-exhausted kg MUST still fire");
        assertEquals(999L, fired.ts);
        q.close();
    }

    /**
     * E-PERF correctness: poll() returns the GLOBAL minimum-ts timer across key groups even as
     * individual kgs exhaust and get skipped — interleaved timestamps across 3 kgs come out
     * strictly ascending.
     */
    @Test
    void pollReturnsGlobalMinAcrossKeyGroupsWithExhaustionSkip() {
        List<Integer> keys = pickKeysInDistinctKeyGroups(3);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q = newMkgQueue(ctx, "globalmin");
        // Interleave timestamps across the 3 kgs: kgA gets 1,4,7; kgB gets 2,5,8; kgC gets 3,6,9.
        for (int i = 0; i < 3; i++) {
            ctx.setCurrentKey(keys.get(i));
            for (int j = 0; j < 3; j++) {
                long ts = 1L + i + 3L * j; // i in {0,1,2}, j in {0,1,2}
                assertTrue(q.add(new TestElement(ts, (int) ts)));
            }
        }
        q.flushPendingToEngine();
        long prev = Long.MIN_VALUE;
        int count = 0;
        TestElement e;
        while ((e = q.poll()) != null) {
            assertTrue(e.ts > prev, "global-min order violated: " + e.ts + " after " + prev);
            prev = e.ts;
            count++;
        }
        assertEquals(9, count, "all 9 timers across 3 kgs must fire");
        assertEquals(9L, prev, "last fired must be the max ts");
        q.close();
    }

    /**
     * E-PERF correctness: peek() must see a PENDING (un-flushed) timer added to a kg whose engine
     * prefix is exhausted — peekMultiKg merges the pending buffer with engine heads, so the
     * exhaustion-skip (which only affects the engine scan) must not hide a buffered timer.
     */
    @Test
    void peekSeesPendingTimerInExhaustedKeyGroup() {
        List<Integer> keys = pickKeysInDistinctKeyGroups(2);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q = newMkgQueue(ctx, "peekpending");
        // Drain kg(A) → all kgs become exhausted.
        ctx.setCurrentKey(keys.get(0));
        assertTrue(q.add(new TestElement(5L, 0)));
        q.flushPendingToEngine();
        assertEquals(5L, q.poll().ts);
        assertTrue(q.isEmpty());
        // Add a PENDING (un-flushed) timer to kg(B); peek WITHOUT flushing.
        ctx.setCurrentKey(keys.get(1));
        assertTrue(q.add(new TestElement(42L, 0)));
        TestElement p = q.peek();
        assertNotEquals(null, p, "peek must see a pending timer even in an exhausted kg");
        assertEquals(42L, p.ts);
        assertEquals(42L, q.poll().ts, "poll fires the pending timer");
        q.close();
    }

    /**
     * E-PERF correctness: interleaving add() and poll() across kgs (the steady-state regime where
     * each poll flushes pending and invalidates the deque cache) still yields strict global-min
     * order with every timer fired exactly once, and refills stay far below the pre-fix
     * O(polls × emptyKgs) blowup.
     */
    @Test
    void interleavedAddPollMaintainsGlobalOrder() {
        List<Integer> keys = pickKeysInDistinctKeyGroups(3);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q = newMkgQueue(ctx, "interleave");
        q.multiKgRefillCount = 0;
        long prev = Long.MIN_VALUE;
        int fired = 0;
        long tsSeq = 1;
        for (int round = 0; round < 20; round++) {
            for (int i = 0; i < 3; i++) {
                ctx.setCurrentKey(keys.get(i));
                assertTrue(q.add(new TestElement(tsSeq++, 0)));
            }
            for (int p = 0; p < 2; p++) {
                TestElement e = q.poll();
                assertNotEquals(null, e);
                assertTrue(e.ts > prev, "interleaved global-min order: " + e.ts + " after " + prev);
                prev = e.ts;
                fired++;
            }
        }
        TestElement e;
        while ((e = q.poll()) != null) {
            assertTrue(e.ts > prev, "drain order: " + e.ts + " after " + prev);
            prev = e.ts;
            fired++;
        }
        assertEquals(60, fired, "all 60 timers fire exactly once in ascending order");
        // Pre-fix this interleave would be ≈ 60 polls × 61 empty kgs ≈ 3660 refills.
        assertTrue(
                q.multiKgRefillCount < 1000,
                "interleaved refills must stay bounded; observed=" + q.multiKgRefillCount);
        q.close();
    }

    /**
     * E-PERF regression for the q5 chained-window pattern: an operator fires near-future timers
     * (poll) while registering FAR-future timers (add) on the same poll — exactly what
     * GlobalWindowAggregate→LocalWindowAggregate does during advanceWatermark. A far-future add
     * (beyond the cached tail) lands in the engine tail and is read on the next refill, so it must
     * NOT invalidate the cached prefix / trigger a per-poll prefix-iterator re-open.
     */
    @Test
    void farFutureAddsWhileFiringDoNotReopen() {
        Integer key = pickKeysInDistinctKeyGroups(1).get(0);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q = newMkgQueue(ctx, "chainadd");
        ctx.setCurrentKey(key);
        // Warm: near-future timers ts 1..200 (the ones being fired).
        for (int i = 1; i <= 200; i++) {
            assertTrue(q.add(new TestElement(i, i)));
        }
        q.flushPendingToEngine();
        q.multiKgRefillCount = 0;

        long far = 1_000_000L;
        long prev = Long.MIN_VALUE;
        for (int i = 0; i < 150; i++) {
            TestElement e = q.poll(); // consume a near-future timer
            assertNotEquals(null, e);
            assertTrue(e.ts > prev, "near-future timers fire ascending");
            prev = e.ts;
            ctx.setCurrentKey(key);
            assertTrue(q.add(new TestElement(far++, 0))); // register a FAR-future timer
        }
        int numKgs = SHARD_END - SHARD_START + 1;
        assertTrue(
                q.multiKgRefillCount < 2 * numKgs,
                "far-future adds while firing must NOT re-open the prefix iterator every poll;"
                        + " observed refills="
                        + q.multiKgRefillCount
                        + " (bound < "
                        + (2 * numKgs)
                        + ")");
        q.close();
    }

    /**
     * Correctness: timers spread across MANY distinct key groups (the keyed-join pattern, q4/q8)
     * all fire in strict global-ts order with bounded refills.
     */
    @Test
    void manyActiveKeyGroupsFireInGlobalOrder() {
        List<Integer> keys = pickKeysInDistinctKeyGroups(20);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q = newMkgQueue(ctx, "manykg");
        // Interleave: kg i gets timers ts = i, i+20, i+40, i+60, i+80 (5 each → 100 total).
        for (int i = 0; i < keys.size(); i++) {
            ctx.setCurrentKey(keys.get(i));
            for (int j = 0; j < 5; j++) {
                long ts = 1L + i + 20L * j;
                assertTrue(q.add(new TestElement(ts, (int) ts)));
            }
        }
        q.flushPendingToEngine();
        q.multiKgRefillCount = 0;
        long prev = Long.MIN_VALUE;
        int count = 0;
        TestElement e;
        while ((e = q.poll()) != null) {
            assertTrue(e.ts > prev, "global-min order across 20 kgs: " + e.ts + " after " + prev);
            prev = e.ts;
            count++;
        }
        assertEquals(100, count, "all 100 timers across 20 kgs fire exactly once");
        // 20 active kgs warm once + the empties skipped; far below per-poll re-open.
        assertTrue(q.multiKgRefillCount < 5 * 64, "refills bounded; observed=" + q.multiKgRefillCount);
        q.close();
    }

    /**
     * Correctness: an add that falls WITHIN the cached window (ts <= cached tail) must fire at its
     * correct position — this is the invalidation path the beyond-tail optimization must NOT skip.
     */
    @Test
    void inWindowAddFiresAtCorrectPosition() {
        Integer key = pickKeysInDistinctKeyGroups(1).get(0);
        MutableKeyContext<Integer> ctx =
                new MutableKeyContext<>(new KeyGroupRange(SHARD_START, SHARD_END), TOTAL_KEY_GROUPS);
        ForStRsKeyGroupedInternalPriorityQueue<TestElement> q = newMkgQueue(ctx, "inwindow");
        ctx.setCurrentKey(key);
        for (long ts : new long[] {10L, 20L, 30L}) {
            assertTrue(q.add(new TestElement(ts, (int) ts)));
        }
        q.flushPendingToEngine();
        assertEquals(10L, q.poll().ts); // warms+consumes head; cache now [20,30], tail=30
        // Add ts=15 (WITHIN the cached window) → must invalidate + fire between 15<20.
        ctx.setCurrentKey(key);
        assertTrue(q.add(new TestElement(15L, 15)));
        q.flushPendingToEngine();
        assertEquals(15L, q.poll().ts, "in-window add must fire at its position");
        assertEquals(20L, q.poll().ts);
        assertEquals(30L, q.poll().ts);
        assertEquals(null, q.poll());
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

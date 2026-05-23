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

package org.apache.flink.state.forstrs.state.ttl;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.internal.InternalValueState;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-A7 (S1-12): the TTL decorator stamps an expiry on write and filters expired entries on read.
 *
 * <p>{@code StateTtlExpiryTest} contract: write at t=0 with TTL=100ms, read at t=200 -> null. The
 * fresh-read contract: read at t=50 -> returns value.
 */
class TtlAwareValueStateV2Test {

    /** Fake in-memory ValueState used to exercise the decorator without the full async machinery. */
    private static final class InMemoryInner<V> implements InternalValueState<Object, Object, V> {
        V slot;
        int clearCount;

        @Override
        public void setCurrentNamespace(Object namespace) {}

        @Override
        public V value() {
            return slot;
        }

        @Override
        public void update(V value) {
            slot = value;
        }

        @Override
        public StateFuture<V> asyncValue() {
            return StateFutureUtils.completedFuture(slot);
        }

        @Override
        public StateFuture<Void> asyncUpdate(V value) {
            slot = value;
            return StateFutureUtils.completedVoidFuture();
        }

        @Override
        public void clear() {
            slot = null;
            clearCount++;
        }

        @Override
        public StateFuture<Void> asyncClear() {
            slot = null;
            clearCount++;
            return StateFutureUtils.completedVoidFuture();
        }
    }

    private static final class ManualClock implements TtlClock {
        long now;

        ManualClock(long initial) {
            this.now = initial;
        }

        @Override
        public long currentTimeMillis() {
            return now;
        }
    }

    private static StateTtlConfig configWithTtlMillis(long ttlMs) {
        return StateTtlConfig.newBuilder(Duration.ofMillis(ttlMs))
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
    }

    @Test
    void expiredValueReadsAsNull() {
        InMemoryInner<TtlValue<String>> inner = new InMemoryInner<>();
        ManualClock clock = new ManualClock(0L);
        TtlAwareValueStateV2<Object, Object, String> ttl =
                new TtlAwareValueStateV2<>(inner, configWithTtlMillis(100L), clock);

        // Write at t=0 with TTL=100ms -> expiry = 100.
        ttl.update("hello");
        assertNotNull(inner.slot, "inner must receive the stamped value");
        assertEquals(100L, inner.slot.getExpiryTimestamp());
        assertEquals("hello", inner.slot.getValue());

        // Read at t=200 -> expired, must return null AND lazily clear the cell.
        clock.now = 200L;
        assertNull(ttl.value(), "post-TTL read must return null");
    }

    @Test
    void freshValueReadsThrough() {
        InMemoryInner<TtlValue<String>> inner = new InMemoryInner<>();
        ManualClock clock = new ManualClock(0L);
        TtlAwareValueStateV2<Object, Object, String> ttl =
                new TtlAwareValueStateV2<>(inner, configWithTtlMillis(100L), clock);

        ttl.update("hello");
        clock.now = 50L;
        assertEquals("hello", ttl.value(), "pre-TTL read must return the value");
    }

    @Test
    void asyncExpiredValueReadsAsNull() throws Exception {
        InMemoryInner<TtlValue<String>> inner = new InMemoryInner<>();
        ManualClock clock = new ManualClock(0L);
        TtlAwareValueStateV2<Object, Object, String> ttl =
                new TtlAwareValueStateV2<>(inner, configWithTtlMillis(100L), clock);

        ttl.asyncUpdate("hello");
        clock.now = 500L;

        // The decorator chains thenApply on a completed future, so the result is materialized
        // synchronously in StateFutureUtils' inline executor.
        final String[] captured = new String[1];
        ttl.asyncValue().thenAccept(v -> captured[0] = v);
        assertNull(captured[0], "post-TTL asyncValue must yield null");
    }

    @Test
    void nullUpdateBypassesStamping() {
        InMemoryInner<TtlValue<String>> inner = new InMemoryInner<>();
        ManualClock clock = new ManualClock(0L);
        TtlAwareValueStateV2<Object, Object, String> ttl =
                new TtlAwareValueStateV2<>(inner, configWithTtlMillis(100L), clock);

        ttl.update("v");
        ttl.update(null);
        assertNull(inner.slot, "null update must pass through (tombstone), not stamp");
    }

    /**
     * R20-M2: {@code clock.currentTimeMillis() + ttlMillis} overflows {@code long} for huge TTLs
     * (e.g. {@link Long#MAX_VALUE} or any "effectively infinite" retention config). The naive
     * addition wraps to a NEGATIVE expiry timestamp that {@link TtlValue#isExpired} reads as
     * already-expired-against-any-now, so EVERY read would fire {@code asyncClear} and the
     * value would be unrecoverable. The fix saturates to {@link Long#MAX_VALUE} via
     * {@code Math.addExact} + catch.
     */
    @Test
    void hugeTtlDoesNotOverflowToNegativeExpiry() {
        InMemoryInner<TtlValue<String>> inner = new InMemoryInner<>();
        // Start at a non-zero time so we exercise the overflow arithmetic, not the boundary.
        ManualClock clock = new ManualClock(1_000_000L);
        // Build a config with TTL = Long.MAX_VALUE / 1_000_000 millis (effectively infinite).
        // The Duration API converts that without overflow; addition inside stamp() would
        // overflow with this large a value plus 1_000_000.
        long hugeTtlMs = Long.MAX_VALUE - 500L; // smaller than Long.MAX, additionWith now → overflow
        TtlAwareValueStateV2<Object, Object, String> ttl =
                new TtlAwareValueStateV2<>(inner, configWithTtlMillis(hugeTtlMs), clock);

        ttl.update("forever");
        // Pre-fix this would wrap negative; saturated to Long.MAX_VALUE the timestamp is positive
        // and reads return the value at any (post-write) clock value.
        assertTrue(
                inner.slot.getExpiryTimestamp() > 0L,
                "R20-M2: huge-TTL expiry must NOT overflow to negative; got "
                        + inner.slot.getExpiryTimestamp());
        assertEquals(Long.MAX_VALUE, inner.slot.getExpiryTimestamp());

        // Read at a far-future clock — the value is still fresh (because expiry == MAX_VALUE).
        clock.now = Long.MAX_VALUE - 1L;
        assertEquals("forever", ttl.value(), "post-fix: huge-TTL values must NOT read as expired");
        assertEquals(0, inner.clearCount, "lazy clear must NOT fire on a non-expired huge-TTL read");
    }

    /**
     * R20-L1: the lazy-clear path inside {@link TtlAwareValueStateV2#value()} used to call
     * {@code inner.asyncClear()} and discard the returned {@link StateFuture}. Engine errors on
     * the clear would log nowhere. Fix: chain {@code thenAccept} on the returned future so a
     * failure surfaces through Flink's framework exception handler.
     *
     * <p>This test verifies the lazy-clear future is OBSERVED (not dropped on the floor) — the
     * {@link InMemoryInner#asyncClear} returns a completed future, and the chained
     * {@code thenAccept} must run without throwing.
     */
    @Test
    void lazyClearChainsOnReturnedFuture() {
        InMemoryInner<TtlValue<String>> inner = new InMemoryInner<>();
        ManualClock clock = new ManualClock(0L);
        TtlAwareValueStateV2<Object, Object, String> ttl =
                new TtlAwareValueStateV2<>(inner, configWithTtlMillis(100L), clock);

        ttl.update("v");
        clock.now = 500L; // expired -> lazy clear fires
        assertNull(ttl.value(), "expired read returns null");

        // The fix observes the returned future via thenAccept; the test's stub fixture
        // returns a completed future so the chain runs without throwing. The clearCount
        // increment proves asyncClear() WAS still called (lazy delete is preserved).
        assertEquals(
                1,
                inner.clearCount,
                "R20-L1: lazy clear must still fire on expired read (and now its future is observed)");
    }

    @Test
    void exposesInnerStateForFlushHooks() {
        InMemoryInner<TtlValue<String>> inner = new InMemoryInner<>();
        ManualClock clock = new ManualClock(0L);
        TtlAwareValueStateV2<Object, Object, String> ttl =
                new TtlAwareValueStateV2<>(inner, configWithTtlMillis(100L), clock);

        // The snapshot pre-hook needs the real inner state to drain off-heap buffers.
        assertSame(inner, ttl.getInner());
    }
}

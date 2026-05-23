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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.v2.internal.InternalValueState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PR-A7 (S1-12) TTL decorator over a {@link ValueState} whose underlying serializer is a {@link
 * TtlSerializer}. The inner state stores {@link TtlValue}; reads filter expired entries (lazy
 * cleanup); writes stamp an expiry timestamp computed as {@code clock.now + ttlMillis}.
 *
 * <p>Cleanup is lazy-on-read only — background cleanup is deferred to a follow-on PR. Expired
 * entries occupy storage until the next compaction reaches them or {@link #asyncClear()} is called.
 *
 * <p>Time characteristic: processing-time only. Event-time TTL is deferred (see {@link TtlClock}).
 */
@Internal
public class TtlAwareValueStateV2<K, N, V> implements InternalValueState<K, N, V> {

    private static final Logger LOG = LoggerFactory.getLogger(TtlAwareValueStateV2.class);

    private final InternalValueState<K, N, TtlValue<V>> inner;
    private final TtlClock clock;
    private final long ttlMillis;
    private final boolean returnExpired;

    public TtlAwareValueStateV2(
            InternalValueState<K, N, TtlValue<V>> inner,
            StateTtlConfig ttlConfig,
            TtlClock clock) {
        this.inner = inner;
        this.ttlMillis = ttlConfig.getTimeToLive().toMillis();
        this.returnExpired =
                ttlConfig.getStateVisibility()
                        == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp;
        this.clock = clock;
    }

    private TtlValue<V> stamp(V value) {
        return new TtlValue<>(safeExpiry(clock.currentTimeMillis()), value);
    }

    /**
     * R20-M2: overflow-safe expiry-timestamp computation. The naive
     * {@code clock.currentTimeMillis() + ttlMillis} overflows {@code long} for huge
     * {@code ttlMillis} (e.g. {@code Long.MAX_VALUE}, infinite-retention configs, or test
     * fixtures setting {@code Duration.ofDays(10_000_000)}). The wraparound produces a NEGATIVE
     * expiry timestamp that {@link TtlValue#isExpired} reads as already-expired-against-any-now —
     * every value would be treated as expired on the next read, firing {@code asyncClear} on
     * every {@link #value()} call and effectively making the TTL state unusable.
     *
     * <p>Saturates to {@link Long#MAX_VALUE} on overflow — a never-expiring expiry timestamp is
     * the semantically correct interpretation of "no upper-bound TTL".
     */
    private long safeExpiry(long now) {
        try {
            return Math.addExact(now, ttlMillis);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private V unwrapIfFresh(TtlValue<V> wrapped) {
        if (wrapped == null) {
            return null;
        }
        if (returnExpired || !wrapped.isExpired(clock.currentTimeMillis())) {
            return wrapped.getValue();
        }
        // R20-L1: expired and visibility is NeverReturnExpired -> lazy delete. We don't AWAIT
        // the delete (subsequent reads will hit the now-empty cell), but we MUST observe the
        // future so an engine-side failure of the lazy clear is surfaced rather than silently
        // dropped on the floor. The previous fire-and-forget {@code inner.asyncClear()}
        // discarded the {@link StateFuture} entirely — a failed clear would log nowhere and
        // the operator would silently re-try the same expired read forever.
        //
        // {@link StateFuture} has no {@code exceptionally(...)} method (unlike
        // {@code CompletableFuture}); chaining via {@link StateFuture#thenAccept} causes any
        // exception on the lazy-clear path to propagate through Flink's
        // {@code AsyncFrameworkExceptionHandler}, which is the canonical place for
        // async-state-pipeline failures to surface in the task log.
        StateFuture<Void> clearFut = inner.asyncClear();
        if (clearFut != null) {
            try {
                clearFut.thenAccept(
                        v -> {
                            /* successful lazy clear — no follow-up action */
                        });
            } catch (Throwable t) {
                // thenAccept on a real ContextAsyncFuture should not throw at registration
                // time; log defensively so an unexpected runtime issue doesn't escape this
                // read path and crash the operator.
                LOG.warn("Lazy TTL clear chain registration failed", t);
            }
        }
        return null;
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        inner.setCurrentNamespace(namespace);
    }

    // -- ValueState (sync) --

    @Override
    public V value() {
        TtlValue<V> wrapped = inner.value();
        return unwrapIfFresh(wrapped);
    }

    @Override
    public void update(V value) {
        if (value == null) {
            inner.update(null);
            return;
        }
        inner.update(stamp(value));
    }

    // -- ValueState (async) --

    @Override
    public StateFuture<V> asyncValue() {
        return inner.asyncValue().thenApply(this::unwrapIfFresh);
    }

    @Override
    public StateFuture<Void> asyncUpdate(V value) {
        if (value == null) {
            return inner.asyncUpdate(null);
        }
        return inner.asyncUpdate(stamp(value));
    }

    // -- State (clear) --

    @Override
    public void clear() {
        inner.clear();
    }

    @Override
    public StateFuture<Void> asyncClear() {
        return inner.asyncClear();
    }

    // -- Test/debug accessors --

    @Internal
    public InternalValueState<K, N, TtlValue<V>> getInner() {
        return inner;
    }

    @Internal
    public long getTtlMillis() {
        return ttlMillis;
    }

    /** No-op helper to silence "unused" warnings when StateFutureUtils import is reserved. */
    @SuppressWarnings("unused")
    private static <T> StateFuture<T> completed(T value) {
        return StateFutureUtils.completedFuture(value);
    }
}

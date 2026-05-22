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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.VectorizedClassifier;
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A5-H2 regression gate for {@link ForStRsAsyncReducingStateV2}.
 *
 * <p>{@code asyncClear()} is {@code final} on {@code AbstractKeyedState} (cannot be overridden),
 * so the cache-invalidation must happen on the dispatch path. The vectorized classifier's
 * {@link VectorizedClassifier#offer(StateRequest)} routes CLEAR via the DISPATCH_TABLE to
 * {@code recordDelete()}, which now fires {@code table.onClear(request)} BEFORE enqueuing the
 * DELETE row. {@code ForStRsAsyncReducingStateV2.onClear} invalidates the cache slot for the
 * current composite key so the next {@code flushOnBarrier()} does NOT write back the stale
 * accumulator and overwrite the engine-side DELETE.
 */
class ReducingStateV2ClearInvalidatesCacheTest {

    private static final ReduceFunction<Long> SUM = (a, b) -> a + b;

    private static <K, N> RecordContext<K> contextWithNamespace(
            K key, InternalPartitionedState<N> state, N namespace) {
        RecordContext<K> ctx =
                new RecordContext<>(
                        new Object(),
                        key,
                        c -> {},
                        0 /* keyGroup */,
                        new EpochManager.Epoch(0L),
                        4 /* variableCount */);
        ctx.setNamespace(state, namespace);
        return ctx;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <K, N, IN, OUT> StateRequest<K, N, IN, OUT> request(
            InternalPartitionedState<N> state,
            StateRequestType type,
            IN payload,
            RecordContext<K> ctx) {
        return new StateRequest<>(
                (org.apache.flink.api.common.state.v2.State) state,
                type,
                false,
                payload,
                null,
                ctx);
    }

    @SuppressWarnings("unchecked")
    private static <V> ReducingAggregatingCache<V, V> reflectCache(
            ForStRsAsyncReducingStateV2<?, ?, V> state) throws Exception {
        Field f = ForStRsAsyncReducingStateV2.class.getDeclaredField("cache");
        f.setAccessible(true);
        return (ReducingAggregatingCache<V, V>) f.get(state);
    }

    @Test
    void clearInvalidatesCacheSlotBeforeDeleteIsDispatched() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsAsyncReducingStateV2<Long, Integer, Long> state =
                    new ForStRsAsyncReducingStateV2<>(
                            null,
                            "myReducing",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            LongSerializer.INSTANCE,
                            SUM);
            ReducingAggregatingCache<Long, Long> cache = reflectCache(state);
            RecordContext<Long> ctx = contextWithNamespace(7L, state, 0);

            // Seed the cache directly: simulate a hit-resolve that left a dirty accumulator for
            // this (operatorKey=7, namespace=0, stateName=myReducing). The cache key is the
            // composite key bytes that ForStRsAsyncReducingStateV2.serializeKey() would produce.
            byte[] compositeKey = state.serializeKey(
                    request(state, StateRequestType.REDUCING_ADD, 42L, ctx));
            cache.put(compositeKey, 42L);
            assertEquals(1, state.cacheSize(), "cache must hold the seeded dirty entry pre-CLEAR");
            assertTrue(cache.contains(compositeKey), "cache must contain the seeded key");

            // Dispatch CLEAR through the vectorized classifier — recordDelete fires onClear which
            // must invalidate the cache slot before the DELETE row lands in the engine batch.
            VectorizedClassifier classifier =
                    new VectorizedClassifier(
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena));
            classifier.initNewKindBuffers(arena);
            classifier.offer(request(state, StateRequestType.CLEAR, null, ctx));

            assertEquals(1, classifier.deleteCount(), "CLEAR routes to DELETE in vectorized path");
            assertEquals(0, state.cacheSize(), "onClear must invalidate the cache slot");
            assertTrue(
                    !cache.contains(compositeKey),
                    "cache must no longer contain the cleared key");
        }
    }

    @Test
    void flushOnBarrierAfterClearDoesNotResurrectClearedKey() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            AtomicInteger flushCount = new AtomicInteger(0);
            ForStRsAsyncReducingStateV2<Long, Integer, Long> state =
                    new ForStRsAsyncReducingStateV2<>(
                            null,
                            "myReducing",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            LongSerializer.INSTANCE,
                            SUM);
            state.setFlushHandler((k, v) -> flushCount.incrementAndGet());
            ReducingAggregatingCache<Long, Long> cache = reflectCache(state);
            RecordContext<Long> ctx = contextWithNamespace(7L, state, 0);

            // Seed two cache slots. Only one will be cleared.
            byte[] keyToClear =
                    state.serializeKey(request(state, StateRequestType.REDUCING_ADD, 1L, ctx));
            cache.put(keyToClear, 1L);
            RecordContext<Long> ctx2 = contextWithNamespace(8L, state, 0);
            byte[] keyToKeep =
                    state.serializeKey(request(state, StateRequestType.REDUCING_ADD, 2L, ctx2));
            cache.put(keyToKeep, 2L);
            assertEquals(2, state.cacheSize());

            // CLEAR on key 7 — recordDelete → onClear invalidates only that slot.
            VectorizedClassifier classifier =
                    new VectorizedClassifier(
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena));
            classifier.initNewKindBuffers(arena);
            classifier.offer(request(state, StateRequestType.CLEAR, null, ctx));
            assertEquals(1, state.cacheSize(), "only the cleared slot must be removed");

            // Drain on barrier — the cleared key must NOT be flushed (it would overwrite the
            // engine-side DELETE). The retained key must be flushed exactly once.
            state.flushOnBarrier();
            assertEquals(
                    1,
                    flushCount.get(),
                    "exactly one entry must flush (the retained key); the cleared key must not");
        }
    }

    @Test
    void onClearMethodOverriddenOnAsyncReducingV2() throws Exception {
        assertSame(
                ForStRsAsyncReducingStateV2.class,
                ForStRsAsyncReducingStateV2.class
                        .getDeclaredMethod("onClear", StateRequest.class)
                        .getDeclaringClass(),
                "onClear must be overridden on ForStRsAsyncReducingStateV2 — without this the "
                        + "default no-op runs and A5-H2 silently regresses");
    }
}

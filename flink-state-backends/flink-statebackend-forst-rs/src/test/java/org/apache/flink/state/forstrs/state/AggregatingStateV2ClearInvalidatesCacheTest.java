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

import org.apache.flink.api.common.functions.AggregateFunction;
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
 * A5-H2 regression gate for {@link ForStRsAsyncAggregatingStateV2}. Sibling of
 * {@link ReducingStateV2ClearInvalidatesCacheTest}; same routing — vectorized classifier's
 * {@code recordDelete()} fires {@code table.onClear(request)} BEFORE enqueuing the DELETE row,
 * and {@code ForStRsAsyncAggregatingStateV2.onClear} invalidates the cache slot so the next
 * {@code flushOnBarrier()} does NOT write back the stale accumulator and overwrite the
 * engine-side DELETE.
 */
class AggregatingStateV2ClearInvalidatesCacheTest {

    /** Identity aggregator: ACC == IN; getResult is the identity function. */
    private static final AggregateFunction<Long, Long, Long> SUM =
            new AggregateFunction<Long, Long, Long>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Long value, Long accumulator) {
                    return accumulator + value;
                }

                @Override
                public Long getResult(Long accumulator) {
                    return accumulator;
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            };

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
    private static <IN, ACC> ReducingAggregatingCache<IN, ACC> reflectCache(
            ForStRsAsyncAggregatingStateV2<?, ?, IN, ACC, ?> state) throws Exception {
        Field f = ForStRsAsyncAggregatingStateV2.class.getDeclaredField("cache");
        f.setAccessible(true);
        return (ReducingAggregatingCache<IN, ACC>) f.get(state);
    }

    @Test
    void clearInvalidatesCacheSlotBeforeDeleteIsDispatched() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsAsyncAggregatingStateV2<Long, Integer, Long, Long, Long> state =
                    new ForStRsAsyncAggregatingStateV2<>(
                            null,
                            "myAgg",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            LongSerializer.INSTANCE,
                            SUM);
            ReducingAggregatingCache<Long, Long> cache = reflectCache(state);
            RecordContext<Long> ctx = contextWithNamespace(7L, state, 0);

            byte[] compositeKey =
                    state.serializeKey(request(state, StateRequestType.AGGREGATING_ADD, 42L, ctx));
            cache.put(compositeKey, 42L);
            assertEquals(1, state.cacheSize(), "cache must hold the seeded entry pre-CLEAR");
            assertTrue(cache.contains(compositeKey));

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
            ForStRsAsyncAggregatingStateV2<Long, Integer, Long, Long, Long> state =
                    new ForStRsAsyncAggregatingStateV2<>(
                            null,
                            "myAgg",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            LongSerializer.INSTANCE,
                            SUM);
            state.setFlushHandler((k, v) -> flushCount.incrementAndGet());
            ReducingAggregatingCache<Long, Long> cache = reflectCache(state);
            RecordContext<Long> ctx = contextWithNamespace(7L, state, 0);

            byte[] keyToClear =
                    state.serializeKey(request(state, StateRequestType.AGGREGATING_ADD, 1L, ctx));
            cache.put(keyToClear, 1L);
            RecordContext<Long> ctx2 = contextWithNamespace(8L, state, 0);
            byte[] keyToKeep =
                    state.serializeKey(request(state, StateRequestType.AGGREGATING_ADD, 2L, ctx2));
            cache.put(keyToKeep, 2L);
            assertEquals(2, state.cacheSize());

            VectorizedClassifier classifier =
                    new VectorizedClassifier(
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena));
            classifier.initNewKindBuffers(arena);
            classifier.offer(request(state, StateRequestType.CLEAR, null, ctx));
            assertEquals(1, state.cacheSize());

            state.flushOnBarrier();
            assertEquals(
                    1,
                    flushCount.get(),
                    "exactly one entry must flush (the retained key); the cleared key must not");
        }
    }

    @Test
    void onClearMethodOverriddenOnAsyncAggregatingV2() throws Exception {
        assertSame(
                ForStRsAsyncAggregatingStateV2.class,
                ForStRsAsyncAggregatingStateV2.class
                        .getDeclaredMethod("onClear", StateRequest.class)
                        .getDeclaringClass(),
                "onClear must be overridden on ForStRsAsyncAggregatingStateV2 — without this the "
                        + "default no-op runs and A5-H2 silently regresses");
    }
}

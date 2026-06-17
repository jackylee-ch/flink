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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R-2 (2026-06-17) regression gate for the {@link ForStRsValueStateV2} write-back RMW cache. The
 * NexMark q11 (session-window count) and q17 (unbounded group-agg) operators store their
 * aggregation accumulator in a {@code ValueState<RowData>} and do a per-record get→fold→put; this
 * cache collapses the per-record engine round-trips and flushes dirty values at the checkpoint
 * barrier. These tests assert the correctness contract: the cache is wired only when a flush
 * handler is set, the combiner is last-write-wins, {@code onClear} invalidates the slot before the
 * engine DELETE, the barrier flush serializes the dirty value, and a cleared key is NOT
 * resurrected.
 */
class ValueStateV2RmwCacheTest {

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
    private static <V> ReducingAggregatingCache<V, V> reflectCache(ForStRsValueStateV2<?, ?, V> state)
            throws Exception {
        Field f = ForStRsValueStateV2.class.getDeclaredField("cache");
        f.setAccessible(true);
        return (ReducingAggregatingCache<V, V>) f.get(state);
    }

    private static ForStRsValueStateV2<Long, Integer, Long> newState() {
        return new ForStRsValueStateV2<>(
                null, "accState", LongSerializer.INSTANCE, IntSerializer.INSTANCE,
                LongSerializer.INSTANCE);
    }

    @Test
    void asyncValueAsyncUpdateOverriddenOnValueStateV2() throws Exception {
        assertSame(
                ForStRsValueStateV2.class,
                ForStRsValueStateV2.class.getDeclaredMethod("asyncValue").getDeclaringClass(),
                "asyncValue must be overridden so the write-back cache intercepts reads");
        assertSame(
                ForStRsValueStateV2.class,
                ForStRsValueStateV2.class
                        .getDeclaredMethod("asyncUpdate", Object.class)
                        .getDeclaringClass(),
                "asyncUpdate must be overridden so writes land in the cache (dirty)");
        assertSame(
                ForStRsValueStateV2.class,
                ForStRsValueStateV2.class
                        .getDeclaredMethod("onClear", StateRequest.class)
                        .getDeclaringClass(),
                "onClear must be overridden so a CLEAR invalidates the cache slot");
    }

    @Test
    void combinerIsLastWriteWinsReplace() throws Exception {
        ForStRsValueStateV2<Long, Integer, Long> state = newState();
        state.setFlushHandler((k, v) -> {});
        ReducingAggregatingCache<Long, Long> cache = reflectCache(state);

        byte[] key = state.serializeKey(request(state, StateRequestType.VALUE_UPDATE, 1L,
                contextWithNamespace(7L, state, 0)));
        cache.put(key, 10L);
        assertTrue(cache.tryFold(key, 20L), "tryFold must hit the seeded slot");
        assertEquals(20L, cache.peek(key), "combiner must REPLACE (last-write-wins), not accumulate");
        assertTrue(cache.tryFold(key, 30L));
        assertEquals(30L, cache.peek(key));
    }

    @Test
    void flushSerializesDirtyValueBytes() throws Exception {
        ForStRsValueStateV2<Long, Integer, Long> state = newState();
        final long[] flushedVal = {Long.MIN_VALUE};
        final int[] flushes = {0};
        state.setFlushHandler(
                (k, v) -> {
                    flushes[0]++;
                    try {
                        DataInputDeserializer in = new DataInputDeserializer(v);
                        flushedVal[0] = LongSerializer.INSTANCE.deserialize(in);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        ReducingAggregatingCache<Long, Long> cache = reflectCache(state);
        byte[] key = state.serializeKey(request(state, StateRequestType.VALUE_UPDATE, 0L,
                contextWithNamespace(7L, state, 0)));
        cache.put(key, 99L);
        state.flushOnBarrier();
        assertEquals(1, flushes[0], "exactly one dirty entry flushes");
        assertEquals(99L, flushedVal[0], "flushed bytes must deserialize to the cached value");
    }

    @Test
    void clearInvalidatesCacheSlotBeforeDeleteIsDispatched() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsValueStateV2<Long, Integer, Long> state = newState();
            state.setFlushHandler((k, v) -> {});
            ReducingAggregatingCache<Long, Long> cache = reflectCache(state);
            RecordContext<Long> ctx = contextWithNamespace(7L, state, 0);

            byte[] key = state.serializeKey(request(state, StateRequestType.VALUE_UPDATE, 5L, ctx));
            cache.put(key, 5L);
            assertEquals(1, state.cacheSize());

            VectorizedClassifier classifier =
                    new VectorizedClassifier(
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena));
            classifier.initNewKindBuffers(arena);
            classifier.offer(request(state, StateRequestType.CLEAR, null, ctx));

            assertEquals(1, classifier.deleteCount(), "CLEAR routes to DELETE in the vectorized path");
            assertEquals(0, state.cacheSize(), "onClear must invalidate the cache slot");
            assertFalse(cache.contains(key));
        }
    }

    @Test
    void flushAfterClearDoesNotResurrectClearedKey() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            AtomicInteger flushCount = new AtomicInteger(0);
            ForStRsValueStateV2<Long, Integer, Long> state = newState();
            state.setFlushHandler((k, v) -> flushCount.incrementAndGet());
            ReducingAggregatingCache<Long, Long> cache = reflectCache(state);

            RecordContext<Long> ctx = contextWithNamespace(7L, state, 0);
            byte[] keyToClear =
                    state.serializeKey(request(state, StateRequestType.VALUE_UPDATE, 1L, ctx));
            cache.put(keyToClear, 1L);
            RecordContext<Long> ctx2 = contextWithNamespace(8L, state, 0);
            byte[] keyToKeep =
                    state.serializeKey(request(state, StateRequestType.VALUE_UPDATE, 2L, ctx2));
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
                    1, flushCount.get(),
                    "only the retained key flushes; the cleared key must not resurrect");
        }
    }

    @Test
    void cacheBypassedUntilFlushHandlerWired() throws Exception {
        // Without setFlushHandler the cache must be OFF (rmwCacheUsable() == false) so an un-wired
        // inner ValueStateV2 (e.g. the TTL-wrapped one, which is never registered/flush-wired)
        // never silently drops dirty values.
        ForStRsValueStateV2<Long, Integer, Long> state = newState();
        Field f = ForStRsValueStateV2.class.getDeclaredField("flushHandlerWired");
        f.setAccessible(true);
        assertEquals(false, f.get(state), "flushHandlerWired must default false (cache off)");
        state.setFlushHandler((k, v) -> {});
        assertEquals(true, f.get(state), "setFlushHandler must arm the cache");
    }

    @Test
    void peekOnEmptyCacheReturnsNull() throws Exception {
        ForStRsValueStateV2<Long, Integer, Long> state = newState();
        state.setFlushHandler((k, v) -> {});
        ReducingAggregatingCache<Long, Long> cache = reflectCache(state);
        byte[] key = state.serializeKey(request(state, StateRequestType.VALUE_UPDATE, 0L,
                contextWithNamespace(7L, state, 0)));
        assertNull(cache.peek(key), "miss must return null");
        assertFalse(cache.contains(key));
    }
}

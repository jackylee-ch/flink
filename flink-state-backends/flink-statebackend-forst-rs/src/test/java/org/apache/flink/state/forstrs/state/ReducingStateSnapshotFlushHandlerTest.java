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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A4-H2 regression: verifies that the {@link ForStRsAsyncReducingStateV2} and {@link
 * ForStRsAsyncAggregatingStateV2} RMW caches actually persist their dirty accumulators to the
 * engine when {@code flushOnBarrier()} is invoked under the production flush-handler wiring
 * installed by {@code ForStRsAsyncKeyedStateBackend#createReducingState} /
 * {@code createAggregatingState}.
 *
 * <p>Before this PR the default flush handler was {@code (k, v) -> {}}, so every cached
 * accumulator was silently dropped on every checkpoint. The fix wires a direct {@link
 * ForStRsLinker#put(FrsDb, FrsCfHandle, byte[], byte[])} closure at state-construction time —
 * this test stands up a live in-memory engine, replicates the same closure shape, drives 5
 * distinct (key, accumulator) entries through the cache, and verifies they survive {@code
 * flushOnBarrier()} by reading them back via {@code linker.get}.
 *
 * <p>We use direct cache population (reflective access to the {@code cache} field) so the test
 * is independent of {@code AsyncExecutionController} / {@code RecordContext} wiring — those
 * dependencies are exercised by other RMW parity tests; the focus here is solely the
 * cache-to-engine durability path.
 */
class ReducingStateSnapshotFlushHandlerTest {

    private static final byte[][] FIVE_KEYS = {
        "k/1/myReducing/".getBytes(),
        "k/2/myReducing/".getBytes(),
        "k/3/myReducing/".getBytes(),
        "k/4/myReducing/".getBytes(),
        "k/5/myReducing/".getBytes(),
    };

    @Test
    void reducingState_dirtyAccumulatorsPersistAfterFlushOnBarrier() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                ReduceFunction<Long> sum = Long::sum;
                ForStRsAsyncReducingStateV2<Long, Integer, Long> state =
                        new ForStRsAsyncReducingStateV2<>(
                                /* stateRequestHandler */ null,
                                "myReducing",
                                LongSerializer.INSTANCE,
                                IntSerializer.INSTANCE,
                                LongSerializer.INSTANCE,
                                sum);

                // Production wiring: the same closure shape installed by
                // ForStRsAsyncKeyedStateBackend#createReducingState (A4-H2 fix).
                state.setFlushHandler(makeEngineFlushHandler(linker, db, cf));

                // Seed 5 dirty accumulators directly into the per-state cache. We bypass the
                // RecordContext-bound asyncAdd path because exercising AEC is out of scope for
                // this correctness test — the contract under test is "cache → flushHandler →
                // engine".
                //
                // B10-H2: for Long-typed states the active cache may be the primitive-long
                // specialization (longCache), not the general cache. Use the backend-agnostic
                // testOnlyDirectCachePut adapter which routes correctly regardless.
                long[] expected = {10L, 20L, 30L, 40L, 50L};
                for (int i = 0; i < 5; i++) {
                    state.testOnlyDirectCachePut(FIVE_KEYS[i], expected[i]);
                }
                assertEquals(5, state.cacheSize());

                // Engine must NOT contain any of these yet — the cache has not been drained.
                for (int i = 0; i < 5; i++) {
                    assertNull(
                            linker.get(db, cf, FIVE_KEYS[i]),
                            "engine must be empty before flushOnBarrier (key " + i + ")");
                }

                // Trigger the snapshot drain.
                state.flushOnBarrier();

                // Every accumulator must now be readable from the engine via linker.get, with
                // the same Long-serializer round-trip the production flushEntry path uses.
                for (int i = 0; i < 5; i++) {
                    byte[] raw = linker.get(db, cf, FIVE_KEYS[i]);
                    assertNotNull(
                            raw,
                            "key " + i + " must be present after flushOnBarrier (A4-H2 regression)");
                    long decoded = decodeLong(raw);
                    assertEquals(expected[i], decoded, "decoded accumulator must match for key " + i);
                }
            }
        }
    }

    @Test
    void reducingState_clearedEntry_routesToDelete() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                ReduceFunction<Long> sum = Long::sum;
                ForStRsAsyncReducingStateV2<Long, Integer, Long> state =
                        new ForStRsAsyncReducingStateV2<>(
                                null,
                                "myReducing",
                                LongSerializer.INSTANCE,
                                IntSerializer.INSTANCE,
                                LongSerializer.INSTANCE,
                                sum);

                state.setFlushHandler(makeEngineFlushHandler(linker, db, cf));

                // Pre-seed the engine via a direct put — represents a row from a previous epoch.
                byte[] pre = encodeLong(99L);
                linker.put(db, cf, FIVE_KEYS[0], pre);
                assertArrayEquals(pre, linker.get(db, cf, FIVE_KEYS[0]));

                // Cache contains a tombstone (null acc) — flushHandler must route to delete.
                // B10-H2: testOnlyDirectCachePut with null acc routes through the tombstone path
                // for both the general cache and the primitive-long specialization.
                state.testOnlyDirectCachePut(FIVE_KEYS[0], null);
                state.flushOnBarrier();

                assertNull(
                        linker.get(db, cf, FIVE_KEYS[0]),
                        "null accumulator (cleared cache entry) must DELETE from engine");
            }
        }
    }

    @Test
    void aggregatingState_dirtyAccumulatorsPersistAfterFlushOnBarrier() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                AggregateFunction<Long, Long, Long> count =
                        new AggregateFunction<Long, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(Long v, Long acc) {
                                return acc + 1L;
                            }

                            @Override
                            public Long getResult(Long acc) {
                                return acc;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return a + b;
                            }
                        };

                ForStRsAsyncAggregatingStateV2<Long, Integer, Long, Long, Long> state =
                        new ForStRsAsyncAggregatingStateV2<>(
                                null,
                                "myAgg",
                                LongSerializer.INSTANCE,
                                IntSerializer.INSTANCE,
                                LongSerializer.INSTANCE,
                                count);

                state.setFlushHandler(makeEngineFlushHandler(linker, db, cf));

                ReducingAggregatingCache<Long, Long> cache = aggregatingCache(state);
                long[] expected = {7L, 14L, 21L, 28L, 35L};
                byte[][] aggKeys = {
                    "k/1/myAgg/".getBytes(),
                    "k/2/myAgg/".getBytes(),
                    "k/3/myAgg/".getBytes(),
                    "k/4/myAgg/".getBytes(),
                    "k/5/myAgg/".getBytes(),
                };
                for (int i = 0; i < 5; i++) {
                    cache.put(aggKeys[i], expected[i]);
                }
                assertEquals(5, state.cacheSize());

                state.flushOnBarrier();

                for (int i = 0; i < 5; i++) {
                    byte[] raw = linker.get(db, cf, aggKeys[i]);
                    assertNotNull(
                            raw,
                            "agg key " + i + " must be present after flushOnBarrier (A4-H2 fix)");
                    assertEquals(expected[i], decodeLong(raw));
                }
            }
        }
    }

    @Test
    void defaultFlushHandlerIsStillNoOp_documentsUnwiredPath() throws Exception {
        // Documents the one remaining guarded path: a state instance constructed outside of
        // ForStRsAsyncKeyedStateBackend (e.g. legacy unit tests that build the state directly
        // and never call setFlushHandler) still receives the no-op default. This is intentional:
        // tests that only exercise serialize/deserialize or namespace encoding (e.g.
        // V2NamespaceEncodingTest) must not require a live engine. Production code paths always
        // go through createReducingState/createAggregatingState which installs the engine-backed
        // handler — see ForStRsAsyncKeyedStateBackend#rmwFlushToEngine.
        ReduceFunction<Long> sum = Long::sum;
        ForStRsAsyncReducingStateV2<Long, Integer, Long> state =
                new ForStRsAsyncReducingStateV2<>(
                        null,
                        "myReducing",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        LongSerializer.INSTANCE,
                        sum);

        // B10-H2: backend-agnostic put (Long-typed state may route to the primitive-long cache).
        state.testOnlyDirectCachePut(FIVE_KEYS[0], 1L);
        // Must not throw: the default handler is (k,v) -> {} and the cache simply marks the
        // entry clean. The accumulator IS discarded — but only because no flush handler was
        // wired. The previous A4-H2 bug was that this same silent-discard behaviour applied to
        // *production* state instances; this test documents that fact has now been split into
        // "production = engine PUT" (covered by the other tests in this file) vs "naked unit
        // test = no-op".
        state.flushOnBarrier();
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    /**
     * Builds the exact closure shape that
     * {@code ForStRsAsyncKeyedStateBackend#createReducingState} installs via
     * {@code state.setFlushHandler(this::rmwFlushToEngine)}. Keeping it identical here is the
     * point of the regression test — if the production wiring drifts, this test must drift with
     * it (or vice versa).
     */
    private static BiConsumer<byte[], byte[]> makeEngineFlushHandler(
            ForStRsLinker linker, FrsDb db, FrsCfHandle cf) {
        return (k, v) -> {
            if (v == null || v.length == 0) {
                linker.delete(db, cf, k);
            } else {
                linker.put(db, cf, k, v);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static ReducingAggregatingCache<Long, Long> reducingCache(
            ForStRsAsyncReducingStateV2<?, ?, ?> state) throws Exception {
        Field f = ForStRsAsyncReducingStateV2.class.getDeclaredField("cache");
        f.setAccessible(true);
        return (ReducingAggregatingCache<Long, Long>) f.get(state);
    }

    @SuppressWarnings("unchecked")
    private static ReducingAggregatingCache<Long, Long> aggregatingCache(
            ForStRsAsyncAggregatingStateV2<?, ?, ?, ?, ?> state) throws Exception {
        Field f = ForStRsAsyncAggregatingStateV2.class.getDeclaredField("cache");
        f.setAccessible(true);
        return (ReducingAggregatingCache<Long, Long>) f.get(state);
    }

    /** LongSerializer writes 8 bytes big-endian. */
    private static byte[] encodeLong(long v) {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (v >>> (8 * (7 - i)));
        }
        return out;
    }

    private static long decodeLong(byte[] raw) {
        assertEquals(8, raw.length, "expected 8-byte Long encoding");
        long v = 0L;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (raw[i] & 0xFFL);
        }
        return v;
    }
}

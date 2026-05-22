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
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-C3 parity test for {@link ForStRsAsyncAggregatingStateV2}.
 *
 * <p>Verifies (a) structurally that {@code asyncAdd} is overridden on the async-V2 class (i.e. the
 * inherited get-add-put round trip from {@code AbstractAggregatingState} is bypassed), and (b)
 * functionally that the underlying {@link ReducingAggregatingCache} produces a result equivalent
 * to the naive read-modify-write path over a random {@code add} sequence.
 *
 * <p>Mirrors {@link ReducingStateRmwCacheParityTest} with the IN/ACC/OUT three-type AggregateFunction
 * twist — we use a count + sum accumulator so the parity check exercises a non-trivial ACC type.
 */
class AggregatingStateRmwCacheParityTest {

    /**
     * Test AggregateFunction: input is Long, accumulator is {@code long[]{count, sum}}, output is
     * average (sum/count). Non-trivial enough that a naive RMW path is observably distinct from
     * the cache path (sum of input vs sum of running averages).
     */
    private static final AggregateFunction<Long, long[], Double> AVG =
            new AggregateFunction<>() {
                @Override
                public long[] createAccumulator() {
                    return new long[2];
                }

                @Override
                public long[] add(Long value, long[] acc) {
                    acc[0] += 1;
                    acc[1] += value;
                    return acc;
                }

                @Override
                public Double getResult(long[] acc) {
                    return acc[0] == 0 ? 0.0 : (double) acc[1] / acc[0];
                }

                @Override
                public long[] merge(long[] a, long[] b) {
                    return new long[] {a[0] + b[0], a[1] + b[1]};
                }
            };

    @Test
    void asyncAddIsOverriddenOnAsyncAggregatingV2() throws Exception {
        Method m =
                ForStRsAsyncAggregatingStateV2.class.getDeclaredMethod("asyncAdd", Object.class);
        assertSame(
                ForStRsAsyncAggregatingStateV2.class,
                m.getDeclaringClass(),
                "asyncAdd must be overridden on ForStRsAsyncAggregatingStateV2 (not inherited "
                        + "from AbstractAggregatingState)");
    }

    @Test
    void flushOnBarrierMethodExists() throws Exception {
        Method m = ForStRsAsyncAggregatingStateV2.class.getDeclaredMethod("flushOnBarrier");
        assertNotNull(m);
        assertSame(
                ForStRsAsyncAggregatingStateV2.class,
                m.getDeclaringClass(),
                "flushOnBarrier must be declared on ForStRsAsyncAggregatingStateV2");
    }

    @Test
    void cacheFieldPresentAndPrivateFinal() throws Exception {
        var f = ForStRsAsyncAggregatingStateV2.class.getDeclaredField("cache");
        assertSame(ReducingAggregatingCache.class, f.getType());
        int mod = f.getModifiers();
        assertTrue(
                Modifier.isPrivate(mod) && Modifier.isFinal(mod),
                "cache field must be private final");
    }

    /**
     * Functional parity over a random add sequence. The naive oracle re-implements
     * {@code AbstractAggregatingState#asyncAdd}'s get-add-put trip; the cache path mirrors the
     * override (tryFold on hit; on miss, seed from {@code createAccumulator()} and apply add).
     */
    @Test
    void randomAddSequenceParityVsNaiveRmw() {
        Random rnd = new Random(7L);
        Map<Long, long[]> oracle = new HashMap<>();
        Map<String, long[]> flushed = new HashMap<>();

        ReducingAggregatingCache<Long, long[]> cache =
                new ReducingAggregatingCache<>(
                        // signature is (acc, in) → acc — reordered AVG.add(in, acc).
                        (acc, in) -> AVG.add(in, acc),
                        (k, v) -> flushed.put(byteKey(k), v));

        int N = 4000;
        long[] keys = new long[12];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = rnd.nextLong();
        }

        for (int i = 0; i < N; i++) {
            long key = keys[rnd.nextInt(keys.length)];
            long value = rnd.nextInt(1_000_000);
            byte[] composite = compositeKey(key);

            // Naive oracle: get-add-put on the heap map (mirrors AbstractAggregatingState.asyncAdd).
            long[] oldAcc = oracle.get(key);
            long[] seed = oldAcc == null ? AVG.createAccumulator() : oldAcc;
            long[] newAcc = AVG.add(value, seed);
            oracle.put(key, newAcc);

            // Cache path: tryFold on hit; on cold miss, seed via createAccumulator() and add
            // — exactly what the async-V2 class's asyncAdd override does.
            var hit = cache.tryFold(composite, value);
            if (hit.isEmpty()) {
                long[] cold = AVG.add(value, AVG.createAccumulator());
                cache.put(composite, cold);
            }
        }

        cache.flushAllDirty();
        assertEquals(oracle.size(), flushed.size());
        for (Map.Entry<Long, long[]> e : oracle.entrySet()) {
            long[] expected = e.getValue();
            long[] actual = flushed.get(byteKey(compositeKey(e.getKey())));
            assertEquals(expected[0], actual[0], "count for key " + e.getKey());
            assertEquals(expected[1], actual[1], "sum for key " + e.getKey());
        }
    }

    @Test
    void flushOnBarrierDrainsDirtyEntries() {
        Map<String, long[]> flushed = new HashMap<>();
        ReducingAggregatingCache<Long, long[]> cache =
                new ReducingAggregatingCache<>(
                        (acc, in) -> AVG.add(in, acc),
                        (k, v) -> flushed.put(byteKey(k), v));

        // First miss-seed for two keys, then a hit on key 1.
        cache.put(compositeKey(1L), AVG.add(10L, AVG.createAccumulator()));
        cache.put(compositeKey(2L), AVG.add(100L, AVG.createAccumulator()));
        cache.tryFold(compositeKey(1L), 20L);

        cache.flushAllDirty();

        assertEquals(2, flushed.size());
        long[] one = flushed.get(byteKey(compositeKey(1L)));
        long[] two = flushed.get(byteKey(compositeKey(2L)));
        assertEquals(2L, one[0]);
        assertEquals(30L, one[1]);
        assertEquals(1L, two[0]);
        assertEquals(100L, two[1]);
    }

    private static byte[] compositeKey(long opKey) {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (opKey >>> (8 * (7 - i)));
        }
        return out;
    }

    private static String byteKey(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}

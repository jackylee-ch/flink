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
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * PR-C3 parity test for {@link ForStRsAsyncReducingStateV2}.
 *
 * <p>Verifies (a) structurally that {@code asyncAdd} is overridden on the async-V2 class (i.e. the
 * inherited get-reduce-put round trip from {@code AbstractReducingState} is bypassed), and (b)
 * functionally that the underlying {@link ReducingAggregatingCache} produces a result equivalent
 * to the naive read-modify-write path over a random {@code add} sequence.
 *
 * <p>Full end-to-end exercise against a live engine + {@code AsyncExecutionController} is part of
 * the integration tests / Nexmark Q4 measurement points listed in the architectural-PR spec.
 */
class ReducingStateRmwCacheParityTest {

    /** Reducing semantics: stored value is the running sum. */
    private static final ReduceFunction<Long> SUM = (a, b) -> a + b;

    @Test
    void asyncAddIsOverriddenOnAsyncReducingV2() throws Exception {
        // Without this override, asyncAdd inherits AbstractReducingState's
        // asyncGetInternal().thenCompose(reduce → asyncUpdateInternal) — a fresh round trip per
        // call. That is exactly the B3-H1 / V12 audit finding PR-C3 is closing.
        Method m = ForStRsAsyncReducingStateV2.class.getDeclaredMethod("asyncAdd", Object.class);
        assertSame(
                ForStRsAsyncReducingStateV2.class,
                m.getDeclaringClass(),
                "asyncAdd must be overridden on ForStRsAsyncReducingStateV2 (not inherited from "
                        + "AbstractReducingState)");
    }

    @Test
    void flushOnBarrierMethodExists() throws Exception {
        Method m = ForStRsAsyncReducingStateV2.class.getDeclaredMethod("flushOnBarrier");
        assertNotNull(m);
        assertSame(
                ForStRsAsyncReducingStateV2.class,
                m.getDeclaringClass(),
                "flushOnBarrier must be declared on ForStRsAsyncReducingStateV2");
    }

    @Test
    void cacheFieldPresentAndPrivateFinal() throws Exception {
        var f = ForStRsAsyncReducingStateV2.class.getDeclaredField("cache");
        assertSame(
                ReducingAggregatingCache.class,
                f.getType(),
                "cache field must be the shared ReducingAggregatingCache (no per-class cache)");
        // Java reflection: private final field expected.
        int mod = f.getModifiers();
        org.junit.jupiter.api.Assertions.assertTrue(
                Modifier.isPrivate(mod) && Modifier.isFinal(mod),
                "cache field must be private final");
    }

    /**
     * Functional parity. Drives the same ReducingAggregatingCache instance that
     * {@link ForStRsAsyncReducingStateV2} constructs internally (same combiner / null-handling
     * contract), runs a random add sequence, then asserts the final per-key sum matches a naive
     * read-modify-write oracle.
     *
     * <p>The miss-path in the real class invokes {@code asyncGetInternal} (yielding null for
     * absent keys, since the engine has no row) and then calls {@code cache.put(key, value)}; this
     * test mirrors that behaviour by inserting {@code value} on first miss (cold seed = the input
     * itself, matching ReducingState semantics in AbstractReducingState).
     */
    @Test
    void randomAddSequenceParityVsNaiveRmw() {
        Random rnd = new Random(42L);
        Map<Long, Long> oracle = new HashMap<>();
        Map<String, Long> flushed = new HashMap<>();

        ReducingAggregatingCache<Long, Long> cache =
                new ReducingAggregatingCache<>(
                        (acc, in) -> {
                            try {
                                return SUM.reduce(acc, in);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        (keyBytes, acc) -> flushed.put(byteKey(keyBytes), acc));

        int N = 5000;
        long[] keys = new long[16];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = rnd.nextLong();
        }

        for (int i = 0; i < N; i++) {
            long key = keys[rnd.nextInt(keys.length)];
            long value = rnd.nextInt(1000) - 500;
            byte[] composite = compositeKey(key);

            // Naive oracle: get-reduce-put on the heap map.
            Long oldOracle = oracle.get(key);
            long newOracle;
            try {
                newOracle = oldOracle == null ? value : SUM.reduce(oldOracle, value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            oracle.put(key, newOracle);

            // Cache path: tryFold on hit; on miss, simulate asyncGetInternal returning null and
            // populate. This is the exact code path of ForStRsAsyncReducingStateV2#asyncAdd's
            // override.
            var hit = cache.tryFold(composite, value);
            if (hit.isEmpty()) {
                cache.put(composite, value);
            }
        }

        // After all asyncAdds, drain on barrier — same as snapshot().
        cache.flushAllDirty();

        assertEquals(oracle.size(), flushed.size(), "every keyed entry must be flushed exactly once");
        for (Map.Entry<Long, Long> e : oracle.entrySet()) {
            assertEquals(
                    e.getValue(),
                    flushed.get(byteKey(compositeKey(e.getKey()))),
                    "flushed value for key " + e.getKey() + " must match naive RMW oracle");
        }
    }

    @Test
    void flushOnBarrierDrainsDirtyEntries() {
        AtomicReference<Integer> flushCount = new AtomicReference<>(0);
        Map<String, Long> flushed = new HashMap<>();
        ReducingAggregatingCache<Long, Long> cache =
                new ReducingAggregatingCache<>(
                        (a, b) -> {
                            try {
                                return SUM.reduce(a, b);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        (k, v) -> {
                            flushCount.set(flushCount.get() + 1);
                            flushed.put(byteKey(k), v);
                        });

        cache.put(compositeKey(1L), 10L);
        cache.put(compositeKey(2L), 20L);
        // Fold into key 1 — still dirty, increments running sum.
        cache.tryFold(compositeKey(1L), 5L);

        cache.flushAllDirty();

        assertEquals(2, flushCount.get(), "exactly two dirty entries must be flushed");
        assertEquals(15L, flushed.get(byteKey(compositeKey(1L))));
        assertEquals(20L, flushed.get(byteKey(compositeKey(2L))));

        // A second flush is a no-op now that all entries are clean.
        cache.flushAllDirty();
        assertEquals(2, flushCount.get(), "second flush must be a no-op (entries are clean)");
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

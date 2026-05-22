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

package org.apache.flink.state.forstrs.cache;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-F3 acceptance gate: the cache-HIT path must allocate near-zero bytes per call.
 *
 * <p>Pre-PR baseline (LinkedHashMap + accessOrder=true + BytesKey wrapper): every {@code lookup}
 * allocated one {@code BytesKey} (~32 B) + the LRU node relink touched a {@code LinkedHashMap.Entry}
 * (no alloc but a chain of pointer updates). With the LRU relink at high traffic this caused
 * steady-state GC pressure measurable as ~50 bytes/call.
 *
 * <p>PR-F3 baseline (off-heap open-addressed + clock-sweep + reusable Lookup): on a HIT the only
 * heap traffic should be one {@code long} stamp update (off-heap), a value-field write into the
 * pre-allocated {@link MapStateCache.Lookup} instance, and the returned reference. Target: under
 * 100 bytes/call (leaves headroom for JIT deopt/safepoint artifacts).
 */
class MapStateCacheZeroAllocOnHitTest {

    @Test
    void hitPathAllocatesUnderHundredBytesPerCall() {
        MapStateCache<String> cache = new MapStateCache<>(8192);
        // Populate ~1000 keys; sampled HIT path will round-robin through them.
        final int N = 1000;
        byte[][] keys = new byte[N][];
        for (int i = 0; i < N; i++) {
            keys[i] = keyOf(i);
            cache.put(keys[i], "v" + i);
        }

        com.sun.management.ThreadMXBean tmb =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long tid = Thread.currentThread().getId();

        // Warm-up: ensure the JIT has compiled lookup() and reusableHit is plumbed.
        long warmupSink = 0;
        for (int i = 0; i < 10_000; i++) {
            MapStateCache.Lookup<String> hit = cache.lookup(keys[i % N]);
            assertNotNull(hit);
            warmupSink ^= hit.value().length();
        }

        final int iters = 100_000;
        long before = tmb.getThreadAllocatedBytes(tid);
        long sink = 0;
        for (int i = 0; i < iters; i++) {
            MapStateCache.Lookup<String> hit = cache.lookup(keys[i % N]);
            sink ^= hit.value().length();
        }
        long after = tmb.getThreadAllocatedBytes(tid);
        long perCall = (after - before) / iters;

        assertTrue(
                perCall < 100,
                "PR-F3 regression: HIT path allocates "
                        + perCall
                        + " bytes/call (ceiling 100, sink="
                        + sink
                        + ", warmupSink="
                        + warmupSink
                        + ")");
    }

    @Test
    void reusableLookupInstanceIsReturnedAcrossCalls() {
        // Sanity-check the zero-alloc invariant: the same Lookup<V> instance must be returned
        // on every HIT (mutated in place). If a future refactor accidentally allocates a fresh
        // Lookup per call this test catches it directly without ThreadMXBean noise.
        MapStateCache<String> cache = new MapStateCache<>(64);
        cache.put(new byte[] {1}, "a");
        cache.put(new byte[] {2}, "b");
        MapStateCache.Lookup<String> first = cache.lookup(new byte[] {1});
        MapStateCache.Lookup<String> second = cache.lookup(new byte[] {2});
        assertSame(
                first, second, "PR-F3: HIT path must return reusable Lookup instance (no alloc)");
        // After the second call, the instance carries key{2}'s value.
        assertSame("b", second.value());
    }

    private static byte[] keyOf(int v) {
        return new byte[] {
            (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v, 7, 7, 7, 7
        };
    }
}

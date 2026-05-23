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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReducingAggregatingCacheTest {

    @Test
    void classExists() {
        assertNotNull(ReducingAggregatingCache.class);
    }

    @Test
    void hitAndMissSemantics() {
        List<byte[]> flushedKeys = new ArrayList<>();
        ReducingAggregatingCache<Integer, Integer> cache =
                new ReducingAggregatingCache<>(
                        (acc, in) -> acc == null ? in : acc + in, (k, v) -> flushedKeys.add(k));

        byte[] key = new byte[] {1, 2, 3};

        // Initial state: miss
        assertFalse(cache.contains(key));
        assertFalse(cache.tryFold(key, 10));

        // After put: hit
        cache.put(key, 5);
        assertTrue(cache.contains(key));
        assertEquals(Integer.valueOf(5), cache.peek(key));
        assertTrue(cache.isDirty(key));

        // B8-H1: tryFold on hit returns true; the post-fold accumulator is read via peek().
        assertTrue(cache.tryFold(key, 10));
        assertEquals(Integer.valueOf(15), cache.peek(key));
    }

    @Test
    void flushAllDirtyCallsCallback() {
        List<byte[]> flushedKeys = new ArrayList<>();
        List<Integer> flushedVals = new ArrayList<>();
        ReducingAggregatingCache<Integer, Integer> cache =
                new ReducingAggregatingCache<>(
                        Integer::sum,
                        (k, v) -> {
                            flushedKeys.add(k);
                            flushedVals.add(v);
                        });

        byte[] k1 = new byte[] {1};
        byte[] k2 = new byte[] {2};
        cache.put(k1, 100);
        cache.put(k2, 200);

        cache.flushAllDirty();

        assertEquals(2, flushedKeys.size());
        assertFalse(cache.isDirty(k1));
        assertFalse(cache.isDirty(k2));
        assertTrue(flushedVals.contains(100));
        assertTrue(flushedVals.contains(200));
    }

    @Test
    void lruEvictionFlushesEldestDirtyEntry() {
        List<byte[]> evicted = new ArrayList<>();
        // Very small capacity: 2 entries max
        ReducingAggregatingCache<Integer, Integer> cache =
                new ReducingAggregatingCache<>(Integer::sum, (k, v) -> evicted.add(k), 2);

        cache.put(new byte[] {1}, 1);
        cache.put(new byte[] {2}, 2);
        // Adding a third entry evicts the eldest (key {1})
        cache.put(new byte[] {3}, 3);

        // Eviction should have triggered the flush callback for key {1}
        assertEquals(1, evicted.size());
        assertArrayEquals(new byte[] {1}, evicted.get(0));
    }

    @Test
    void clearDropsEntries() {
        ReducingAggregatingCache<Integer, Integer> cache =
                new ReducingAggregatingCache<>(Integer::sum, (k, v) -> {});
        cache.put(new byte[] {1}, 42);
        assertEquals(1, cache.size());
        cache.clear();
        assertEquals(0, cache.size());
    }

    // ----------------- A6-H1: generation-counter race tests -----------------

    /**
     * A6-H1 regression: an in-flight miss-resolve lambda that captured its generation BEFORE
     * a concurrent {@code onClear → invalidate} fired must SKIP its put — otherwise the stale
     * accumulator survives the DELETE and the next {@code flushAllDirty} writes it back,
     * resurrecting the cleared entry past the engine-side DELETE.
     */
    @Test
    void invalidateBumpsGenAndPutIfGenRefusesStaleResolve() {
        List<byte[]> flushed = new ArrayList<>();
        ReducingAggregatingCache<Integer, Integer> cache =
                new ReducingAggregatingCache<>(Integer::sum, (k, v) -> flushed.add(k));
        byte[] key = new byte[] {7, 7, 7};

        // Resolve lambda captures gen=0 BEFORE issuing GET (no prior invalidations).
        long capturedGen = cache.currentGen(key);
        assertEquals(0L, capturedGen);

        // Concurrent onClear fires BEFORE the resolve lambda runs its put.
        cache.invalidate(key);
        assertTrue(cache.currentGen(key) > capturedGen, "invalidate must bump the gen");

        // The stale resolve callback now tries to put — putIfGen must refuse.
        boolean stored = cache.putIfGen(key, 42, capturedGen);
        assertFalse(stored, "putIfGen must refuse a stale-gen put");
        assertFalse(cache.contains(key), "no entry must be in cache after stale put is refused");

        // A flushAllDirty must NOT call the flush callback (the cache is empty / clean).
        cache.flushAllDirty();
        assertEquals(0, flushed.size(), "no entry must be flushed — DELETE survives the race");
    }

    /**
     * A6-H1 happy path: when no {@code invalidate} fires between resolve-issue and resolve-store,
     * the miss-resolve put proceeds normally and the entry is cached / dirty.
     */
    @Test
    void putIfGenAcceptsMatchingGen() {
        ReducingAggregatingCache<Integer, Integer> cache =
                new ReducingAggregatingCache<>(Integer::sum, (k, v) -> {});
        byte[] key = new byte[] {9};
        long g = cache.currentGen(key);
        assertTrue(cache.putIfGen(key, 100, g), "putIfGen must succeed when gen matches");
        assertTrue(cache.contains(key));
        assertEquals(Integer.valueOf(100), cache.peek(key));
        assertTrue(cache.isDirty(key));
    }

    /**
     * A6-H1 concurrency probe: a {@link java.util.concurrent.CountDownLatch} interleaves a
     * mailbox-thread {@code onClear} with a resolver-thread {@code putIfGen}. The resolver
     * captures the gen first; the mailbox bumps it; the resolver's put must be refused. Runs
     * the interleave 64 times to amplify any TOCTOU window — every iteration must observe the
     * same outcome (no resurrection).
     */
    @Test
    void putIfGenRaceUnderConcurrentInvalidate() throws Exception {
        for (int iter = 0; iter < 64; iter++) {
            ReducingAggregatingCache<Integer, Integer> cache =
                    new ReducingAggregatingCache<>(Integer::sum, (k, v) -> {});
            byte[] key = new byte[] {(byte) iter};

            final long capturedGen = cache.currentGen(key);
            final java.util.concurrent.CountDownLatch resolverReady =
                    new java.util.concurrent.CountDownLatch(1);
            final java.util.concurrent.CountDownLatch invalidateDone =
                    new java.util.concurrent.CountDownLatch(1);
            final java.util.concurrent.atomic.AtomicBoolean putAccepted =
                    new java.util.concurrent.atomic.AtomicBoolean(false);

            Thread resolver =
                    new Thread(
                            () -> {
                                try {
                                    resolverReady.countDown();
                                    // Wait until the invalidate has run, then attempt the
                                    // stale-gen put. Must always be refused.
                                    invalidateDone.await();
                                    putAccepted.set(cache.putIfGen(key, 99, capturedGen));
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            });
            resolver.start();

            resolverReady.await();
            cache.invalidate(key);
            invalidateDone.countDown();
            resolver.join(1000L);

            assertFalse(
                    putAccepted.get(),
                    "iter "
                            + iter
                            + ": putIfGen must refuse after a concurrent invalidate bumped"
                            + " the gen");
            assertFalse(cache.contains(key));
        }
    }

    // ----------------- E9-H2: drainPendingFlush throw safety -----------------

    /**
     * E9-H2 regression: when the deferred LRU-eviction flush callback throws (e.g. an FFI
     * engine-PUT failure), the entry MUST be re-stashed into {@code entries} marked dirty so
     * the next {@code flushAllDirty} retries it. Prior shape nulled the slot BEFORE invoking
     * the callback — on throw, the entry was already removed from {@code entries} by
     * {@code removeEldestEntry}, the slot was nulled, and the data was silently lost.
     */
    @Test
    void drainPendingFlushReStashesOnCallbackThrow() {
        final java.util.concurrent.atomic.AtomicBoolean shouldThrow =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        final List<byte[]> deliveredKeys = new ArrayList<>();
        final List<Integer> deliveredVals = new ArrayList<>();

        ReducingAggregatingCache<Integer, Integer> cache =
                new ReducingAggregatingCache<>(
                        Integer::sum,
                        (k, v) -> {
                            if (shouldThrow.get()) {
                                throw new RuntimeException("simulated FFI engine-PUT failure");
                            }
                            deliveredKeys.add(k);
                            deliveredVals.add(v);
                        },
                        2);

        cache.put(new byte[] {1}, 100);
        cache.put(new byte[] {2}, 200);

        // Eviction of {1} triggers drainPendingFlush; the callback throws. The current
        // implementation must re-stash {1} into entries dirty so flushAllDirty retries it.
        RuntimeException thrown = null;
        try {
            cache.put(new byte[] {3}, 300);
        } catch (RuntimeException e) {
            thrown = e;
        }
        assertNotNull(thrown, "the simulated FFI throw must propagate to the caller");
        assertEquals("simulated FFI engine-PUT failure", thrown.getMessage());

        // {1} was evicted from entries before the deferred flush, so re-stash should restore
        // it. {2} and {3} should still be present (no eviction race).
        assertTrue(cache.contains(new byte[] {1}), "evicted key must be re-stashed on flush throw");
        assertEquals(Integer.valueOf(100), cache.peek(new byte[] {1}));
        assertTrue(cache.isDirty(new byte[] {1}), "re-stashed entry must be dirty for retry");

        // Now let the next drain succeed; flushAllDirty should deliver {1}'s accumulator.
        shouldThrow.set(false);
        cache.flushAllDirty();
        assertTrue(
                containsKeyBytes(deliveredKeys, new byte[] {1}),
                "the re-stashed entry must be flushed on the retry");
        // Verify the accumulator value survived the throw round-trip.
        int idx = indexOfKeyBytes(deliveredKeys, new byte[] {1});
        assertEquals(Integer.valueOf(100), deliveredVals.get(idx));
    }

    private static boolean containsKeyBytes(List<byte[]> keys, byte[] needle) {
        return indexOfKeyBytes(keys, needle) >= 0;
    }

    private static int indexOfKeyBytes(List<byte[]> keys, byte[] needle) {
        for (int i = 0; i < keys.size(); i++) {
            if (java.util.Arrays.equals(keys.get(i), needle)) {
                return i;
            }
        }
        return -1;
    }
}

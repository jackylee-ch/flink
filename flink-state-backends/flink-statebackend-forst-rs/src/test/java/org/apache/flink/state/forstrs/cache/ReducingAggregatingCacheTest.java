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
import java.util.Optional;

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
        ReducingAggregatingCache<Integer, Integer> cache = new ReducingAggregatingCache<>(
                (acc, in) -> acc == null ? in : acc + in,
                (k, v) -> flushedKeys.add(k));

        byte[] key = new byte[]{1, 2, 3};

        // Initial state: miss
        assertFalse(cache.contains(key));
        assertEquals(Optional.empty(), cache.tryFold(key, 10));

        // After put: hit
        cache.put(key, 5);
        assertTrue(cache.contains(key));
        assertEquals(Integer.valueOf(5), cache.peek(key));
        assertTrue(cache.isDirty(key));

        // tryFold on hit: combines correctly
        Optional<Integer> result = cache.tryFold(key, 10);
        assertTrue(result.isPresent());
        assertEquals(Integer.valueOf(15), result.get());
        assertEquals(Integer.valueOf(15), cache.peek(key));
    }

    @Test
    void flushAllDirtyCallsCallback() {
        List<byte[]> flushedKeys = new ArrayList<>();
        List<Integer> flushedVals = new ArrayList<>();
        ReducingAggregatingCache<Integer, Integer> cache = new ReducingAggregatingCache<>(
                Integer::sum,
                (k, v) -> { flushedKeys.add(k); flushedVals.add(v); });

        byte[] k1 = new byte[]{1};
        byte[] k2 = new byte[]{2};
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
        ReducingAggregatingCache<Integer, Integer> cache = new ReducingAggregatingCache<>(
                Integer::sum,
                (k, v) -> evicted.add(k),
                2);

        cache.put(new byte[]{1}, 1);
        cache.put(new byte[]{2}, 2);
        // Adding a third entry evicts the eldest (key {1})
        cache.put(new byte[]{3}, 3);

        // Eviction should have triggered the flush callback for key {1}
        assertEquals(1, evicted.size());
        assertArrayEquals(new byte[]{1}, evicted.get(0));
    }

    @Test
    void clearDropsEntries() {
        ReducingAggregatingCache<Integer, Integer> cache = new ReducingAggregatingCache<>(
                Integer::sum,
                (k, v) -> {});
        cache.put(new byte[]{1}, 42);
        assertEquals(1, cache.size());
        cache.clear();
        assertEquals(0, cache.size());
    }
}

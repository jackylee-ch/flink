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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-F3 acceptance gate: verifies clock-sweep eviction correctness at the 1M cap.
 *
 * <p>The previous LinkedHashMap implementation evicted strictly by access order. The new
 * implementation uses per-row access-time stamps + a clock-sweep cursor: on insert when {@code
 * size == maxEntries}, the row with the smallest stamp is evicted.
 *
 * <p>These tests verify:
 *
 * <ul>
 *   <li>Eviction kicks in only when {@code size == maxEntries} (capacity is honored).
 *   <li>The least-recently-touched row is evicted, not a random one.
 *   <li>A lookup promotes the entry (it survives the next eviction).
 *   <li>Tombstones (entries inserted via {@link MapStateCache#remove}) participate in eviction
 *       like any other row.
 *   <li>Default capacity of 1M is honored (smoke test — full 1M would be slow; we verify at 4K).
 * </ul>
 */
class MapStateCacheClockSweepEvictionTest {

    @Test
    void defaultCapIsOneMillion() {
        // Smoke: confirm the public constant matches ArrowBinaryBuffer.MAX_CAPACITY (1M).
        assertEquals(1_048_576, MapStateCache.DEFAULT_MAX_ENTRIES);
    }

    @Test
    void evictionKicksInAtCapacityNotEarlier() {
        MapStateCache<Integer> cache = new MapStateCache<>(8);
        // Fill exactly to capacity — no evictions yet.
        for (int i = 0; i < 8; i++) {
            cache.put(keyOf(i), i);
        }
        assertEquals(8, cache.size());
        for (int i = 0; i < 8; i++) {
            assertNotNull(cache.lookup(keyOf(i)), "entry " + i + " evicted prematurely");
        }
        // 9th insert must evict exactly one.
        cache.put(keyOf(100), 100);
        assertEquals(8, cache.size());
    }

    @Test
    void clockSweepEvictsColdestEntry() {
        MapStateCache<Integer> cache = new MapStateCache<>(4);
        cache.put(keyOf(0), 0); // ts=1
        cache.put(keyOf(1), 1); // ts=2
        cache.put(keyOf(2), 2); // ts=3
        cache.put(keyOf(3), 3); // ts=4
        // Touch keys 1, 2, 3 — key 0 is now coldest.
        cache.lookup(keyOf(1)); // ts=5
        cache.lookup(keyOf(2)); // ts=6
        cache.lookup(keyOf(3)); // ts=7
        // Insert a new key — must evict key 0.
        cache.put(keyOf(4), 4); // evicts coldest, ts=8
        assertNull(cache.lookup(keyOf(0)), "coldest entry must be evicted");
        assertNotNull(cache.lookup(keyOf(1)));
        assertNotNull(cache.lookup(keyOf(2)));
        assertNotNull(cache.lookup(keyOf(3)));
        assertNotNull(cache.lookup(keyOf(4)));
    }

    @Test
    void lookupRefreshesAccessStamp() {
        MapStateCache<Integer> cache = new MapStateCache<>(3);
        cache.put(keyOf(0), 0);
        cache.put(keyOf(1), 1);
        cache.put(keyOf(2), 2);
        // Refresh key 0 — should now be the youngest.
        cache.lookup(keyOf(0));
        // Insert two more; with key 0 most-recently-used, keys 1 and 2 should be evicted first.
        cache.put(keyOf(3), 3); // evicts oldest ≠ key 0 → key 1
        cache.put(keyOf(4), 4); // evicts key 2
        assertNotNull(cache.lookup(keyOf(0)), "refreshed key must survive");
        assertNull(cache.lookup(keyOf(1)));
        assertNull(cache.lookup(keyOf(2)));
        assertNotNull(cache.lookup(keyOf(3)));
        assertNotNull(cache.lookup(keyOf(4)));
    }

    @Test
    void tombstonesEvictedLikeLiveEntries() {
        // remove() inserts a tombstone — it counts toward capacity and is subject to eviction.
        MapStateCache<Integer> cache = new MapStateCache<>(2);
        cache.remove(keyOf(0)); // tombstone, ts=1
        cache.put(keyOf(1), 1); // live, ts=2
        cache.put(keyOf(2), 2); // evicts tombstone (oldest), ts=3
        assertNull(cache.lookup(keyOf(0)), "tombstone must be evicted as the oldest entry");
        assertNotNull(cache.lookup(keyOf(1)));
        assertNotNull(cache.lookup(keyOf(2)));
    }

    @Test
    void capacityHonoredAtLargeScale() {
        // Use a capacity of 4096 (not the full 1M) so the test stays fast but still exercises
        // many eviction cycles. 8× cap inserts -> 7× cap evictions.
        final int cap = 4096;
        MapStateCache<Integer> cache = new MapStateCache<>(cap);
        // Insert 8 × cap entries.
        for (int i = 0; i < cap * 8; i++) {
            cache.put(keyOf(i), i);
            assertTrue(
                    cache.size() <= cap,
                    "capacity exceeded at insert " + i + " size=" + cache.size());
        }
        assertEquals(cap, cache.size(), "after stress insert, size == cap");
        // Most-recently-inserted entries should still be present (last cap inserts at minimum
        // some subset due to clock-sweep selecting coldest).
        int lastIdx = cap * 8 - 1;
        assertNotNull(cache.lookup(keyOf(lastIdx)), "most recent insert must be present");
    }

    @Test
    void overwriteDoesNotCountAsNewInsert() {
        MapStateCache<Integer> cache = new MapStateCache<>(2);
        cache.put(keyOf(0), 0);
        cache.put(keyOf(1), 1);
        cache.put(keyOf(0), 99); // overwrite — should NOT trigger eviction.
        assertEquals(2, cache.size());
        assertEquals(Integer.valueOf(99), cache.lookup(keyOf(0)).value());
        assertEquals(Integer.valueOf(1), cache.lookup(keyOf(1)).value());
    }

    @Test
    void clearResetsCapacityAndState() {
        MapStateCache<Integer> cache = new MapStateCache<>(4);
        for (int i = 0; i < 4; i++) {
            cache.put(keyOf(i), i);
        }
        assertEquals(4, cache.size());
        cache.clear();
        assertEquals(0, cache.size());
        // After clear, fresh inserts should not trigger eviction until capacity is full again.
        for (int i = 100; i < 104; i++) {
            cache.put(keyOf(i), i);
        }
        assertEquals(4, cache.size());
        for (int i = 100; i < 104; i++) {
            assertNotNull(cache.lookup(keyOf(i)));
        }
    }

    private static byte[] keyOf(int v) {
        return new byte[] {
            (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v
        };
    }
}

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapStateCacheTest {

    @Test
    void classExists() {
        assertNotNull(MapStateCache.class);
    }

    @Test
    void missReturnsNull() {
        MapStateCache<String> cache = new MapStateCache<>();
        assertNull(cache.lookup(new byte[] {1, 2, 3}));
    }

    @Test
    void putThenLookupReturnsValue() {
        MapStateCache<String> cache = new MapStateCache<>();
        byte[] key = new byte[] {1, 2, 3};
        cache.put(key, "hello");
        MapStateCache.Lookup<String> hit = cache.lookup(key);
        assertNotNull(hit);
        assertTrue(hit.cached());
        assertEquals("hello", hit.value());
    }

    @Test
    void putNullStoresTombstoneAndReturnsCached() {
        MapStateCache<String> cache = new MapStateCache<>();
        byte[] key = new byte[] {4, 5, 6};
        cache.put(key, null);
        MapStateCache.Lookup<String> hit = cache.lookup(key);
        // tombstone — cached but value is null
        assertNotNull(hit);
        assertTrue(hit.cached());
        assertNull(hit.value());
    }

    @Test
    void removeStoresTombstone() {
        MapStateCache<String> cache = new MapStateCache<>();
        byte[] key = new byte[] {7, 8, 9};
        cache.put(key, "v");
        cache.remove(key);
        MapStateCache.Lookup<String> hit = cache.lookup(key);
        assertNotNull(hit, "remove must record a tombstone, not delete the entry");
        assertTrue(hit.cached());
        assertNull(hit.value());
    }

    @Test
    void contentEqualityForByteArrays() {
        // Different byte[] instances with same contents must hit the same cache slot.
        MapStateCache<Integer> cache = new MapStateCache<>();
        cache.put(new byte[] {10, 20, 30}, 42);
        MapStateCache.Lookup<Integer> hit = cache.lookup(new byte[] {10, 20, 30});
        assertNotNull(hit);
        assertEquals(Integer.valueOf(42), hit.value());
    }

    @Test
    void clearRemovesAllEntries() {
        MapStateCache<String> cache = new MapStateCache<>();
        cache.put(new byte[] {1}, "a");
        cache.put(new byte[] {2}, "b");
        assertEquals(2, cache.size());
        cache.clear();
        assertEquals(0, cache.size());
        assertNull(cache.lookup(new byte[] {1}));
    }

    @Test
    void lruEvictionRespectsMaxEntries() {
        MapStateCache<Integer> cache = new MapStateCache<>(/* maxEntries */ 2);
        cache.put(new byte[] {1}, 1);
        cache.put(new byte[] {2}, 2);
        cache.put(new byte[] {3}, 3); // evicts key {1} (oldest, accessOrder)
        assertEquals(2, cache.size());
        assertNull(cache.lookup(new byte[] {1}), "oldest entry should have been evicted");
        assertNotNull(cache.lookup(new byte[] {2}));
        assertNotNull(cache.lookup(new byte[] {3}));
    }

    @Test
    void lruAccessOrderPromotesOnLookup() {
        MapStateCache<Integer> cache = new MapStateCache<>(2);
        cache.put(new byte[] {1}, 1);
        cache.put(new byte[] {2}, 2);
        // Lookup key {1} promotes it to most-recently-used
        assertNotNull(cache.lookup(new byte[] {1}));
        cache.put(new byte[] {3}, 3); // should evict key {2} now, not key {1}
        assertNotNull(cache.lookup(new byte[] {1}));
        assertNull(cache.lookup(new byte[] {2}));
        assertNotNull(cache.lookup(new byte[] {3}));
    }

    @Test
    void overwriteUpdatesValue() {
        MapStateCache<String> cache = new MapStateCache<>();
        byte[] key = new byte[] {99};
        cache.put(key, "old");
        cache.put(key, "new");
        MapStateCache.Lookup<String> hit = cache.lookup(key);
        assertNotNull(hit);
        assertEquals("new", hit.value());
        assertEquals(1, cache.size());
    }

    @Test
    void tombstoneReplacedByLiveValue() {
        // A subsequent put after remove must convert the tombstone back to a live entry.
        MapStateCache<String> cache = new MapStateCache<>();
        byte[] key = new byte[] {7};
        cache.remove(key);
        cache.put(key, "alive");
        MapStateCache.Lookup<String> hit = cache.lookup(key);
        assertNotNull(hit);
        assertTrue(hit.cached());
        assertEquals("alive", hit.value());
    }

    @Test
    void defaultMaxEntriesIsBounded() {
        // Sanity: the default constructor uses a finite cap.
        MapStateCache<Integer> cache = new MapStateCache<>();
        // Stuff a lot but stay well under DEFAULT_MAX_ENTRIES so nothing evicts.
        for (int i = 0; i < 1000; i++) {
            cache.put(intBytes(i), i);
        }
        assertEquals(1000, cache.size());
        MapStateCache.Lookup<Integer> hit = cache.lookup(intBytes(500));
        assertNotNull(hit);
        assertEquals(Integer.valueOf(500), hit.value());
        assertFalse(cache.size() > MapStateCache.DEFAULT_MAX_ENTRIES);
    }

    private static byte[] intBytes(int v) {
        return new byte[] {
            (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v
        };
    }
}

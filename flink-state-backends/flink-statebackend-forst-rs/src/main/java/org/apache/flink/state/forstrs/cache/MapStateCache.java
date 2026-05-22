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

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-state-instance LRU cache for MapState values. Eliminates engine round-trips for repeated
 * reads of the same {@code (operatorKey, userKey)} pair within a window.
 *
 * <h3>Write-through semantics</h3>
 *
 * <p>Every {@link #put} or {@link #remove} updates the cache AND must be followed by the
 * corresponding engine write by the caller. The cache and engine are always in sync after a write
 * completes; there is no in-flight dirty state to flush at barriers.
 *
 * <h3>Single-threaded</h3>
 *
 * <p>Assumes the caller (a ForStRs state class) is invoked from Flink's async-state framework which
 * serializes per-record operations via the RecordContext lock. No internal synchronization.
 *
 * <h3>Bounded capacity</h3>
 *
 * <p>LRU eviction at {@code maxEntries} (default 256K). Memory cost: ~64 bytes/entry × 256K = ~16
 * MB per state instance — acceptable for state-heavy workloads where the cache hit rate is high.
 *
 * <h3>Tombstones</h3>
 *
 * <p>A {@code null} value (deletion or known-missing) is stored as a sentinel so that we can
 * distinguish "cached: known-missing" from "not in cache" and avoid re-dispatching the GET. Callers
 * receive a {@link Lookup} which carries an explicit {@code cached} flag.
 *
 * @param <V> the user value type
 */
@Internal
public final class MapStateCache<V> {

    /** Default max entries; sized for Nexmark Q11/Q12 working sets. */
    public static final int DEFAULT_MAX_ENTRIES = 256 * 1024;

    /** Sentinel used to distinguish "cached: known-missing" from "not in cache". */
    private static final Object TOMBSTONE = new Object();

    private final LinkedHashMap<BytesKey, Object> entries;

    public MapStateCache() {
        this(DEFAULT_MAX_ENTRIES);
    }

    public MapStateCache(int maxEntries) {
        this.entries =
                new LinkedHashMap<>(Math.min(maxEntries, 1024), 0.75f, /* accessOrder */ true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<BytesKey, Object> eldest) {
                        return size() > maxEntries;
                    }
                };
    }

    /**
     * Looks up the value for {@code key}. Returns {@code null} on cache miss, or a {@link Lookup}
     * with {@code cached=true} if the entry exists (hit or known-missing tombstone).
     *
     * @param key raw composite key bytes (must match the bytes used in {@link #put}/{@link #remove})
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public Lookup<V> lookup(byte[] key) {
        Object v = entries.get(new BytesKey(key));
        if (v == null) {
            return null; // miss
        }
        if (v == TOMBSTONE) {
            return new Lookup<>(null, true); // known missing
        }
        return new Lookup<>((V) v, true); // hit
    }

    /**
     * Write-through cache update. Caller must subsequently dispatch the PUT to the engine.
     *
     * <p>A {@code null} value is stored as a tombstone so that subsequent lookups return a {@code
     * cached=true} result and avoid hitting the engine.
     */
    public void put(byte[] key, @Nullable V value) {
        entries.put(new BytesKey(key), value == null ? TOMBSTONE : value);
    }

    /** Records a tombstone for {@code key}. Caller must subsequently dispatch the REMOVE. */
    public void remove(byte[] key) {
        entries.put(new BytesKey(key), TOMBSTONE);
    }

    /**
     * Clears all cached entries. Called when the framework issues a {@code MapState.clear()} —
     * conservatively nukes everything (the cache key includes the operator key, but a partial
     * invalidation would require a scan).
     */
    public void clear() {
        entries.clear();
    }

    /**
     * PR-A6 (S1-11 / E2-HIGH-2): targeted invalidation of cache entries whose composite key starts
     * with {@code prefix}. Called by {@link
     * org.apache.flink.state.forstrs.state.ForStRsMapStateV2#buildDBPutRequest} when the framework
     * dispatches a {@code StateRequestType.CLEAR} — the engine will delete the namespace prefix on
     * the next batch dispatch, and we must drop matching cache rows here so that a subsequent
     * {@code asyncGet} for any userKey under the cleared (operatorKey, namespace) does not return a
     * stale cached value.
     *
     * <p>The cache key produced by {@code ForStRsMapStateV2.serializeMapEntryKey} is
     * {@code [KEY_PREFIX][serialize(K)][/][stateName][/][serialize(N)][serialize(UK)]}. Passing the
     * iter-prefix bytes (everything up to and including the serialized namespace) here invalidates
     * exactly the entries owned by the cleared map-state row, without touching entries under other
     * namespaces or other operator keys that still live in the cache.
     *
     * <p>O(n) linear scan over the LRU; acceptable because CLEAR is end-of-window and rare relative
     * to the per-record GET/PUT path that this cache exists to accelerate. Returns the number of
     * entries removed (visible for tests / observability).
     */
    public int clearForPrefix(byte[] prefix) {
        if (entries.isEmpty()) {
            return 0;
        }
        int removed = 0;
        java.util.Iterator<Map.Entry<BytesKey, Object>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            byte[] k = it.next().getKey().bytes;
            if (startsWith(k, prefix)) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    public int size() {
        return entries.size();
    }

    /** Lookup result: value plus an explicit cache-hit indicator. */
    public record Lookup<V>(@Nullable V value, boolean cached) {}

    // -----------------------------------------------------------------
    // Content-equality wrapper for byte[] keys in LinkedHashMap.
    // -----------------------------------------------------------------

    private static final class BytesKey {
        final byte[] bytes;
        final int hash;

        BytesKey(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof BytesKey bk && Arrays.equals(bytes, bk.bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}

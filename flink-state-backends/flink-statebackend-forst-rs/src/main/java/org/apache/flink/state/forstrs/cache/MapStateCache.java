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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Per-state-instance off-heap cache for MapState values. Eliminates engine round-trips for repeated
 * reads of the same {@code (operatorKey, userKey)} pair within a window.
 *
 * <h3>PR-F3 — off-heap open-addressed, clock-sweep eviction</h3>
 *
 * <p>This class replaces a previous {@code LinkedHashMap<BytesKey, Object>} implementation that
 * relinked an entry node on every cache hit and required a {@code new BytesKey(byte[])} wrapper on
 * every lookup. Both costs were on the V2 async hot path (Q16: 200K bidders × 100K auctions) and
 * caused steady-state allocation pressure + LRU-churn cache misses when the working set exceeded
 * 256K entries.
 *
 * <p>Storage layout (all off-heap, except the {@code values} reference array which must remain
 * on-heap because user values are arbitrary Java objects):
 *
 * <ul>
 *   <li>{@link #keyData} — flat byte buffer holding all cache keys concatenated.
 *   <li>{@link #keyOffsets} — {@code int4} per row: byte offset into {@code keyData} for that
 *       row's key (the next row's offset gives the length).
 *   <li>{@link #hashIndex} — open-addressed {@code (hash:int, row:int)} slot pairs; sized to
 *       {@code 2 × capacity} for ~50 % load factor.
 *   <li>{@link #accessTime} — {@code long8} per row: monotonic clock stamp updated on every
 *       lookup/put. Clock-sweep eviction picks the row with the smallest stamp.
 *   <li>{@link #values} — on-heap {@code Object[capacity]}: user values (or {@link #TOMBSTONE}).
 * </ul>
 *
 * <h3>Write-through semantics</h3>
 *
 * <p>Every {@link #put} or {@link #remove} updates the cache AND must be followed by the
 * corresponding engine write by the caller. The cache and engine are always in sync after a write
 * completes; there is no in-flight dirty state to flush at barriers.
 *
 * <h3>Single-threaded</h3>
 *
 * <p>Assumes the caller (a ForStRs state class) is invoked from Flink's async-state framework
 * which serializes per-record operations via the RecordContext lock. No internal synchronization.
 *
 * <h3>Bounded capacity</h3>
 *
 * <p>Default capacity bumped from 256K to 1M (matches {@link
 * org.apache.flink.state.forstrs.state.ArrowBinaryBuffer#MAX_CAPACITY}). Memory cost: roughly
 * {@code (avg key bytes + 24) × capacity} ≈ 80 MB at 1M with 56-byte keys — acceptable for
 * state-heavy workloads where the cache hit rate is high.
 *
 * <h3>Tombstones</h3>
 *
 * <p>A {@code null} value (deletion or known-missing) is stored as the {@link #TOMBSTONE}
 * sentinel so that we can distinguish "cached: known-missing" from "not in cache" and avoid
 * re-dispatching the GET. Callers receive a {@link Lookup} which carries an explicit {@code
 * cached} flag.
 *
 * <h3>Zero-alloc HIT path</h3>
 *
 * <p>{@link #lookup} returns the same per-cache reusable {@link Lookup} instance on every hit
 * (mutated in place). Callers MUST consume the returned value before issuing another {@code
 * lookup} on the same cache — which matches the single-threaded contract above. No {@code
 * BytesKey} wrapper is allocated; the key bytes are compared directly via {@link
 * MemorySegment#mismatch}.
 *
 * @param <V> the user value type
 */
@Internal
public final class MapStateCache<V> implements AutoCloseable {

    /**
     * Default max entries; PR-F3 bumped from 256K to 1M to absorb Q16 working sets without LRU
     * churn.
     */
    public static final int DEFAULT_MAX_ENTRIES = 1_048_576;

    /** Sentinel used to distinguish "cached: known-missing" from "not in cache". */
    private static final Object TOMBSTONE = new Object();

    private static final int EMPTY_SLOT = -1;
    private static final int TOMBSTONE_SLOT = -2;

    private final int maxEntries;
    private final int capacity; // power of 2 >= maxEntries (for hash mask)
    private final int hashSlots; // 2 * capacity
    private final int hashMask;

    private final Arena arena;
    private final MemorySegment keyOffsets; // capacity × int4 — start offset into keyData
    private final MemorySegment keyLengths; // capacity × int4 — length of row's key bytes
    private MemorySegment keyData; // grows on demand
    private final MemorySegment hashIndex; // 2 × capacity × (int hash, int row)
    private final MemorySegment accessTime; // capacity × long8
    private final Object[] values; // capacity Object refs (on-heap; user values can be anything)
    /**
     * B6-H6: reverse map row → hash-slot. Lifecycle and capacity mirror {@link #keyOffsets} /
     * {@link #keyLengths} (allocated on the same {@link #arena}, sized to {@link #capacity}
     * entries × 4 bytes). Lets {@link #removeFromHashIndex} and {@link #relabelHashIndex} drop
     * from O(hashSlots) linear scans to O(1) pointer reads.
     *
     * <p>Slot id is the integer index into the {@code (hash:int, row:int)} pair table — i.e. the
     * same {@code i} that the open-addressed probe loop in {@link #insertHashIndex} settled on.
     * {@code -1} means "no slot" (row is being initialized or has just been evicted).
     */
    private final MemorySegment rowToSlot; // capacity × int4

    private long keyDataCapacity;
    private long keyDataUsed;
    private int size;
    private long clock; // monotonic counter for access-time stamps
    private int clockHand; // sweep cursor for eviction

    /**
     * Reusable Lookup instance returned by {@link #lookup}. Caller must consume before re-calling
     * lookup on the same cache instance. Cache is single-threaded per Flink slot so this is safe.
     */
    private final Lookup<V> reusableHit = new Lookup<>();

    public MapStateCache() {
        this(DEFAULT_MAX_ENTRIES);
    }

    public MapStateCache(int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be >= 1");
        }
        this.maxEntries = maxEntries;
        // Round capacity up to next power of 2 so hashMask works.
        int cap = 1;
        while (cap < maxEntries) {
            cap <<= 1;
        }
        this.capacity = cap;
        this.hashSlots = cap * 2;
        this.hashMask = hashSlots - 1;
        this.arena = Arena.ofShared();
        this.keyOffsets = arena.allocate((long) cap * Integer.BYTES);
        this.keyLengths = arena.allocate((long) cap * Integer.BYTES);
        this.keyDataCapacity = Math.max(1024L, (long) cap * 32L);
        this.keyData = arena.allocate(keyDataCapacity);
        this.hashIndex = arena.allocate((long) hashSlots * 2 * Integer.BYTES);
        this.accessTime = arena.allocate((long) cap * Long.BYTES);
        this.values = new Object[cap];
        // B6-H6: reverse map row → slot. Initialized to -1 ("no slot"); written by
        // insertHashIndex on each successful insert and read by removeFromHashIndex /
        // relabelHashIndex for O(1) eviction. The -1 init guarantees that a
        // removeFromHashIndex on a never-inserted row reads the defensive `slot < 0` branch
        // rather than mis-tombstoning slot 0.
        this.rowToSlot = arena.allocate((long) cap * Integer.BYTES);
        for (int i = 0; i < cap; i++) {
            rowToSlot.set(ValueLayout.JAVA_INT, (long) i * Integer.BYTES, -1);
        }
        // initialize hash-index rows to EMPTY_SLOT.
        for (int i = 0; i < hashSlots; i++) {
            hashIndex.set(
                    ValueLayout.JAVA_INT,
                    (long) i * 2 * Integer.BYTES + Integer.BYTES,
                    EMPTY_SLOT);
        }
    }

    /**
     * Looks up the value for {@code key}. Returns {@code null} on cache miss, or a {@link Lookup}
     * with {@code cached=true} if the entry exists (hit or known-missing tombstone).
     *
     * <p>HIT path: zero allocation. The same {@link Lookup} instance is returned every call and
     * mutated in place.
     *
     * @param key raw composite key bytes (must match the bytes used in {@link #put}/{@link
     *     #remove})
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public Lookup<V> lookup(byte[] key) {
        int row = findRow(key);
        if (row < 0) {
            return null; // miss
        }
        // Update access-time stamp for clock-sweep eviction (no node relink).
        accessTime.set(ValueLayout.JAVA_LONG, (long) row * Long.BYTES, ++clock);
        Object v = values[row];
        if (v == TOMBSTONE) {
            reusableHit.value = null;
        } else {
            reusableHit.value = (V) v;
        }
        reusableHit.cached = true;
        return reusableHit;
    }

    /**
     * Write-through cache update. Caller must subsequently dispatch the PUT to the engine.
     *
     * <p>A {@code null} value is stored as a tombstone so that subsequent lookups return a
     * {@code cached=true} result and avoid hitting the engine.
     */
    public void put(byte[] key, @Nullable V value) {
        Object stored = value == null ? TOMBSTONE : value;
        int row = findRow(key);
        if (row >= 0) {
            values[row] = stored;
            accessTime.set(ValueLayout.JAVA_LONG, (long) row * Long.BYTES, ++clock);
            return;
        }
        // Insert.
        if (size >= maxEntries) {
            evictClockSweep();
        }
        row = size++;
        appendKey(row, key);
        values[row] = stored;
        accessTime.set(ValueLayout.JAVA_LONG, (long) row * Long.BYTES, ++clock);
        insertHashIndex(hashOf(key), row);
    }

    /** Records a tombstone for {@code key}. Caller must subsequently dispatch the REMOVE. */
    public void remove(byte[] key) {
        put(key, null);
    }

    /**
     * Clears all cached entries. Called when the framework issues a {@code MapState.clear()} —
     * conservatively nukes everything (the cache key includes the operator key, but a partial
     * invalidation would require a scan).
     */
    public void clear() {
        int prevSize = size;
        size = 0;
        keyDataUsed = 0;
        clock = 0;
        clockHand = 0;
        // R24-M3: reset the LIVE PREFIX of accessTime to 0 so the next eviction after a
        // re-fill cannot inherit a stale clock stamp from before the clear. We bound the
        // sweep to {@code prevSize} (not {@code capacity}) so the cost stays proportional
        // to the working set, not the total allocation. The {@link #evictClockSweep} fix
        // (seed `oldest=Long.MAX_VALUE`) handles this defensively too; the two fixes work
        // together — this one is correctness in {@code clear()}, that one is correctness
        // at the comparison site.
        for (int i = 0; i < prevSize; i++) {
            accessTime.set(ValueLayout.JAVA_LONG, (long) i * Long.BYTES, 0L);
        }
        // We don't need to zero the values[] entries individually — they're shadowed by size.
        // But for GC promptness, drop references on the live prefix only.
        // (Not iterating the whole capacity to avoid touching 1M slots on every clear.)
        // Re-initialize hash slots to EMPTY_SLOT.
        for (int i = 0; i < hashSlots; i++) {
            hashIndex.set(
                    ValueLayout.JAVA_INT,
                    (long) i * 2 * Integer.BYTES + Integer.BYTES,
                    EMPTY_SLOT);
        }
        // B6-H6: reset only the live prefix of rowToSlot — same bookkeeping cost as the
        // explicit-null on values[] below. Stale entries past `prevSize` were already -1 from
        // construction or from prior eviction.
        for (int i = 0; i < prevSize; i++) {
            rowToSlot.set(ValueLayout.JAVA_INT, (long) i * Integer.BYTES, -1);
        }
        // Null out values up to the previous size to release Java refs for GC.
        // Captured implicitly by overwriting on next insert — but explicit-null here on clear()
        // is the rare event that justifies the small bookkeeping cost.
        java.util.Arrays.fill(values, null);
    }

    /**
     * PR-A6 (S1-11 / E2-HIGH-2): targeted invalidation of cache entries whose composite key starts
     * with {@code prefix}. Called by {@link
     * org.apache.flink.state.forstrs.state.ForStRsMapStateV2#buildDBPutRequest} when the framework
     * dispatches a {@code StateRequestType.CLEAR} — the engine will delete the namespace prefix on
     * the next batch dispatch, and we must drop matching cache rows here so that a subsequent
     * {@code asyncGet} for any userKey under the cleared (operatorKey, namespace) does not return
     * a stale cached value.
     *
     * <p>O(size) linear scan over the row table; acceptable because CLEAR is end-of-window and
     * rare relative to the per-record GET/PUT path that this cache exists to accelerate. Returns
     * the number of entries removed (visible for tests / observability).
     */
    public int clearForPrefix(byte[] prefix) {
        if (size == 0) {
            return 0;
        }
        int removed = 0;
        // We must iterate rows directly (not the hash index) because we need to rebuild
        // the index after removal. Strategy: mark removed rows, then compact in a second pass.
        // For simplicity (CLEAR is rare), do a full rebuild.
        // Stash surviving (key, value) into temp arrays, clear, then re-insert.
        // To preserve the C-A6 contract, we MUST also wipe tombstones (known-missing entries).
        // Detect matches first.
        boolean[] toRemove = new boolean[size];
        for (int row = 0; row < size; row++) {
            if (rowKeyStartsWith(row, prefix)) {
                toRemove[row] = true;
                removed++;
            }
        }
        if (removed == 0) {
            return 0;
        }
        if (removed == size) {
            clear();
            return removed;
        }
        // Partial: compact surviving rows into a fresh layout. Cheap because CLEAR is rare.
        compactExcluding(toRemove);
        return removed;
    }

    public int size() {
        return size;
    }

    /**
     * D5-H2: releases the off-heap arena that backs {@link #keyOffsets}, {@link #keyLengths},
     * {@link #keyData}, {@link #hashIndex}, and {@link #accessTime}. Idempotent — safe to call
     * multiple times; subsequent calls after the arena has been closed are observed as a no-op
     * via {@link Arena#close} throwing {@code IllegalStateException}, which we swallow.
     *
     * <p>Owning class invariant: every {@link MapStateCache} instance is owned by exactly one
     * {@code ForStRsMapStateV2} (see {@code state/ForStRsMapStateV2.java:91}). The owner state
     * is in turn registered with the backend ({@code registeredMapStatesV2}) and disposed when
     * the backend disposes; that disposal chain MUST invoke this method so the shared arena
     * is released. Without it, every V2 MapState created during the lifetime of the JVM kept
     * its 5 off-heap segments live until process exit (perma-leak).
     */
    @Override
    public void close() {
        try {
            arena.close();
        } catch (IllegalStateException alreadyClosedOrInUse) {
            // Arena.close throws IllegalStateException if a thread still holds a confined
            // resource or if the arena was already closed. The cache is single-threaded by
            // contract; an already-closed arena is the only realistic case here and is
            // benign (idempotent close).
        }
        // Drop references on the on-heap value array so GC can reclaim user values promptly.
        java.util.Arrays.fill(values, null);
        size = 0;
    }

    // -------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------

    /** Returns the row id for the given key, or -1 if not present. */
    private int findRow(byte[] key) {
        int h = hashOf(key);
        int probe = h & hashMask;
        for (int i = 0; i < hashSlots; i++) {
            int slot = (probe + i) & hashMask;
            int row =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) slot * 2 * Integer.BYTES + Integer.BYTES);
            if (row == EMPTY_SLOT) {
                return -1;
            }
            if (row == TOMBSTONE_SLOT) {
                continue;
            }
            int storedHash =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES);
            if (storedHash == h && keyEquals(row, key)) {
                return row;
            }
        }
        return -1;
    }

    private void insertHashIndex(int hash, int row) {
        int probe = hash & hashMask;
        for (int i = 0; i < hashSlots; i++) {
            int slot = (probe + i) & hashMask;
            int existing =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) slot * 2 * Integer.BYTES + Integer.BYTES);
            if (existing == EMPTY_SLOT || existing == TOMBSTONE_SLOT) {
                hashIndex.set(
                        ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES, hash);
                hashIndex.set(
                        ValueLayout.JAVA_INT,
                        (long) slot * 2 * Integer.BYTES + Integer.BYTES,
                        row);
                // B6-H6: remember which slot this row landed in so eviction is O(1).
                rowToSlot.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, slot);
                return;
            }
        }
        throw new IllegalStateException(
                "MapStateCache hash index full at size=" + size + " capacity=" + capacity);
    }

    /**
     * Clock-sweep eviction. Walks the {@link #accessTime} array starting at {@link #clockHand},
     * decrementing each row's stamp toward zero; when a row's stamp reaches zero (i.e. we've
     * second-chance'd it) we evict that row. This is the classic "second chance" clock policy:
     * approximates LRU with no node-relink work.
     *
     * <p>In practice we just find the row with the smallest access stamp and evict it. The full
     * second-chance walk is unnecessary because the {@link #accessTime} sweep already gives us
     * temporal ordering. We bound the scan length to {@link #size} to keep eviction O(N) in the
     * worst case but amortized closer to O(N / hot-set-size) by starting at clockHand.
     */
    private void evictClockSweep() {
        // R24-M3: seed `oldest` with Long.MAX_VALUE rather than reading accessTime[clockHand].
        // Pre-fix, immediately after {@link #clear()} the {@code accessTime} segment still
        // held stamps from before the clear (clear() reset {@code clock=0} and {@code
        // clockHand=0} but did NOT zero the live prefix of accessTime), so the first eviction
        // after a re-fill compared fresh post-clear stamps against a stale pre-clear stamp
        // at slot 0 and biased the victim toward whatever row happened to inherit row 0.
        // Seeding with MAX_VALUE forces the scan to pick a real row's stamp on the first
        // iteration regardless of any residual data at clockHand.
        int victim = clockHand;
        long oldest = Long.MAX_VALUE;
        for (int i = 0; i < size; i++) {
            int row = (clockHand + i) % size;
            long ts = accessTime.get(ValueLayout.JAVA_LONG, (long) row * Long.BYTES);
            if (ts < oldest) {
                oldest = ts;
                victim = row;
            }
        }
        // Advance clock hand for next sweep so we don't always start at row 0.
        clockHand = (victim + 1) % Math.max(1, size);
        evictRow(victim);
    }

    private void evictRow(int row) {
        // Remove from hash index.
        removeFromHashIndex(row);
        // Swap with last row to keep the row table dense.
        int lastRow = size - 1;
        if (row != lastRow) {
            // Relabel: row `row` inherits lastRow's key (by aliasing the offset/length fields)
            // and value. The actual key bytes in keyData stay put — we just relabel which row
            // points at them. The slot that used to hold `row`'s key is now orphaned in keyData
            // (reclaimed on next clear()).
            int lastKeyStart =
                    keyOffsets.get(ValueLayout.JAVA_INT, (long) lastRow * Integer.BYTES);
            int lastKeyLen =
                    keyLengths.get(ValueLayout.JAVA_INT, (long) lastRow * Integer.BYTES);
            keyOffsets.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, lastKeyStart);
            keyLengths.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, lastKeyLen);
            values[row] = values[lastRow];
            accessTime.set(
                    ValueLayout.JAVA_LONG,
                    (long) row * Long.BYTES,
                    accessTime.get(ValueLayout.JAVA_LONG, (long) lastRow * Long.BYTES));
            // Update hash index: rewrite lastRow -> row.
            relabelHashIndex(lastRow, row);
        }
        values[lastRow] = null;
        size--;
    }

    private void removeFromHashIndex(int row) {
        // B6-H6: O(1) — direct lookup via the reverse map. The previous implementation scanned
        // all `hashSlots` (= 2 × capacity) entries linearly, which dominated insert cost once
        // the working set exceeded `maxEntries` and clock-sweep evictions fired on every put.
        int slot = rowToSlot.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        if (slot < 0) {
            // Defensive: row had no recorded slot. Should not occur in steady state because every
            // live row was inserted via insertHashIndex which writes rowToSlot.
            return;
        }
        hashIndex.set(
                ValueLayout.JAVA_INT,
                (long) slot * 2 * Integer.BYTES + Integer.BYTES,
                TOMBSTONE_SLOT);
        rowToSlot.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, -1);
    }

    private void relabelHashIndex(int fromRow, int toRow) {
        // B6-H6: O(1) — single pointer update on both the hashIndex row slot and the reverse map.
        int slot = rowToSlot.get(ValueLayout.JAVA_INT, (long) fromRow * Integer.BYTES);
        if (slot < 0) {
            return;
        }
        hashIndex.set(
                ValueLayout.JAVA_INT,
                (long) slot * 2 * Integer.BYTES + Integer.BYTES,
                toRow);
        rowToSlot.set(ValueLayout.JAVA_INT, (long) toRow * Integer.BYTES, slot);
        rowToSlot.set(ValueLayout.JAVA_INT, (long) fromRow * Integer.BYTES, -1);
    }

    private void appendKey(int row, byte[] key) {
        if (keyDataUsed + key.length > keyDataCapacity) {
            growKeyData(keyDataUsed + key.length);
        }
        int start = (int) keyDataUsed;
        MemorySegment.copy(
                key, 0, keyData, ValueLayout.JAVA_BYTE, keyDataUsed, key.length);
        keyDataUsed += key.length;
        keyOffsets.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, start);
        keyLengths.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, key.length);
    }

    private void growKeyData(long needed) {
        long newCap = Math.max(keyDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(keyData, 0, newSeg, 0, keyDataUsed);
        this.keyData = newSeg;
        this.keyDataCapacity = newCap;
    }

    private boolean keyEquals(int row, byte[] key) {
        int kStart = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int len = keyLengths.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        if (len != key.length) {
            return false;
        }
        if (len == 0) {
            return true;
        }
        // B6-H7: JIT-intrinsified vectorized compare. Replaces the ~56 single-byte off-heap loads
        // the previous loop performed on Q12's 56-byte composite key. The MemorySegment.ofArray
        // wrapper is escape-eliminated by C2 (the segment never escapes this frame), and
        // MemorySegment.mismatch unrolls to a SIMD-friendly long-stride compare on HotSpot 21+.
        MemorySegment queryHeapSeg = MemorySegment.ofArray(key);
        return MemorySegment.mismatch(keyData, kStart, (long) kStart + len, queryHeapSeg, 0L, len)
                == -1L;
    }

    /** Hashes a heap byte[] using the same recurrence as ArrowBinaryBuffer (compat). */
    private static int hashOf(byte[] key) {
        int h = 1;
        for (int i = 0; i < key.length; i++) {
            h = 31 * h + key[i];
        }
        return h;
    }

    private boolean rowKeyStartsWith(int row, byte[] prefix) {
        int kStart = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int len = keyLengths.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        if (len < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (keyData.get(ValueLayout.JAVA_BYTE, kStart + i) != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compacts the cache, removing rows flagged in {@code toRemove}. Rebuilds the hash index from
     * scratch. Used by {@link #clearForPrefix} when only a subset of entries match the prefix.
     */
    private void compactExcluding(boolean[] toRemove) {
        // Pull surviving (key, value, ts) into local heap arrays, reset state, re-insert.
        int survivors = 0;
        for (int row = 0; row < size; row++) {
            if (!toRemove[row]) {
                survivors++;
            }
        }
        byte[][] survKeys = new byte[survivors][];
        Object[] survVals = new Object[survivors];
        long[] survTimes = new long[survivors];
        int o = 0;
        for (int row = 0; row < size; row++) {
            if (toRemove[row]) {
                continue;
            }
            int kStart = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int len = keyLengths.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            byte[] k = new byte[len];
            MemorySegment.copy(keyData, ValueLayout.JAVA_BYTE, kStart, k, 0, len);
            survKeys[o] = k;
            survVals[o] = values[row];
            survTimes[o] = accessTime.get(ValueLayout.JAVA_LONG, (long) row * Long.BYTES);
            o++;
        }
        // Reset structures.
        int prevSize = size;
        size = 0;
        keyDataUsed = 0;
        for (int i = 0; i < hashSlots; i++) {
            hashIndex.set(
                    ValueLayout.JAVA_INT,
                    (long) i * 2 * Integer.BYTES + Integer.BYTES,
                    EMPTY_SLOT);
        }
        // B6-H6: also reset the row → slot reverse map for live rows so the re-insert loop
        // below writes fresh slot ids via insertHashIndex.
        for (int i = 0; i < prevSize; i++) {
            rowToSlot.set(ValueLayout.JAVA_INT, (long) i * Integer.BYTES, -1);
        }
        java.util.Arrays.fill(values, null);
        // Re-insert survivors preserving access-time stamps and tombstones.
        for (int i = 0; i < survivors; i++) {
            int row = size++;
            appendKey(row, survKeys[i]);
            values[row] = survVals[i];
            accessTime.set(ValueLayout.JAVA_LONG, (long) row * Long.BYTES, survTimes[i]);
            insertHashIndex(hashOf(survKeys[i]), row);
        }
    }

    // -----------------------------------------------------------------
    // Public types
    // -----------------------------------------------------------------

    /**
     * Lookup result: value plus an explicit cache-hit indicator.
     *
     * <p>PR-F3: changed from a {@code record} to a mutable class so the {@link MapStateCache} can
     * return the same instance on every {@link #lookup} call (zero allocation on the cache-HIT
     * path). Source compatibility preserved: the accessors {@code cached()} and {@code value()}
     * have the same signature as the prior record component accessors.
     */
    public static final class Lookup<V> {
        @Nullable V value;
        boolean cached;

        Lookup() {}

        /** Constructor retained for any external test that builds Lookups directly. */
        public Lookup(@Nullable V value, boolean cached) {
            this.value = value;
            this.cached = cached;
        }

        @Nullable
        public V value() {
            return value;
        }

        public boolean cached() {
            return cached;
        }
    }
}

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

import org.apache.flink.annotation.Internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap key/value buffer in Arrow BinaryArray layout (offsets + flat data) + primitive
 * open-addressed long→int hash index keyed by per-key hash.
 *
 * <p>Designed for the V1-sync state hot path: insert/find/remove take {@link MemorySegment}
 * views (segment + offset + length) and copy bytes into the off-heap key/value data regions
 * exactly once per insert. Lookup is alloc-free (no boxing, no byte[]).
 *
 * <p>Capacity grows on demand (doubles) up to {@link #MAX_CAPACITY}. Hash collisions resolved
 * via linear probing with per-byte MemorySegment equality fallback.
 *
 * <p>Single-threaded per Flink slot (no synchronization).
 */
@Internal
public final class ArrowBinaryBuffer implements AutoCloseable {

    public static final int MAX_CAPACITY = 1_048_576;
    public static final int MAX_CAPACITY_MAP_STATE = 524288; // matches legacy MAP_WRITE_BUFFER_THRESHOLD
    public static final int MIN_CAPACITY = 1024;

    /**
     * Signal returned by {@link #insert} when the buffer is at its current capacity AND the
     * AutoTuner refused to grow it. Callers MUST flush + clear before retrying the insert.
     */
    public static final int INSERT_NEEDS_FLUSH = -2;

    /**
     * Cleanup-C1 (zero-copy tombstone): returned by {@link #findOrTombstone} when the probed key
     * was previously marked via {@link #tombstone}. Distinct from {@code -1} (never inserted) so
     * callers can short-circuit a {@code null} result without consulting the engine.
     */
    public static final int TOMBSTONE_FOUND = -3;

    private static final int EMPTY_SLOT = -1;
    private static final int TOMBSTONE = -2;

    private Arena arena;
    private MemorySegment keyOffsets; // (capacity + 1) int4 — offset of row i's key in keyData
    private MemorySegment keyData; // raw bytes — capacity × avgKeyBytes
    private MemorySegment valueOffsets; // (capacity) int4 — start offset of row i's value in valueData
    private MemorySegment valueLengths; // (capacity) int4 — length of row i's value (overwrite-safe sidecar)
    private MemorySegment valueData; // raw bytes
    private MemorySegment hashIndex; // 2 × capacity int4 entries (hash, row#) — open addressing
    // Cleanup-C1: per-row tombstone bitmap (1 byte/row). 0 = live, 1 = tombstoned. Allocated
    // alongside the row-indexed arrays in {@link #allocate} so a tombstone marker survives
    // overwrites/reads without a heap HashSet. {@link #flushTo} routes tombstoned rows to
    // {@code linker.delete} instead of {@code linker.batchPut}.
    private MemorySegment tombstoneBits;
    // Pre-allocated staging segments for batched flush — written per-row in flushTo, reused
    // across flushes so we don't pay per-flush Arena allocation cost. Sized to capacity slots.
    private MemorySegment flushKeyPtrs;
    private MemorySegment flushKeyLens;
    private MemorySegment flushValuePtrs;
    private MemorySegment flushValueLens;

    private final int maxCapacity;
    private int capacity;
    private int size;
    private int flushHighWaterMark; // flush triggers when size >= this
    private long keyDataUsed;
    private long valueDataUsed;
    private long keyDataCapacity;
    private long valueDataCapacity;

    /**
     * Optional dual-gate AutoTuner; when non-null, {@link #insert} consults it before growing
     * the buffer on a full-buffer event. If the tuner refuses to grow (size-gate not met),
     * {@link #insert} returns {@link #INSERT_NEEDS_FLUSH} and the caller must flush + retry.
     */
    private ArrowBinaryBufferAutoTuner tuner;

    public ArrowBinaryBuffer(int initialCapacity) {
        this(initialCapacity, MAX_CAPACITY, null);
    }

    public ArrowBinaryBuffer(int initialCapacity, int maxCapacity) {
        this(initialCapacity, maxCapacity, null);
    }

    public ArrowBinaryBuffer(
            int initialCapacity, int maxCapacity, ArrowBinaryBufferAutoTuner tuner) {
        // R15-L1: capacity MUST be a power of two — the hash-index linear-probe mask
        // {@code (capacity * 2) - 1} only matches all slots when {@code capacity * 2}
        // is a power of two. A non-pow2 capacity would skip slots and silently drop
        // entries on insert. Mirrors the FlatStateCache:60 rounding pattern.
        this.capacity = nextPow2(Math.max(initialCapacity, 8));
        // R16-M1: round maxCapacity up to the next power of two too. Pre-fix, the resize path
        // {@code Math.min(capacity * 2, maxCapacity)} could land on a non-pow2 if maxCapacity
        // itself was non-pow2 (e.g. user passed 1000 — resize would clamp to 1000, breaking the
        // hash-index mask invariant). Rounding here keeps every observable {@code capacity}
        // value a power of two for the buffer's lifetime.
        this.maxCapacity = nextPow2(Math.max(maxCapacity, this.capacity));
        this.arena = Arena.ofShared();
        this.tuner = tuner;
        allocate(this.capacity, 64 /* avg key bytes */, 64 /* avg value bytes */);
    }

    /** R15-L1: round {@code n} up to the next power of two (capped at 2^30). */
    private static int nextPow2(int n) {
        if (n <= 1) {
            return 1;
        }
        int p = Integer.highestOneBit(n - 1) << 1;
        // Guard against overflow on very large requests; cap at 2^30 which is
        // already larger than any plausible buffer capacity.
        return p > 0 ? p : (1 << 30);
    }

    private void allocate(int cap, int avgKeyBytes, int avgValueBytes) {
        this.keyDataCapacity = (long) cap * avgKeyBytes;
        this.valueDataCapacity = (long) cap * avgValueBytes;
        this.keyOffsets = arena.allocate((long) (cap + 1) * Integer.BYTES);
        this.keyData = arena.allocate(keyDataCapacity == 0 ? 1 : keyDataCapacity);
        this.valueOffsets = arena.allocate((long) (cap + 1) * Integer.BYTES);
        this.valueLengths = arena.allocate((long) cap * Integer.BYTES);
        this.valueData = arena.allocate(valueDataCapacity == 0 ? 1 : valueDataCapacity);
        // hashIndex layout: 2 × capacity slots, each slot is (hash:int, rowOrSentinel:int)
        this.hashIndex = arena.allocate((long) cap * 2 * 2 * Integer.BYTES);
        // Cleanup-C1: per-row tombstone bitmap (1 byte/row, zero-initialized by Arena).
        this.tombstoneBits = arena.allocate(cap);
        // Flush staging arrays — pointer/length pairs sized to capacity rows (max live rows).
        this.flushKeyPtrs = arena.allocate((long) cap * ValueLayout.ADDRESS.byteSize());
        this.flushKeyLens = arena.allocate((long) cap * ValueLayout.JAVA_LONG.byteSize());
        this.flushValuePtrs = arena.allocate((long) cap * ValueLayout.ADDRESS.byteSize());
        this.flushValueLens = arena.allocate((long) cap * ValueLayout.JAVA_LONG.byteSize());
        // initialize hashIndex slots to EMPTY_SLOT
        for (int i = 0; i < cap * 2; i++) {
            hashIndex.set(
                    ValueLayout.JAVA_INT,
                    (long) i * 2 * Integer.BYTES + Integer.BYTES,
                    EMPTY_SLOT);
        }
        this.flushHighWaterMark = Math.max(1, cap / 2);
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    /** Returns the row id for the given key, or -1 if not present. */
    public int find(MemorySegment keySeg, long keyOffset, int keyLen) {
        int h = hash(keySeg, keyOffset, keyLen);
        int mask = (capacity * 2) - 1;
        int probe = h & mask;
        for (int i = 0; i < capacity * 2; i++) {
            int slot = (probe + i) & mask;
            int row =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) slot * 2 * Integer.BYTES + Integer.BYTES);
            if (row == EMPTY_SLOT) {
                return -1;
            }
            if (row == TOMBSTONE) {
                continue;
            }
            int storedHash =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES);
            if (storedHash == h && keysEqual(row, keySeg, keyOffset, keyLen)) {
                return row;
            }
        }
        return -1;
    }

    /**
     * Inserts or overwrites the (key, value) pair. Copies key/value bytes into the off-heap data
     * regions. Returns the row id (stable across overwrites).
     */
    public int insert(
            MemorySegment keySeg,
            long keyOffset,
            int keyLen,
            MemorySegment valueSeg,
            long valueOffset,
            int valueLen) {
        int existing = find(keySeg, keyOffset, keyLen);
        if (existing >= 0) {
            // Append new value (don't reclaim old space — flush will reset).
            // CORRECTNESS: only touch row `existing`'s own offset/length. Touching
            // valueOffsets[existing + 1] would corrupt row (existing+1)'s start offset
            // under the old Arrow end-offset layout. Using a sidecar valueLengths[]
            // makes overwrite local to the row.
            int newValOffset = appendValue(valueSeg, valueOffset, valueLen);
            valueOffsets.set(
                    ValueLayout.JAVA_INT, (long) existing * Integer.BYTES, newValOffset);
            valueLengths.set(
                    ValueLayout.JAVA_INT, (long) existing * Integer.BYTES, valueLen);
            // Cleanup-C1: a PUT supersedes any pending tombstone for this row.
            tombstoneBits.set(ValueLayout.JAVA_BYTE, existing, (byte) 0);
            return existing;
        }
        if (size >= capacity) {
            // Dual-gate path: if a tuner is wired, ask it whether to grow. The tuner's
            // size-gate refuses growth when occupancy stays low (small-WS workloads like Q11);
            // in that case we return INSERT_NEEDS_FLUSH and the caller must flush + retry.
            //
            // Without a tuner, fall back to the legacy "always grow up to maxCapacity" path
            // so existing direct ArrowBinaryBuffer users (e.g. unit tests, legacy mode) keep
            // their auto-grow semantics.
            int suggested;
            if (tuner != null) {
                suggested = tuner.shouldResizeTo(capacity);
            } else {
                suggested = capacity < maxCapacity ? Math.min(capacity * 2, maxCapacity) : capacity;
            }
            if (suggested > capacity) {
                resize(suggested);
            }
            if (size >= capacity) {
                // Either at maxCapacity or the gate refused growth. Caller must flush.
                if (tuner != null) {
                    return INSERT_NEEDS_FLUSH;
                }
                // Legacy path (no tuner): keep historical IllegalStateException behavior so
                // direct callers learn they need to call flushTo before the next insert.
                throw new IllegalStateException(
                        "ArrowBinaryBuffer at maxCapacity="
                                + maxCapacity
                                + " — caller must flushTo(...) before next insert");
            }
        }
        int row = size;
        int keyStart = appendKey(keySeg, keyOffset, keyLen);
        keyOffsets.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, keyStart);
        keyOffsets.set(
                ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES, keyStart + keyLen);
        int valStart = appendValue(valueSeg, valueOffset, valueLen);
        valueOffsets.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, valStart);
        valueLengths.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, valueLen);
        size++;
        insertHashIndex(row, hash(keySeg, keyOffset, keyLen));
        return row;
    }

    /**
     * Cleanup-C1: marks the row identified by {@code (keySeg, keyOffset, keyLen)} as tombstoned
     * WITHOUT dropping the row data. Subsequent {@link #findOrTombstone} probes for the same
     * key return {@link #TOMBSTONE_FOUND}, so callers can short-circuit a {@code null} result
     * without an engine round-trip and without the per-call heap allocation of a wrapper key
     * object.
     *
     * <p>If the key is not already in the buffer, this method inserts an empty-value row first so
     * the tombstone is recorded in the row table. A future PUT for the same key will lift the
     * tombstone (see {@link #insert} — the overwrite branch clears the bit).
     *
     * <p>On {@link #flushTo} tombstoned rows are routed to {@code linker.delete} per row instead
     * of being included in the {@code linker.batchPut} batch.
     */
    public void tombstone(MemorySegment keySeg, long keyOffset, int keyLen) {
        // insert() handles both the new-row and existing-row cases. For a fresh key it adds a
        // row with empty value; for an existing key it overwrites the value with empty AND
        // clears the tombstone bit (PUT-supersedes-tombstone branch). Either way, the bit flip
        // below stamps the tombstone — and on flush the row is routed to linker.delete instead
        // of being staged into the batchPut.
        int row = insert(keySeg, keyOffset, keyLen, keySeg, keyOffset, 0);
        if (row == INSERT_NEEDS_FLUSH) {
            // Buffer is at maxCapacity and the tuner refused growth. The caller must flush
            // before retrying — surface this as an exception since tombstone() doesn't have a
            // flush handle available. The V2 wrapper (MapStateArrowBuffer) calls flushTo
            // opportunistically before tombstone() so this shouldn't fire in production.
            throw new IllegalStateException(
                    "ArrowBinaryBuffer.tombstone called on full buffer — flushTo required");
        }
        tombstoneBits.set(ValueLayout.JAVA_BYTE, row, (byte) 1);
    }

    /**
     * Cleanup-C1: probe variant of {@link #find} that distinguishes a live hit from a tombstone
     * hit from a complete miss. Returns:
     *
     * <ul>
     *   <li>row id ({@code >= 0}) — the key is present and live (not tombstoned);
     *   <li>{@link #TOMBSTONE_FOUND} — the key was previously {@link #tombstone}d;
     *   <li>{@code -1} — the key has never been inserted (or has been {@link #clear}ed).
     * </ul>
     *
     * <p>This is the alloc-free replacement for the HashSet-tombstone path. Callers (e.g.
     * {@code MapStateArrowBuffer.lookup}) use the tri-state result to short-circuit GET/CONTAINS
     * without instantiating a heap wrapper for the composite key.
     */
    public int findOrTombstone(MemorySegment keySeg, long keyOffset, int keyLen) {
        int row = find(keySeg, keyOffset, keyLen);
        if (row < 0) {
            return -1;
        }
        if (tombstoneBits.get(ValueLayout.JAVA_BYTE, row) != 0) {
            return TOMBSTONE_FOUND;
        }
        return row;
    }

    /** Returns true if the given row is currently flagged as tombstoned. Visible for tests. */
    public boolean isTombstoned(int row) {
        if (row < 0 || row >= size) {
            return false;
        }
        return tombstoneBits.get(ValueLayout.JAVA_BYTE, row) != 0;
    }

    /**
     * Iterates all tombstoned rows and returns their row ids in insertion order. Used by
     * {@link MapStateArrowBuffer#flushTo} to issue per-tombstone {@code linker.delete} calls.
     * O(size) scan; allocation = one int[] sized to live tombstone count.
     */
    public int[] tombstonedRows() {
        int n = 0;
        for (int row = 0; row < size; row++) {
            if (tombstoneBits.get(ValueLayout.JAVA_BYTE, row) != 0) {
                n++;
            }
        }
        if (n == 0) {
            return EMPTY_INT_ARRAY;
        }
        int[] out = new int[n];
        int o = 0;
        for (int row = 0; row < size; row++) {
            if (tombstoneBits.get(ValueLayout.JAVA_BYTE, row) != 0) {
                out[o++] = row;
            }
        }
        return out;
    }

    private static final int[] EMPTY_INT_ARRAY = new int[0];

    /**
     * Copies the key bytes for the given row into a fresh byte[]. Used by the per-row delete path
     * in {@code MapStateArrowBuffer.flushTo} to hand a heap byte[] to {@code linker.delete}. The
     * alloc here is on the cold flush path (one per tombstoned row, not per remove() call), so
     * it does NOT violate the C1 zero-copy contract that applies to the hot path.
     */
    public byte[] copyKey(int row) {
        int kStart = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int kEnd = keyOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        byte[] out = new byte[kEnd - kStart];
        MemorySegment.copy(keyData, ValueLayout.JAVA_BYTE, kStart, out, 0, out.length);
        return out;
    }

    public void remove(MemorySegment keySeg, long keyOffset, int keyLen) {
        int h = hash(keySeg, keyOffset, keyLen);
        int mask = (capacity * 2) - 1;
        int probe = h & mask;
        for (int i = 0; i < capacity * 2; i++) {
            int slot = (probe + i) & mask;
            int row =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) slot * 2 * Integer.BYTES + Integer.BYTES);
            if (row == EMPTY_SLOT) {
                return;
            }
            if (row == TOMBSTONE) {
                continue;
            }
            int storedHash =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES);
            if (storedHash == h && keysEqual(row, keySeg, keyOffset, keyLen)) {
                hashIndex.set(
                        ValueLayout.JAVA_INT,
                        (long) slot * 2 * Integer.BYTES + Integer.BYTES,
                        TOMBSTONE);
                return;
            }
        }
    }

    public void clear() {
        size = 0;
        keyDataUsed = 0;
        valueDataUsed = 0;
        for (int i = 0; i < capacity * 2; i++) {
            hashIndex.set(
                    ValueLayout.JAVA_INT,
                    (long) i * 2 * Integer.BYTES + Integer.BYTES,
                    EMPTY_SLOT);
        }
        // Cleanup-C1: drop all tombstone bits when the buffer resets to empty state.
        tombstoneBits.fill((byte) 0);
    }

    /**
     * Returns true when the buffer is at MAX_CAPACITY and cannot accept more inserts without
     * first being drained. Callers should check this BEFORE calling {@link #insert} and call
     * {@link #flushTo} when true.
     */
    public boolean needsFlush() {
        return size >= capacity && capacity >= maxCapacity;
    }

    /**
     * Returns true when {@code size} has reached the high-water mark (capacity / 2). Used as an
     * opportunistic auto-flush trigger so the buffer stays half-empty during steady-state.
     */
    public boolean shouldAutoFlush() {
        return size >= flushHighWaterMark;
    }

    /**
     * Flushes all buffered (key, value) pairs to the native engine via per-row putSegment,
     * then clears the buffer. MUST be called on checkpoint and on close for correctness.
     *
     * <p>{@link #insert} may need an auto-flush when {@link #shouldAutoFlush} returns true or
     * a forced flush when {@link #needsFlush} returns true; both conditions are opportunistic
     * and don't replace explicit flush calls from checkpoint/close.
     */
    public void flushTo(
            org.apache.flink.state.forstrs.ffm.ForStRsLinker linker,
            org.apache.flink.state.forstrs.ffm.FrsDb db,
            org.apache.flink.state.forstrs.ffm.FrsCfHandle cf) {
        if (size == 0) {
            return;
        }
        // Batched flush: stage per-row (ptr, len) into pre-allocated segments, then a single
        // linker.batchPut(...) crosses the FFM boundary once per flush — not once per row.
        //
        // We iterate the HASH INDEX (not the row data array) so tombstoned rows — rows whose
        // key was clear()ed via remove() — are skipped. The data row is still in
        // keyData/valueData but the hash-index slot is TOMBSTONE; without this filter we'd
        // re-publish deleted keys to the engine on flush (correctness bug).
        final long addrSz = ValueLayout.ADDRESS.byteSize();
        final long longSz = ValueLayout.JAVA_LONG.byteSize();
        final long keyDataAddr = keyData.address();
        final long valueDataAddr = valueData.address();
        int outIdx = 0;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int row =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) i * 2 * Integer.BYTES + Integer.BYTES);
            if (row == EMPTY_SLOT || row == TOMBSTONE) {
                continue;
            }
            // Cleanup-C1: tombstoned rows are excluded from the batchPut; the wrapper buffer
            // (MapStateArrowBuffer) handles them via its own per-row linker.delete loop.
            if (tombstoneBits.get(ValueLayout.JAVA_BYTE, row) != 0) {
                continue;
            }
            int kStart =
                    keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int kEnd =
                    keyOffsets.get(
                            ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
            int vStart =
                    valueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int vLen =
                    valueLengths.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            flushKeyPtrs.set(
                    ValueLayout.ADDRESS,
                    (long) outIdx * addrSz,
                    MemorySegment.ofAddress(keyDataAddr + kStart));
            flushValuePtrs.set(
                    ValueLayout.ADDRESS,
                    (long) outIdx * addrSz,
                    MemorySegment.ofAddress(valueDataAddr + vStart));
            flushKeyLens.set(
                    ValueLayout.JAVA_LONG, (long) outIdx * longSz, (long) (kEnd - kStart));
            flushValueLens.set(
                    ValueLayout.JAVA_LONG, (long) outIdx * longSz, (long) vLen);
            outIdx++;
        }
        if (outIdx > 0) {
            linker.batchPut(
                    db, cf, flushKeyPtrs, flushKeyLens, flushValuePtrs, flushValueLens, outIdx);
        }
        clear();
    }

    /** For tests / debugging — copies the value bytes for the given row into a fresh byte[]. */
    public byte[] copyValue(int row) {
        int vOff = valueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int len = valueLengths.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        byte[] out = new byte[len];
        MemorySegment.copy(valueData, ValueLayout.JAVA_BYTE, vOff, out, 0, len);
        return out;
    }

    public MemorySegment valueDataSegment() {
        return valueData;
    }

    public MemorySegment valueOffsetsSegment() {
        return valueOffsets;
    }

    public MemorySegment keyDataSegment() {
        return keyData;
    }

    public MemorySegment keyOffsetsSegment() {
        return keyOffsets;
    }

    public int valueOffsetOf(int row) {
        return valueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
    }

    public int valueLengthOf(int row) {
        return valueLengths.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
    }

    /** Returns the byte offset into {@link #keyDataSegment()} for row {@code row}'s key. */
    public int keyOffsetOf(int row) {
        return keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
    }

    /** Returns the byte length of row {@code row}'s key. */
    public int keyLengthOf(int row) {
        int kStart = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int kEnd = keyOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        return kEnd - kStart;
    }

    /**
     * Returns true if the row at index {@code row} is live (i.e., not tombstoned by a prior
     * {@link #remove}). Linear scan of the hash index; used by iter walkers that need to know
     * which row slots are still valid for emission.
     *
     * <p>NOTE: O(capacity) per call. If you're walking all rows, prefer caller-side tombstone
     * tracking. Used by iter-merge in MapState which walks the full statebuf once per scan.
     */
    public boolean isRowLive(int row) {
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int r =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) i * 2 * Integer.BYTES + Integer.BYTES);
            if (r == row) {
                return true;
            }
        }
        return false;
    }

    /**
     * Iterates the hash index slots and yields the live row ids in slot order. Convenience for
     * iter-merge walkers that want a deterministic, tombstone-free row enumeration without
     * paying O(capacity) per row in {@link #isRowLive}.
     */
    public int[] liveRows() {
        int[] tmp = new int[size];
        int n = 0;
        int slots = capacity * 2;
        for (int i = 0; i < slots && n < size; i++) {
            int r =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) i * 2 * Integer.BYTES + Integer.BYTES);
            if (r == EMPTY_SLOT || r == TOMBSTONE) {
                continue;
            }
            tmp[n++] = r;
        }
        if (n == size) {
            return tmp;
        }
        int[] out = new int[n];
        System.arraycopy(tmp, 0, out, 0, n);
        return out;
    }

    /**
     * Removes the row at the given slot from the hash index (marks it tombstone). Used by iter
     * walkers that need to delete-by-row (clear() flow).
     */
    public void tombstoneRow(int row) {
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int r =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) i * 2 * Integer.BYTES + Integer.BYTES);
            if (r == row) {
                hashIndex.set(
                        ValueLayout.JAVA_INT,
                        (long) i * 2 * Integer.BYTES + Integer.BYTES,
                        TOMBSTONE);
                return;
            }
        }
    }

    private int hash(MemorySegment seg, long offset, int len) {
        // PR-B2 (V2-11 / D-R3-1) — scalar loop retained intentionally.
        //
        // The Java byte[] hashCode recurrence `h = 31*h + b[i]` is sequentially
        // data-dependent: each step needs the previous `h`. A straight SIMD
        // translation would require either (a) a precomputed 31^k power table
        // unrolled per VL-byte block, which complicates the code path for a
        // hash that is *already* JIT-friendly (single-issue ALU op, no loads
        // beyond the segment scan), or (b) replacing the hash function entirely
        // (e.g. FNV-1a / xxhash) — which is binary-incompatible with cached
        // entries and the engine-side hash slot layout.
        //
        // The actual measured-hot path on Q11 (V2-9 sweep) was the per-byte
        // key-equality check, not the hash itself. That was fixed by switching
        // {@link #keysEqual} to MemorySegment.mismatch() (JDK 22+ JIT
        // intrinsic; see comment in keysEqual). Profiling after that change
        // shows hash() at < 5% of cache-lookup cost, so the cost/benefit of an
        // alternative-hash rewrite is deferred to a follow-up ticket
        // ("PR-B2.1 alternative hash function") that can take the
        // serialization-format hit as part of a coordinated migration.
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + seg.get(ValueLayout.JAVA_BYTE, offset + i);
        }
        return h;
    }

    private boolean keysEqual(int row, MemorySegment seg, long offset, int len) {
        int kStart = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int kEnd = keyOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        if (kEnd - kStart != len) {
            return false;
        }
        // Round-3 fix V2-9 / J4-5: JDK 22+ MemorySegment.mismatch() is JIT-intrinsified
        // (vector-compare instructions on AVX2/NEON). Replaces a scalar byte-by-byte loop
        // that was on the V1-sync hot path (92M ops on Q11).
        // Returns -1 when slices are equal, or the first mismatch index otherwise.
        return keyData.asSlice(kStart, len).mismatch(seg.asSlice(offset, len)) < 0;
    }

    private int appendKey(MemorySegment seg, long off, int len) {
        if (keyDataUsed + len > keyDataCapacity) {
            growKeyData(keyDataUsed + len);
        }
        int start = (int) keyDataUsed;
        MemorySegment.copy(seg, off, keyData, keyDataUsed, len);
        keyDataUsed += len;
        return start;
    }

    private int appendValue(MemorySegment seg, long off, int len) {
        if (valueDataUsed + len > valueDataCapacity) {
            growValueData(valueDataUsed + len);
        }
        int start = (int) valueDataUsed;
        MemorySegment.copy(seg, off, valueData, valueDataUsed, len);
        valueDataUsed += len;
        return start;
    }

    private void growKeyData(long needed) {
        long newCap = Math.max(keyDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(keyData, 0, newSeg, 0, keyDataUsed);
        keyData = newSeg;
        keyDataCapacity = newCap;
    }

    private void growValueData(long needed) {
        long newCap = Math.max(valueDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(valueData, 0, newSeg, 0, valueDataUsed);
        valueData = newSeg;
        valueDataCapacity = newCap;
    }

    private void insertHashIndex(int row, int hash) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        for (int i = 0; i < capacity * 2; i++) {
            int slot = (probe + i) & mask;
            int existing =
                    hashIndex.get(
                            ValueLayout.JAVA_INT,
                            (long) slot * 2 * Integer.BYTES + Integer.BYTES);
            if (existing == EMPTY_SLOT || existing == TOMBSTONE) {
                hashIndex.set(
                        ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES, hash);
                hashIndex.set(
                        ValueLayout.JAVA_INT,
                        (long) slot * 2 * Integer.BYTES + Integer.BYTES,
                        row);
                return;
            }
        }
        throw new IllegalStateException("hash index full — should not happen after resize");
    }

    private void resize(int newCapacity) {
        if (newCapacity > maxCapacity) {
            newCapacity = maxCapacity;
        }
        // Allocate fresh storage and rebuild — keeps the implementation simple. Rare event.
        Arena oldArena = arena;
        Arena newArena = Arena.ofShared();
        MemorySegment oldKeyOffsets = keyOffsets;
        MemorySegment oldKeyData = keyData;
        MemorySegment oldValueOffsets = valueOffsets;
        MemorySegment oldValueLengths = valueLengths;
        MemorySegment oldValueData = valueData;
        MemorySegment oldTombstoneBits = tombstoneBits;
        int oldSize = size;
        long oldKeyDataCap = keyDataCapacity;
        long oldValueDataCap = valueDataCapacity;

        this.arena = newArena;
        this.capacity = newCapacity;
        this.size = 0;
        this.keyDataUsed = 0;
        this.valueDataUsed = 0;
        allocate(
                newCapacity,
                (int) Math.max(64, oldKeyDataCap / Math.max(oldSize, 1) + 1),
                (int) Math.max(64, oldValueDataCap / Math.max(oldSize, 1) + 1));

        for (int row = 0; row < oldSize; row++) {
            int kStart = oldKeyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int kEnd =
                    oldKeyOffsets.get(
                            ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
            int vStart =
                    oldValueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int vLen =
                    oldValueLengths.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int newRow = insert(oldKeyData, kStart, kEnd - kStart, oldValueData, vStart, vLen);
            // Cleanup-C1: preserve the tombstone bit across resize.
            if (newRow >= 0 && oldTombstoneBits.get(ValueLayout.JAVA_BYTE, row) != 0) {
                tombstoneBits.set(ValueLayout.JAVA_BYTE, newRow, (byte) 1);
            }
        }
        oldArena.close();
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
        }
    }
}

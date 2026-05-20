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

    public static final int MAX_CAPACITY = 65536;
    public static final int MAX_CAPACITY_MAP_STATE = 524288; // matches legacy MAP_WRITE_BUFFER_THRESHOLD
    public static final int MIN_CAPACITY = 1024;

    private static final int EMPTY_SLOT = -1;
    private static final int TOMBSTONE = -2;

    private Arena arena;
    private MemorySegment keyOffsets; // (capacity + 1) int4 — offset of row i's key in keyData
    private MemorySegment keyData; // raw bytes — capacity × avgKeyBytes
    private MemorySegment valueOffsets; // (capacity) int4 — start offset of row i's value in valueData
    private MemorySegment valueLengths; // (capacity) int4 — length of row i's value (overwrite-safe sidecar)
    private MemorySegment valueData; // raw bytes
    private MemorySegment hashIndex; // 2 × capacity int4 entries (hash, row#) — open addressing
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

    public ArrowBinaryBuffer(int initialCapacity) {
        this(initialCapacity, MAX_CAPACITY);
    }

    public ArrowBinaryBuffer(int initialCapacity, int maxCapacity) {
        this.capacity = Math.max(initialCapacity, 8);
        this.maxCapacity = maxCapacity;
        this.arena = Arena.ofShared();
        allocate(this.capacity, 64 /* avg key bytes */, 64 /* avg value bytes */);
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
            return existing;
        }
        if (size >= capacity) {
            if (capacity < maxCapacity) {
                resize(Math.min(capacity * 2, maxCapacity));
            }
            // If we're at maxCapacity OR still full after resize, the CALLER must flush
            // before inserting more. needsFlush() exposes this state for callers to check
            // BEFORE calling insert. We throw here only as a safety net; the well-behaved
            // path is caller-side: if (buf.needsFlush()) buf.flushTo(...); buf.insert(...).
            if (size >= capacity) {
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
        // Java byte[] hashCode equivalent for a MemorySegment range.
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
        for (int i = 0; i < len; i++) {
            if (keyData.get(ValueLayout.JAVA_BYTE, kStart + i)
                    != seg.get(ValueLayout.JAVA_BYTE, offset + i)) {
                return false;
            }
        }
        return true;
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
            insert(oldKeyData, kStart, kEnd - kStart, oldValueData, vStart, vLen);
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

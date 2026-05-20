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
    public static final int MIN_CAPACITY = 1024;

    private static final int EMPTY_SLOT = -1;
    private static final int TOMBSTONE = -2;

    private Arena arena;
    private MemorySegment keyOffsets; // (capacity + 1) int4 — offset of row i's key in keyData
    private MemorySegment keyData; // raw bytes — capacity × avgKeyBytes
    private MemorySegment valueOffsets; // (capacity + 1) int4
    private MemorySegment valueData; // raw bytes
    private MemorySegment hashIndex; // 2 × capacity int4 entries (hash, row#) — open addressing

    private int capacity;
    private int size;
    private long keyDataUsed;
    private long valueDataUsed;
    private long keyDataCapacity;
    private long valueDataCapacity;

    public ArrowBinaryBuffer(int initialCapacity) {
        this.capacity = Math.max(initialCapacity, 8);
        this.arena = Arena.ofShared();
        allocate(this.capacity, 64 /* avg key bytes */, 64 /* avg value bytes */);
    }

    private void allocate(int cap, int avgKeyBytes, int avgValueBytes) {
        this.keyDataCapacity = (long) cap * avgKeyBytes;
        this.valueDataCapacity = (long) cap * avgValueBytes;
        this.keyOffsets = arena.allocate((long) (cap + 1) * Integer.BYTES);
        this.keyData = arena.allocate(keyDataCapacity == 0 ? 1 : keyDataCapacity);
        this.valueOffsets = arena.allocate((long) (cap + 1) * Integer.BYTES);
        this.valueData = arena.allocate(valueDataCapacity == 0 ? 1 : valueDataCapacity);
        // hashIndex layout: 2 × capacity slots, each slot is (hash:int, rowOrSentinel:int)
        this.hashIndex = arena.allocate((long) cap * 2 * 2 * Integer.BYTES);
        // initialize hashIndex slots to EMPTY_SLOT
        for (int i = 0; i < cap * 2; i++) {
            hashIndex.set(
                    ValueLayout.JAVA_INT,
                    (long) i * 2 * Integer.BYTES + Integer.BYTES,
                    EMPTY_SLOT);
        }
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
            int newValOffset = appendValue(valueSeg, valueOffset, valueLen);
            valueOffsets.set(
                    ValueLayout.JAVA_INT, (long) existing * Integer.BYTES, newValOffset);
            valueOffsets.set(
                    ValueLayout.JAVA_INT,
                    (long) (existing + 1) * Integer.BYTES,
                    newValOffset + valueLen);
            return existing;
        }
        if (size >= capacity) {
            resize(Math.min(capacity * 2, MAX_CAPACITY));
            if (size >= capacity) {
                throw new IllegalStateException(
                        "ArrowBinaryBuffer at MAX_CAPACITY=" + MAX_CAPACITY);
            }
        }
        int row = size;
        int keyStart = appendKey(keySeg, keyOffset, keyLen);
        keyOffsets.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, keyStart);
        keyOffsets.set(
                ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES, keyStart + keyLen);
        int valStart = appendValue(valueSeg, valueOffset, valueLen);
        valueOffsets.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, valStart);
        valueOffsets.set(
                ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES, valStart + valueLen);
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

    /** For tests / debugging — copies the value bytes for the given row into a fresh byte[]. */
    public byte[] copyValue(int row) {
        int vOff = valueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int vEnd = valueOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        int len = vEnd - vOff;
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
        int s = valueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int e = valueOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        return e - s;
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
        if (newCapacity > MAX_CAPACITY) {
            newCapacity = MAX_CAPACITY;
        }
        // Allocate fresh storage and rebuild — keeps the implementation simple. Rare event.
        Arena oldArena = arena;
        Arena newArena = Arena.ofShared();
        MemorySegment oldKeyOffsets = keyOffsets;
        MemorySegment oldKeyData = keyData;
        MemorySegment oldValueOffsets = valueOffsets;
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
            int vEnd =
                    oldValueOffsets.get(
                            ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
            insert(oldKeyData, kStart, kEnd - kStart, oldValueData, vStart, vEnd - vStart);
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

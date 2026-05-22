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

package org.apache.flink.state.forstrs.timer;

import org.apache.flink.annotation.Internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap binary min-heap of timer pending-buffer entries (analogue of {@code
 * ArrowBinaryBuffer} for the timer queue).
 *
 * <p>Layout:
 *
 * <ul>
 *   <li><b>heapArray</b> — fixed-stride 24-byte rows: {@code {ts:long@0, op:int@8, keyOffset:int@12,
 *       keyLen:int@16, hash:int@20}}. Min-heap by {@code ts} (parent = (i-1)/2, children = 2i+1,
 *       2i+2).
 *   <li><b>keyData</b> — variable-length composite key bytes appended on insert. Grows on demand
 *       (doubles).
 *   <li><b>hashIndex</b> — open-addressed table of (hash:int, heapPos:int) slot pairs sized at
 *       {@code 2 × heap capacity}. EMPTY = -1, TOMBSTONE = -2.
 * </ul>
 *
 * <p>Zero Java alloc on the hot path. All state lives in a shared {@link Arena} closed by {@link
 * #close()}.
 *
 * <p>Single-threaded per Flink slot — no synchronization.
 */
@Internal
public final class ArrowTimerBuffer implements AutoCloseable {

    public static final int OP_ADD = 1;
    public static final int OP_REMOVE = 2;

    public static final int DEFAULT_INITIAL_CAPACITY = 1024;
    public static final int DEFAULT_MAX_CAPACITY = 65_536;

    /** Stride (bytes) per heap row: ts(8) + op(4) + keyOffset(4) + keyLen(4) + hash(4) = 24. */
    private static final int HEAP_ROW_BYTES = 24;

    private static final long TS_OFF = 0L;
    private static final long OP_OFF = 8L;
    private static final long KOFF_OFF = 12L;
    private static final long KLEN_OFF = 16L;
    private static final long HASH_OFF = 20L;

    /** Hash-index slot: (hash:int, heapPos:int) = 8 bytes per slot. */
    private static final int HASH_SLOT_BYTES = 8;

    private static final int EMPTY_SLOT = -1;
    private static final int TOMBSTONE = -2;

    private Arena arena;

    private MemorySegment heapArray; // capacity × HEAP_ROW_BYTES
    private MemorySegment keyData; // raw bytes — variable length
    private MemorySegment hashIndex; // (capacity * 2) × HASH_SLOT_BYTES

    private final int maxCapacity;
    private int capacity;
    private int size;

    private long keyDataUsed;
    private long keyDataCapacity;

    /** Visitor for {@link #drainUnordered(FlushVisitor)} / {@link #drainTo(FlushVisitor)}. */
    public interface FlushVisitor {
        void visit(int op, MemorySegment keyData, int keyOff, int keyLen, long ts);
    }

    public ArrowTimerBuffer() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY);
    }

    public ArrowTimerBuffer(int initialCapacity) {
        this(initialCapacity, DEFAULT_MAX_CAPACITY);
    }

    public ArrowTimerBuffer(int initialCapacity, int maxCapacity) {
        this.capacity = Math.max(initialCapacity, 16);
        this.maxCapacity = Math.max(maxCapacity, this.capacity);
        this.arena = Arena.ofShared();
        allocate(this.capacity, 64 /* avg key bytes */);
    }

    private void allocate(int cap, int avgKeyBytes) {
        this.keyDataCapacity = (long) cap * avgKeyBytes;
        this.heapArray = arena.allocate((long) cap * HEAP_ROW_BYTES);
        this.keyData = arena.allocate(keyDataCapacity == 0 ? 1 : keyDataCapacity);
        this.hashIndex = arena.allocate((long) cap * 2 * HASH_SLOT_BYTES);
        // initialize hashIndex slots to EMPTY_SLOT
        for (int i = 0; i < cap * 2; i++) {
            hashIndex.set(ValueLayout.JAVA_INT, (long) i * HASH_SLOT_BYTES + 4L, EMPTY_SLOT);
        }
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    public MemorySegment keyDataSegment() {
        return keyData;
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Inserts an ADD op for {@code (keySeg, keyOffset, keyLen)} with timestamp {@code ts}. Returns
     * the heap position.
     */
    public int insertAdd(MemorySegment keySeg, long keyOffset, int keyLen, long ts) {
        return insertNew(keySeg, keyOffset, keyLen, ts, OP_ADD);
    }

    /** Inserts a REMOVE op. Returns the heap position. */
    public int insertRemove(MemorySegment keySeg, long keyOffset, int keyLen, long ts) {
        return insertNew(keySeg, keyOffset, keyLen, ts, OP_REMOVE);
    }

    /** Lookup by key bytes. Returns heap position or -1 if absent. */
    public int find(MemorySegment keySeg, long keyOffset, int keyLen) {
        int h = hashOf(keySeg, keyOffset, keyLen);
        return probeFind(h, keySeg, keyOffset, keyLen);
    }

    /** Returns the op-code of the entry at the given heap position. */
    public int opAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + OP_OFF);
    }

    /** Returns the timestamp of the entry at the given heap position. */
    public long tsAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_LONG, (long) heapPos * HEAP_ROW_BYTES + TS_OFF);
    }

    /** Returns the keyData offset of the entry at the given heap position. */
    public int keyOffsetAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + KOFF_OFF);
    }

    /** Returns the keyData length of the entry at the given heap position. */
    public int keyLenAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + KLEN_OFF);
    }

    /**
     * Removes the entry at {@code heapPos}: swaps it with the last live row, decrements {@link
     * #size}, and re-heapifies. Also removes the hash-index entry that referenced {@code heapPos}.
     */
    public void removeAt(int heapPos) {
        if (heapPos < 0 || heapPos >= size) {
            return;
        }
        int hashRemoved =
                heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + HASH_OFF);
        // Mark the hash-index slot referencing heapPos as TOMBSTONE.
        tombstoneHashSlot(hashRemoved, heapPos);

        int last = size - 1;
        if (heapPos == last) {
            size--;
            return;
        }
        // Move last row into heapPos.
        copyRow(last, heapPos);
        // Update hash-index slot for the moved row from `last` to `heapPos`.
        int movedHash =
                heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + HASH_OFF);
        updateHashSlotPos(movedHash, last, heapPos);

        size--;
        // Re-heapify: sift down then up (one direction will be a no-op).
        long parentTs =
                heapPos == 0
                        ? Long.MIN_VALUE
                        : heapArray.get(
                                ValueLayout.JAVA_LONG,
                                (long) ((heapPos - 1) >>> 1) * HEAP_ROW_BYTES + TS_OFF);
        long curTs =
                heapArray.get(ValueLayout.JAVA_LONG, (long) heapPos * HEAP_ROW_BYTES + TS_OFF);
        if (heapPos > 0 && curTs < parentTs) {
            siftUp(heapPos);
        } else {
            siftDown(heapPos);
        }
    }

    /**
     * Iterates ALL entries (in heap-array index order) and passes each to the visitor. Used by
     * the priority queue's flush path — engine-side re-sorts on insert. Caller is responsible
     * for calling {@link #clear()} after draining if buffer reuse is desired.
     *
     * <p><b>Round-3 fix S1-7:</b> renamed from {@code drainTo} to make ordering explicit. The
     * old name implied min-heap timestamp order which it never delivered. Any future
     * savepoint/migration caller that needs ordered drain must use {@link #drainOrdered}.
     */
    public void drainUnordered(FlushVisitor v) {
        for (int i = 0; i < size; i++) {
            int op = opAt(i);
            int kOff = keyOffsetAt(i);
            int kLen = keyLenAt(i);
            long ts = tsAt(i);
            v.visit(op, keyData, kOff, kLen, ts);
        }
    }

    /**
     * Backward-compat shim for the old {@code drainTo} name. Delegates to {@link
     * #drainUnordered}. New callers should pick {@code drainUnordered} or {@code drainOrdered}
     * explicitly.
     *
     * @deprecated use {@link #drainUnordered} (heap-index order) or a future {@code
     *     drainOrdered} (strict timestamp order).
     */
    @Deprecated
    public void drainTo(FlushVisitor v) {
        drainUnordered(v);
    }

    /** Clears all entries; reuses capacity for next call. */
    public void clear() {
        size = 0;
        keyDataUsed = 0;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            hashIndex.set(ValueLayout.JAVA_INT, (long) i * HASH_SLOT_BYTES + 4L, EMPTY_SLOT);
        }
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
        }
    }

    // ------------------------------------------------------------------
    // Internal — insert path
    // ------------------------------------------------------------------

    private int insertNew(MemorySegment keySeg, long keyOffset, int keyLen, long ts, int op) {
        if (size >= capacity) {
            if (capacity < maxCapacity) {
                resize(Math.min(capacity * 2, maxCapacity));
            } else {
                throw new IllegalStateException(
                        "ArrowTimerBuffer at maxCapacity=" + maxCapacity + "; caller must flush");
            }
        }
        int hash = hashOf(keySeg, keyOffset, keyLen);
        int kStart = appendKey(keySeg, keyOffset, keyLen);
        int row = size;
        writeRow(row, ts, op, kStart, keyLen, hash);
        size++;
        hashInsert(hash, row);
        return siftUp(row);
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

    private void growKeyData(long needed) {
        long newCap = Math.max(keyDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(keyData, 0, newSeg, 0, keyDataUsed);
        keyData = newSeg;
        keyDataCapacity = newCap;
    }

    // ------------------------------------------------------------------
    // Internal — row helpers
    // ------------------------------------------------------------------

    private void writeRow(int row, long ts, int op, int keyOff, int keyLen, int hash) {
        long base = (long) row * HEAP_ROW_BYTES;
        heapArray.set(ValueLayout.JAVA_LONG, base + TS_OFF, ts);
        heapArray.set(ValueLayout.JAVA_INT, base + OP_OFF, op);
        heapArray.set(ValueLayout.JAVA_INT, base + KOFF_OFF, keyOff);
        heapArray.set(ValueLayout.JAVA_INT, base + KLEN_OFF, keyLen);
        heapArray.set(ValueLayout.JAVA_INT, base + HASH_OFF, hash);
    }

    private void copyRow(int srcRow, int dstRow) {
        MemorySegment.copy(
                heapArray,
                (long) srcRow * HEAP_ROW_BYTES,
                heapArray,
                (long) dstRow * HEAP_ROW_BYTES,
                HEAP_ROW_BYTES);
    }

    /**
     * Swaps rows {@code i} and {@code j} (heap + hashIndex). Hash slots are updated so the row
     * reference stays accurate.
     */
    private void swapHeap(int i, int j) {
        if (i == j) {
            return;
        }
        int hi = heapArray.get(ValueLayout.JAVA_INT, (long) i * HEAP_ROW_BYTES + HASH_OFF);
        int hj = heapArray.get(ValueLayout.JAVA_INT, (long) j * HEAP_ROW_BYTES + HASH_OFF);

        // Swap row bytes using a scratch buffer (24 bytes is small).
        byte[] tmp = new byte[HEAP_ROW_BYTES];
        MemorySegment.copy(
                heapArray, ValueLayout.JAVA_BYTE, (long) i * HEAP_ROW_BYTES, tmp, 0, HEAP_ROW_BYTES);
        copyRow(j, i);
        MemorySegment.copy(
                tmp, 0, heapArray, ValueLayout.JAVA_BYTE, (long) j * HEAP_ROW_BYTES, HEAP_ROW_BYTES);

        // Update hash-index slots referencing i / j to point at their new rows.
        updateHashSlotPos(hi, i, j);
        updateHashSlotPos(hj, j, i);
    }

    private int siftUp(int i) {
        while (i > 0) {
            int parent = (i - 1) >>> 1;
            long childTs =
                    heapArray.get(ValueLayout.JAVA_LONG, (long) i * HEAP_ROW_BYTES + TS_OFF);
            long parentTs =
                    heapArray.get(
                            ValueLayout.JAVA_LONG, (long) parent * HEAP_ROW_BYTES + TS_OFF);
            if (childTs < parentTs) {
                swapHeap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
        return i;
    }

    private void siftDown(int i) {
        while (true) {
            int left = 2 * i + 1;
            int right = 2 * i + 2;
            int smallest = i;
            long smallestTs =
                    heapArray.get(ValueLayout.JAVA_LONG, (long) i * HEAP_ROW_BYTES + TS_OFF);
            if (left < size) {
                long leftTs =
                        heapArray.get(
                                ValueLayout.JAVA_LONG, (long) left * HEAP_ROW_BYTES + TS_OFF);
                if (leftTs < smallestTs) {
                    smallest = left;
                    smallestTs = leftTs;
                }
            }
            if (right < size) {
                long rightTs =
                        heapArray.get(
                                ValueLayout.JAVA_LONG, (long) right * HEAP_ROW_BYTES + TS_OFF);
                if (rightTs < smallestTs) {
                    smallest = right;
                    smallestTs = rightTs;
                }
            }
            if (smallest != i) {
                swapHeap(i, smallest);
                i = smallest;
            } else {
                break;
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal — hash index
    // ------------------------------------------------------------------

    private int hashOf(MemorySegment seg, long offset, int len) {
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + seg.get(ValueLayout.JAVA_BYTE, offset + i);
        }
        return h;
    }

    private boolean rowKeyEquals(int row, MemorySegment seg, long offset, int len) {
        int kStart =
                heapArray.get(ValueLayout.JAVA_INT, (long) row * HEAP_ROW_BYTES + KOFF_OFF);
        int kLen =
                heapArray.get(ValueLayout.JAVA_INT, (long) row * HEAP_ROW_BYTES + KLEN_OFF);
        if (kLen != len) {
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

    private int probeFind(int hash, MemorySegment seg, long offset, int len) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int slot = (probe + i) & mask;
            int row =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L);
            if (row == EMPTY_SLOT) {
                return -1;
            }
            if (row == TOMBSTONE) {
                continue;
            }
            int storedHash =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES);
            if (storedHash == hash && rowKeyEquals(row, seg, offset, len)) {
                return row;
            }
        }
        return -1;
    }

    private void hashInsert(int hash, int heapPos) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int slot = (probe + i) & mask;
            int existing =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L);
            if (existing == EMPTY_SLOT || existing == TOMBSTONE) {
                hashIndex.set(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES, hash);
                hashIndex.set(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L, heapPos);
                return;
            }
        }
        throw new IllegalStateException("hashIndex full — should not happen after resize");
    }

    /** Finds the slot with (hash, oldPos) and marks it TOMBSTONE. */
    private void tombstoneHashSlot(int hash, int heapPos) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int slot = (probe + i) & mask;
            int row =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L);
            if (row == EMPTY_SLOT) {
                return;
            }
            if (row == heapPos) {
                int storedHash =
                        hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES);
                if (storedHash == hash) {
                    hashIndex.set(
                            ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L, TOMBSTONE);
                    return;
                }
            }
        }
    }

    /** Re-targets the slot that held (hash, oldPos) to (hash, newPos). */
    private void updateHashSlotPos(int hash, int oldPos, int newPos) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int slot = (probe + i) & mask;
            int row =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L);
            if (row == EMPTY_SLOT) {
                return;
            }
            if (row == oldPos) {
                int storedHash =
                        hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES);
                if (storedHash == hash) {
                    hashIndex.set(
                            ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L, newPos);
                    return;
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal — resize
    // ------------------------------------------------------------------

    private void resize(int newCapacity) {
        if (newCapacity > maxCapacity) {
            newCapacity = maxCapacity;
        }
        Arena oldArena = arena;
        MemorySegment oldHeapArray = heapArray;
        MemorySegment oldKeyData = keyData;
        int oldSize = size;
        long oldKeyDataUsed = keyDataUsed;

        Arena newArena = Arena.ofShared();
        this.arena = newArena;
        this.capacity = newCapacity;
        // Re-allocate fresh (heapArray + keyData + hashIndex).
        this.keyDataCapacity = Math.max((long) newCapacity * 64L, oldKeyDataUsed);
        this.heapArray = arena.allocate((long) newCapacity * HEAP_ROW_BYTES);
        this.keyData = arena.allocate(keyDataCapacity == 0 ? 1 : keyDataCapacity);
        this.hashIndex = arena.allocate((long) newCapacity * 2 * HASH_SLOT_BYTES);
        for (int i = 0; i < newCapacity * 2; i++) {
            hashIndex.set(ValueLayout.JAVA_INT, (long) i * HASH_SLOT_BYTES + 4L, EMPTY_SLOT);
        }

        // Copy heapArray rows + keyData bytes verbatim — keeps row indices stable, so the hash
        // index can be rebuilt by re-inserting from each existing row's stored hash.
        MemorySegment.copy(oldHeapArray, 0, heapArray, 0, (long) oldSize * HEAP_ROW_BYTES);
        MemorySegment.copy(oldKeyData, 0, keyData, 0, oldKeyDataUsed);
        this.keyDataUsed = oldKeyDataUsed;
        this.size = oldSize;
        for (int row = 0; row < oldSize; row++) {
            int hash =
                    heapArray.get(ValueLayout.JAVA_INT, (long) row * HEAP_ROW_BYTES + HASH_OFF);
            hashInsert(hash, row);
        }
        oldArena.close();
    }
}

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

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;

/**
 * Open-addressing hash map keyed by {@code byte[]} (by value) with primitive {@code long} values.
 * Designed to replace {@code HashMap<ByteArrayWrapper, Long>} on the V1-sync write-buffer hot path.
 *
 * <p><b>Why this exists (B9-H1/H3).</b> The previous {@code Map<ByteArrayWrapper, Long>}
 * write-buffer allocated two objects per put/get/remove on the Q11 ValueState hot path:
 *
 * <ul>
 *   <li>a fresh {@code ByteArrayWrapper} wrapping the composite ForSt key, and
 *   <li>a boxed {@code Long} for the packed {@code (offset << 32) | length} index value
 *       (pointer-magnitude longs fall outside the JDK's {@code Long.valueOf} -128..127 cache, so
 *       each {@code put} autoboxes a fresh object).
 * </ul>
 *
 * <p>This class stores keys as raw {@code byte[]} references plus a sidecar {@code int[]} of
 * pre-computed hashes (so lookup probes can short-circuit non-matching slots without touching the
 * key bytes), values as a flat {@code long[]}, and uses linear probing with a 0.75 load factor.
 * Removal performs the standard open-addressing "backward shift" so probe chains remain compact —
 * no tombstones.
 *
 * <p><b>Hot-path allocation profile.</b> {@code put}/{@code get}/{@code remove} perform zero
 * allocations once the table has reached steady-state capacity. The caller retains ownership of the
 * {@code byte[]} passed to {@code put}; this map does NOT defensively copy. Mutating a stored key
 * after insertion will corrupt the map (same contract as {@link java.util.IdentityHashMap}, but
 * keyed by content equality at insert/lookup time). The write-buffer caller satisfies this contract
 * because the composite key {@code byte[]} is freshly produced by {@code keyComputer} per
 * {@code setCurrentKey} cycle and never mutated thereafter.
 *
 * <p><b>Not thread safe.</b> The write-buffer is accessed only from the task thread (synchronously
 * during state update/value/clear), matching the single-threaded contract of the backing keyed
 * state backend.
 */
@Internal
final class ByteArrayLongMap {

    /** Sentinel returned by {@link #get} on miss and by {@link #remove} when the key was absent. */
    static final long ABSENT = Long.MIN_VALUE;

    private static final int MIN_CAPACITY = 16;
    /** Load factor numerator/denominator (3/4 = 0.75). */
    private static final int LOAD_NUM = 3;

    private static final int LOAD_DEN = 4;

    private byte[][] keys;
    private int[] hashes;
    private long[] values;
    private int size;
    private int threshold;
    private int mask;

    ByteArrayLongMap() {
        this(MIN_CAPACITY);
    }

    ByteArrayLongMap(int initialCapacity) {
        int cap = MIN_CAPACITY;
        while (cap < initialCapacity) {
            cap <<= 1;
        }
        allocate(cap);
    }

    private void allocate(int cap) {
        this.keys = new byte[cap][];
        this.hashes = new int[cap];
        this.values = new long[cap];
        this.mask = cap - 1;
        this.threshold = (cap * LOAD_NUM) / LOAD_DEN;
        this.size = 0;
    }

    /**
     * Mix the {@link Arrays#hashCode(byte[])} output to avoid clustering on low-entropy hashes —
     * composite ForSt keys share a long common prefix so the raw Arrays hash exhibits poor spread
     * in the lower bits used by the open-addressing mask.
     */
    private static int mix(int h) {
        h ^= h >>> 16;
        h *= 0x7feb352d;
        h ^= h >>> 15;
        h *= 0x846ca68b;
        h ^= h >>> 16;
        return h;
    }

    /** Locate the slot for {@code key} (matching slot if present, else first empty slot in probe). */
    private int slotFor(byte[] key, int h) {
        int idx = h & mask;
        while (true) {
            byte[] k = keys[idx];
            if (k == null) {
                return idx;
            }
            if (hashes[idx] == h && Arrays.equals(k, key)) {
                return idx;
            }
            idx = (idx + 1) & mask;
        }
    }

    /** Returns the value for {@code key}, or {@link #ABSENT} on miss. */
    long get(byte[] key) {
        int h = mix(Arrays.hashCode(key));
        int idx = h & mask;
        while (true) {
            byte[] k = keys[idx];
            if (k == null) {
                return ABSENT;
            }
            if (hashes[idx] == h && Arrays.equals(k, key)) {
                return values[idx];
            }
            idx = (idx + 1) & mask;
        }
    }

    /**
     * Inserts or overwrites the value for {@code key}. The caller MUST NOT mutate {@code key} after
     * this call — the map stores the array reference verbatim (see class javadoc).
     */
    void put(byte[] key, long value) {
        int h = mix(Arrays.hashCode(key));
        int idx = slotFor(key, h);
        if (keys[idx] == null) {
            keys[idx] = key;
            hashes[idx] = h;
            values[idx] = value;
            size++;
            if (size > threshold) {
                resize(keys.length << 1);
            }
        } else {
            values[idx] = value;
        }
    }

    /** Removes {@code key} and returns its prior value, or {@link #ABSENT} if absent. */
    long remove(byte[] key) {
        int h = mix(Arrays.hashCode(key));
        int idx = h & mask;
        while (true) {
            byte[] k = keys[idx];
            if (k == null) {
                return ABSENT;
            }
            if (hashes[idx] == h && Arrays.equals(k, key)) {
                long prior = values[idx];
                shiftBackward(idx);
                size--;
                return prior;
            }
            idx = (idx + 1) & mask;
        }
    }

    /**
     * Standard open-addressing backward-shift removal (Knuth TAOCP vol. 3, §6.4 algorithm R).
     * {@code free} is the position currently holding the gap; we scan forward and for each
     * occupied slot {@code j} decide whether it can be relocated into {@code free} without
     * violating its probe-chain invariant — namely, the entry's natural slot must NOT lie strictly
     * between {@code free} and {@code j} in the forward (wrap-aware) direction. When relocation is
     * possible, move the entry and update {@code free} to {@code j}. Stops at the first empty
     * slot. No tombstones — {@link #get} terminates on the first {@code null}.
     */
    private void shiftBackward(int gap) {
        int free = gap;
        int j = (gap + 1) & mask;
        while (true) {
            byte[] k = keys[j];
            if (k == null) {
                keys[free] = null;
                hashes[free] = 0;
                values[free] = 0L;
                return;
            }
            int natural = hashes[j] & mask;
            // distFreeJ = forward wrap distance from free to j (>= 1).
            // distFreeNatural = forward wrap distance from free to entry j's natural slot.
            // The entry can be relocated to free iff its natural slot is NOT strictly between
            // free and j — i.e. distFreeNatural == 0 (natural == free, only possible if j wrapped
            // around to the slot AT free) or distFreeNatural > distFreeJ.
            int distFreeJ = (j - free) & mask;
            int distFreeNatural = (natural - free) & mask;
            if (distFreeNatural == 0 || distFreeNatural > distFreeJ) {
                keys[free] = k;
                hashes[free] = hashes[j];
                values[free] = values[j];
                free = j;
            }
            j = (j + 1) & mask;
        }
    }

    private void resize(int newCap) {
        byte[][] oldKeys = this.keys;
        int[] oldHashes = this.hashes;
        long[] oldValues = this.values;
        allocate(newCap);
        for (int i = 0; i < oldKeys.length; i++) {
            byte[] k = oldKeys[i];
            if (k != null) {
                int h = oldHashes[i];
                int idx = h & mask;
                while (keys[idx] != null) {
                    idx = (idx + 1) & mask;
                }
                keys[idx] = k;
                hashes[idx] = h;
                values[idx] = oldValues[i];
                size++;
            }
        }
    }

    int size() {
        return size;
    }

    boolean isEmpty() {
        return size == 0;
    }

    /** Clears all entries. Keeps allocated backing arrays for reuse — no allocation on next put. */
    void clear() {
        if (size == 0) {
            return;
        }
        Arrays.fill(keys, null);
        Arrays.fill(hashes, 0);
        // values can stay; null keys gate reads.
        size = 0;
    }

    /**
     * Iterates all live entries by invoking {@code visitor} for each. The visitor receives the raw
     * key reference (do not mutate) and the long value. Iteration order is unspecified.
     */
    void forEach(EntryVisitor visitor) {
        for (int i = 0; i < keys.length; i++) {
            byte[] k = keys[i];
            if (k != null) {
                visitor.accept(k, values[i]);
            }
        }
    }

    /**
     * Returns the table capacity (slot count). Together with {@link #keyAt} and {@link #valueAt}
     * this lets callers iterate by index without allocating {@code Map.Entry} boxes — used by the
     * write-buffer flush which must pause between fixed-size chunks of the staging segments.
     */
    int capacity() {
        return keys.length;
    }

    /** Returns the key at slot {@code i}, or {@code null} if the slot is empty. */
    byte[] keyAt(int i) {
        return keys[i];
    }

    /** Returns the value at slot {@code i}. Only meaningful when {@link #keyAt}{@code (i) != null}. */
    long valueAt(int i) {
        return values[i];
    }

    @FunctionalInterface
    interface EntryVisitor {
        void accept(byte[] key, long value);
    }
}

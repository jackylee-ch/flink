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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * GC-free flat hash table for caching state key→value mappings. Uses a single {@code long[]} as the
 * index (open-addressing, linear probing) and a single {@code byte[]} as the data store
 * (append-only). Zero per-op object allocation.
 *
 * <p>Each data entry is laid out as: [keyLen:4][valLen:4][keyBytes][valBytes]. The index maps hash
 * slots to data offsets (or -1 for empty).
 */
@Internal
public final class FlatStateCache {

    private static final long EMPTY = -1L;
    private static final int HEADER_SIZE = 8; // 4 bytes keyLen + 4 bytes valLen

    /**
     * Byte-array view VarHandle for 4-byte big-endian int access. PR-B2 (V2-15): replaces the
     * manual shift-pack implementation of {@code readInt}/{@code writeInt}. The view-VarHandle
     * is JIT-intrinsified down to a single big-endian load/store (with a byteswap on
     * little-endian hosts) and elides the per-byte bounds checks the shift form did
     * implicitly. Byte order matches the legacy implementation (MSB first), so on-disk
     * payloads stay binary-compatible across versions.
     */
    private static final VarHandle INT_VH =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    private final long[] index;
    private final int mask;
    private byte[] data;
    private int dataPos;
    private int entryCount;
    private final int maxEntries;

    FlatStateCache(int capacitySlots, int dataCapacityBytes) {
        int cap = Integer.highestOneBit(capacitySlots - 1) << 1;
        if (cap < 16) {
            cap = 16;
        }
        this.index = new long[cap];
        this.mask = cap - 1;
        this.data = new byte[dataCapacityBytes];
        this.dataPos = 0;
        this.entryCount = 0;
        this.maxEntries = (int) (cap * 0.75);
        java.util.Arrays.fill(index, EMPTY);
    }

    public byte[] get(byte[] key) {
        int h = hash(key);
        int slot = h & mask;
        while (true) {
            long offset = index[slot];
            if (offset == EMPTY) {
                return null;
            }
            int off = (int) offset;
            int kLen = readInt(data, off);
            int vLen = readInt(data, off + 4);
            if (kLen == key.length && arraysEqual(data, off + HEADER_SIZE, key, 0, kLen)) {
                byte[] val = new byte[vLen];
                System.arraycopy(data, off + HEADER_SIZE + kLen, val, 0, vLen);
                return val;
            }
            slot = (slot + 1) & mask;
        }
    }

    public void put(byte[] key, byte[] value) {
        if (entryCount >= maxEntries) {
            return; // full — skip caching, don't crash
        }
        int needed = HEADER_SIZE + key.length + value.length;
        if (dataPos + needed > data.length) {
            if (data.length < 512 * 1024 * 1024) {
                byte[] grown = new byte[Math.min(data.length * 2, 512 * 1024 * 1024)];
                System.arraycopy(data, 0, grown, 0, dataPos);
                data = grown;
            } else {
                return; // data full
            }
        }
        int h = hash(key);
        int slot = h & mask;
        while (true) {
            long offset = index[slot];
            if (offset == EMPTY) {
                break;
            }
            int off = (int) offset;
            int kLen = readInt(data, off);
            if (kLen == key.length && arraysEqual(data, off + HEADER_SIZE, key, 0, kLen)) {
                // Update existing: overwrite value in-place if same size, else append new
                int vLen = readInt(data, off + 4);
                if (vLen == value.length) {
                    System.arraycopy(value, 0, data, off + HEADER_SIZE + kLen, value.length);
                    return;
                }
                // Different size: append new entry, update index
                int newOff = dataPos;
                writeInt(data, newOff, key.length);
                writeInt(data, newOff + 4, value.length);
                System.arraycopy(key, 0, data, newOff + HEADER_SIZE, key.length);
                System.arraycopy(value, 0, data, newOff + HEADER_SIZE + key.length, value.length);
                dataPos += needed;
                index[slot] = newOff;
                return;
            }
            slot = (slot + 1) & mask;
        }
        // New entry
        int newOff = dataPos;
        writeInt(data, newOff, key.length);
        writeInt(data, newOff + 4, value.length);
        System.arraycopy(key, 0, data, newOff + HEADER_SIZE, key.length);
        System.arraycopy(value, 0, data, newOff + HEADER_SIZE + key.length, value.length);
        dataPos += needed;
        index[slot] = newOff;
        entryCount++;
    }

    public boolean contains(byte[] key) {
        int h = hash(key);
        int slot = h & mask;
        while (true) {
            long offset = index[slot];
            if (offset == EMPTY) {
                return false;
            }
            int off = (int) offset;
            int kLen = readInt(data, off);
            if (kLen == key.length && arraysEqual(data, off + HEADER_SIZE, key, 0, kLen)) {
                return true;
            }
            slot = (slot + 1) & mask;
        }
    }

    private static int hash(byte[] key) {
        return java.util.Arrays.hashCode(key);
    }

    private static int readInt(byte[] buf, int off) {
        return (int) INT_VH.get(buf, off);
    }

    private static void writeInt(byte[] buf, int off, int val) {
        INT_VH.set(buf, off, val);
    }

    private static boolean arraysEqual(byte[] a, int aOff, byte[] b, int bOff, int len) {
        return java.util.Arrays.equals(a, aOff, aOff + len, b, bOff, bOff + len);
    }
}

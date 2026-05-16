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

package org.apache.flink.state.forstrs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap columnar buffer in Arrow BinaryArray layout: {@code offsets[count + 1]} ({@code int})
 * plus a flat {@code data} byte segment of length {@code offsets[count]}. Designed to be allocated
 * once per executor and reused across batches (call {@link #reset()} between batches).
 *
 * <p>This is the Java-side mirror of the Rust-side {@code frs_vectorized_batch_*} FFI calls — see
 * {@code crates/forst-rs-ffi/src/lib.rs} and spec {@code
 * docs/superpowers/specs/2026-05-15-forst-rs-vectorized-executor-design.md} §C1.
 */
@Internal
public final class ColumnarBatchBuffer {

    /** Initial slot capacity (number of entries). Grows on overflow. */
    private static final int INIT_CAPACITY = 4096;

    /** Initial data byte capacity. Grows on overflow. */
    private static final int INIT_DATA_CAPACITY = 64 * 1024;

    private final Arena arena;
    private MemorySegment offsets; // capacity + 1 i32 slots
    private MemorySegment data; // dataCapacity bytes
    private int capacity;
    private int dataCapacity;
    private int count;
    private int dataPos;

    public ColumnarBatchBuffer(Arena arena) {
        this(arena, INIT_CAPACITY, INIT_DATA_CAPACITY);
    }

    public ColumnarBatchBuffer(Arena arena, int initialCapacity, int initialDataCapacity) {
        this.arena = arena;
        this.capacity = Math.max(16, initialCapacity);
        this.dataCapacity = Math.max(256, initialDataCapacity);
        this.offsets = arena.allocate(ValueLayout.JAVA_INT, (long) capacity + 1);
        this.data = arena.allocate(this.dataCapacity);
        this.count = 0;
        this.dataPos = 0;
        offsets.set(ValueLayout.JAVA_INT, 0L, 0);
    }

    /** Clears the buffer for reuse. No deallocation. */
    public void reset() {
        this.count = 0;
        this.dataPos = 0;
        offsets.set(ValueLayout.JAVA_INT, 0L, 0);
    }

    /**
     * Appends a byte[] entry. Grows offsets / data segments if needed. Returns the index of the
     * appended entry.
     */
    public int append(byte[] src, int srcOff, int len) {
        ensureCapacity(1);
        ensureData(len);
        if (len > 0) {
            MemorySegment.copy(src, srcOff, data, ValueLayout.JAVA_BYTE, dataPos, len);
        }
        dataPos += len;
        int idx = count++;
        offsets.set(ValueLayout.JAVA_INT, (long) (idx + 1) * Integer.BYTES, dataPos);
        return idx;
    }

    /** Appends a full byte[] entry. */
    public int append(byte[] src) {
        return append(src, 0, src.length);
    }

    /**
     * Appends the contents of a {@link DataOutputSerializer}'s shared backing buffer. Avoids the
     * intermediate {@code getCopyOfBuffer()} byte[] allocation that the legacy
     * {@code serializeKey} → {@code byte[]} path incurs.
     */
    public int append(DataOutputSerializer ser) {
        return append(ser.getSharedBuffer(), 0, ser.length());
    }

    /**
     * Appends an empty (zero-length) entry. Used when a request has no associated value (e.g.
     * VALUE_UPDATE with null payload) so the entry index lines up with the keys buffer.
     */
    public int appendEmpty() {
        ensureCapacity(1);
        int idx = count++;
        offsets.set(ValueLayout.JAVA_INT, (long) (idx + 1) * Integer.BYTES, dataPos);
        return idx;
    }

    public int count() {
        return count;
    }

    public int dataPos() {
        return dataPos;
    }

    /** Offsets segment, length {@code (count + 1) * 4} bytes. Pointer for FFI. */
    public MemorySegment offsetsSegment() {
        return offsets;
    }

    /** Data segment, capacity {@code dataCapacity} but logically used up to {@code dataPos}. */
    public MemorySegment dataSegment() {
        return data;
    }

    public int dataCapacity() {
        return dataCapacity;
    }

    /** Read the (offset, length) of entry {@code i}. */
    public long sliceLength(int i) {
        int start = offsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
        int end = offsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
        return ((long) start << 32) | ((long) (end - start) & 0xFFFF_FFFFL);
    }

    /** Copies entry {@code i} into a fresh byte[]. */
    public byte[] copyAt(int i) {
        int start = offsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
        int end = offsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
        int len = end - start;
        if (len <= 0) {
            return null;
        }
        byte[] out = new byte[len];
        MemorySegment.copy(data, ValueLayout.JAVA_BYTE, start, out, 0, len);
        return out;
    }

    private void ensureCapacity(int additional) {
        int needed = count + additional;
        if (needed <= capacity) {
            return;
        }
        int newCap = capacity;
        while (newCap < needed) {
            newCap <<= 1;
        }
        MemorySegment grown = arena.allocate(ValueLayout.JAVA_INT, (long) newCap + 1);
        MemorySegment.copy(offsets, 0L, grown, 0L, (long) (count + 1) * Integer.BYTES);
        this.offsets = grown;
        this.capacity = newCap;
    }

    private void ensureData(int additional) {
        int needed = dataPos + additional;
        if (needed <= dataCapacity) {
            return;
        }
        int newCap = dataCapacity;
        while (newCap < needed) {
            newCap <<= 1;
        }
        MemorySegment grown = arena.allocate(newCap);
        if (dataPos > 0) {
            MemorySegment.copy(data, 0L, grown, 0L, dataPos);
        }
        this.data = grown;
        this.dataCapacity = newCap;
    }
}

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
 *
 * <p><b>D-R4-2 (JAVA_INT alignment audit).</b> Every {@code offsets} access in this class uses
 * the natively-aligned {@link ValueLayout#JAVA_INT} layout: the backing segment is allocated
 * via {@code arena.allocate(JAVA_INT, count + 1)} which guarantees 4-byte alignment, and every
 * indexed access computes the offset as {@code (long) i * Integer.BYTES} — always a multiple of
 * {@code 4}. Unlike the {@code FrsBytes} struct in {@link
 * org.apache.flink.state.forstrs.ffm.ForStRsLinker} (which lives in a heap {@code byte[24]} and
 * therefore uses {@code JAVA_LONG_UNALIGNED}), no segment here is a heap-backed slice of a
 * Rust-side struct; {@code JAVA_INT} is the correct layout. {@code JAVA_INT_UNALIGNED} would only
 * be needed if a Rust struct laid out a {@code u32} at an offset that is not a multiple of 4 —
 * which does not occur on this path.
 */
@Internal
public final class ColumnarBatchBuffer {

    /** Initial slot capacity (number of entries). Grows on overflow. */
    private static final int INIT_CAPACITY = 4096;

    /** Initial data byte capacity. Grows on overflow. */
    private static final int INIT_DATA_CAPACITY = 64 * 1024;

    /**
     * FRS-FFM-BOUND (2026-06-16, PMC-1): the data buffer is shrunk back toward the recent usage
     * high-water if it ballooned for an outlier batch and the next {@value #SHRINK_HYSTERESIS}
     * batches all used less than {@value #SHRINK_FRACTION_NUM}/{@value #SHRINK_FRACTION_DEN} of the
     * current capacity. This caps the FFM off-heap working set at the STEADY-STATE batch size rather
     * than the largest batch EVER seen, so a single transient large batch on the join build cannot
     * permanently commit its peak. Pure memory accounting — never affects emitted bytes.
     */
    private static final int SHRINK_HYSTERESIS = 16;

    private static final long SHRINK_FRACTION_NUM = 1L;
    private static final long SHRINK_FRACTION_DEN = 4L;

    /** Never shrink below this (avoids re-grow thrash on normal batches). */
    private static final int MIN_SHRINK_FLOOR = INIT_DATA_CAPACITY;

    /**
     * FRS-FFM-BOUND: per-buffer SHARED sub-arena that owns {@link #offsets} + {@link #data}. The
     * worker arena passed to the constructor is {@link Arena#ofShared()} and NEVER frees an
     * individual allocation (only on arena close at slot teardown) — so the legacy {@code
     * arena.allocate} doubling-growth leaked every predecessor segment for the slot's whole life.
     * Owning a dedicated shared sub-arena lets {@link #grow}/{@link #reset} CLOSE the prior arena
     * and reclaim the predecessor's native memory immediately, while preserving cross-thread
     * (mailbox fills, worker reads) access because the sub-arena is also shared. Closing is only
     * ever done on the grow / shrink paths, which run while the buffer is exclusively owned by the
     * filling-or-dispatching thread (a buffer is never filled and read concurrently).
     */
    private Arena bufArena;
    private MemorySegment offsets; // capacity + 1 i32 slots
    private MemorySegment data; // dataCapacity bytes
    private int capacity;
    private int dataCapacity;
    private int count;
    private int dataPos;

    /** Largest dataPos seen across the last {@link #SHRINK_HYSTERESIS} resets (shrink trigger). */
    private int recentDataHighWater;
    private int smallBatchStreak;

    public ColumnarBatchBuffer(Arena arena) {
        this(arena, INIT_CAPACITY, INIT_DATA_CAPACITY);
    }

    public ColumnarBatchBuffer(Arena arena, int initialCapacity, int initialDataCapacity) {
        this.capacity = Math.max(16, initialCapacity);
        this.dataCapacity = Math.max(256, initialDataCapacity);
        this.bufArena = Arena.ofShared();
        this.offsets = bufArena.allocate(ValueLayout.JAVA_INT, (long) capacity + 1);
        this.data = bufArena.allocate(this.dataCapacity);
        this.count = 0;
        this.dataPos = 0;
        this.recentDataHighWater = 0;
        this.smallBatchStreak = 0;
        offsets.set(ValueLayout.JAVA_INT, 0L, 0);
        FfmOffHeapAccounting.COLUMNAR_BYTES.addAndGet(currentBytes());
    }

    private long currentBytes() {
        return ((long) capacity + 1) * Integer.BYTES + (long) dataCapacity;
    }

    /**
     * Clears the buffer for reuse, and shrinks the data segment back toward the recent usage
     * high-water if it ballooned for an outlier batch (FRS-FFM-BOUND). No-op fast path when the
     * buffer is already at a sensible size for recent traffic.
     */
    public void reset() {
        // Track the just-finished batch's footprint, then maybe shrink.
        if (dataPos > recentDataHighWater) {
            recentDataHighWater = dataPos;
        }
        // A batch is "small" if it used at most NUM/DEN of the current capacity.
        if ((long) dataPos * SHRINK_FRACTION_DEN <= (long) dataCapacity * SHRINK_FRACTION_NUM) {
            smallBatchStreak++;
        } else {
            smallBatchStreak = 0;
        }
        this.count = 0;
        this.dataPos = 0;
        if (smallBatchStreak >= SHRINK_HYSTERESIS && dataCapacity > MIN_SHRINK_FLOOR) {
            // Shrink data capacity to a power-of-two cover of the recent high-water, floored.
            // count/dataPos are already 0, so reallocData copies nothing live.
            long target = Math.max(MIN_SHRINK_FLOOR, recentDataHighWater);
            long newCap = MIN_SHRINK_FLOOR;
            while (newCap < target) {
                newCap <<= 1;
            }
            if (newCap < dataCapacity) {
                reallocData((int) newCap, 0 /* live bytes after reset is 0 */);
                FfmOffHeapAccounting.SHRINK_ON_RESET.incrementAndGet();
            }
            smallBatchStreak = 0;
            recentDataHighWater = 0;
        }
        offsets.set(ValueLayout.JAVA_INT, 0L, 0);
        FfmOffHeapAccounting.maybeDump();
    }

    /**
     * FRS-FFM-BOUND: re-home {@link #offsets} + {@link #data} into a FRESH shared sub-arena at the
     * requested data capacity, copying {@code liveBytes} of data forward, then close the OLD arena
     * to release the predecessor segments' native memory immediately. {@code offsets} capacity is
     * preserved (it is tiny relative to data).
     */
    private void reallocData(int newDataCap, int liveBytes) {
        Arena old = bufArena;
        long before = currentBytes();
        Arena fresh = Arena.ofShared();
        MemorySegment newOffsets = fresh.allocate(ValueLayout.JAVA_INT, (long) capacity + 1);
        MemorySegment newData = fresh.allocate(newDataCap);
        MemorySegment.copy(offsets, 0L, newOffsets, 0L, (long) (count + 1) * Integer.BYTES);
        if (liveBytes > 0) {
            MemorySegment.copy(data, 0L, newData, 0L, liveBytes);
        }
        this.bufArena = fresh;
        this.offsets = newOffsets;
        this.data = newData;
        this.dataCapacity = newDataCap;
        long after = currentBytes();
        FfmOffHeapAccounting.COLUMNAR_BYTES.addAndGet(after - before);
        old.close(); // frees predecessor offsets+data immediately
        FfmOffHeapAccounting.FREED_ON_GROW.incrementAndGet();
    }

    /**
     * FRS-FFM-BOUND: release this buffer's off-heap segments. Called from executor shutdown; the
     * per-buffer sub-arena replaces the worker arena's ownership of these segments, so it must be
     * closed explicitly at slot teardown (otherwise the segments leak past the slot's life).
     */
    public void close() {
        Arena a = bufArena;
        if (a != null) {
            FfmOffHeapAccounting.COLUMNAR_BYTES.addAndGet(-currentBytes());
            bufArena = null;
            a.close();
        }
    }

    /**
     * Appends a byte[] entry. Grows offsets / data segments if needed. Returns the index of the
     * appended entry.
     */
    public int append(byte[] src, int srcOff, int len) {
        // R72-L1: validate (srcOff, len) against src.length BEFORE any state
        // mutation. Pre-fix a buggy caller with len < 0 silently corrupted
        // dataPos (via the `dataPos += len` decrement); srcOff + len > src.length
        // was caught only when MemorySegment.copy threw AIOOBE, AFTER ensureData
        // had already grown the data segment. checkFromIndexSize handles both
        // (and the integer-overflow case `srcOff + len < 0`).
        java.util.Objects.checkFromIndexSize(srcOff, len, src.length);
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
     * B5-H5 zero-copy variant: appends {@code len} bytes from a {@link MemorySegment} source
     * starting at {@code srcOff}. Flows the bytes directly into the off-heap {@code data}
     * segment without an intermediate heap {@code byte[]} hop. Mirrors the
     * {@link org.apache.flink.state.forstrs.state.ArrowBinaryBuffer#appendKey(MemorySegment, long,
     * int)} pattern used elsewhere in this backend.
     *
     * <p>Returns the index of the appended entry.
     */
    public int append(MemorySegment src, long srcOff, int len) {
        // R73-M1: mirror R72-L1's validation for the MemorySegment-source
        // overload. Pre-fix a buggy caller with `len < 0` silently corrupted
        // `dataPos` (via `dataPos += len`); `srcOff + len > src.byteSize()`
        // threw AIOOBE inside MemorySegment.copy AFTER `ensureData` had
        // already grown the data segment. byteSize() returns long, so we
        // can't use Objects.checkFromIndexSize; manually validate.
        if (len < 0) {
            throw new IllegalArgumentException(
                    "ColumnarBatchBuffer.append: len=" + len + " must be non-negative");
        }
        if (srcOff < 0) {
            throw new IllegalArgumentException(
                    "ColumnarBatchBuffer.append: srcOff=" + srcOff + " must be non-negative");
        }
        long srcEnd = srcOff + len;
        if (srcEnd < srcOff /* overflow */ || srcEnd > src.byteSize()) {
            throw new IllegalArgumentException(
                    "ColumnarBatchBuffer.append: srcOff="
                            + srcOff
                            + " len="
                            + len
                            + " exceeds src.byteSize()="
                            + src.byteSize());
        }
        ensureCapacity(1);
        ensureData(len);
        if (len > 0) {
            MemorySegment.copy(src, srcOff, data, dataPos, len);
        }
        dataPos += len;
        int idx = count++;
        offsets.set(ValueLayout.JAVA_INT, (long) (idx + 1) * Integer.BYTES, dataPos);
        return idx;
    }

    /**
     * Appends the contents of a {@link DataOutputSerializer}'s shared backing buffer. Avoids the
     * intermediate {@code getCopyOfBuffer()} byte[] allocation that the legacy {@code serializeKey}
     * → {@code byte[]} path incurs.
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
        // R71-M1: int-overflow-safe doubling. Pre-fix `newCap <<= 1` wrapped to
        // a negative value once capacity exceeded 2^30, causing an infinite loop
        // (`newCap < needed` always true for negative newCap). Compute the
        // next capacity in `long`, clamp to Integer.MAX_VALUE, and explicitly
        // refuse `needed > Integer.MAX_VALUE` since the JDK array machinery
        // backing MemorySegment.allocate cannot allocate beyond int range.
        if (needed < 0) {
            throw new IllegalArgumentException(
                    "ColumnarBatchBuffer.ensureCapacity: needed=" + needed + " overflowed int");
        }
        long newCap = capacity;
        while (newCap < needed) {
            newCap = Math.min(newCap * 2L, Integer.MAX_VALUE);
            if (newCap == Integer.MAX_VALUE && newCap < needed) {
                throw new IllegalArgumentException(
                        "ColumnarBatchBuffer.ensureCapacity: needed="
                                + needed
                                + " exceeds Integer.MAX_VALUE — batch too large");
            }
        }
        int newCapInt = (int) newCap;
        // FRS-FFM-BOUND: re-home offsets+data into a fresh shared sub-arena at the larger offsets
        // capacity (data capacity unchanged), then free the predecessor arena. Avoids leaking the
        // old offsets segment into the never-freeing worker arena.
        Arena old = bufArena;
        long before = currentBytes();
        Arena fresh = Arena.ofShared();
        MemorySegment newOffsets = fresh.allocate(ValueLayout.JAVA_INT, (long) newCapInt + 1);
        MemorySegment newData = fresh.allocate(dataCapacity);
        MemorySegment.copy(offsets, 0L, newOffsets, 0L, (long) (count + 1) * Integer.BYTES);
        if (dataPos > 0) {
            MemorySegment.copy(data, 0L, newData, 0L, dataPos);
        }
        this.bufArena = fresh;
        this.offsets = newOffsets;
        this.data = newData;
        this.capacity = newCapInt;
        long after = currentBytes();
        FfmOffHeapAccounting.COLUMNAR_BYTES.addAndGet(after - before);
        old.close();
        FfmOffHeapAccounting.FREED_ON_GROW.incrementAndGet();
    }

    private void ensureData(int additional) {
        int needed = dataPos + additional;
        if (needed <= dataCapacity) {
            return;
        }
        // R71-M1: int-overflow-safe doubling — see ensureCapacity above.
        if (needed < 0) {
            throw new IllegalArgumentException(
                    "ColumnarBatchBuffer.ensureData: needed=" + needed + " overflowed int");
        }
        long newCap = dataCapacity;
        while (newCap < needed) {
            newCap = Math.min(newCap * 2L, Integer.MAX_VALUE);
            if (newCap == Integer.MAX_VALUE && newCap < needed) {
                throw new IllegalArgumentException(
                        "ColumnarBatchBuffer.ensureData: needed="
                                + needed
                                + " exceeds Integer.MAX_VALUE — batch too large");
            }
        }
        int newCapInt = (int) newCap;
        // FRS-FFM-BOUND: free-on-grow via the per-buffer sub-arena (copies dataPos live bytes).
        reallocData(newCapInt, dataPos);
    }
}

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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Batch buffer for APPEND_MERGE requests.
 *
 * <p>Accumulates {@link AppendMergeRequest} entries so the executor can dispatch them as a batch.
 * Each entry carries one key slice and N value slices (one per list element). The multi-operand
 * per-key layout is stored as a parallel list of requests; the key {@link ColumnarBatchBuffer}
 * holds the key bytes.
 *
 * <p><b>B6-H1 (Round 6, heap-path 5-alloc burst).</b> The heap-fallback path used to allocate a
 * {@code byte[]} key, {@code byte[]} value, two {@link MemorySegment#ofArray} wrappers, a
 * 1-element {@code MemorySegment[]}, and an {@link AppendMergeRequest} wrapper per LIST_ADD row.
 * The buffer now exposes a parallel {@code valueBuffer} {@link ColumnarBatchBuffer} so the
 * classifier can flow heap-path values directly through the state's {@code serializeValueInto}
 * (which writes into the shared {@code DataOutputSerializer} and then copies into the off-heap
 * Arrow data column) — eliminating the {@code byte[] valBytes} hop, the wrapper segments, and the
 * {@code AppendMergeRequest} object on the hot path. The legacy {@link
 * #append(AppendMergeRequest)} entry remains for callers that still construct an
 * {@code AppendMergeRequest}; the new {@link #appendHeapRow(java.util.concurrent.CompletableFuture)}
 * path is preferred for in-classifier dispatch where keys + values were already serialized through
 * {@link #keyBuffer} / {@link #valueBuffer}.
 *
 * <p>Layout decisions deferred to P6 (real FFI wiring): the key buffer follows the standard Arrow
 * BinaryArray layout ({@link ColumnarBatchBuffer}); value slices are retained as a {@code List} of
 * {@code MemorySegment[]} arrays (one per request) for backward compat with multi-operand requests
 * — single-operand heap-path rows lift their bytes into {@code valueBuffer} and stash a
 * sentinel-marker so the executor's per-row dispatch can detect "value is in valueBuffer at index
 * i" without an extra list lookup.
 */
@Internal
public final class AppendMergeBatchBuffer {

    private final ColumnarBatchBuffer keyBuffer;
    /**
     * B6-H1: parallel value column for single-operand heap-path rows. {@code null} entries in
     * {@link #valueSliceLists} signal "value bytes live in {@code valueBuffer} at the same row
     * index"; non-null entries are legacy multi-operand or pre-built {@link AppendMergeRequest}
     * inputs that still carry their own {@code MemorySegment[]}.
     */
    private final ColumnarBatchBuffer valueBuffer;
    // Per-request: the list of value slices (one AppendMergeRequest may carry N slices). Entry is
    // null when the row's value bytes were appended through valueBuffer (B6-H1 fast path).
    private final List<MemorySegment[]> valueSliceLists = new ArrayList<>();
    private final List<CompletableFuture<Void>> futures = new ArrayList<>();

    public AppendMergeBatchBuffer(Arena arena) {
        this.keyBuffer = new ColumnarBatchBuffer(arena);
        this.valueBuffer = new ColumnarBatchBuffer(arena);
    }

    /**
     * Appends an {@link AppendMergeRequest} into this buffer.
     *
     * <p>B5-H5: routes the key slice directly into the off-heap {@code ColumnarBatchBuffer.data}
     * via the {@link ColumnarBatchBuffer#append(MemorySegment, long, int)} overload — no heap
     * {@code byte[]} hop per APPEND_MERGE row.
     *
     * <p>The pre-built {@link AppendMergeRequest} wrapper is preserved here for tests and any
     * external caller still constructing it directly; classifier heap-path now prefers
     * {@link #appendHeapRow(CompletableFuture)} to skip the wrapper allocation.
     */
    public void append(AppendMergeRequest req) {
        MemorySegment keySlice = req.keySlice();
        if (keySlice == null || keySlice == MemorySegment.NULL) {
            keyBuffer.appendEmpty();
        } else {
            long byteSize = keySlice.byteSize();
            if (byteSize == 0) {
                keyBuffer.appendEmpty();
            } else {
                keyBuffer.append(keySlice, 0L, (int) byteSize);
            }
        }
        // Keep valueBuffer columns aligned with row index so the executor can index either source
        // by `row` without a separate counter.
        valueBuffer.appendEmpty();
        valueSliceLists.add(req.valueSlices());
        futures.add(req.future());
    }

    /**
     * B6-H1: classifier heap-path entry — keys and values were already written into
     * {@link #keyBuffer} / {@link #valueBuffer} via {@code serializeKeyInto} /
     * {@code serializeValueInto}. Caller supplies the per-row {@link CompletableFuture} so the
     * executor can complete it after the batched FFI returns.
     */
    public void appendHeapRow(CompletableFuture<Void> future) {
        // null entry in valueSliceLists signals "value lives in valueBuffer at this index".
        valueSliceLists.add(null);
        futures.add(future);
    }

    /** Number of requests buffered. */
    public int count() {
        return valueSliceLists.size();
    }

    public boolean isEmpty() {
        return valueSliceLists.isEmpty();
    }

    /** The key column in Arrow BinaryArray layout (for FFI in P6). */
    public ColumnarBatchBuffer keyBuffer() {
        return keyBuffer;
    }

    /**
     * B6-H1: value column in Arrow BinaryArray layout. Rows whose {@link #valueSliceLists} entry
     * is {@code null} have their value bytes here at the same row index.
     */
    public ColumnarBatchBuffer valueBuffer() {
        return valueBuffer;
    }

    /** Per-request value slices. Index {@code i} corresponds to key entry {@code i}. */
    public List<MemorySegment[]> valueSliceLists() {
        return valueSliceLists;
    }

    /** Per-request futures to complete after dispatch. */
    public List<CompletableFuture<Void>> futures() {
        return futures;
    }

    /** Clears all state for reuse in the next batch. */
    public void reset() {
        keyBuffer.reset();
        valueBuffer.reset();
        valueSliceLists.clear();
        futures.clear();
    }
}

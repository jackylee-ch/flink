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
 * <p>Layout decisions deferred to P6 (real FFI wiring): the key buffer follows the standard Arrow
 * BinaryArray layout ({@link ColumnarBatchBuffer}); value slices are retained as a {@code List} of
 * {@code MemorySegment[]} arrays (one per request) to keep the column layout flexible until the P6
 * {@code frs_vec_merge_append} FFI signature is finalised.
 */
@Internal
public final class AppendMergeBatchBuffer {

    private final ColumnarBatchBuffer keyBuffer;
    // Per-request: the list of value slices (one AppendMergeRequest may carry N slices).
    private final List<MemorySegment[]> valueSliceLists = new ArrayList<>();
    private final List<CompletableFuture<Void>> futures = new ArrayList<>();

    public AppendMergeBatchBuffer(Arena arena) {
        this.keyBuffer = new ColumnarBatchBuffer(arena);
    }

    /** Appends an {@link AppendMergeRequest} into this buffer. */
    public void append(AppendMergeRequest req) {
        MemorySegment keySlice = req.keySlice();
        if (keySlice == null || keySlice == MemorySegment.NULL) {
            keyBuffer.appendEmpty();
        } else {
            long byteSize = keySlice.byteSize();
            if (byteSize == 0) {
                keyBuffer.appendEmpty();
            } else {
                byte[] keyBytes = new byte[(int) byteSize];
                MemorySegment.copy(keySlice, 0L, MemorySegment.ofArray(keyBytes), 0L, byteSize);
                keyBuffer.append(keyBytes);
            }
        }
        valueSliceLists.add(req.valueSlices());
        futures.add(req.future());
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
        valueSliceLists.clear();
        futures.clear();
    }
}

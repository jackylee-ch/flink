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

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Batch buffer for ITER_RANGE requests.
 *
 * <p>Accumulates {@link IterRangeRequest} entries. Each entry carries lo/hi bound slices plus a
 * caller-owned chunk-output buffer. Batch dispatch of range iterators is deferred to P9 ({@code
 * frs_vec_iter_range_open} FFI). Until then, the executor reads entries sequentially from this
 * buffer and throws {@link UnsupportedOperationException}.
 *
 * <p>Column layout: lo/hi slices are stored as raw {@link MemorySegment} references (not copied)
 * because they remain valid for the duration of the owning {@link
 * org.apache.flink.state.forstrs.exec.SlotArenaScope}. The P9 implementer decides the final
 * flat-buffer or scatter-list encoding.
 */
@Internal
public final class IterRangeBatchBuffer {

    private final List<MemorySegment> loSlices = new ArrayList<>();
    private final List<MemorySegment> hiSlices = new ArrayList<>();
    private final List<MemorySegment> chunkBufSlices = new ArrayList<>();
    private final List<CompletableFuture<IterRangeRequest.IterFirstChunk>> futures =
            new ArrayList<>();

    /** Appends an {@link IterRangeRequest} into this buffer. */
    public void append(IterRangeRequest req) {
        loSlices.add(req.loSlice());
        hiSlices.add(req.hiSlice());
        chunkBufSlices.add(req.chunkBufSlice());
        futures.add(req.future());
    }

    /** Number of requests buffered. */
    public int count() {
        return loSlices.size();
    }

    public boolean isEmpty() {
        return loSlices.isEmpty();
    }

    /** Per-request lower-bound slices (inclusive). Valid for the duration of the owning scope. */
    public List<MemorySegment> loSlices() {
        return loSlices;
    }

    /** Per-request upper-bound slices (exclusive). Valid for the duration of the owning scope. */
    public List<MemorySegment> hiSlices() {
        return hiSlices;
    }

    /** Per-request caller chunk buffers. Engine writes first-chunk results here. */
    public List<MemorySegment> chunkBufSlices() {
        return chunkBufSlices;
    }

    /** Per-request futures to complete after dispatch. */
    public List<CompletableFuture<IterRangeRequest.IterFirstChunk>> futures() {
        return futures;
    }

    /** Clears all state for reuse in the next batch. */
    public void reset() {
        loSlices.clear();
        hiSlices.clear();
        chunkBufSlices.clear();
        futures.clear();
    }
}

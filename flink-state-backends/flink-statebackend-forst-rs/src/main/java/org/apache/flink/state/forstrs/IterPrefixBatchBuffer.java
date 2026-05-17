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
 * Batch buffer for ITER_PREFIX requests.
 *
 * <p>Accumulates {@link IterPrefixRequest} entries. Each entry carries a prefix-bytes slice and a
 * caller-owned chunk-output buffer. Batch dispatch of prefix iterators is deferred to P3 ({@code
 * frs_vec_iter_prefix_open} FFI). Until then, the executor reads entries sequentially from this
 * buffer and throws {@link UnsupportedOperationException}.
 *
 * <p>Column layout: prefix slices are stored as raw {@link MemorySegment} references (not copied)
 * because they remain valid for the duration of the owning {@link
 * org.apache.flink.state.forstrs.exec.SlotArenaScope}. The P3 implementer decides whether to pack
 * them into a flat buffer or pass them as a scatter list.
 */
@Internal
public final class IterPrefixBatchBuffer {

    private final List<MemorySegment> prefixSlices = new ArrayList<>();
    private final List<MemorySegment> chunkBufSlices = new ArrayList<>();
    private final List<CompletableFuture<IterPrefixRequest.IterFirstChunk>> futures =
            new ArrayList<>();

    /** Appends an {@link IterPrefixRequest} into this buffer. */
    public void append(IterPrefixRequest req) {
        prefixSlices.add(req.prefixSlice());
        chunkBufSlices.add(req.chunkBufSlice());
        futures.add(req.future());
    }

    /** Number of requests buffered. */
    public int count() {
        return prefixSlices.size();
    }

    public boolean isEmpty() {
        return prefixSlices.isEmpty();
    }

    /** Per-request prefix slices. Valid for the duration of the owning SlotArenaScope. */
    public List<MemorySegment> prefixSlices() {
        return prefixSlices;
    }

    /** Per-request caller chunk buffers. Engine writes first-chunk results here. */
    public List<MemorySegment> chunkBufSlices() {
        return chunkBufSlices;
    }

    /** Per-request futures to complete after dispatch. */
    public List<CompletableFuture<IterPrefixRequest.IterFirstChunk>> futures() {
        return futures;
    }

    /** Clears all state for reuse in the next batch. */
    public void reset() {
        prefixSlices.clear();
        chunkBufSlices.clear();
        futures.clear();
    }
}

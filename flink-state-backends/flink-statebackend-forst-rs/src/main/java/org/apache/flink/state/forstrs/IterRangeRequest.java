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

import org.apache.flink.state.forstrs.exec.FrsIterHandle;
import org.apache.flink.state.forstrs.exec.SlotArenaScope;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.CompletableFuture;

/**
 * ITER_RANGE request — open a range-bounded iterator over the half-open interval {@code [lo, hi)}
 * and return the first chunk of results.
 *
 * <p>Similar to {@link IterPrefixRequest} but uses explicit lower/upper bound slices instead of a
 * prefix. Suitable for MapState range-scan queries or sorted-set window scans.
 */
public final class IterRangeRequest implements VectorizedStateRequest {

    private final String stateName;
    private final MemorySegment loSlice;
    private final MemorySegment hiSlice;
    private final MemorySegment chunkBufSlice;
    private final CompletableFuture<IterFirstChunk> future = new CompletableFuture<>();

    /**
     * @param stateName state name for classifier grouping and per-state metrics
     * @param loSlice off-heap lower-bound (inclusive); valid for the duration of the owning {@link
     *     SlotArenaScope}
     * @param hiSlice off-heap upper-bound (exclusive); valid for the duration of the owning {@link
     *     SlotArenaScope}
     * @param chunkBufSlice caller-allocated output buffer for the first chunk of iterator results
     */
    public IterRangeRequest(
            String stateName,
            MemorySegment loSlice,
            MemorySegment hiSlice,
            MemorySegment chunkBufSlice) {
        this.stateName = stateName;
        this.loSlice = loSlice;
        this.hiSlice = hiSlice;
        this.chunkBufSlice = chunkBufSlice;
    }

    /** Off-heap lower-bound key slice (inclusive). */
    public MemorySegment loSlice() {
        return loSlice;
    }

    /** Off-heap upper-bound key slice (exclusive). */
    public MemorySegment hiSlice() {
        return hiSlice;
    }

    /** Caller-allocated output buffer that the engine writes the first chunk of results into. */
    public MemorySegment chunkBufSlice() {
        return chunkBufSlice;
    }

    @Override
    public Kind kind() {
        return Kind.ITER_RANGE;
    }

    @Override
    public String stateName() {
        return stateName;
    }

    @Override
    public CompletableFuture<IterFirstChunk> future() {
        return future;
    }

    /**
     * First-chunk payload returned when the iterator is opened.
     *
     * @param handle native iterator handle for continuation calls (null if iteration is complete
     *     after the first chunk)
     * @param firstChunkRows number of rows written into the caller's chunk buffer
     */
    public record IterFirstChunk(FrsIterHandle handle, int firstChunkRows) {}
}

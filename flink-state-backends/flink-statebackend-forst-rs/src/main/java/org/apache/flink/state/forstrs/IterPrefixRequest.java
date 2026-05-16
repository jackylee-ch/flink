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
 * ITER_PREFIX request — open a prefix-bounded iterator and return the first chunk of results.
 *
 * <p>This is the off-heap successor to {@link ForStRsDBIterRequest}. The caller provides a chunk
 * buffer ({@link #chunkBufSlice()}) that the engine writes the first page of key/value pairs into.
 * The resolved {@link IterFirstChunk} record carries the native iterator handle (for continuation
 * via the {@code frs_iter_next_chunk} FFI call) plus the number of rows written into the chunk
 * buffer.
 */
public final class IterPrefixRequest implements VectorizedStateRequest {

    private final String stateName;
    private final MemorySegment prefixSlice;
    private final MemorySegment chunkBufSlice;
    private final CompletableFuture<IterFirstChunk> future = new CompletableFuture<>();

    /**
     * @param stateName state name for classifier grouping and per-state metrics
     * @param prefixSlice off-heap prefix bytes; valid for the duration of the owning {@link
     *     SlotArenaScope}
     * @param chunkBufSlice caller-allocated output buffer for the first chunk of iterator results
     */
    public IterPrefixRequest(
            String stateName, MemorySegment prefixSlice, MemorySegment chunkBufSlice) {
        this.stateName = stateName;
        this.prefixSlice = prefixSlice;
        this.chunkBufSlice = chunkBufSlice;
    }

    /** Off-heap prefix bytes used to open the iterator. */
    public MemorySegment prefixSlice() {
        return prefixSlice;
    }

    /** Caller-allocated output buffer that the engine writes the first chunk of results into. */
    public MemorySegment chunkBufSlice() {
        return chunkBufSlice;
    }

    @Override
    public Kind kind() {
        return Kind.ITER_PREFIX;
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

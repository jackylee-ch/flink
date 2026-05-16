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

import org.apache.flink.state.forstrs.exec.SlotArenaScope;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.CompletableFuture;

/**
 * APPEND_MERGE request — ListState-only per spec §1 §a.
 *
 * <p>The classifier (P2 Batch C) enforces this restriction: Reducing/Aggregating state MUST use
 * GET+combine+PUT via the RMW cache (P7), not append-merge. Violating callers will receive an
 * {@link IllegalArgumentException} at classification time.
 *
 * <p>Carries N operand slices ({@link #valueSlices()}) that are appended as a single merge run
 * into the engine's merge operator for the target key.
 */
public final class AppendMergeRequest implements VectorizedStateRequest {

    private final String stateName;
    private final MemorySegment keySlice;
    private final MemorySegment[] valueSlices;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    /**
     * @param stateName state name for classifier grouping and per-state metrics
     * @param keySlice off-heap key slice; valid for the duration of the owning {@link SlotArenaScope}
     * @param valueSlices N operand slices to append as a single merge run; each slice is one list
     *     element serialized to bytes
     */
    public AppendMergeRequest(
            String stateName, MemorySegment keySlice, MemorySegment[] valueSlices) {
        this.stateName = stateName;
        this.keySlice = keySlice;
        this.valueSlices = valueSlices;
    }

    /** Off-heap key slice. */
    public MemorySegment keySlice() {
        return keySlice;
    }

    /**
     * Off-heap value slices to append as a merge run. Each slice is a single list element
     * serialized to bytes.
     */
    public MemorySegment[] valueSlices() {
        return valueSlices;
    }

    @Override
    public Kind kind() {
        return Kind.APPEND_MERGE;
    }

    @Override
    public String stateName() {
        return stateName;
    }

    @Override
    public CompletableFuture<Void> future() {
        return future;
    }
}

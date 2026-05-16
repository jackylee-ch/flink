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
 * DELETE request for a single key via the off-heap vectorized path.
 *
 * <p>This is a new-style request (post-{@link VectorizedStateRequest}) that uses a real {@link
 * CompletableFuture} for completion rather than Flink's internal async future.
 */
public final class DeleteRequest implements VectorizedStateRequest {

    private final String stateName;
    private final MemorySegment keySlice;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    /**
     * @param stateName state name for classifier grouping and per-state metrics
     * @param keySlice off-heap key slice; valid for the duration of the owning {@link SlotArenaScope}
     */
    public DeleteRequest(String stateName, MemorySegment keySlice) {
        this.stateName = stateName;
        this.keySlice = keySlice;
    }

    /** Off-heap key slice. Valid for the duration of the owning {@link SlotArenaScope}. */
    public MemorySegment keySlice() {
        return keySlice;
    }

    @Override
    public Kind kind() {
        return Kind.DELETE;
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

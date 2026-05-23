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
import org.apache.flink.runtime.asyncprocessing.StateRequest;

import java.util.concurrent.CompletableFuture;

/**
 * Encapsulates a single put (or delete) request for batch execution. Holds serialized key + value
 * bytes and a reference to the original StateRequest.
 *
 * <p>Implements {@link VectorizedStateRequest} as Kind.PUT. The {@link #future()} method returns
 * {@code null} because completion is handled via Flink's internal {@code InternalAsyncFuture}. Use
 * {@link #complete()} instead.
 */
@Internal
public non-sealed class ForStRsDBPutRequest<K, N, V> implements VectorizedStateRequest {

    private final byte[] serializedKey;
    private final byte[] serializedValue;
    private final StateRequest<K, N, ?, ?> request;
    private String stateName = "unknown";

    public ForStRsDBPutRequest(
            byte[] serializedKey, byte[] serializedValue, StateRequest<K, N, ?, ?> request) {
        this.serializedKey = serializedKey;
        this.serializedValue = serializedValue;
        this.request = request;
    }

    public byte[] getSerializedKey() {
        return serializedKey;
    }

    /** Returns null for delete operations. */
    public byte[] getSerializedValue() {
        return serializedValue;
    }

    // --- VectorizedStateRequest implementation ---

    @Override
    public Kind kind() {
        return Kind.PUT;
    }

    /**
     * V1 transition placeholder — returns {@code "unknown"} unless {@link #setStateName} called.
     */
    @Override
    public String stateName() {
        return stateName;
    }

    /** Sets the state name for classifier grouping and per-state metrics. */
    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    /**
     * Returns {@code null} — completion uses Flink's {@code InternalAsyncFuture} via {@link
     * #complete()}.
     */
    @Override
    public CompletableFuture<?> future() {
        return null;
    }

    @SuppressWarnings("unchecked")
    public void complete() {
        ((org.apache.flink.core.asyncprocessing.InternalAsyncFuture<Object>) request.getFuture())
                .complete(null);
    }

    /**
     * E8-H3: completes the underlying {@code InternalAsyncFuture} with a failure so the
     * runtime mailbox unblocks when the executor catches a throw before calling {@link
     * #complete()}.
     */
    @SuppressWarnings("unchecked")
    public void completeExceptionally(Throwable t) {
        ((org.apache.flink.core.asyncprocessing.InternalAsyncFuture<Object>) request.getFuture())
                .completeExceptionally("PUT request failed", t);
    }
}

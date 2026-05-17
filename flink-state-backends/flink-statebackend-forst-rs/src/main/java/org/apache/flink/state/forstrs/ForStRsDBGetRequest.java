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
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.concurrent.CompletableFuture;

/**
 * Encapsulates a single get request for batch execution. Holds the serialized key and a reference
 * to the original StateRequest so the result can be completed.
 *
 * <p>Implements {@link VectorizedStateRequest} as Kind.GET. The {@link #future()} method returns
 * {@code null} because completion is handled via Flink's internal {@code InternalAsyncFuture}
 * (pre-{@link VectorizedStateRequest} completion path). Use {@link #complete(byte[])} instead. The
 * {@link #stateName()} method returns {@code "unknown"} as a V1 transition placeholder; callers
 * that need accurate per-state attribution should call {@link #setStateName(String)} after
 * construction.
 */
@Internal
public non-sealed class ForStRsDBGetRequest<K, N, V> implements VectorizedStateRequest {

    private final byte[] serializedKey;
    private final StateRequest<K, N, ?, V> request;
    private final ForStRsInnerTable<K, N, ?> table;
    private String stateName = "unknown";

    @SuppressWarnings("unchecked")
    public ForStRsDBGetRequest(
            byte[] serializedKey,
            StateRequest<K, N, ?, ?> request,
            ForStRsInnerTable<K, N, ?> table) {
        this.serializedKey = serializedKey;
        this.request = (StateRequest<K, N, ?, V>) request;
        this.table = table;
    }

    public byte[] getSerializedKey() {
        return serializedKey;
    }

    public StateRequest<K, N, ?, V> getRequest() {
        return request;
    }

    // --- VectorizedStateRequest implementation ---

    @Override
    public Kind kind() {
        return Kind.GET;
    }

    /**
     * V1 transition placeholder — returns {@code "unknown"} unless {@link #setStateName(String)}
     * has been called.
     */
    @Override
    public String stateName() {
        return stateName;
    }

    /**
     * Sets the state name for classifier grouping and per-state metrics. Call this after
     * construction when the originating state name is known.
     */
    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    /**
     * Returns {@code null} — completion uses Flink's {@code InternalAsyncFuture} via {@link
     * #complete(byte[])}. This is the pre-{@link VectorizedStateRequest} completion path retained
     * for backward compatibility.
     */
    @Override
    public CompletableFuture<?> future() {
        return null;
    }

    @SuppressWarnings("unchecked")
    public void complete(byte[] rawValue) {
        Object result;
        if (request.getRequestType() == StateRequestType.MAP_CONTAINS) {
            result = rawValue != null;
        } else {
            result = rawValue == null ? null : table.deserializeValue(rawValue);
        }
        ((org.apache.flink.core.asyncprocessing.InternalAsyncFuture<Object>) request.getFuture())
                .complete(result);
    }
}

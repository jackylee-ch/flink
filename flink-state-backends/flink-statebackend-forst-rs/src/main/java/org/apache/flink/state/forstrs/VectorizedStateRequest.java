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

import java.util.concurrent.CompletableFuture;

/**
 * Sealed envelope for every state request that flows through the V1 dispatch path (umbrella spec §1
 * component 1). Six kinds:
 *
 * <pre>
 *   GET, PUT, DELETE, APPEND_MERGE, ITER_PREFIX, ITER_RANGE
 * </pre>
 *
 * <p><b>Package placement note:</b> Java sealed interfaces (JEP 409) require all permitted subtypes
 * to reside in the same package when the unnamed module is in use (no {@code module-info.java}).
 * The three pre-existing concrete classes ({@link ForStRsDBGetRequest}, {@link
 * ForStRsDBPutRequest}, {@link ForStRsDBIterRequest}) are in {@code
 * org.apache.flink.state.forstrs}, so this interface lives here too. The four new subtypes
 * ({@link DeleteRequest}, {@link AppendMergeRequest}, {@link IterPrefixRequest}, {@link
 * IterRangeRequest}) are also placed in this package for the same reason.
 *
 * <p>The three pre-existing concrete classes were introduced before this interface and complete
 * their futures via Flink's {@code InternalAsyncFuture} rather than a {@code CompletableFuture}.
 * Their {@link #future()} implementations return {@code null} as a sentinel meaning "uses
 * Flink-internal completion path". The four new subtypes are pure off-heap types that expose a real
 * {@code CompletableFuture}.
 *
 * <p>V1.x will introduce classifier-internal sub-kinds (GET_SAME_KEY / GET_CROSS_KEY,
 * POINT_DELETE / PREFIX_DELETE) without breaking this Java-facing API.
 */
public sealed interface VectorizedStateRequest
        permits ForStRsDBGetRequest,
                ForStRsDBPutRequest,
                ForStRsDBIterRequest,
                DeleteRequest,
                AppendMergeRequest,
                IterPrefixRequest,
                IterRangeRequest {

    /**
     * The six dispatch kinds for the V1 dispatch table (spec §1).
     *
     * <p>Note: {@code ITER_PREFIX} maps to the existing {@link ForStRsDBIterRequest} which performs
     * prefix-bounded iteration. {@code ITER_RANGE} is a new kind for explicit [lo, hi) range scans.
     */
    enum Kind {
        GET,
        PUT,
        DELETE,
        APPEND_MERGE,
        ITER_PREFIX,
        ITER_RANGE
    }

    /** Which kind of request this is. */
    Kind kind();

    /**
     * State name used for classifier grouping and per-state metrics.
     *
     * <p>Pre-existing classes ({@link ForStRsDBGetRequest}, {@link ForStRsDBPutRequest}, {@link
     * ForStRsDBIterRequest}) return {@code "unknown"} as a transition-state placeholder. Callers
     * should use the setter {@code setStateName(String)} on those classes if they need accurate
     * per-state attribution.
     */
    String stateName();

    /**
     * Future that resolves with the request's result.
     *
     * <p>Returns {@code null} for pre-existing Flink-internal-completion classes. Returns a real
     * {@link CompletableFuture} for the new off-heap subtypes.
     */
    CompletableFuture<?> future();
}

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

package org.apache.flink.state.forstrs.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;
import org.apache.flink.state.forstrs.state.ForStRsMapState;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Async wrapper around {@link ForStRsMapState} for Flink 2.x async stateful operators (spec §6e).
 *
 * <p><b>Iteration semantics.</b> {@link ForStRsMapState#iterator()} returns a stateful, FFM-backed
 * cursor; calling that off-thread is unsafe because the underlying ForSt-RS iterator + Arena
 * lifetime is owned by the worker thread. To keep the async surface easy to reason about we
 * materialize iterations into snapshot {@link List}s on the worker thread before completing the
 * future. This costs O(N) memory per call but is correct under concurrent {@code setCurrentKey}
 * calls and outlives the worker arena. Callers who need streaming-iteration semantics can fall back
 * to the sync state via the keyed-backend.
 *
 * @param <K> backend key type
 * @param <UK> user-key type
 * @param <UV> user-value type
 */
@Internal
public final class ForStRsAsyncMapState<K, UK, UV> {

    private final ForStRsKeyedStateBackend<K> backend;
    private final String stateName;
    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;
    private final PerKeyFuturesChain<K> chain;

    public ForStRsAsyncMapState(
            ForStRsKeyedStateBackend<K> backend,
            String stateName,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer,
            PerKeyFuturesChain<K> chain) {
        this.backend = backend;
        this.stateName = stateName;
        this.userKeySerializer = userKeySerializer;
        this.userValueSerializer = userValueSerializer;
        this.chain = chain;
    }

    /** Async {@link ForStRsMapState#get(Object)}. */
    public CompletableFuture<UV> get(UK uk) {
        return runOnKey(
                state -> {
                    try {
                        return state.get(uk);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsMapState#put(Object, Object)}. */
    public CompletableFuture<Void> put(UK uk, UV uv) {
        return runOnKey(
                state -> {
                    try {
                        state.put(uk, uv);
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsMapState#remove(Object)}. */
    public CompletableFuture<Void> remove(UK uk) {
        return runOnKey(
                state -> {
                    state.remove(uk);
                    return null;
                });
    }

    /** Async {@link ForStRsMapState#contains(Object)}. */
    public CompletableFuture<Boolean> contains(UK uk) {
        return runOnKey(
                state -> {
                    try {
                        return state.contains(uk);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsMapState#isEmpty()}. */
    public CompletableFuture<Boolean> isEmpty() {
        return runOnKey(
                state -> {
                    try {
                        return state.isEmpty();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Async snapshot of {@link ForStRsMapState#keys()}: returns a fully-materialized list of user
     * keys captured on the worker thread. Iteration is safe to consume on any thread.
     */
    public CompletableFuture<List<UK>> keys() {
        return runOnKey(
                state -> {
                    try {
                        List<UK> out = new ArrayList<>();
                        for (UK k : state.keys()) {
                            out.add(k);
                        }
                        return out;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Async snapshot of {@link ForStRsMapState#values()}: returns a fully-materialized list of user
     * values captured on the worker thread.
     */
    public CompletableFuture<List<UV>> values() {
        return runOnKey(
                state -> {
                    try {
                        List<UV> out = new ArrayList<>();
                        for (UV v : state.values()) {
                            out.add(v);
                        }
                        return out;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Async snapshot of {@link ForStRsMapState#entries()}: returns a fully-materialized list of
     * immutable {@link Map.Entry} pairs captured on the worker thread.
     *
     * <p>This is the recommended replacement for the sync {@code iterator()} call — see class
     * Javadoc for the rationale on returning {@code List} instead of {@code Iterator}.
     */
    public CompletableFuture<List<Map.Entry<UK, UV>>> entries() {
        return runOnKey(
                state -> {
                    try {
                        List<Map.Entry<UK, UV>> out = new ArrayList<>();
                        for (Map.Entry<UK, UV> e : state.entries()) {
                            out.add(
                                    new AbstractMap.SimpleImmutableEntry<>(
                                            e.getKey(), e.getValue()));
                        }
                        return out;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsMapState#clear()}. */
    public CompletableFuture<Void> clear() {
        return runOnKey(
                state -> {
                    state.clear();
                    return null;
                });
    }

    private <R> CompletableFuture<R> runOnKey(Function<ForStRsMapState<UK, UV>, R> op) {
        final K capturedKey = backend.getCurrentKey();
        if (capturedKey == null) {
            CompletableFuture<R> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new IllegalStateException(
                            "ForStRsAsyncMapState: getCurrentKey() returned null"));
            return failed;
        }
        return chain.enqueue(
                capturedKey,
                () -> {
                    // See ForStRsAsyncValueState.runOnKey for the rationale on locking the
                    // delegate during the setCurrentKey + buildPrefix + state-op window.
                    synchronized (backend) {
                        backend.setCurrentKey(capturedKey);
                        ForStRsMapState<UK, UV> state =
                                backend.getMapState(
                                        stateName, userKeySerializer, userValueSerializer);
                        return op.apply(state);
                    }
                });
    }
}

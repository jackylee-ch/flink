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
import org.apache.flink.state.forstrs.state.ForStRsValueState;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Async wrapper around {@link ForStRsValueState} for Flink 2.x async stateful operators (spec §6e).
 *
 * <p><b>Two key-binding modes.</b>
 *
 * <ol>
 *   <li><b>Implicit (current-key)</b>: {@link #value()} / {@link #update(Object)} / {@link
 *       #clear()} capture {@linkplain ForStRsKeyedStateBackend#getCurrentKey
 *       backend.getCurrentKey()} at the moment of the API call. Caller is responsible for ensuring
 *       no other thread is concurrently mutating the backend's current key (i.e., this is the
 *       natural mode for single-threaded operator code that issues async ops).
 *   <li><b>Explicit-key</b>: {@link #value(Object)} / {@link #update(Object, Object)} / {@link
 *       #clear(Object)} accept the key directly. This is the recommended mode for chained
 *       continuations (e.g., {@code state.value(k).thenCompose(v -> state.update(k, v + 1))}) and
 *       for thread-pool submitters where a stable key is captured ahead of time.
 * </ol>
 *
 * <p><b>Worker-thread safety.</b> The underlying L5 delegate is single-threaded by design — its
 * {@code setCurrentKey}/{@code buildPrefix} use shared mutable buffers. To make the async API safe
 * under cross-key parallelism, every worker-thread step (set-key + state-fetch + op) synchronises
 * on the {@linkplain ForStRsKeyedStateBackend backend}. Per-key serialisation is still provided by
 * {@link PerKeyFuturesChain}; the lock just protects the shared-buffer window.
 *
 * @param <K> backend key type
 * @param <T> state value type
 */
@Internal
public final class ForStRsAsyncValueState<K, T> {

    private final ForStRsKeyedStateBackend<K> backend;
    private final String stateName;
    private final TypeSerializer<T> valueSerializer;
    private final PerKeyFuturesChain<K> chain;

    public ForStRsAsyncValueState(
            ForStRsKeyedStateBackend<K> backend,
            String stateName,
            TypeSerializer<T> valueSerializer,
            PerKeyFuturesChain<K> chain) {
        this.backend = backend;
        this.stateName = stateName;
        this.valueSerializer = valueSerializer;
        this.chain = chain;
    }

    /**
     * Implicit-key {@link ForStRsValueState#value()} — captures {@code backend.getCurrentKey()}.
     */
    public CompletableFuture<T> value() {
        K k = backend.getCurrentKey();
        if (k == null) {
            return failedFuture("ForStRsAsyncValueState.value()");
        }
        return value(k);
    }

    /** Explicit-key {@link ForStRsValueState#value()}. */
    public CompletableFuture<T> value(K key) {
        return runOnKey(
                key,
                state -> {
                    try {
                        return state.value();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Implicit-key {@link ForStRsValueState#update(Object)}. */
    public CompletableFuture<Void> update(T newValue) {
        K k = backend.getCurrentKey();
        if (k == null) {
            return failedFuture("ForStRsAsyncValueState.update()");
        }
        return update(k, newValue);
    }

    /** Explicit-key {@link ForStRsValueState#update(Object)}. */
    public CompletableFuture<Void> update(K key, T newValue) {
        return runOnKey(
                key,
                state -> {
                    try {
                        state.update(newValue);
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Implicit-key {@link ForStRsValueState#clear()}. */
    public CompletableFuture<Void> clear() {
        K k = backend.getCurrentKey();
        if (k == null) {
            return failedFuture("ForStRsAsyncValueState.clear()");
        }
        return clear(k);
    }

    /** Explicit-key {@link ForStRsValueState#clear()}. */
    public CompletableFuture<Void> clear(K key) {
        return runOnKey(
                key,
                state -> {
                    state.clear();
                    return null;
                });
    }

    private <R> CompletableFuture<R> failedFuture(String caller) {
        CompletableFuture<R> failed = new CompletableFuture<>();
        failed.completeExceptionally(
                new IllegalStateException(
                        caller
                                + ": getCurrentKey() returned null — setCurrentKey must be"
                                + " invoked before implicit-key async ops"));
        return failed;
    }

    /**
     * Enqueues {@code op} on the per-key chain for {@code key}. The worker thread acquires the
     * backend monitor before binding the delegate's current key + fetching the sync state +
     * invoking the op.
     */
    private <R> CompletableFuture<R> runOnKey(K key, Function<ForStRsValueState<T>, R> op) {
        if (key == null) {
            CompletableFuture<R> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("key must be non-null"));
            return failed;
        }
        return chain.enqueue(
                key,
                () -> {
                    synchronized (backend) {
                        backend.setCurrentKey(key);
                        ForStRsValueState<T> state =
                                backend.getValueState(stateName, valueSerializer);
                        return op.apply(state);
                    }
                });
    }
}

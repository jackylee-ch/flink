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
import org.apache.flink.state.forstrs.state.ForStRsListState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Async wrapper around {@link ForStRsListState} for Flink 2.x async stateful operators (spec §6e).
 *
 * <p>Note on {@code get()}: the underlying sync state returns an {@link Iterable}, but to make the
 * result safe to consume on any thread (and immune to subsequent backend mutations from other keys)
 * we materialize it into a {@link List} before completing the future.
 *
 * @param <K> backend key type
 * @param <T> element type
 */
@Internal
public final class ForStRsAsyncListState<K, T> {

    private final ForStRsKeyedStateBackend<K> backend;
    private final String stateName;
    private final TypeSerializer<T> elementSerializer;
    private final PerKeyFuturesChain<K> chain;

    public ForStRsAsyncListState(
            ForStRsKeyedStateBackend<K> backend,
            String stateName,
            TypeSerializer<T> elementSerializer,
            PerKeyFuturesChain<K> chain) {
        this.backend = backend;
        this.stateName = stateName;
        this.elementSerializer = elementSerializer;
        this.chain = chain;
    }

    /** Async {@link ForStRsListState#get()} — materialized into a snapshot {@link List}. */
    public CompletableFuture<List<T>> get() {
        return runOnKey(
                state -> {
                    try {
                        List<T> out = new ArrayList<>();
                        for (T t : state.get()) {
                            out.add(t);
                        }
                        return out;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsListState#add(Object)}. */
    public CompletableFuture<Void> add(T value) {
        return runOnKey(
                state -> {
                    try {
                        state.add(value);
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsListState#update(List)}. */
    public CompletableFuture<Void> update(List<T> values) {
        return runOnKey(
                state -> {
                    try {
                        state.update(values);
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsListState#addAll(List)}. */
    public CompletableFuture<Void> addAll(List<T> values) {
        return runOnKey(
                state -> {
                    try {
                        state.addAll(values);
                        return null;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsListState#clear()}. */
    public CompletableFuture<Void> clear() {
        return runOnKey(
                state -> {
                    state.clear();
                    return null;
                });
    }

    private <R> CompletableFuture<R> runOnKey(Function<ForStRsListState<T>, R> op) {
        final K capturedKey = backend.getCurrentKey();
        if (capturedKey == null) {
            CompletableFuture<R> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new IllegalStateException(
                            "ForStRsAsyncListState: getCurrentKey() returned null"));
            return failed;
        }
        return chain.enqueue(
                capturedKey,
                () -> {
                    // E-R4-H1: save/restore currentKey — see
                    // ForStRsAsyncMapState.runOnKey for the rationale.
                    synchronized (backend) {
                        @SuppressWarnings("unchecked")
                        K priorKey = (K) backend.getCurrentKey();
                        backend.setCurrentKey(capturedKey);
                        try {
                            ForStRsListState<T> state =
                                    backend.getListState(stateName, elementSerializer);
                            return op.apply(state);
                        } finally {
                            backend.setCurrentKey(priorKey);
                        }
                    }
                });
    }
}

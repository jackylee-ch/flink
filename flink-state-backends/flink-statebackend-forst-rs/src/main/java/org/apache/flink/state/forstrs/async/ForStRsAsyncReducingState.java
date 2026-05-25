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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;
import org.apache.flink.state.forstrs.state.ForStRsReducingState;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Async wrapper around {@link ForStRsReducingState} for Flink 2.x async stateful operators (spec
 * §6e).
 *
 * @param <K> backend key type
 * @param <T> element / accumulator type
 */
@Internal
public final class ForStRsAsyncReducingState<K, T> {

    private final ForStRsKeyedStateBackend<K> backend;
    private final String stateName;
    private final TypeSerializer<T> serializer;
    private final ReduceFunction<T> reduceFunction;
    private final PerKeyFuturesChain<K> chain;

    public ForStRsAsyncReducingState(
            ForStRsKeyedStateBackend<K> backend,
            String stateName,
            TypeSerializer<T> serializer,
            ReduceFunction<T> reduceFunction,
            PerKeyFuturesChain<K> chain) {
        this.backend = backend;
        this.stateName = stateName;
        this.serializer = serializer;
        this.reduceFunction = reduceFunction;
        this.chain = chain;
    }

    /** Async {@link ForStRsReducingState#get()}. */
    public CompletableFuture<T> get() {
        return runOnKey(
                state -> {
                    try {
                        return state.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsReducingState#add(Object)}. */
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

    /** Async {@link ForStRsReducingState#clear()}. */
    public CompletableFuture<Void> clear() {
        return runOnKey(
                state -> {
                    state.clear();
                    return null;
                });
    }

    private <R> CompletableFuture<R> runOnKey(Function<ForStRsReducingState<T>, R> op) {
        final K capturedKey = backend.getCurrentKey();
        if (capturedKey == null) {
            CompletableFuture<R> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new IllegalStateException(
                            "ForStRsAsyncReducingState: getCurrentKey() returned null"));
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
                            ForStRsReducingState<T> state =
                                    backend.getReducingState(
                                            stateName, serializer, reduceFunction);
                            return op.apply(state);
                        } finally {
                            backend.setCurrentKey(priorKey);
                        }
                    }
                });
    }
}

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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;
import org.apache.flink.state.forstrs.state.ForStRsAggregatingState;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Async wrapper around {@link ForStRsAggregatingState} for Flink 2.x async stateful operators (spec
 * §6e).
 *
 * @param <K> backend key type
 * @param <IN> input value type
 * @param <ACC> accumulator type (persisted)
 * @param <OUT> result type
 */
@Internal
public final class ForStRsAsyncAggregatingState<K, IN, ACC, OUT> {

    private final ForStRsKeyedStateBackend<K> backend;
    private final String stateName;
    private final TypeSerializer<ACC> accSerializer;
    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;
    private final PerKeyFuturesChain<K> chain;

    public ForStRsAsyncAggregatingState(
            ForStRsKeyedStateBackend<K> backend,
            String stateName,
            TypeSerializer<ACC> accSerializer,
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            PerKeyFuturesChain<K> chain) {
        this.backend = backend;
        this.stateName = stateName;
        this.accSerializer = accSerializer;
        this.aggregateFunction = aggregateFunction;
        this.chain = chain;
    }

    /** Async {@link ForStRsAggregatingState#get()}. */
    public CompletableFuture<OUT> get() {
        return runOnKey(
                state -> {
                    try {
                        return state.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /** Async {@link ForStRsAggregatingState#add(Object)}. */
    public CompletableFuture<Void> add(IN value) {
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

    /** Async {@link ForStRsAggregatingState#clear()}. */
    public CompletableFuture<Void> clear() {
        return runOnKey(
                state -> {
                    state.clear();
                    return null;
                });
    }

    private <R> CompletableFuture<R> runOnKey(
            Function<ForStRsAggregatingState<IN, ACC, OUT>, R> op) {
        final K capturedKey = backend.getCurrentKey();
        if (capturedKey == null) {
            CompletableFuture<R> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new IllegalStateException(
                            "ForStRsAsyncAggregatingState: getCurrentKey() returned null"));
            return failed;
        }
        return chain.enqueue(
                capturedKey,
                () -> {
                    // See ForStRsAsyncValueState.runOnKey for the rationale on locking the
                    // delegate during the setCurrentKey + buildPrefix + state-op window.
                    synchronized (backend) {
                        backend.setCurrentKey(capturedKey);
                        ForStRsAggregatingState<IN, ACC, OUT> state =
                                backend.getAggregatingState(
                                        stateName, accSerializer, aggregateFunction);
                        return op.apply(state);
                    }
                });
    }
}

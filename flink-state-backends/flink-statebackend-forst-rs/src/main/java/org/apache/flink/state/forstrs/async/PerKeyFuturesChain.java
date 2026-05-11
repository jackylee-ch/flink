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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Per-key serialization of asynchronous work via a {@link ConcurrentHashMap} of {@link
 * CompletableFuture chain heads}.
 *
 * <p>Spec §6e (async state API): Flink 2.x async stateful operators may submit multiple async state
 * ops for the same key in flight (e.g., {@code value().thenCompose(v -> update(v + 1))}). Per-key
 * serialization preserves "happens-before" semantics: ops for the same key execute in submit order
 * on a worker thread; ops for distinct keys run in parallel.
 *
 * <p><b>Wire-up.</b> One {@code PerKeyFuturesChain} instance is shared across all async state
 * objects of a single keyed-backend (so {@code valueState.update(K1, v).thenCompose(__ ->
 * listState.add(K1, x))} respects ordering across state types). The {@code Executor} is typically
 * {@link java.util.concurrent.Executors#newVirtualThreadPerTaskExecutor()} (JDK 21+) so the chain
 * can scale to many in-flight per-key tails without exhausting platform threads.
 *
 * <p><b>Memory.</b> The chain head is removed from the map when its future completes, so chains
 * shrink to empty when there is no in-flight work for a key. Cross-key parallelism is unbounded;
 * the {@code Executor} alone caps total parallelism.
 *
 * @param <K> key type
 */
@Internal
public final class PerKeyFuturesChain<K> {

    private final ConcurrentHashMap<K, CompletableFuture<?>> chains = new ConcurrentHashMap<>();
    private final Executor executor;

    /**
     * Constructs a new chain with the supplied executor for all enqueued work.
     *
     * @param executor the executor on which the {@link Supplier work} will run; must be non-{@code
     *     null}. Typically {@link
     *     java.util.concurrent.Executors#newVirtualThreadPerTaskExecutor()}.
     */
    public PerKeyFuturesChain(Executor executor) {
        if (executor == null) {
            throw new NullPointerException("executor");
        }
        this.executor = executor;
    }

    /**
     * Atomically enqueues {@code work} on the per-key chain for {@code key}. The returned future
     * completes after the work runs on the configured executor; subsequent {@code enqueue(key, …)}
     * calls observe the same head and chain off this future, so per-key ordering is preserved.
     *
     * <p>If the supplied {@code work} throws, the returned future completes exceptionally with the
     * original throwable; the chain still advances so subsequent ops for the same key proceed.
     *
     * @param key the key under which to serialize the work
     * @param work supplier to invoke on the worker thread; its return value becomes the future's
     *     value
     * @param <T> result type
     * @return a {@link CompletableFuture} that completes with the work's result (or exception) once
     *     the chain reaches it
     */
    public <T> CompletableFuture<T> enqueue(K key, Supplier<T> work) {
        if (key == null) {
            throw new NullPointerException("key");
        }
        if (work == null) {
            throw new NullPointerException("work");
        }
        final CompletableFuture<T> result = new CompletableFuture<>();
        chains.compute(
                key,
                (k, prior) -> {
                    CompletableFuture<?> base =
                            prior != null ? prior : CompletableFuture.completedFuture(null);
                    base.whenCompleteAsync(
                            (__, ___) -> {
                                try {
                                    result.complete(work.get());
                                } catch (Throwable t) {
                                    result.completeExceptionally(t);
                                }
                            },
                            executor);
                    return result;
                });
        // Cleanup: when this future completes, remove from the map IF still the head.
        // Using compute() preserves atomicity vs. concurrent enqueues that might have already
        // installed a successor head.
        result.whenComplete(
                (__, ___) ->
                        chains.compute(key, (k, current) -> current == result ? null : current));
        return result;
    }

    /**
     * Returns the number of distinct keys with at least one in-flight or recently-completed
     * (cleanup not yet observed) chain head. Test/diagnostic only — production code must not make
     * decisions based on this number, since it has no meaningful happens-before relationship with
     * concurrent {@link #enqueue} calls.
     */
    public int activeKeyCount() {
        return chains.size();
    }
}

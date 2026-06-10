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

package org.apache.flink.state.forstrs.exec;

import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers.RecordingFuture;
import org.apache.flink.state.forstrs.VectorizedExecutor;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PR-1 contract tests for {@link CoordinatedStateExecutor} — the three properties that
 * distinguish it from {@link RoutingStateExecutor}:
 *
 * <ol>
 *   <li>{@code executeBatchRequests} returns an INCOMPLETE future (no caller-thread blocking);
 *   <li>{@code fullyLoaded()} reflects outstanding sub-batches (real AEC backpressure);
 *   <li>batches execute on worker threads, routed by key-group, and the future completes
 *       asynchronously (success and failure paths).
 * </ol>
 *
 * <p>Workers are latch-instrumented {@link VectorizedExecutor} subclasses — no native FFI is
 * touched (same pattern as {@code VectorizedExecutorOrderingHazardTest}'s RecordingExecutor).
 */
class CoordinatedStateExecutorTest {

    private static final int WORKERS = 3;

    private Arena arena;
    private StubWorker[] workers;
    private CoordinatedStateExecutor executor;

    /** A worker whose executeBatchRequests parks on a latch and records its executing thread. */
    private static final class StubWorker extends VectorizedExecutor {
        final CountDownLatch gate = new CountDownLatch(1);
        final List<String> executedOnThreads = new CopyOnWriteArrayList<>();
        final AtomicInteger executed = new AtomicInteger();
        volatile boolean failNextBatch;

        StubWorker(ForStRsLinker linker, Arena arena) {
            super(linker, BatchedFailurePropagationTestHelpers.stubDb(),
                    BatchedFailurePropagationTestHelpers.stubCf(), arena);
        }

        @Override
        public CompletableFuture<Void> executeBatchRequests(
                AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
            executedOnThreads.add(Thread.currentThread().getName());
            try {
                gate.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executed.incrementAndGet();
            if (failNextBatch) {
                return CompletableFuture.failedFuture(new IllegalStateException("boom"));
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    @BeforeEach
    void setUp() {
        arena = Arena.ofShared();
        ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
        workers = new StubWorker[WORKERS];
        for (int i = 0; i < WORKERS; i++) {
            workers[i] = new StubWorker(linker, arena);
        }
        executor = new CoordinatedStateExecutor(workers, null);
    }

    @AfterEach
    void tearDown() {
        for (StubWorker w : workers) {
            w.gate.countDown(); // release any parked worker
        }
        arena.close();
    }

    private AsyncRequestContainer<StateRequest<?, ?, ?, ?>> containerWithKeyGroups(int... kgs) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> c = executor.createRequestContainer();
        for (int kg : kgs) {
            c.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_GET,
                            null,
                            ("k" + kg).getBytes(StandardCharsets.UTF_8),
                            null,
                            new RecordingFuture<>(),
                            kg));
        }
        return c;
    }

    @Test
    void batchFutureIsIncompleteAtReturnAndCompletesOnWorkerThread() throws Exception {
        CompletableFuture<Void> f = executor.executeBatchRequests(containerWithKeyGroups(0));
        // Worker 0 is parked on its gate → the future CANNOT be complete: non-blocking contract.
        assertThat(f).as("future must be incomplete while the worker is busy").isNotDone();
        workers[0].gate.countDown();
        f.get(30, TimeUnit.SECONDS);
        assertThat(workers[0].executedOnThreads)
                .as("batch must run on the named worker thread, not the caller")
                .allMatch(n -> n.startsWith("forst-rs-state-worker-0-"));
        assertThat(workers[1].executed.get()).isZero();
        assertThat(workers[2].executed.get()).isZero();
    }

    @Test
    void fullyLoadedReflectsOutstandingBatches() throws Exception {
        List<CompletableFuture<Void>> fs = new ArrayList<>();
        // One batch per worker (kg 0,1,2 → workers 0,1,2); all workers parked.
        for (int kg = 0; kg < WORKERS; kg++) {
            fs.add(executor.executeBatchRequests(containerWithKeyGroups(kg)));
        }
        assertThat(executor.fullyLoaded()).as("all workers busy => fullyLoaded").isTrue();
        for (StubWorker w : workers) {
            w.gate.countDown();
        }
        for (CompletableFuture<Void> f : fs) {
            f.get(30, TimeUnit.SECONDS);
        }
        assertThat(executor.fullyLoaded()).as("drained => not fullyLoaded").isFalse();
    }

    @Test
    void multiWorkerBatchCompletesWhenAllSubBatchesFinish() throws Exception {
        // kg 0,1,2 in ONE container → fans out to all three workers.
        CompletableFuture<Void> f = executor.executeBatchRequests(containerWithKeyGroups(0, 1, 2));
        assertThat(f).isNotDone();
        workers[0].gate.countDown();
        workers[1].gate.countDown();
        assertThat(f).as("incomplete until the LAST worker finishes").isNotDone();
        workers[2].gate.countDown();
        f.get(30, TimeUnit.SECONDS);
        assertThat(workers[0].executed.get()).isEqualTo(1);
        assertThat(workers[1].executed.get()).isEqualTo(1);
        assertThat(workers[2].executed.get()).isEqualTo(1);
    }

    @Test
    void workerFailureCompletesBatchFutureExceptionally() {
        workers[0].failNextBatch = true;
        workers[0].gate.countDown();
        CompletableFuture<Void> f = executor.executeBatchRequests(containerWithKeyGroups(0));
        assertThat(catchThrowable(f)).isInstanceOf(IllegalStateException.class).hasMessage("boom");
    }

    private static Throwable catchThrowable(CompletableFuture<Void> f) {
        try {
            f.get(30, TimeUnit.SECONDS);
            return null;
        } catch (java.util.concurrent.ExecutionException e) {
            return e.getCause();
        } catch (Exception e) {
            return e;
        }
    }
}

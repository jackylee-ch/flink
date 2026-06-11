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
 * FRS-ROUTING-ASYNC contract tests for the non-blocking mode of {@link RoutingStateExecutor}
 * ({@code FRS_RS_EXECUTOR=routing-async}) — the properties that distinguish it from the
 * blocking routing mode:
 *
 * <ol>
 *   <li>{@code executeBatchRequests} returns an INCOMPLETE future and does NOT block the caller
 *       (mailbox) thread while a worker is busy;
 *   <li>the aggregate future is TRUTHFUL: it completes only when every sub-batch finishes
 *       (success and failure paths) — no {@code getNow} early-success discard;
 *   <li>{@code fullyLoaded()} reflects outstanding batches (real AEC backpressure);
 *   <li>same-key-group batches execute on the SAME worker thread in dispatch order across
 *       batches (the no-overtaking invariant that the 2026-06-10 inline variant violated).
 * </ol>
 *
 * <p>Workers are latch-instrumented {@link VectorizedExecutor} subclasses — no native FFI is
 * touched (same pattern as {@link CoordinatedStateExecutorTest}).
 */
class RoutingStateExecutorAsyncTest {

    private static final int WORKERS = 3;

    private Arena arena;
    private StubWorker[] workers;
    private RoutingStateExecutor executor;

    /** A worker whose executeBatchRequests parks on a latch and records its executing thread. */
    private static final class StubWorker extends VectorizedExecutor {
        final CountDownLatch gate = new CountDownLatch(1);
        final List<String> executedOnThreads = new CopyOnWriteArrayList<>();
        final AtomicInteger executed = new AtomicInteger();
        volatile boolean failNextBatch;

        StubWorker(ForStRsLinker linker, Arena arena) {
            super(
                    linker,
                    BatchedFailurePropagationTestHelpers.stubDb(),
                    BatchedFailurePropagationTestHelpers.stubCf(),
                    arena);
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
        executor = new RoutingStateExecutor(workers, false, true);
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
    void sameKeyGroupBatchesQueueFifoOnOneWorkerNoOvertaking() throws Exception {
        // Two batches for kg 0 dispatched back-to-back while worker 0 is parked: both must land
        // on worker 0's FIFO and execute in dispatch order — a later batch can never overtake.
        CompletableFuture<Void> f1 = executor.executeBatchRequests(containerWithKeyGroups(0));
        CompletableFuture<Void> f2 = executor.executeBatchRequests(containerWithKeyGroups(0));
        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();
        workers[0].gate.countDown();
        f1.get(30, TimeUnit.SECONDS);
        f2.get(30, TimeUnit.SECONDS);
        assertThat(workers[0].executed.get()).isEqualTo(2);
        assertThat(workers[0].executedOnThreads)
                .as("both batches on worker 0's single thread (FIFO order by construction)")
                .hasSize(2)
                .allMatch(n -> n.startsWith("forst-rs-state-worker-0-"));
        assertThat(workers[1].executed.get()).isZero();
        assertThat(workers[2].executed.get()).isZero();
    }

    @Test
    void fullyLoadedReflectsOutstandingBatchesAtCap() throws Exception {
        // Default cap = 2 × workers = 6. Dispatch 6 batches (2 per parked worker) → fullyLoaded.
        int cap = RoutingStateExecutor.maxInFlightBatches(WORKERS);
        List<CompletableFuture<Void>> fs = new ArrayList<>();
        for (int i = 0; i < cap; i++) {
            fs.add(executor.executeBatchRequests(containerWithKeyGroups(i % WORKERS)));
        }
        assertThat(executor.fullyLoaded()).as("at cap => fullyLoaded").isTrue();
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

    @Test
    void blockingModeStillCompletesSynchronously() throws Exception {
        // Regression guard: the default (blocking) mode is untouched by FRS-ROUTING-ASYNC.
        RoutingStateExecutor blocking = new RoutingStateExecutor(workers, false, false);
        workers[0].gate.countDown();
        CompletableFuture<Void> f =
                blocking.executeBatchRequests(blockingContainer(blocking, 0));
        assertThat(f).as("blocking mode returns an already-completed future").isDone();
    }

    private AsyncRequestContainer<StateRequest<?, ?, ?, ?>> blockingContainer(
            RoutingStateExecutor ex, int kg) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> c = ex.createRequestContainer();
        c.offer(
                BatchedFailurePropagationTestHelpers.newRequest(
                        StateRequestType.VALUE_GET,
                        null,
                        ("k" + kg).getBytes(StandardCharsets.UTF_8),
                        null,
                        new RecordingFuture<>(),
                        kg));
        return c;
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

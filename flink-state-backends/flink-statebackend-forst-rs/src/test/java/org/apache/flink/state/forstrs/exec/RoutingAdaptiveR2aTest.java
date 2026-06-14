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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R2a (content-adaptive executor depth, the SAFE realization — 2026-06-14) falsifier suite.
 *
 * <p>R2a routes each batch by content while keeping every batch kg-affine (one cache per
 * key-group):
 *
 * <ul>
 *   <li><b>ITER-FREE batches (q17 carve-out)</b> run INLINE on the caller (mailbox) thread with
 *       ZERO worker-thread handoff — the q17 tight point-RMW path that beats ForSt 3.3×;
 *   <li><b>ITER batches</b> fan out to the kg-affine worker threads for cross-probe overlap (the
 *       q11 318.9→135.7s lever) and complete SYNCHRONOUSLY before return (byte-identical OUTPUT
 *       contract to {@code routing}).
 * </ul>
 *
 * <p>Because the real {@link org.apache.flink.state.forstrs.VectorizedClassifier#hasIterRequests()}
 * signal is only raised at FFI dispatch-build time (not exercised by the stub-worker harness), the
 * iter / iter-free branch is driven deterministically by overriding the package-private
 * {@code anySubHasItersForBatch} seam — exactly the policy the e2e q8/q11/q17 gates then exercise
 * end-to-end. Stub workers record which thread executed each sub-batch; no native FFI is touched
 * (same pattern as {@link RoutingStateExecutorAsyncTest}).
 */
class RoutingAdaptiveR2aTest {

    private static final int WORKERS = 3;

    private Arena arena;
    private RecordingWorker[] workers;

    /** A worker that records the thread it ran on and counts executions. No FFI. */
    private static final class RecordingWorker extends VectorizedExecutor {
        final CopyOnWriteArrayList<String> executedOnThreads = new CopyOnWriteArrayList<>();
        final AtomicInteger executed = new AtomicInteger();

        RecordingWorker(ForStRsLinker linker, Arena arena) {
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
            executed.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    /** Builds an R2a executor over the stub workers with the iter verdict forced. */
    private RoutingStateExecutor r2a(boolean forceIters) {
        RoutingStateExecutor ex =
                new RoutingStateExecutor(
                        workers,
                        /* adaptiveInline= */ false,
                        /* nonBlocking= */ false,
                        /* routingAdaptive= */ true);
        ex.setIterVerdictOverrideForTest(forceIters);
        return ex;
    }

    @BeforeEach
    void setUp() {
        arena = Arena.ofShared();
        ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
        workers = new RecordingWorker[WORKERS];
        for (int i = 0; i < WORKERS; i++) {
            workers[i] = new RecordingWorker(linker, arena);
        }
    }

    @AfterEach
    void tearDown() {
        arena.close();
    }

    private AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container(
            RoutingStateExecutor ex, int... kgs) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> c = ex.createRequestContainer();
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
    void iterFreeMultiKgRunsInlineWithZeroWorkerHandoff() throws Exception {
        // q17 carve-out: an iter-free batch spanning 3 key-groups must execute entirely on the
        // CALLER thread — zero worker-thread dispatch — and complete synchronously.
        RoutingStateExecutor ex = r2a(false);
        String caller = Thread.currentThread().getName();

        CompletableFuture<Void> f = ex.executeBatchRequests(container(ex, 0, 1, 2));

        assertThat(f).as("iter-free batch completes synchronously (zero handoff)").isDone();
        f.get(30, TimeUnit.SECONDS);
        assertThat(ex.workerDispatchedSubBatchCount())
                .as("q17 carve-out: NO worker-thread dispatch")
                .isZero();
        assertThat(ex.inlineSubBatchCount()).as("all sub-batches inline").isEqualTo(3);
        for (RecordingWorker w : workers) {
            assertThat(w.executedOnThreads)
                    .as("each sub-batch ran on the CALLER (mailbox) thread, not a worker")
                    .allMatch(n -> n.equals(caller));
        }
        ex.shutdown();
    }

    @Test
    void iterFreeSingleKgIsZeroHandoffToo() throws Exception {
        // The narrow q17 hot shape (one key-group): still zero handoff, inline on the caller.
        RoutingStateExecutor ex = r2a(false);
        String caller = Thread.currentThread().getName();

        CompletableFuture<Void> f = ex.executeBatchRequests(container(ex, 1));

        assertThat(f).isDone();
        f.get(30, TimeUnit.SECONDS);
        assertThat(ex.workerDispatchedSubBatchCount()).isZero();
        assertThat(ex.inlineSubBatchCount()).isEqualTo(1);
        assertThat(workers[1].executedOnThreads).hasSize(1).allMatch(n -> n.equals(caller));
        assertThat(workers[0].executed.get()).isZero();
        assertThat(workers[2].executed.get()).isZero();
        ex.shutdown();
    }

    @Test
    void iterBatchFansOutToWorkerThreadsAndCompletesSynchronously() throws Exception {
        // q11 lever: an iter batch spanning 3 key-groups dispatches to the worker threads for
        // cross-probe overlap, and STILL completes synchronously before return (byte-identical
        // OUTPUT contract to routing — no async reordering).
        RoutingStateExecutor ex = r2a(true);
        String caller = Thread.currentThread().getName();

        CompletableFuture<Void> f = ex.executeBatchRequests(container(ex, 0, 1, 2));

        assertThat(f).as("synchronous completion (blocking fan-out)").isDone();
        f.get(30, TimeUnit.SECONDS);
        assertThat(ex.workerDispatchedSubBatchCount())
                .as("iter batch fans out to all 3 workers")
                .isEqualTo(3);
        assertThat(ex.inlineSubBatchCount()).as("no inline execution for iter batch").isZero();
        for (RecordingWorker w : workers) {
            assertThat(w.executed.get()).isEqualTo(1);
            assertThat(w.executedOnThreads)
                    .as("ran on a WORKER thread, not the caller")
                    .allMatch(n -> n.startsWith("forst-rs-state-worker-") && !n.equals(caller));
        }
        ex.shutdown();
    }

    @Test
    void iterSingleKgRunsOnItsWorkerThread() throws Exception {
        // Single-kg iter batch: dispatched to that one worker's thread (no latch), synchronous.
        RoutingStateExecutor ex = r2a(true);

        CompletableFuture<Void> f = ex.executeBatchRequests(container(ex, 2));

        assertThat(f).isDone();
        f.get(30, TimeUnit.SECONDS);
        assertThat(ex.workerDispatchedSubBatchCount()).isEqualTo(1);
        assertThat(workers[2].executed.get()).isEqualTo(1);
        assertThat(workers[2].executedOnThreads)
                .allMatch(n -> n.startsWith("forst-rs-state-worker-2-"));
        assertThat(workers[0].executed.get()).isZero();
        assertThat(workers[1].executed.get()).isZero();
        ex.shutdown();
    }

    @Test
    void emptyBatchIsANoOpUnderR2a() {
        // Byte-identical to routing/inline: an empty batch returns a completed future, no dispatch.
        RoutingStateExecutor ex = r2a(false);
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> empty = ex.createRequestContainer();
        CompletableFuture<Void> f = ex.executeBatchRequests(empty);
        assertThat(f).isDone();
        assertThat(ex.inlineSubBatchCount()).isZero();
        assertThat(ex.workerDispatchedSubBatchCount()).isZero();
        ex.shutdown();
    }
}

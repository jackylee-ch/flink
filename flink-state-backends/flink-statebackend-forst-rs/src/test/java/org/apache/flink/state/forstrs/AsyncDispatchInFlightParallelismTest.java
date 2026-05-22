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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers.RecordingFuture;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.metrics.DispatchMetrics;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PR-E2 / F5-3 contract test for {@link VectorizedExecutor#executeBatchRequests}.
 *
 * <p>The async-state V2 framework's {@link
 * org.apache.flink.runtime.asyncprocessing.AsyncExecutionController#triggerIfNeeded} ignores the
 * returned container future and tracks completion per-row via {@link
 * org.apache.flink.core.asyncprocessing.InternalAsyncFuture}. The community ForSt backend exploits
 * this to offload the engine call to a separate {@code coordinatorThread} and pipeline successive
 * batches.
 *
 * <p>forst-rs's current C1 design parks Arrow {@link ColumnarBatchBuffer}s and GET-result
 * segments at the executor level (long-lived, shared across batches) — handing batch N off to a
 * worker while the mailbox prepares batch N+1 would race the {@code reset()} calls in {@link
 * VectorizedClassifier} against the worker's reads. PR-E2 therefore documents the synchronous
 * mailbox-thread contract and instruments the in-flight depth so the invariant is observable in
 * production.
 *
 * <p>This test enforces three facts:
 *
 * <ol>
 *   <li>Each batch's container future is already completed when {@code executeBatchRequests}
 *       returns — i.e., no incomplete-future pipelining is happening today.
 *   <li>The {@link DispatchMetrics#inFlightBatchCount()} gauge returns 0 after every batch
 *       returns. (If a future structural refactor adopted async dispatch, this assertion would
 *       become invalid at the synchronous return point and the test would fail — that's the
 *       intent: it serves as a regression gate for the buffer-ownership invariant.)
 *   <li>Submitting 4 successive batches advances {@link DispatchMetrics#totalBatchCount()} by 4
 *       — proving the metric is being recorded for every batch.
 * </ol>
 *
 * <p>If/when PR-E2 is reopened to actually pipeline (after per-batch buffer ownership lands), the
 * test's first assertion is the falsifiable contract that the new design must change.
 */
class AsyncDispatchInFlightParallelismTest {

    @Test
    void successiveBatches_eachReturnSynchronouslyWithDepthOne() {
        // Track maximum observed in-flight depth across all batches.
        AtomicLong maxObservedDepth = new AtomicLong(0);

        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();

            MetricGroup root = new UnregisteredMetricsGroup();
            DispatchMetrics metrics = new DispatchMetrics(root);

            VectorizedExecutor exec =
                    new VectorizedExecutor(linker, db, cf, arena) {
                        @Override
                        protected void invokeVectorizedBatchPut(
                                MemorySegment keyOffsetsSeg,
                                MemorySegment keyDataSeg,
                                MemorySegment valOffsetsSeg,
                                MemorySegment valDataSeg,
                                long count) {
                            // Observe the in-flight depth WHILE we are dispatching.
                            // PR-E2 invariant: depth must be exactly 1 here (this very batch),
                            // because the mailbox thread cannot be running a sibling
                            // executeBatchRequests concurrently under the current contract.
                            long depth = metrics.inFlightBatchCount();
                            maxObservedDepth.updateAndGet(prev -> Math.max(prev, depth));
                            // No engine work — return normally so the per-row futures complete.
                        }
                    };
            exec.setDispatchMetrics(metrics);

            int batches = 4;
            int rowsPerBatch = 3;

            // Track containers + futures across batches to assert per-batch synchronous completion.
            List<CompletableFuture<Void>> containerFutures = new ArrayList<>(batches);
            List<RecordingFuture<Object>> rowFutures = new ArrayList<>();

            for (int b = 0; b < batches; b++) {
                AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                        exec.createRequestContainer();
                for (int i = 0; i < rowsPerBatch; i++) {
                    RecordingFuture<Object> fut = new RecordingFuture<>();
                    rowFutures.add(fut);
                    StateRequest<Object, Object, Object, Object> req =
                            BatchedFailurePropagationTestHelpers.newRequest(
                                    StateRequestType.VALUE_UPDATE,
                                    new Object(),
                                    ("k" + b + "_" + i).getBytes(),
                                    ("v" + b + "_" + i).getBytes(),
                                    fut);
                    container.offer(req);
                }

                // Snapshot the depth BEFORE dispatching — must be 0 since prior batch returned.
                assertThat(metrics.inFlightBatchCount())
                        .as(
                                "batch %d: depth must be 0 before dispatch (prior batch returned"
                                        + " synchronously)",
                                b)
                        .isEqualTo(0L);

                CompletableFuture<Void> containerFut = exec.executeBatchRequests(container);
                containerFutures.add(containerFut);

                // (1) Container future is COMPLETE when executeBatchRequests returns.
                // This is the falsifiable synchronous-dispatch invariant: if a future structural
                // refactor turns the dispatch into a worker-thread offload, the future returned
                // here would still be INCOMPLETE at this point.
                assertThat(containerFut.isDone())
                        .as("batch %d: container future must be done on return", b)
                        .isTrue();
                assertThat(containerFut.isCompletedExceptionally())
                        .as("batch %d: container future must complete normally on happy path", b)
                        .isFalse();

                // (2) Depth back to 0 after return.
                assertThat(metrics.inFlightBatchCount())
                        .as("batch %d: depth must return to 0 after dispatch", b)
                        .isEqualTo(0L);
            }

            // (3) Total batches recorded == batches dispatched.
            assertThat(metrics.totalBatchCount())
                    .as("totalBatchCount must equal batches dispatched")
                    .isEqualTo((long) batches);

            // Mid-dispatch the depth was exactly 1 every time.
            assertThat(maxObservedDepth.get())
                    .as("max in-flight depth observed mid-dispatch must be exactly 1 (sync)")
                    .isEqualTo(1L);

            // Every per-row future was completed normally.
            for (int i = 0; i < rowFutures.size(); i++) {
                RecordingFuture<Object> fut = rowFutures.get(i);
                assertThat(fut.normalCalls.get())
                        .as("row %d future must be normally completed exactly once", i)
                        .isEqualTo(1);
                assertThat(fut.exceptionalCalls.get())
                        .as("row %d future must NOT be exceptionally completed", i)
                        .isEqualTo(0);
            }
        }
    }

    @Test
    void inFlightDepthRecordedEvenOnFailurePath() {
        // The finally-block must call recordBatchEnd() even when the FFI seam throws.
        RuntimeException cause = new RuntimeException("simulated FFI failure for depth-tracking");

        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();

            MetricGroup root = new UnregisteredMetricsGroup();
            DispatchMetrics metrics = new DispatchMetrics(root);

            VectorizedExecutor exec =
                    new VectorizedExecutor(linker, db, cf, arena) {
                        @Override
                        protected void invokeVectorizedBatchPut(
                                MemorySegment keyOffsetsSeg,
                                MemorySegment keyDataSeg,
                                MemorySegment valOffsetsSeg,
                                MemorySegment valDataSeg,
                                long count) {
                            throw cause;
                        }
                    };
            exec.setDispatchMetrics(metrics);

            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            RecordingFuture<Object> fut = new RecordingFuture<>();
            StateRequest<Object, Object, Object, Object> req =
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_UPDATE,
                            new Object(),
                            "k".getBytes(),
                            "v".getBytes(),
                            fut);
            container.offer(req);

            CompletableFuture<Void> containerFut = exec.executeBatchRequests(container);

            // Failure path: depth must still return to 0.
            assertThat(metrics.inFlightBatchCount())
                    .as("depth must return to 0 even on failure path")
                    .isEqualTo(0L);
            assertThat(metrics.totalBatchCount()).isEqualTo(1L);

            assertThat(containerFut.isCompletedExceptionally()).isTrue();
            assertThat(fut.exceptionalCalls.get()).isEqualTo(1);
        }
    }
}

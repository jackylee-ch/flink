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

import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers.RecordingFuture;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PR-A10 / S1-9 regression test for the PUT batch dispatcher. When {@code
 * VectorizedExecutor.invokeVectorizedBatchPut} throws inside {@link
 * VectorizedExecutor#executeBatchRequests}, every per-row {@link StateRequest}'s future must be
 * completed exceptionally with the thrown cause. Before A10 the outer catch returned a failed
 * container future but the per-row futures stayed unresolved — Flink's async-state runtime would
 * wait on them forever and the operator would hang on the first transient engine error.
 */
class BatchedPutFailurePropagationTest {

    @Test
    void batchedPutFailure_propagatesToEveryPerRowFuture() {
        RuntimeException cause = new RuntimeException("simulated FFI failure on PUT batch");

        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();

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

            int n = 5;
            List<RecordingFuture<Object>> futures = new ArrayList<>(n);
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            for (int i = 0; i < n; i++) {
                RecordingFuture<Object> fut = new RecordingFuture<>();
                futures.add(fut);
                // VALUE_UPDATE with non-null payload routes through recordPut.
                StateRequest<Object, Object, Object, Object> req =
                        BatchedFailurePropagationTestHelpers.newRequest(
                                StateRequestType.VALUE_UPDATE,
                                /* payload */ new Object(),
                                ("k" + i).getBytes(),
                                ("v" + i).getBytes(),
                                fut);
                container.offer(req);
            }

            CompletableFuture<Void> result = exec.executeBatchRequests(container);

            assertThat(result.isCompletedExceptionally())
                    .as("container future must reflect the FFI failure")
                    .isTrue();

            for (int i = 0; i < n; i++) {
                RecordingFuture<Object> fut = futures.get(i);
                assertThat(fut.exceptionalCalls.get())
                        .as("row %d future must be completed exceptionally exactly once", i)
                        .isEqualTo(1);
                assertThat(fut.recordedCause.get())
                        .as("row %d future must record the propagated cause", i)
                        .isSameAs(cause);
                assertThat(fut.normalCalls.get())
                        .as("row %d future must NOT also receive a normal completion", i)
                        .isEqualTo(0);
            }
        }
    }
}

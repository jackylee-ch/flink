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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers.RecordingFuture;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2;
import org.apache.flink.state.forstrs.state.ListStateArrowBuffer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression coverage for same-key ordering hazards across op-type buckets. */
class VectorizedExecutorOrderingHazardTest {

    @Test
    void sameKeyGetThenPutUsesOrderedDispatch() {
        // A pure GET+PUT batch can still represent a read-modify-write chain on the same state
        // cell. Running the vectorized buckets as PUT -> GET makes the GET observe the just-written
        // value instead of the prior accumulator. HOP window aggregation exposed this as a 5x q5
        // over-count, so exact same-key overlap must preserve offer order even without deletes or
        // append-merge rows.
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();
            RecordingExecutor exec = new RecordingExecutor(linker, db, cf, arena);

            byte[] key = "same-key".getBytes(StandardCharsets.UTF_8);
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_GET,
                            null,
                            key,
                            null,
                            new RecordingFuture<>()));
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_UPDATE,
                            "v",
                            key,
                            "v".getBytes(StandardCharsets.UTF_8),
                            new RecordingFuture<>()));

            assertThat(exec.executeBatchRequests(container)).isCompleted();
            assertThat(exec.calls).containsExactly("GET", "PUT");
        }
    }

    @Test
    void differentKeyGetThenPutStaysVectorizedWhenNoDeleteOrMergeHazard() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();
            RecordingExecutor exec = new RecordingExecutor(linker, db, cf, arena);

            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_GET,
                            null,
                            "get-key".getBytes(StandardCharsets.UTF_8),
                            null,
                            new RecordingFuture<>()));
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_UPDATE,
                            "v",
                            "put-key".getBytes(StandardCharsets.UTF_8),
                            "v".getBytes(StandardCharsets.UTF_8),
                            new RecordingFuture<>()));

            assertThat(exec.executeBatchRequests(container)).isCompleted();
            assertThat(exec.calls).containsExactly("PUT", "GET");
        }
    }

    @Test
    void listAppendThenGetUsesHeapOrderedFallbackByDefault() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();
            RecordingExecutor exec = new RecordingExecutor(linker, db, cf, arena);

            ListStateArrowBuffer buffer = new ListStateArrowBuffer();
            ForStRsAsyncListStateV2<Long, Integer, String> state =
                    new ForStRsAsyncListStateV2<>(
                            null,
                            "listState",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            StringSerializer.INSTANCE,
                            buffer,
                            linker,
                            db,
                            cf);
            RecordContext<Long> ctx = contextWithNamespace(7L, state, 3);
            RecordingFuture<Object> addFuture = new RecordingFuture<>();
            RecordingFuture<Object> getFuture = new RecordingFuture<>();

            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            container.offer(
                    request(state, StateRequestType.LIST_ADD, "v0", ctx, addFuture));
            container.offer(
                    request(state, StateRequestType.LIST_GET, null, ctx, getFuture));

            assertThat(exec.executeBatchRequests(container)).isCompleted();
            assertThat(buffer.rowCount()).isZero();
            assertThat(addFuture.exceptionalCalls.get()).isZero();
            assertThat(getFuture.exceptionalCalls.get()).isZero();
            assertThat(exec.calls).containsExactly("APPEND", "GET");
        }
    }

    private static <K, N> RecordContext<K> contextWithNamespace(
            K key, InternalPartitionedState<N> state, N namespace) {
        RecordContext<K> ctx =
                new RecordContext<>(
                        new Object(),
                        key,
                        c -> {},
                        0,
                        new EpochManager.Epoch(0L),
                        4);
        ctx.setNamespace(state, namespace);
        return ctx;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <K, N, IN, OUT> StateRequest<K, N, IN, OUT> request(
            InternalPartitionedState<N> state,
            StateRequestType type,
            IN payload,
            RecordContext<K> ctx,
            RecordingFuture<OUT> future) {
        return new StateRequest<>(
                (org.apache.flink.api.common.state.v2.State) state,
                type,
                false,
                payload,
                future,
                ctx);
    }

    private static final class RecordingExecutor extends VectorizedExecutor {
        private final List<String> calls = new ArrayList<>();

        private RecordingExecutor(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
            super(linker, db, cf, arena);
        }

        @Override
        protected void invokeVectorizedBatchPut(
                MemorySegment keyOffsetsSeg,
                MemorySegment keyDataSeg,
                MemorySegment valOffsetsSeg,
                MemorySegment valDataSeg,
                long count) {
            calls.add("PUT");
        }

        @Override
        protected int invokeVectorizedBatchGet(
                MemorySegment keyOffsetsSeg,
                MemorySegment keyDataSeg,
                long count,
                MemorySegment outOffsetsSeg,
                MemorySegment outDataSeg,
                MemorySegment outValiditySeg,
                long outDataCapArg,
                MemorySegment outDataLenSegArg) {
            calls.add("GET");
            outOffsetsSeg.set(ValueLayout.JAVA_INT, 0L, 0);
            outOffsetsSeg.set(ValueLayout.JAVA_INT, Integer.BYTES, 0);
            outValiditySeg.set(ValueLayout.JAVA_BYTE, 0L, (byte) 0);
            outDataLenSegArg.set(ValueLayout.JAVA_LONG, 0L, 0L);
            return 0;
        }

        @Override
        protected int invokeVecMergeAppendBatch(
                MemorySegment keysOffSeg,
                MemorySegment keysDataSeg,
                MemorySegment opsOffSeg,
                MemorySegment opsDataSeg,
                int count) {
            calls.add("APPEND");
            return 0;
        }
    }
}

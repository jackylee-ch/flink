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

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.asyncprocessing.AsyncFutureImpl;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R21-H1 regression: when an FFI call throws on the sync path
 * ({@link VectorizedExecutor#executeRequestSync}) — e.g. {@code invokeVectorizedBatchPut} fails —
 * the per-op catch in {@link VectorizedExecutor#executePuts} drains the row's future via
 * {@code completePutExceptionally} and re-throws. Pre-R21-H1, the outer catch in
 * {@code executeRequestSync} then ALSO called {@code completePutExceptionally} (because
 * {@link VectorizedClassifier#takeClassifierCompletedExceptionally} returned {@code false} — the
 * marker set was populated ONLY by {@link VectorizedClassifier#recordDelete}'s onClear path, not
 * by the executor-side per-op catches). The framework
 * {@link AsyncFutureImpl.AsyncFrameworkExceptionHandler#handleException} fired TWICE, producing a
 * double task-failure log.
 *
 * <p>R21-H1 fix: every executor-side per-op catch (executePuts / executeDeletes / executeGets /
 * dispatchAppendMerge*) calls
 * {@link VectorizedClassifier#markCompletedExceptionally(StateRequest)} BEFORE the per-row
 * exception completion. The outer catch then sees the marker via
 * {@code takeClassifierCompletedExceptionally} and short-circuits its own duplicate completion.
 *
 * <p>This test pairs a real {@link AsyncFutureImpl} (production semantics: {@code isDone()} is
 * NOT mutated by {@code completeExceptionally}) with a counting framework exception handler — the
 * SAME side-channel the production runtime uses to surface async-state failures. Asserting
 * exactly one handler invocation enforces the one-shot contract end-to-end.
 */
class SyncExecuteFfiPutFailureSingleCompletionTest {

    @Test
    void ffiPutThrow_completesRequestFutureExceptionallyExactlyOnce() {
        RuntimeException cause = new RuntimeException("simulated FFI PUT failure");

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

            CountingExceptionHandler handler = new CountingExceptionHandler();
            AsyncFutureImpl<Object> realFut =
                    new AsyncFutureImpl<>(
                            task -> {
                                try {
                                    task.run();
                                } catch (Exception e) {
                                    handler.handleException("inline-runner", e);
                                }
                            },
                            handler);

            StubPutState state = new StubPutState();
            RecordContext<Object> ctx =
                    new RecordContext<>(
                            /* record */ null,
                            /* key */ null,
                            /* disposer */ rc -> {},
                            /* keyGroup */ 0,
                            /* epoch */ new Epoch(0L),
                            new AtomicReferenceArray<>(0),
                            /* priority */ 0);
            // VALUE_UPDATE with non-null payload routes through recordPut, then executePuts
            // dispatches to the overridden invokeVectorizedBatchPut which throws.
            StateRequest<Object, Object, Object, Object> req =
                    new StateRequest<>(
                            state,
                            StateRequestType.VALUE_UPDATE,
                            /* sync */ true,
                            /* payload */ new Object(),
                            realFut,
                            ctx);

            // executeRequestSync MUST NOT propagate the FFI throw — it must catch it and complete
            // the request's future exceptionally. The framework handler MUST fire EXACTLY ONCE.
            try {
                exec.executeRequestSync(req);
            } catch (Throwable t) {
                org.junit.jupiter.api.Assertions.fail(
                        "R21-H1: executeRequestSync must not propagate the FFI throw — got: " + t);
            }

            assertThat(handler.invocations.get())
                    .as("R21-H1: AsyncFrameworkExceptionHandler.handleException must fire EXACTLY"
                            + " once on the FFI-PUT-throw path. Pre-R21-H1 the executor-side per-op"
                            + " catch did not populate the classifier marker set, so the outer"
                            + " catch in executeRequestSync re-completed the future and the"
                            + " framework handler fired twice (double task-failure log).")
                    .isEqualTo(1);
            assertThat(handler.lastCause.get())
                    .as("handler must observe the original FFI-PUT throw cause")
                    .isSameAs(cause);
        }
    }

    @Test
    void ffiDeleteThrow_completesRequestFutureExceptionallyExactlyOnce() {
        RuntimeException cause = new RuntimeException("simulated FFI DELETE failure");

        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();

            VectorizedExecutor exec =
                    new VectorizedExecutor(linker, db, cf, arena) {
                        @Override
                        protected void invokeVectorizedBatchDelete(
                                MemorySegment keyOffsetsSeg,
                                MemorySegment keyDataSeg,
                                long count) {
                            throw cause;
                        }
                    };

            CountingExceptionHandler handler = new CountingExceptionHandler();
            AsyncFutureImpl<Object> realFut =
                    new AsyncFutureImpl<>(
                            task -> {
                                try {
                                    task.run();
                                } catch (Exception e) {
                                    handler.handleException("inline-runner", e);
                                }
                            },
                            handler);

            // NoopOnClearStubState's onClear is a no-op, so recordDelete routes the CLEAR
            // request through executeDeletes WITHOUT pre-completing the future. The FFI
            // invocation then throws.
            NoopOnClearStubState state = new NoopOnClearStubState();
            RecordContext<Object> ctx =
                    new RecordContext<>(
                            /* record */ null,
                            /* key */ null,
                            /* disposer */ rc -> {},
                            /* keyGroup */ 0,
                            /* epoch */ new Epoch(0L),
                            new AtomicReferenceArray<>(0),
                            /* priority */ 0);
            StateRequest<Object, Object, Object, Object> req =
                    new StateRequest<>(
                            state, StateRequestType.CLEAR, /* sync */ true, null, realFut, ctx);

            try {
                exec.executeRequestSync(req);
            } catch (Throwable t) {
                org.junit.jupiter.api.Assertions.fail(
                        "R21-H1: executeRequestSync must not propagate the FFI DELETE throw — got: "
                                + t);
            }

            assertThat(handler.invocations.get())
                    .as("R21-H1: handleException must fire EXACTLY once on the FFI-DELETE-throw"
                            + " path")
                    .isEqualTo(1);
            assertThat(handler.lastCause.get())
                    .as("handler must observe the original FFI-DELETE throw cause")
                    .isSameAs(cause);
        }
    }

    @Test
    void ffiGetThrow_completesRequestFutureExceptionallyExactlyOnce() {
        RuntimeException cause = new RuntimeException("simulated FFI GET failure");

        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();

            VectorizedExecutor exec =
                    new VectorizedExecutor(linker, db, cf, arena) {
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
                            throw cause;
                        }
                    };

            CountingExceptionHandler handler = new CountingExceptionHandler();
            AsyncFutureImpl<Object> realFut =
                    new AsyncFutureImpl<>(
                            task -> {
                                try {
                                    task.run();
                                } catch (Exception e) {
                                    handler.handleException("inline-runner", e);
                                }
                            },
                            handler);

            StubPutState state = new StubPutState();
            RecordContext<Object> ctx =
                    new RecordContext<>(
                            /* record */ null,
                            /* key */ null,
                            /* disposer */ rc -> {},
                            /* keyGroup */ 0,
                            /* epoch */ new Epoch(0L),
                            new AtomicReferenceArray<>(0),
                            /* priority */ 0);
            // VALUE_GET routes through recordGet → executeGets.
            StateRequest<Object, Object, Object, Object> req =
                    new StateRequest<>(
                            state,
                            StateRequestType.VALUE_GET,
                            /* sync */ true,
                            null,
                            realFut,
                            ctx);

            try {
                exec.executeRequestSync(req);
            } catch (Throwable t) {
                org.junit.jupiter.api.Assertions.fail(
                        "R21-H1: executeRequestSync must not propagate the FFI GET throw — got: "
                                + t);
            }

            assertThat(handler.invocations.get())
                    .as("R21-H1: handleException must fire EXACTLY once on the FFI-GET-throw path")
                    .isEqualTo(1);
            assertThat(handler.lastCause.get())
                    .as("handler must observe the original FFI-GET throw cause")
                    .isSameAs(cause);
        }
    }

    /** Counts {@code handleException} calls; production framework signal. */
    static final class CountingExceptionHandler
            implements AsyncFutureImpl.AsyncFrameworkExceptionHandler {
        final AtomicInteger invocations = new AtomicInteger(0);
        final AtomicReference<Throwable> lastCause = new AtomicReference<>();

        @Override
        public void handleException(String message, Throwable exception) {
            invocations.incrementAndGet();
            lastCause.set(exception);
        }
    }

    /** State stub that serialises PUT/GET — no onClear side effect. */
    static final class StubPutState
            implements State,
                    InternalPartitionedState<Object>,
                    ForStRsInnerTable<Object, Object, Object> {
        @Override
        public byte[] serializeKey(StateRequest<Object, Object, ?, ?> request) {
            return new byte[] {1};
        }

        @Override
        public byte[] serializeValue(Object v) {
            return new byte[] {2};
        }

        @Override
        public Object deserializeValue(byte[] raw) {
            return raw;
        }

        @Override
        public ForStRsDBGetRequest<Object, Object, ?> buildDBGetRequest(
                StateRequest<Object, Object, ?, ?> request) {
            return null;
        }

        @Override
        public ForStRsDBPutRequest<Object, Object, ?> buildDBPutRequest(
                StateRequest<Object, Object, ?, ?> request) {
            return null;
        }

        @Override
        public void clear() {}

        @Override
        public StateFuture<Void> asyncClear() {
            return null;
        }

        @Override
        public void setCurrentNamespace(Object namespace) {}
    }

    /** State stub whose onClear is a no-op (so the DELETE row reaches executeDeletes). */
    static final class NoopOnClearStubState
            implements State,
                    InternalPartitionedState<Object>,
                    ForStRsInnerTable<Object, Object, Object> {
        @Override
        public byte[] serializeKey(StateRequest<Object, Object, ?, ?> request) {
            return new byte[] {1};
        }

        @Override
        public byte[] serializeValue(Object v) {
            return new byte[0];
        }

        @Override
        public Object deserializeValue(byte[] raw) {
            return raw;
        }

        @Override
        public ForStRsDBGetRequest<Object, Object, ?> buildDBGetRequest(
                StateRequest<Object, Object, ?, ?> request) {
            return null;
        }

        @Override
        public ForStRsDBPutRequest<Object, Object, ?> buildDBPutRequest(
                StateRequest<Object, Object, ?, ?> request) {
            return null;
        }

        @Override
        public void onClear(StateRequest<Object, Object, ?, ?> request) {
            // no-op — let recordDelete route to executeDeletes without pre-completion.
        }

        @Override
        public void clear() {}

        @Override
        public StateFuture<Void> asyncClear() {
            return null;
        }

        @Override
        public void setCurrentNamespace(Object namespace) {}
    }
}

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
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers.RecordingFuture;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R18-H2 regression: {@link VectorizedExecutor#executeRequestSync} must complete the
 * per-row {@link StateRequest}'s future exceptionally when ANY phase of the request setup
 * (including {@code single.offer(request)} → {@code recordDelete} → {@code onClear}) throws.
 *
 * <p>Pre-fix the try/catch in {@code executeRequestSync} only wrapped
 * {@code executeRequestSyncInner}; a throw escaping from {@code single.offer} (when
 * {@link VectorizedClassifier#recordDelete} rethrows the {@code onClear} flush failure)
 * propagated out of {@code executeRequestSync} without ever completing the StateRequest's
 * future, leaving the operator hung on the unresolved {@code CompletableFuture}.
 *
 * <p>Fix widens the try block to cover {@code reset / registerListState / initNewKindBuffers /
 * offer / executeRequestSyncInner}; on any throw the test verifies the future is
 * exceptionally complete (i.e. {@code completePutExceptionally} was reached).
 */
class SyncExecuteRequestOnClearThrowTest {

    @Test
    void onClearThrow_completesRequestFutureExceptionally() {
        RuntimeException cause = new RuntimeException("simulated onClear failure");

        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();

            VectorizedExecutor exec = new VectorizedExecutor(linker, db, cf, arena);

            RecordingFuture<Object> fut = new RecordingFuture<>();
            // Use a stub that throws from onClear — recordDelete rethrows after marking
            // batchPoisonCause, and the throw propagates out of single.offer(request).
            ThrowingOnClearStubState state = new ThrowingOnClearStubState(cause);
            RecordContext<Object> ctx =
                    new RecordContext<>(
                            /* record */ null,
                            /* key */ null,
                            /* disposer */ rc -> {},
                            /* keyGroup */ 0,
                            /* epoch */ new Epoch(0L),
                            new AtomicReferenceArray<>(0),
                            /* priority */ 0);
            // CLEAR routes through recordDelete → onClear → throw.
            StateRequest<Object, Object, Object, Object> req =
                    new StateRequest<>(state, StateRequestType.CLEAR, /* sync */ true, null, fut, ctx);

            // executeRequestSync MUST NOT propagate the cause out; instead it must catch it and
            // complete the request's future exceptionally. Pre-fix the throw escaped and the
            // future was left unfinished.
            try {
                exec.executeRequestSync(req);
            } catch (Throwable t) {
                // The fix's catch block must absorb the throw. If anything escapes,
                // the operator hangs in real execution.
                org.junit.jupiter.api.Assertions.fail(
                        "R18-H2: executeRequestSync must not propagate the onClear throw — "
                                + "got: "
                                + t);
            }

            // R19-H1: must be EXACTLY 1, not >=1. {@code VectorizedClassifier.recordDelete}'s
            // onClear-throw handler pre-completes the future exceptionally before rethrowing;
            // the outer catch in {@code executeRequestSync} would re-complete the SAME future
            // without an idempotence guard. In production
            // {@code AsyncFutureImpl.completeExceptionally} delegates to
            // {@code AsyncFrameworkExceptionHandler.handleException} with no idempotence —
            // double-completion → double task-failure log. The R19-H1 isDone() guard ensures
            // exactly one exceptional completion.
            assertThat(fut.exceptionalCalls.get())
                    .as("R19-H1: request future must be completed exceptionally EXACTLY once on"
                            + " onClear throw (pre-fix: double-completion via recordDelete +"
                            + " executeRequestSync outer catch)")
                    .isEqualTo(1);
            assertThat(fut.normalCalls.get())
                    .as("request future must NOT receive a normal completion on the failure path")
                    .isEqualTo(0);
        }
    }

    /** Stub state whose {@code onClear} hook throws — drives the recordDelete failure path. */
    static final class ThrowingOnClearStubState
            implements State,
                    InternalPartitionedState<Object>,
                    ForStRsInnerTable<Object, Object, Object> {
        private final RuntimeException toThrow;

        ThrowingOnClearStubState(RuntimeException toThrow) {
            this.toThrow = toThrow;
        }

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
            throw toThrow;
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

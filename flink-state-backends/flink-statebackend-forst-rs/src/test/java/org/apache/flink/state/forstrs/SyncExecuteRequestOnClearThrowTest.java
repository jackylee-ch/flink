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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R18-H2 / R19-H1 / R20-H1 regression: {@link VectorizedExecutor#executeRequestSync} must
 * complete the per-row {@link StateRequest}'s future exceptionally EXACTLY ONCE when ANY phase
 * of the request setup (including {@code single.offer(request)} → {@code recordDelete} →
 * {@code onClear}) throws.
 *
 * <p>Pre-R18 the try/catch in {@code executeRequestSync} only wrapped
 * {@code executeRequestSyncInner}; a throw escaping from {@code single.offer} (when
 * {@link VectorizedClassifier#recordDelete} rethrows the {@code onClear} flush failure)
 * propagated out of {@code executeRequestSync} without ever completing the StateRequest's
 * future, leaving the operator hung on the unresolved {@code CompletableFuture}.
 *
 * <p>R18-H2 widened the try block. R19-H1 added an {@code isDone()} guard inside the outer
 * catch to prevent double-completion (since {@code recordDelete} also pre-completes the
 * request's future before rethrowing). <b>R20-H1</b> replaced that {@code isDone()} guard with
 * explicit set-based tracking
 * ({@link VectorizedClassifier#takeClassifierCompletedExceptionally}) because the production
 * {@link AsyncFutureImpl#completeExceptionally(String, Throwable)} delegates ONLY to the
 * framework {@code AsyncFrameworkExceptionHandler.handleException} — it does NOT mutate the
 * internal {@code completableFuture}, so {@code getFuture().isDone()} keeps returning
 * {@code false} even after exceptional completion fired. The R19-H1 guard would therefore
 * have been ineffective in production (the older mock's {@code isDone()} was wired to a
 * counter — inverted from production semantics — so the test passed but the field never
 * exercised the real guard path).
 *
 * <p>This test now uses a REAL {@link AsyncFutureImpl} with an
 * {@link AsyncFutureImpl.AsyncFrameworkExceptionHandler} that counts
 * {@code handleException} invocations directly. That counter is the SAME side-channel the
 * production framework uses to surface async-state failures (task log + lifecycle), so an
 * assertion of "exactly one handler call on the failure path" enforces the same one-shot
 * contract the production framework expects.
 */
class SyncExecuteRequestOnClearThrowTest {

    @Test
    void onClearThrow_completesRequestFutureExceptionallyExactlyOnce() {
        RuntimeException cause = new RuntimeException("simulated onClear failure");

        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();

            VectorizedExecutor exec = new VectorizedExecutor(linker, db, cf, arena);

            // R20-H1: use a REAL AsyncFutureImpl whose completeExceptionally semantics MATCH
            // production. The AsyncFrameworkExceptionHandler increments a counter on every
            // handleException call — this counter is the production-equivalent "did the
            // framework see this failure?" side channel.
            CountingExceptionHandler handler = new CountingExceptionHandler();
            AsyncFutureImpl<Object> realFut =
                    new AsyncFutureImpl<>(
                            // CallbackRunner: unused on the failure path because
                            // completeExceptionally bypasses the completableFuture entirely.
                            task -> {
                                try {
                                    task.run();
                                } catch (Exception e) {
                                    handler.handleException("inline-runner", e);
                                }
                            },
                            handler);

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
                    new StateRequest<>(
                            state, StateRequestType.CLEAR, /* sync */ true, null, realFut, ctx);

            // executeRequestSync MUST NOT propagate the cause out; instead it must catch it
            // and complete the request's future exceptionally. Pre-R18 the throw escaped and
            // the future was left unfinished.
            try {
                exec.executeRequestSync(req);
            } catch (Throwable t) {
                org.junit.jupiter.api.Assertions.fail(
                        "R18-H2: executeRequestSync must not propagate the onClear throw — got: "
                                + t);
            }

            // R20-H1 invariant: handleException MUST fire EXACTLY ONCE.
            //
            //  - Pre-R19 (no guard): recordDelete completed once, executeRequestSync outer
            //    catch completed again → handler count = 2 (double task-failure log).
            //  - R19-H1 with isDone() guard: production AsyncFutureImpl.isDone() always
            //    returns false after completeExceptionally → guard ineffective → still 2.
            //  - R20-H1 with takeClassifierCompletedExceptionally(): set marker correctly
            //    short-circuits the outer catch → handler count = 1. (Asserted here.)
            assertThat(handler.invocations.get())
                    .as("R20-H1: AsyncFrameworkExceptionHandler.handleException must fire EXACTLY"
                            + " once on the onClear-throw path. Pre-R20 the isDone() guard was"
                            + " ineffective because production AsyncFutureImpl.completeExceptionally"
                            + " does not mutate the internal completableFuture; double-completion"
                            + " produced double task-failure logs.")
                    .isEqualTo(1);
            assertThat(handler.lastCause.get())
                    .as("handler must observe the original onClear-throw cause")
                    .isSameAs(cause);
        }
    }

    /**
     * R20-H1: real-semantics exception-handler stub. Counts every {@code handleException}
     * invocation (which is what production {@link AsyncFutureImpl#completeExceptionally}
     * routes to). Equivalent test signal to "completion fired" in production.
     */
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

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

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R20-M1 regression: when {@link VectorizedClassifier#recordDelete}'s {@code onClear} hook
 * throws on row N of a multi-row batch (post-{@code offer}), the rows 0..N-1 that were already
 * classified into the per-op-type buffers must have their per-row futures completed
 * exceptionally before the throw escapes. Without this drain, the batched dispatch path
 * ({@code executeBatchRequests}) reads {@code batchPoisonCause()}, returns a failed CONTAINER
 * future, and never enters the per-op executors — so rows 0..N-1's per-row futures hang
 * forever and the AEC waits on them at the operator level.
 *
 * <p>This test drives {@link VectorizedClassifier#offer(StateRequest)} directly so we observe
 * the classifier's drain behaviour in isolation (independent of the executor's batched-path
 * orchestration). Each row uses a real {@link AsyncFutureImpl} with a counting framework
 * exception handler so we can assert "every row's future was completed exceptionally exactly
 * once" using the same side-channel the production framework uses for failure surfacing.
 */
class ClassifierOnClearMultiRowDrainTest {

    @Test
    void multiRowBatch_classifierDrainsPreviousRows_onLateOnClearThrow() {
        RuntimeException cause = new RuntimeException("simulated onClear failure at row N");

        try (Arena arena = Arena.ofConfined()) {
            // Build classifier with executor-style columnar buffers backed by the test arena.
            ColumnarBatchBuffer getKeys = new ColumnarBatchBuffer(arena);
            ColumnarBatchBuffer putKeys = new ColumnarBatchBuffer(arena);
            ColumnarBatchBuffer putValues = new ColumnarBatchBuffer(arena);
            ColumnarBatchBuffer deleteKeys = new ColumnarBatchBuffer(arena);
            VectorizedClassifier classifier =
                    new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys);

            // Build a multi-row batch where the last row throws on onClear (CLEAR routes through
            // recordDelete → onClear). Layout chosen to exercise BOTH the GET and PUT classified
            // arrays in the drain pass:
            //   row 0: VALUE_GET   (GoodOnClearStubState)  -> ends up in getRequests[0]
            //   row 1: VALUE_UPDATE (GoodOnClearStubState) -> ends up in putRequests[0]
            //   row 2: VALUE_UPDATE (GoodOnClearStubState) -> ends up in putRequests[1]
            //   row 3: CLEAR        (ThrowingOnClearStub)  -> recordDelete catches the onClear
            //                                                 throw and must drain rows 0..2 +
            //                                                 also itself.
            List<TestRow> rows = new ArrayList<>();
            rows.add(makeRow(StateRequestType.VALUE_GET, new GoodOnClearStubState()));
            rows.add(
                    makeRow(
                            StateRequestType.VALUE_UPDATE,
                            new GoodOnClearStubState(),
                            new byte[] {0x01}));
            rows.add(
                    makeRow(
                            StateRequestType.VALUE_UPDATE,
                            new GoodOnClearStubState(),
                            new byte[] {0x02}));
            rows.add(makeRow(StateRequestType.CLEAR, new ThrowingOnClearStubState(cause)));

            // Offer rows 0..2 — should succeed.
            for (int i = 0; i < 3; i++) {
                classifier.offer(rows.get(i).request);
            }

            // Offer the row that throws on onClear — the throw must escape (mailbox-thread
            // contract) but the classifier must drain rows 0..2's futures + the failing row
            // itself before rethrowing.
            Throwable thrown = null;
            try {
                classifier.offer(rows.get(3).request);
            } catch (Throwable t) {
                thrown = t;
            }
            assertThat(thrown)
                    .as("offer() must propagate the onClear throw so AEC's mailbox sees the"
                            + " failure")
                    .isSameAs(cause);

            // Verify EVERY row's framework exception handler fired EXACTLY ONCE.
            for (int i = 0; i < rows.size(); i++) {
                assertThat(rows.get(i).handler.invocations.get())
                        .as("R20-M1: row %d future must be completed exceptionally exactly once"
                                + " when sibling row N throws on onClear (pre-fix the row's"
                                + " future would hang because the executor short-circuits via"
                                + " batchPoisonCause and never enters the per-op executor that"
                                + " would have completed it)",
                                i)
                        .isEqualTo(1);
            }

            // The batch is poisoned with the original cause.
            assertThat(classifier.batchPoisonCause())
                    .as("classifier must record the onClear cause as batch poison")
                    .isSameAs(cause);

            // The classifier-side completion tracker must contain every drained row so that
            // executeRequestSync's outer catch (or any re-entry) does not double-fire the
            // handler.
            for (int i = 0; i < rows.size(); i++) {
                assertThat(classifier.takeClassifierCompletedExceptionally(rows.get(i).request))
                        .as("row %d must be marked as classifier-completed-exceptionally", i)
                        .isTrue();
            }
            // Idempotence: a second take returns false.
            assertThat(classifier.takeClassifierCompletedExceptionally(rows.get(0).request))
                    .as("take must be idempotent: a second call returns false")
                    .isFalse();
        }
    }

    // -- helpers ----------------------------------------------------------------------------

    private static TestRow makeRow(StateRequestType type, ForStRsInnerTable<Object, Object, Object> state) {
        return makeRow(type, state, null);
    }

    private static TestRow makeRow(
            StateRequestType type,
            ForStRsInnerTable<Object, Object, Object> state,
            byte[] payload) {
        CountingExceptionHandler handler = new CountingExceptionHandler();
        AsyncFutureImpl<Object> fut =
                new AsyncFutureImpl<>(
                        task -> {
                            try {
                                task.run();
                            } catch (Exception e) {
                                handler.handleException("inline-runner", e);
                            }
                        },
                        handler);
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
                        (State) state, type, /* sync */ false, payload, fut, ctx);
        return new TestRow(req, handler);
    }

    private static final class TestRow {
        final StateRequest<Object, Object, Object, Object> request;
        final CountingExceptionHandler handler;

        TestRow(StateRequest<Object, Object, Object, Object> request, CountingExceptionHandler h) {
            this.request = request;
            this.handler = h;
        }
    }

    /** Counts handleException calls — production side-channel for async-state failures. */
    private static final class CountingExceptionHandler
            implements AsyncFutureImpl.AsyncFrameworkExceptionHandler {
        final AtomicInteger invocations = new AtomicInteger(0);
        final AtomicReference<Throwable> lastCause = new AtomicReference<>();

        @Override
        public void handleException(String message, Throwable exception) {
            invocations.incrementAndGet();
            lastCause.set(exception);
        }
    }

    /** No-throw stub state — covers the "row already classified" arm of the drain. */
    private static final class GoodOnClearStubState
            implements State,
                    InternalPartitionedState<Object>,
                    ForStRsInnerTable<Object, Object, Object> {
        @Override
        public byte[] serializeKey(StateRequest<Object, Object, ?, ?> request) {
            return new byte[] {7};
        }

        @Override
        public byte[] serializeValue(Object v) {
            return v instanceof byte[] ? (byte[]) v : new byte[0];
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
            // no-op — the classified row that throws is row N (separate state instance).
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

    /** State whose onClear throws — drives the recordDelete drain path. */
    private static final class ThrowingOnClearStubState
            implements State,
                    InternalPartitionedState<Object>,
                    ForStRsInnerTable<Object, Object, Object> {
        private final RuntimeException toThrow;

        ThrowingOnClearStubState(RuntimeException toThrow) {
            this.toThrow = toThrow;
        }

        @Override
        public byte[] serializeKey(StateRequest<Object, Object, ?, ?> request) {
            return new byte[] {8};
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

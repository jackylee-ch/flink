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

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E9-H1 regression: when a per-row {@code complete()} or {@code completeExceptionally()} throws
 * inside one of the {@code execute*} helpers in {@link ForStRsStateExecutor}, the outer
 * {@code executeBatchRequests} catch block must NOT double-complete the rows that successfully
 * completed before the throw.
 *
 * <p>Prior shape: helpers returned {@code int} on the success path only. A row-level throw
 * never reached the return statement, so the outer {@code getsCompleted} stayed at 0 and the
 * drain re-completed every row exceptionally — double-completing rows [0, k-1] and invoking
 * the framework {@code exceptionHandler.handleException} for each, raising spurious task
 * failures.
 *
 * <p>Current shape passes {@code int[1] progressOut} into the helpers and advances it AFTER
 * each successful per-row completion. A throw leaves a precise count for the drain to skip.
 */
class ForStRsStateExecutorProgressTrackingTest {

    /**
     * Construct an executor whose FFI seams are no-ops. The arena is real (the linker ctor
     * touches native), but {@code db} / {@code cf} are unused because the seam overrides
     * never touch them — the test exercises ONLY the per-row completion loop.
     */
    private static ForStRsStateExecutor newExecutor(Arena arena, byte[][] fakeGetResults) {
        ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
        FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
        FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();
        return new ForStRsStateExecutor(linker, db, cf, arena) {
            @Override
            protected byte[][] invokeBatchGet(byte[][] keys) {
                return fakeGetResults;
            }

            @Override
            protected void invokeBatchPut(byte[][] keys, byte[][] values) {
                // no-op
            }

            @Override
            protected void invokeDelete(byte[] key) {
                // no-op
            }
        };
    }

    /**
     * Per-row {@link ForStRsDBGetRequest} fake that throws from {@code complete()} on the
     * row index passed at construction. Tracks every successful {@code complete()} and every
     * {@code completeExceptionally()} call so the test can assert no double-completion.
     */
    private static final class ThrowingGet extends ForStRsDBGetRequest<Object, Object, Object> {
        final int rowIndex;
        final int throwOnRow;
        final AtomicInteger completeCount = new AtomicInteger();
        final AtomicInteger completeExceptionallyCount = new AtomicInteger();

        ThrowingGet(int rowIndex, int throwOnRow) {
            // The base ctor stores nulls; we never call super.complete/completeExceptionally.
            super(new byte[] {(byte) rowIndex}, /* request */ null, /* table */ null);
            this.rowIndex = rowIndex;
            this.throwOnRow = throwOnRow;
        }

        @Override
        public void complete(byte[] rawValue) {
            if (rowIndex == throwOnRow) {
                throw new RuntimeException("simulated complete() throw at row " + rowIndex);
            }
            completeCount.incrementAndGet();
        }

        @Override
        public void completeExceptionally(Throwable t) {
            completeExceptionallyCount.incrementAndGet();
        }
    }

    /**
     * Per-row {@link ForStRsDBPutRequest} fake mirroring {@link ThrowingGet} for the PUT phase.
     */
    private static final class ThrowingPut extends ForStRsDBPutRequest<Object, Object, Object> {
        final int rowIndex;
        final int throwOnRow;
        final AtomicInteger completeCount = new AtomicInteger();
        final AtomicInteger completeExceptionallyCount = new AtomicInteger();

        ThrowingPut(int rowIndex, int throwOnRow) {
            super(new byte[] {(byte) rowIndex}, new byte[] {0}, /* request */ null);
            this.rowIndex = rowIndex;
            this.throwOnRow = throwOnRow;
        }

        @Override
        public void complete() {
            if (rowIndex == throwOnRow) {
                throw new RuntimeException("simulated complete() throw at row " + rowIndex);
            }
            completeCount.incrementAndGet();
        }

        @Override
        public void completeExceptionally(Throwable t) {
            completeExceptionallyCount.incrementAndGet();
        }
    }

    @Test
    void getsPhaseThrowAtRowK_RowsBeforeKNotDoubleCompleted() throws Exception {
        final int n = 6;
        final int k = 3;
        try (Arena arena = Arena.ofConfined()) {
            // Fake FFI results: any non-null byte[] per row.
            byte[][] fakeResults = new byte[n][];
            for (int i = 0; i < n; i++) {
                fakeResults[i] = new byte[] {(byte) i};
            }
            ForStRsStateExecutor executor = newExecutor(arena, fakeResults);

            ForStRsStateRequestClassifier classifier = new ForStRsStateRequestClassifier();
            ThrowingGet[] requests = new ThrowingGet[n];
            for (int i = 0; i < n; i++) {
                requests[i] = new ThrowingGet(i, k);
                classifier.getGetRequests().add(requests[i]);
            }

            CompletableFuture<Void> result = executor.executeBatchRequests(classifier);

            // The batch future must surface the failure (executeBatchRequests catches the
            // throw and returns a failedFuture).
            assertTrue(result.isCompletedExceptionally(), "batch future must surface the throw");

            // Rows [0, k): exactly one complete(), zero completeExceptionally() — the E9-H1
            // invariant. Pre-fix shape would have completeExceptionally() called on each
            // (double-completion).
            for (int i = 0; i < k; i++) {
                assertEquals(
                        1,
                        requests[i].completeCount.get(),
                        "row " + i + " must be completed exactly once on success");
                assertEquals(
                        0,
                        requests[i].completeExceptionallyCount.get(),
                        "row " + i + " must NOT be completeExceptionally'd (double-completion)");
            }

            // Row k itself: complete() threw (no success counter incremented); the drain
            // completes it exceptionally exactly once.
            assertEquals(0, requests[k].completeCount.get(), "row k's complete() threw");
            assertEquals(
                    1,
                    requests[k].completeExceptionallyCount.get(),
                    "row k must be drained exceptionally exactly once");

            // Rows (k, n): drained exceptionally exactly once.
            for (int i = k + 1; i < n; i++) {
                assertEquals(0, requests[i].completeCount.get(), "row " + i + " never reached");
                assertEquals(
                        1,
                        requests[i].completeExceptionallyCount.get(),
                        "row " + i + " drained once");
            }
        }
    }

    @Test
    void putsPhaseThrowAtRowK_RowsBeforeKNotDoubleCompleted() throws Exception {
        final int n = 5;
        final int k = 2;
        try (Arena arena = Arena.ofConfined()) {
            ForStRsStateExecutor executor = newExecutor(arena, new byte[0][]);

            ForStRsStateRequestClassifier classifier = new ForStRsStateRequestClassifier();
            ThrowingPut[] requests = new ThrowingPut[n];
            for (int i = 0; i < n; i++) {
                requests[i] = new ThrowingPut(i, k);
                classifier.getPutRequests().add(requests[i]);
            }

            CompletableFuture<Void> result = executor.executeBatchRequests(classifier);
            assertTrue(result.isCompletedExceptionally(), "batch future must surface the throw");

            for (int i = 0; i < k; i++) {
                assertEquals(
                        1,
                        requests[i].completeCount.get(),
                        "row " + i + " must be completed exactly once on success");
                assertEquals(
                        0,
                        requests[i].completeExceptionallyCount.get(),
                        "row " + i + " must NOT be completeExceptionally'd (double-completion)");
            }
            assertEquals(0, requests[k].completeCount.get());
            assertEquals(1, requests[k].completeExceptionallyCount.get());
            for (int i = k + 1; i < n; i++) {
                assertEquals(0, requests[i].completeCount.get());
                assertEquals(1, requests[i].completeExceptionallyCount.get());
            }
        }
    }

    /**
     * Happy-path: no throw → every row completes exactly once on success, no drain runs.
     */
    @Test
    void noThrow_AllRowsCompletedExactlyOnce() throws Exception {
        final int n = 4;
        try (Arena arena = Arena.ofConfined()) {
            byte[][] fakeResults = new byte[n][];
            for (int i = 0; i < n; i++) {
                fakeResults[i] = new byte[] {(byte) i};
            }
            ForStRsStateExecutor executor = newExecutor(arena, fakeResults);

            ForStRsStateRequestClassifier classifier = new ForStRsStateRequestClassifier();
            ThrowingGet[] requests = new ThrowingGet[n];
            for (int i = 0; i < n; i++) {
                requests[i] = new ThrowingGet(i, /* throwOnRow */ -1);
                classifier.getGetRequests().add(requests[i]);
            }

            CompletableFuture<Void> result = executor.executeBatchRequests(classifier);
            assertFalse(result.isCompletedExceptionally(), "happy-path future must succeed");
            assertSame(null, result.getNow(null), "happy-path future yields null");

            for (int i = 0; i < n; i++) {
                assertEquals(1, requests[i].completeCount.get(), "row " + i);
                assertEquals(0, requests[i].completeExceptionallyCount.get(), "row " + i);
            }
        }
    }
}

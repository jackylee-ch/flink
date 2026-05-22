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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.lang.foreign.Arena;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Reusable fakes for the PR-A10 / S1-9 per-row failure-propagation tests. Replaces the Mockito
 * pattern (forbidden by Flink's coding guidelines outside the legacy-suppression allowlist) with
 * thin manually-written stubs so the tests stay self-contained.
 */
final class BatchedFailurePropagationTestHelpers {

    private BatchedFailurePropagationTestHelpers() {}

    /**
     * Builds a real {@link StateRequest} backed by stubbed {@link State} / {@link InternalAsyncFuture}.
     * Used as the per-row input the {@link VectorizedClassifier} routes through the FFI dispatcher
     * under test.
     */
    static StateRequest<Object, Object, Object, Object> newRequest(
            StateRequestType type,
            Object payload,
            byte[] key,
            byte[] value,
            RecordingFuture<Object> future) {
        StubState state = new StubState(key, value);
        RecordContext<Object> ctx =
                new RecordContext<>(
                        /* record */ null,
                        /* key */ null,
                        /* disposer */ rc -> {},
                        /* keyGroup */ 0,
                        /* epoch */ new Epoch(0L),
                        new AtomicReferenceArray<>(0),
                        /* priority */ 0);
        return new StateRequest<>(state, type, /* sync */ false, payload, future, ctx);
    }

    /**
     * A {@link ForStRsLinker} placeholder for {@link VectorizedExecutor}'s constructor. The
     * production {@code VectorizedExecutor} only touches the linker through the package-private
     * {@code invokeVectorizedBatch*} seams, so the test subclass overrides those seams and this
     * stub never gets called.
     */
    static ForStRsLinker stubLinker(Arena arena) {
        // ForStRsLinker is final and only constructible via its public Arena ctor. Real
        // construction loads native methodhandles which we then never invoke (the test
        // subclass overrides the seam methods).
        return new ForStRsLinker(arena);
    }

    /**
     * A {@link FrsDb} placeholder — used only as an argument that the seam overrides discard.
     * {@link FrsDb} is final, so the test relies on its public no-op fields being safely ignored.
     */
    static FrsDb stubDb() {
        // The VectorizedExecutor never dereferences db.* on the failure path that the
        // dispatcher's catch-block traverses, because the test subclass intercepts the
        // FFI call before db is read by the linker.
        return null;
    }

    /** Same rationale as {@link #stubDb()} for the column-family handle. */
    static FrsCfHandle stubCf() {
        return null;
    }

    /** A state stub that implements every interface {@link VectorizedClassifier#offer} requires. */
    static final class StubState
            implements State, InternalPartitionedState<Object>, ForStRsInnerTable<Object, Object, Object> {
        private final byte[] key;
        private final byte[] value;

        StubState(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public byte[] serializeKey(StateRequest<Object, Object, ?, ?> request) {
            return key;
        }

        @Override
        public byte[] serializeValue(Object v) {
            return value;
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

    /**
     * Records {@code completeExceptionally} calls. The PR-A10 dispatchers route every per-row
     * failure through this method; the test only needs to verify it was called with the
     * propagated cause.
     */
    static final class RecordingFuture<T> implements InternalAsyncFuture<T> {
        final AtomicInteger exceptionalCalls = new AtomicInteger(0);
        final AtomicReference<Throwable> recordedCause = new AtomicReference<>();
        final AtomicInteger normalCalls = new AtomicInteger(0);

        @Override
        public boolean isDone() {
            return exceptionalCalls.get() > 0 || normalCalls.get() > 0;
        }

        @Override
        public T get() {
            return null;
        }

        @Override
        public void complete(T result) {
            normalCalls.incrementAndGet();
        }

        @Override
        public void completeExceptionally(String message, Throwable ex) {
            exceptionalCalls.incrementAndGet();
            recordedCause.set(ex);
        }

        @Override
        public void thenSyncAccept(ThrowingConsumer<? super T, ? extends Exception> action) {}

        @Override
        public <U> InternalAsyncFuture<U> thenApply(
                FunctionWithException<? super T, ? extends U, ? extends Exception> fn) {
            return null;
        }

        @Override
        public InternalAsyncFuture<Void> thenAccept(
                ThrowingConsumer<? super T, ? extends Exception> action) {
            return null;
        }

        @Override
        public <U> InternalAsyncFuture<U> thenCompose(
                FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                        action) {
            return null;
        }

        @Override
        public <U, V> InternalAsyncFuture<V> thenCombine(
                StateFuture<? extends U> other,
                BiFunctionWithException<? super T, ? super U, ? extends V, ? extends Exception>
                        fn) {
            return null;
        }

        @Override
        public <U, V> InternalAsyncFuture<Tuple2<Boolean, Object>> thenConditionallyApply(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue,
                FunctionWithException<? super T, ? extends V, ? extends Exception> actionIfFalse) {
            return null;
        }

        @Override
        public <U> InternalAsyncFuture<Tuple2<Boolean, U>> thenConditionallyApply(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue) {
            return null;
        }

        @Override
        public InternalAsyncFuture<Boolean> thenConditionallyAccept(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                ThrowingConsumer<? super T, ? extends Exception> actionIfTrue,
                ThrowingConsumer<? super T, ? extends Exception> actionIfFalse) {
            return null;
        }

        @Override
        public InternalAsyncFuture<Boolean> thenConditionallyAccept(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                ThrowingConsumer<? super T, ? extends Exception> actionIfTrue) {
            return null;
        }

        @Override
        public <U, V> InternalAsyncFuture<Tuple2<Boolean, Object>> thenConditionallyCompose(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                        actionIfTrue,
                FunctionWithException<? super T, ? extends StateFuture<V>, ? extends Exception>
                        actionIfFalse) {
            return null;
        }

        @Override
        public <U> InternalAsyncFuture<Tuple2<Boolean, U>> thenConditionallyCompose(
                FunctionWithException<? super T, Boolean, ? extends Exception> condition,
                FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                        actionIfTrue) {
            return null;
        }
    }
}

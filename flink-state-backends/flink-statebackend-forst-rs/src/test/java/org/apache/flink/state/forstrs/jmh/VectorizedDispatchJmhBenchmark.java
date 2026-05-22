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

package org.apache.flink.state.forstrs.jmh;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;
import org.apache.flink.state.forstrs.VectorizedExecutor;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * JMH bench (PR-F4) for the vectorized async-V2 dispatch hot path. Closes B3-JMH: the legacy
 * harnesses in this package were annotation-free and exercised neither {@code
 * VectorizedExecutor.executeBatchRequests} nor the vectorized FFM seam.
 *
 * <p>Each {@link #dispatchBatch}, {@link #dispatchPutOnly}, and {@link #dispatchGetOnly} call:
 *
 * <ol>
 *   <li>Asks the executor for a fresh {@link org.apache.flink.state.forstrs.VectorizedClassifier}
 *       via {@code createRequestContainer()} — same call the runtime makes per batch.
 *   <li>Offers {@code N=batchSize} synthetic state requests via {@link
 *       org.apache.flink.state.forstrs.VectorizedClassifier#offer(StateRequest)}, exercising the
 *       columnar buffer accumulation path.
 *   <li>Calls {@link VectorizedExecutor#executeBatchRequests} which routes through the {@code
 *       invokeVectorizedBatchPut} / {@code invokeVectorizedBatchDelete} / {@code
 *       invokeVectorizedBatchGet} FFM seams (overridden here as no-ops so we measure pure
 *       Java-side cost) and completes the per-row futures.
 *   <li>Blackholes the returned container future + the per-row future-completion counters.
 * </ol>
 *
 * <p>The FFM call is stubbed because the goal of this bench is to measure the per-batch dispatch
 * overhead (classifier offer, columnar accumulation, future completion routing) in isolation —
 * the engine cost is benchmarked elsewhere. With the FFM seam stubbed to a no-op, the reported
 * {@code ns/batch} is the Java-layer dispatch ceiling.
 *
 * <p>Invoke via the {@code jmh} maven profile or {@code java -jar benchmarks.jar
 * VectorizedDispatchJmh}. Not part of {@code mvn test} (no {@code @Test} annotation; JMH wires
 * itself via the annotation processor).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class VectorizedDispatchJmhBenchmark {

    /** Mix of op types inside a single batch — drives the executor's three-way op-partitioning. */
    @Param({"64"})
    public int batchSize;

    private Arena arena;
    private VectorizedExecutor executor;

    // Pre-allocated byte arrays so the offer-loop body doesn't measure GC pressure from
    // per-request allocations. Same shape as the production hot path where a {@code byte[]}
    // already exists on the StateRequest at offer-time.
    private byte[][] keys;
    private byte[][] values;
    private StubState[] states;

    @Setup(Level.Trial)
    public void setUp() {
        this.arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        executor = new NoOpFfmVectorizedExecutor(linker, /* db */ null, /* cf */ null, arena);

        keys = new byte[batchSize][];
        values = new byte[batchSize][];
        states = new StubState[batchSize];
        for (int i = 0; i < batchSize; i++) {
            keys[i] = ("k_" + i).getBytes();
            values[i] = ("v_" + i + "_payload").getBytes();
            states[i] = new StubState(keys[i], values[i]);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        arena.close();
    }

    // -----------------------------------------------------------------
    // @Benchmark methods
    // -----------------------------------------------------------------

    /**
     * Mixed-op batch (~33% PUT / 33% GET / 33% DELETE). Exercises all three op-type partitions of
     * the classifier in a single dispatch — closest to the Q11/Q12-style workload.
     */
    @Benchmark
    public void dispatchBatchMixed(Blackhole bh) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container = executor.createRequestContainer();
        for (int i = 0; i < batchSize; i++) {
            StateRequestType type;
            switch (i % 3) {
                case 0:
                    type = StateRequestType.VALUE_GET;
                    break;
                case 1:
                    type = StateRequestType.VALUE_UPDATE;
                    break;
                default:
                    type = StateRequestType.CLEAR;
                    break;
            }
            container.offer(buildRequest(type, i));
        }
        CompletableFuture<Void> done = executor.executeBatchRequests(container);
        bh.consume(done);
    }

    /** PUT-only batch — exercises the {@code vectorizedBatchPut} seam exclusively. */
    @Benchmark
    public void dispatchPutOnly(Blackhole bh) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container = executor.createRequestContainer();
        for (int i = 0; i < batchSize; i++) {
            container.offer(buildRequest(StateRequestType.VALUE_UPDATE, i));
        }
        bh.consume(executor.executeBatchRequests(container));
    }

    /** GET-only batch — exercises the {@code vectorizedBatchGet} seam + result-segment plumbing. */
    @Benchmark
    public void dispatchGetOnly(Blackhole bh) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container = executor.createRequestContainer();
        for (int i = 0; i < batchSize; i++) {
            container.offer(buildRequest(StateRequestType.VALUE_GET, i));
        }
        bh.consume(executor.executeBatchRequests(container));
    }

    /** DELETE-only batch — exercises the {@code vectorizedBatchDelete} seam. */
    @Benchmark
    public void dispatchDeleteOnly(Blackhole bh) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container = executor.createRequestContainer();
        for (int i = 0; i < batchSize; i++) {
            container.offer(buildRequest(StateRequestType.CLEAR, i));
        }
        bh.consume(executor.executeBatchRequests(container));
    }

    // -----------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------

    private StateRequest<Object, Object, Object, Object> buildRequest(StateRequestType type, int i) {
        RecordContext<Object> ctx =
                new RecordContext<>(
                        /* record */ null,
                        /* key */ null,
                        /* disposer */ rc -> {},
                        /* keyGroup */ 0,
                        /* epoch */ new Epoch(0L),
                        new AtomicReferenceArray<>(0),
                        /* priority */ 0);
        return new StateRequest<>(
                states[i],
                type,
                /* sync */ false,
                /* payload */ null,
                new NoopAsyncFuture<>(),
                ctx);
    }

    /**
     * {@link VectorizedExecutor} subclass that no-ops every FFM seam so the benchmark measures the
     * Java-side dispatch path in isolation (no engine cost, no real native call). Mirrors the
     * pattern used by {@code AsyncDispatchInFlightParallelismTest}.
     */
    private static final class NoOpFfmVectorizedExecutor extends VectorizedExecutor {
        NoOpFfmVectorizedExecutor(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
            super(linker, db, cf, arena);
        }

        @Override
        protected void invokeVectorizedBatchPut(
                MemorySegment keyOffsetsSeg,
                MemorySegment keyDataSeg,
                MemorySegment valOffsetsSeg,
                MemorySegment valDataSeg,
                long count) {
            // no-op
        }

        @Override
        protected void invokeVectorizedBatchDelete(
                MemorySegment keyOffsetsSeg, MemorySegment keyDataSeg, long count) {
            // no-op
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
            // Return FRS_STATUS_OK (0) so the executor treats every row as a "key not present"
            // miss. Validates the GET-result plumbing without touching the engine.
            return 0;
        }
    }

    /**
     * Minimal {@link InternalPartitionedState} + {@link ForStRsInnerTable} that returns canned key
     * / value bytes — same shape as {@code BatchedFailurePropagationTestHelpers.StubState} but
     * inlined here so the benchmark stays self-contained in the {@code .jmh} package.
     */
    private static final class StubState
            implements org.apache.flink.api.common.state.v2.State,
                    InternalPartitionedState<Object>,
                    ForStRsInnerTable<Object, Object, Object> {
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

    /** Drop-on-the-floor future — JMH doesn't need to observe completion semantics. */
    private static final class NoopAsyncFuture<T> implements InternalAsyncFuture<T> {
        final AtomicInteger normalCalls = new AtomicInteger(0);
        final AtomicInteger exceptionalCalls = new AtomicInteger(0);
        final AtomicReference<Throwable> recordedCause = new AtomicReference<>();

        @Override
        public boolean isDone() {
            return normalCalls.get() > 0 || exceptionalCalls.get() > 0;
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

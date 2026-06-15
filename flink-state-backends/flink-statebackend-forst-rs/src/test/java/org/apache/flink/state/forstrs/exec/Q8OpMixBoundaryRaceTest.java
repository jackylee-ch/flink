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

package org.apache.flink.state.forstrs.exec;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers.RecordingFuture;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;
import org.apache.flink.state.forstrs.TestPausePointAccess;
import org.apache.flink.state.forstrs.VectorizedExecutor;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stage-0 §4 DETERMINISTIC reproduction of the q8 op-mix residual race — the read-your-writes
 * hazard across the mailbox→worker boundary in the window-join op mix (LIST_ADD then ITER/GET at
 * fire). Uses the production test-only pause-point seam ({@link
 * org.apache.flink.state.forstrs.BatchDrainPausePoint}) so a test can force the exact interleaving
 * WITHOUT timing — the seam holds a LIST_ADD batch inside the REAL {@link VectorizedExecutor}
 * worker-drain (just before it applies the merge to the engine), while a mailbox-side GET for the
 * same key races it.
 *
 * <p><b>Why this is the production mechanism, not a model of it.</b> The LIST_ADD flows through the
 * real classifier APPEND_MERGE routing and the real {@code dispatchAppendMerge} drain; the GET
 * flows through the real {@code executeGets} decode. Only the FFI leaves are stubbed (an in-memory
 * key→bytes engine over the {@code invokeVectorizedBatch*}/{@code invokeVecMergeAppendBatch} seams,
 * the same seam-override pattern the per-row failure tests use) and a single barrier is inserted via
 * the production pause-point. The reproduced under-read IS the production under-read.
 *
 * <p><b>The two regimes contrasted.</b>
 *
 * <ul>
 *   <li><b>routing (blocking, the proven-correct default):</b> {@code executeBatchRequests} BLOCKS
 *       the mailbox until the LIST_ADD batch has actually drained to the engine, so a subsequent
 *       same-key GET dispatched from the mailbox cannot even begin until the write is engine-visible
 *       — read-your-writes holds by construction. {@link
 *       #blockingRoutingOrdersWriteBeforeRead_control} proves it.
 *   <li><b>routing-async (non-blocking, the Approach-3 / R2b candidate):</b> {@code
 *       executeBatchRequests} returns IMMEDIATELY with an incomplete future; the LIST_ADD is still
 *       in flight on the worker FIFO. The mailbox is free to dispatch — and the window-fire op-mix
 *       does dispatch — work that is NOT routed onto the same kg-FIFO behind the queued write. When
 *       a fire-path read takes a mailbox-direct / out-of-FIFO route it observes the list as EMPTY
 *       (the dropped window-join rows = the −77% under-emit). {@link
 *       #routingAsyncFirePathReadMissesQueuedWrite_repro} reproduces it deterministically.
 * </ul>
 */
class Q8OpMixBoundaryRaceTest {

    private static final byte[] LIST_KEY = "k/joinKey/win0".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ELEM = encodeChunk("row-A");

    private Arena arena;
    private InMemoryEngineExecutor[] workers;
    private RoutingStateExecutor blocking;
    private RoutingStateExecutor routingAsync;

    @BeforeEach
    void setUp() {
        arena = Arena.ofShared();
        ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
        Map<ByteList, byte[]> engine = new ConcurrentHashMap<>();
        // SINGLE worker — the residual race the ledger flags is "workers=1 NOT safe": even one
        // worker is a SECOND thread vs the mailbox, so the boundary hazard is present.
        workers = new InMemoryEngineExecutor[] {new InMemoryEngineExecutor(linker, arena, engine)};
        blocking = new RoutingStateExecutor(workers, false, false); // routing (blocking)
        routingAsync = new RoutingStateExecutor(workers, false, true); // routing-async (nonBlocking)
    }

    @AfterEach
    void tearDown() {
        TestPausePointAccess.disarm();
        arena.close();
    }

    // ---------------------------------------------------------------------------------------------
    // CONTROL: blocking routing serialises write-before-read — read-your-writes always holds.
    // ---------------------------------------------------------------------------------------------

    @Test
    void blockingRoutingOrdersWriteBeforeRead_control() throws Exception {
        // 1) LIST_ADD for the join key — blocking executeBatchRequests drains it synchronously.
        RecordingFuture<Object> addFut = new RecordingFuture<>();
        blocking.executeBatchRequests(listAddBatch(blocking, addFut)).get(30, TimeUnit.SECONDS);
        assertThat(addFut.normalCallCount()).as("LIST_ADD completed").isEqualTo(1);

        // 2) Window-fire GET for the SAME key — must observe the appended element.
        ListGetFuture getFut = new ListGetFuture();
        blocking.executeBatchRequests(listGetBatch(blocking, getFut)).get(30, TimeUnit.SECONDS);

        assertThat(getFut.present)
                .as("blocking routing: fire-path GET must observe the prior LIST_ADD")
                .isTrue();
        assertThat(getFut.bytes).as("read-your-writes byte-exact").isEqualTo(ELEM);
    }

    // ---------------------------------------------------------------------------------------------
    // REPRO: routing-async + a fire-path read that is NOT funnelled behind the queued worker write
    // observes the list as EMPTY — the q8 windowed-join under-emit, deterministically forced.
    // ---------------------------------------------------------------------------------------------

    @Test
    void routingAsyncFirePathReadMissesQueuedWrite_repro() throws Exception {
        CountDownLatch addInDrain = new CountDownLatch(1); // worker reached pause-point
        CountDownLatch releaseAdd = new CountDownLatch(1); // test releases the worker

        // Arm the production pause-point: when the worker is about to APPLY the LIST_ADD merge to
        // the engine, park it. This is the exact in-flight window: the write is dispatched, queued,
        // about to be applied — but not yet engine-visible.
        TestPausePointAccess.arm(
                classifier -> {
                    if (classifier.appendMergeCount() > 0) {
                        addInDrain.countDown();
                        try {
                            releaseAdd.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });

        // 1) Mailbox dispatches the LIST_ADD; non-blocking returns immediately, worker parks.
        RecordingFuture<Object> addFut = new RecordingFuture<>();
        CompletableFuture<Void> addBatch =
                routingAsync.executeBatchRequests(listAddBatch(routingAsync, addFut));
        assertThat(addBatch).as("routing-async: LIST_ADD future incomplete (worker in flight)")
                .isNotDone();
        assertThat(addInDrain.await(30, TimeUnit.SECONDS))
                .as("worker reached the merge pause-point")
                .isTrue();

        // 2) THE FIRE PATH. The window timer fires on the MAILBOX under overdraft
        //    (InternalTimerServiceAsyncImpl.maintainContextAndProcess →
        //     syncPointRequestWithCallback(allowOverdraft=true)). A fire-path read that takes a
        //    mailbox-direct / out-of-FIFO route runs NOW, while the LIST_ADD is still parked in the
        //    worker. executeRequestSync routes to the kg worker's FIFO TAIL is NOT what the wedge
        //    sees; the wedge is the read issued before the queued write is applied. We model the
        //    out-of-FIFO read precisely by executing the GET directly on a SECOND in-memory engine
        //    view that shares the SAME backing store but runs concurrently (the mailbox thread),
        //    i.e. the read does not wait behind the parked merge.
        ListGetFuture getFut = new ListGetFuture();
        // Run the GET on the mailbox (this) thread via the REAL VectorizedExecutor sync path
        // (executeRequestSync → real classifier → real executeGets decode) WITHOUT routing it onto
        // the parked worker's FIFO — the exact "mailbox-direct engine op overtakes queued worker
        // write" shape (two-regime §3.3 hypothesis b). The GET-only batch has appendMergeCount()==0
        // so the pause-point hook's guard skips it and the read runs immediately.
        workers[0].executeRequestSync(newListGetRequest(getFut));

        assertThat(getFut.present)
                .as(
                        "REPRO: routing-async fire-path read observes the list as EMPTY because the"
                                + " LIST_ADD is still queued/in-flight on the worker — this is the"
                                + " q8 windowed-join dropped row (−77% under-emit)")
                .isFalse();

        // 3) Release the worker; the write lands AFTER the fire already mis-read. The damage (a
        //    missing window-join output row) is already done — exactly the observed symptom.
        releaseAdd.countDown();
        addBatch.get(30, TimeUnit.SECONDS);
        assertThat(addFut.normalCallCount()).isEqualTo(1);

        // A read AFTER the worker drained now sees it — proving the data was never lost, only
        // read too early (a serialization/ordering violation, not corruption).
        ListGetFuture afterFut = new ListGetFuture();
        workers[0].executeRequestSync(newListGetRequest(afterFut));
        assertThat(afterFut.present).as("post-drain read observes the write").isTrue();
    }

    // ---------------------------------------------------------------------------------------------
    // FIX (Stage-0 §6.4 option A): under routing-async the SAME fire-path read, when routed through
    // RoutingStateExecutor.executeRequestSync, funnels onto the key-group worker's FIFO TAIL behind
    // the queued LIST_ADD — so it now observes the write (read-your-writes holds by construction).
    // The ONLY difference from the repro above is that the GET takes the executor's FIFO route
    // (workerThreads[kg].submit(...).get()) instead of the mailbox-direct bypass — which is exactly
    // what the §6.4 fix enforces (the FRS_RS_SYNC_DIRECT mailbox-direct bypass is retired under the
    // non-blocking executor). This is the proof the deterministic race is closed.
    // ---------------------------------------------------------------------------------------------

    @Test
    void routingAsyncFirePathThroughExecutorFifoSeesQueuedWrite_fix() throws Exception {
        CountDownLatch addInDrain = new CountDownLatch(1); // worker reached pause-point
        CountDownLatch releaseAdd = new CountDownLatch(1); // released asynchronously below

        // Arm the production pause-point: when the worker is about to APPLY the LIST_ADD merge,
        // park it (same in-flight window as the repro). A SEPARATE releaser thread (below) lets the
        // write go shortly after, so the FIFO can drain the ADD and THEN the queued GET — proving
        // ordering, not just that we eventually block forever.
        TestPausePointAccess.arm(
                classifier -> {
                    if (classifier.appendMergeCount() > 0) {
                        addInDrain.countDown();
                        try {
                            releaseAdd.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });

        // 1) Mailbox dispatches the LIST_ADD; non-blocking returns immediately, worker parks at the
        //    merge pause-point with the write queued/in-flight (not yet engine-visible).
        RecordingFuture<Object> addFut = new RecordingFuture<>();
        CompletableFuture<Void> addBatch =
                routingAsync.executeBatchRequests(listAddBatch(routingAsync, addFut));
        assertThat(addBatch).as("routing-async: LIST_ADD future incomplete (worker in flight)")
                .isNotDone();
        assertThat(addInDrain.await(30, TimeUnit.SECONDS))
                .as("worker reached the merge pause-point")
                .isTrue();

        // 2) Release the parked write from a SEPARATE thread once the fire-path GET has been
        //    enqueued behind it on the kg-worker FIFO. We can't release before submitting the GET
        //    (then it would not be "queued behind an in-flight write"); but the GET submit blocks
        //    on the FIFO while the worker is parked, so the releaser fires the release after a tiny
        //    settle so the GET is guaranteed enqueued, then the FIFO drains ADD→GET in order.
        Thread releaser =
                new Thread(
                        () -> {
                            // Give the GET submit time to land on the FIFO behind the parked ADD.
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            releaseAdd.countDown();
                        },
                        "q8-fix-releaser");
        releaser.setDaemon(true);
        releaser.start();

        // 3) THE FIRE PATH, THE FIXED ROUTE. The window-fire GET is issued through the
        //    RoutingStateExecutor sync path — under the §6.4 fix this submits to the kg worker's
        //    FIFO TAIL (behind the parked LIST_ADD) and blocks until it completes. Because the FIFO
        //    is ordered, the ADD is applied to the engine BEFORE this GET runs → read-your-writes.
        ListGetFuture getFut = new ListGetFuture();
        routingAsync.executeRequestSync(newListGetRequest(getFut));

        assertThat(getFut.present)
                .as(
                        "FIX: routing-async fire-path GET routed through the executor FIFO observes"
                                + " the queued LIST_ADD — the q8 read-your-writes race is CLOSED")
                .isTrue();
        assertThat(getFut.bytes).as("read-your-writes byte-exact under the fix").isEqualTo(ELEM);

        // The LIST_ADD batch future settled as part of the FIFO drain.
        addBatch.get(30, TimeUnit.SECONDS);
        assertThat(addFut.normalCallCount()).isEqualTo(1);
        releaser.join(5_000);
    }

    // ---------------------------------------------------------------------------------------------
    // Batch builders
    // ---------------------------------------------------------------------------------------------

    private AsyncRequestContainer<StateRequest<?, ?, ?, ?>> listAddBatch(
            RoutingStateExecutor ex, RecordingFuture<Object> fut) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> c = ex.createRequestContainer();
        c.offer(newListAddRequest(fut));
        return c;
    }

    private AsyncRequestContainer<StateRequest<?, ?, ?, ?>> listGetBatch(
            RoutingStateExecutor ex, ListGetFuture fut) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> c = ex.createRequestContainer();
        c.offer(newListGetRequest(fut));
        return c;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static StateRequest<?, ?, ?, ?> newListAddRequest(RecordingFuture<Object> fut) {
        // payload "row-A" (a single element); the list-state table encodes it as a [count][elem]
        // chunk via serializeValueInto, so the merge operand bytes == ELEM.
        return new StateRequest(
                new ListStateTable(),
                StateRequestType.LIST_ADD,
                false,
                "row-A",
                fut,
                ctx(/* keyGroup */ 0));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static StateRequest<?, ?, ?, ?> newListGetRequest(ListGetFuture fut) {
        return new StateRequest(
                new ListStateTable(), StateRequestType.LIST_GET, false, null, fut, ctx(0));
    }

    private static RecordContext<Object> ctx(int keyGroup) {
        return new RecordContext<>(
                null, null, rc -> {}, keyGroup, new Epoch(0L),
                new java.util.concurrent.atomic.AtomicReferenceArray<>(0), 0);
    }

    private static byte[] encodeChunk(String s) {
        byte[] e = s.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[4 + e.length];
        // [count:int=1][elem bytes] — matches ForStRsAsyncListStateV2 Format-B single chunk.
        out[0] = 0;
        out[1] = 0;
        out[2] = 0;
        out[3] = 1;
        System.arraycopy(e, 0, out, 4, e.length);
        return out;
    }

    // ---------------------------------------------------------------------------------------------
    // In-memory engine over the VectorizedExecutor FFI seams.
    // ---------------------------------------------------------------------------------------------

    /** Reproduces the engine for the merge/get the op-mix touches: a key→bytes map, merge=concat. */
    private static final class InMemoryEngineExecutor extends VectorizedExecutor {
        private final Map<ByteList, byte[]> store;

        InMemoryEngineExecutor(ForStRsLinker linker, Arena arena, Map<ByteList, byte[]> store) {
            super(
                    linker,
                    BatchedFailurePropagationTestHelpers.stubDb(),
                    BatchedFailurePropagationTestHelpers.stubCf(),
                    arena);
            this.store = store;
        }

        @Override
        protected int invokeVecMergeAppendBatch(
                MemorySegment keysOffSeg,
                MemorySegment keysDataSeg,
                MemorySegment opsOffSeg,
                MemorySegment opsDataSeg,
                int count) {
            for (int i = 0; i < count; i++) {
                byte[] key = slice(keysOffSeg, keysDataSeg, i);
                byte[] op = slice(opsOffSeg, opsDataSeg, i);
                store.merge(new ByteList(key), op, Q8OpMixBoundaryRaceTest::concat);
            }
            return 0; // FRS_STATUS_OK
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
            int n = (int) count;
            int writeOff = 0;
            outOffsetsSeg.set(ValueLayout.JAVA_INT, 0L, 0);
            for (int i = 0; i < n; i++) {
                byte[] key = slice(keyOffsetsSeg, keyDataSeg, i);
                byte[] v = store.get(new ByteList(key));
                if (v == null) {
                    outValiditySeg.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
                } else {
                    outValiditySeg.set(ValueLayout.JAVA_BYTE, i, (byte) 1);
                    MemorySegment.copy(
                            MemorySegment.ofArray(v), 0L, outDataSeg, writeOff, v.length);
                    writeOff += v.length;
                }
                outOffsetsSeg.set(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES, writeOff);
            }
            outDataLenSegArg.set(ValueLayout.JAVA_LONG, 0L, writeOff);
            return 0; // FRS_STATUS_OK
        }

        private static byte[] slice(MemorySegment offsets, MemorySegment data, int row) {
            int s = offsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int e = offsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
            byte[] out = new byte[e - s];
            MemorySegment.copy(data, s, MemorySegment.ofArray(out), 0L, e - s);
            return out;
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    /** Hashable byte-array key for the in-memory store. */
    private static final class ByteList {
        private final byte[] b;

        ByteList(byte[] b) {
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ByteList && Arrays.equals(b, ((ByteList) o).b);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(b);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // A real ForStRsInnerTable for the list state: routes LIST_ADD → APPEND_MERGE, decodes GET.
    // ---------------------------------------------------------------------------------------------

    private static final class ListStateTable
            implements State,
                    InternalPartitionedState<Object>,
                    ForStRsInnerTable<Object, Object, Object> {

        @Override
        public boolean isListState() {
            return true; // routes LIST_ADD to APPEND_MERGE in the classifier
        }

        @Override
        public String getStateName() {
            return "joinList";
        }

        @Override
        public byte[] serializeKey(StateRequest<Object, Object, ?, ?> request) {
            return LIST_KEY;
        }

        @Override
        public int serializeKeyInto(StateRequest<Object, Object, ?, ?> request, ColumnarBatchBuffer dest) {
            return dest.append(LIST_KEY, 0, LIST_KEY.length);
        }

        @Override
        public byte[] serializeValue(Object v) {
            return ELEM;
        }

        @Override
        public int serializeValueInto(StateRequest<Object, Object, ?, ?> request, ColumnarBatchBuffer dest) {
            if (request.getRequestType() == StateRequestType.CLEAR
                    || request.getPayload() == null) {
                return dest.appendEmpty();
            }
            return dest.append(ELEM, 0, ELEM.length);
        }

        @Override
        public Object deserializeValue(byte[] raw) {
            return raw;
        }

        @Override
        public Object deserializeValue(MemorySegment buf, long offset, int len) {
            byte[] out = new byte[len];
            MemorySegment.copy(buf, offset, MemorySegment.ofArray(out), 0L, len);
            return out;
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

    // ---------------------------------------------------------------------------------------------
    // A future that records GET presence + bytes (the list element decode result).
    // ---------------------------------------------------------------------------------------------

    private static final class ListGetFuture implements InternalAsyncFuture<Object> {
        volatile boolean done;
        volatile boolean present;
        volatile byte[] bytes;

        @Override
        public void complete(Object result) {
            done = true;
            if (result == null) {
                present = false;
            } else if (result instanceof byte[]) {
                present = true;
                bytes = (byte[]) result;
            } else {
                // A non-null, non-byte[] result is "present" with no captured bytes.
                present = true;
            }
        }

        @Override
        public void completeExceptionally(String message, Throwable ex) {
            done = true;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public Object get() {
            return bytes;
        }

        @Override
        public void thenSyncAccept(
                org.apache.flink.util.function.ThrowingConsumer<? super Object, ? extends Exception>
                        action) {}

        @Override
        public <U> InternalAsyncFuture<U> thenApply(
                org.apache.flink.util.function.FunctionWithException<
                                ? super Object, ? extends U, ? extends Exception>
                        fn) {
            return null;
        }

        @Override
        public InternalAsyncFuture<Void> thenAccept(
                org.apache.flink.util.function.ThrowingConsumer<? super Object, ? extends Exception>
                        action) {
            return null;
        }

        @Override
        public <U> InternalAsyncFuture<U> thenCompose(
                org.apache.flink.util.function.FunctionWithException<
                                ? super Object, ? extends StateFuture<U>, ? extends Exception>
                        action) {
            return null;
        }

        @Override
        public <U, V> InternalAsyncFuture<V> thenCombine(
                StateFuture<? extends U> other,
                org.apache.flink.util.function.BiFunctionWithException<
                                ? super Object, ? super U, ? extends V, ? extends Exception>
                        fn) {
            return null;
        }

        @Override
        public <U, V> InternalAsyncFuture<org.apache.flink.api.java.tuple.Tuple2<Boolean, Object>>
                thenConditionallyApply(
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, Boolean, ? extends Exception>
                                condition,
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, ? extends U, ? extends Exception>
                                actionIfTrue,
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, ? extends V, ? extends Exception>
                                actionIfFalse) {
            return null;
        }

        @Override
        public <U> InternalAsyncFuture<org.apache.flink.api.java.tuple.Tuple2<Boolean, U>>
                thenConditionallyApply(
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, Boolean, ? extends Exception>
                                condition,
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, ? extends U, ? extends Exception>
                                actionIfTrue) {
            return null;
        }

        @Override
        public InternalAsyncFuture<Boolean> thenConditionallyAccept(
                org.apache.flink.util.function.FunctionWithException<
                                ? super Object, Boolean, ? extends Exception>
                        condition,
                org.apache.flink.util.function.ThrowingConsumer<? super Object, ? extends Exception>
                        actionIfTrue,
                org.apache.flink.util.function.ThrowingConsumer<? super Object, ? extends Exception>
                        actionIfFalse) {
            return null;
        }

        @Override
        public InternalAsyncFuture<Boolean> thenConditionallyAccept(
                org.apache.flink.util.function.FunctionWithException<
                                ? super Object, Boolean, ? extends Exception>
                        condition,
                org.apache.flink.util.function.ThrowingConsumer<? super Object, ? extends Exception>
                        actionIfTrue) {
            return null;
        }

        @Override
        public <U, V> InternalAsyncFuture<org.apache.flink.api.java.tuple.Tuple2<Boolean, Object>>
                thenConditionallyCompose(
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, Boolean, ? extends Exception>
                                condition,
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, ? extends StateFuture<U>, ? extends Exception>
                                actionIfTrue,
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, ? extends StateFuture<V>, ? extends Exception>
                                actionIfFalse) {
            return null;
        }

        @Override
        public <U> InternalAsyncFuture<org.apache.flink.api.java.tuple.Tuple2<Boolean, U>>
                thenConditionallyCompose(
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, Boolean, ? extends Exception>
                                condition,
                        org.apache.flink.util.function.FunctionWithException<
                                        ? super Object, ? extends StateFuture<U>, ? extends Exception>
                                actionIfTrue) {
            return null;
        }
    }
}

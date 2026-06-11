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

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stage-3 Unit-2 wiring coverage for the MIXED write batch ({@code frs_vectorized_batch_mixed}):
 *
 * <ul>
 *   <li>ONE FFI crossing for a put + delete + appendMerge batch, ZERO-COPY (the classifier's
 *       unified off-heap columns are handed to the seam verbatim, kinds written per-op);
 *   <li>round-trip equality against the default multi-call path on a REAL in-memory engine;
 *   <li>the engine ordering contract — later same-key row wins (delete-then-put yields the put),
 *       merge operands concatenate in row order;
 *   <li>ordering hazards still route to offer-order sync dispatch under mixed staging;
 *   <li>default-off: without {@code FRS_RS_MIXED_BATCH=true} the multi-call path is untouched.
 * </ul>
 *
 * <p>All tests (including the seam-stubbed ones) construct a real {@link ForStRsLinker}, so the
 * module-level {@code -Dforstrs.native.libpath} must point at a dylib built from engine commit
 * b5b85a9b9 or later (the binding fails fast on older dylibs missing the symbol).
 */
class VectorizedMixedBatchTest {

    // -----------------------------------------------------------------
    // 1. Single crossing + zero-copy (seam-stubbed)
    // -----------------------------------------------------------------

    @Test
    void mixedBatchCommitsAllWriteKindsInOneZeroCopyCrossing() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            RecordingExecutor exec =
                    new RecordingExecutor(
                            linker,
                            BatchedFailurePropagationTestHelpers.stubDb(),
                            BatchedFailurePropagationTestHelpers.stubCf(),
                            arena);
            exec.setMixedBatchEnabledForTests(true);

            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            VectorizedClassifier classifier = (VectorizedClassifier) container;
            assertThat(classifier.isMixedStaging()).isTrue();

            RecordingFuture<Object> putFut = new RecordingFuture<>();
            RecordingFuture<Object> delFut = new RecordingFuture<>();
            RecordingFuture<Object> mergeFut = new RecordingFuture<>();
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_UPDATE,
                            "v",
                            bytes("k1"),
                            bytes("v1"),
                            putFut));
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.CLEAR, null, bytes("k2"), null, delFut));
            container.offer(newListAddRequest(bytes("k3"), bytes("A"), mergeFut));

            assertThat(exec.executeBatchRequests(container)).isCompleted();

            // ONE crossing covering all three write kinds; no per-kind crossing fired.
            assertThat(exec.calls).containsExactly("MIXED");
            assertThat(exec.capturedCount).isEqualTo(3);

            // Zero-copy: the seam received the classifier's own staging segments.
            assertThat(exec.capturedKinds.address())
                    .isEqualTo(classifier.mixedKindsSegment().address());
            assertThat(exec.capturedKeyOffsets.address())
                    .isEqualTo(classifier.mixedKeys().offsetsSegment().address());
            assertThat(exec.capturedKeyData.address())
                    .isEqualTo(classifier.mixedKeys().dataSegment().address());
            assertThat(exec.capturedValOffsets.address())
                    .isEqualTo(classifier.mixedValues().offsetsSegment().address());
            assertThat(exec.capturedValData.address())
                    .isEqualTo(classifier.mixedValues().dataSegment().address());

            // Kinds written per-op, in offer order: Put(1), Delete(0), Merge(2).
            assertThat(kindAt(exec.capturedKinds, 0)).isEqualTo((byte) 1);
            assertThat(kindAt(exec.capturedKinds, 1)).isEqualTo((byte) 0);
            assertThat(kindAt(exec.capturedKinds, 2)).isEqualTo((byte) 2);

            // Columns hold the offer-order rows; the delete row's value slice is EMPTY
            // (engine contract: non-empty delete value = BatchHeaderMalformed).
            assertThat(sliceBytes(classifier.mixedKeys(), 0)).isEqualTo(bytes("k1"));
            assertThat(sliceBytes(classifier.mixedKeys(), 1)).isEqualTo(bytes("k2"));
            assertThat(sliceBytes(classifier.mixedKeys(), 2)).isEqualTo(bytes("k3"));
            assertThat(sliceBytes(classifier.mixedValues(), 0)).isEqualTo(bytes("v1"));
            assertThat(sliceBytes(classifier.mixedValues(), 1)).isEmpty();
            assertThat(sliceBytes(classifier.mixedValues(), 2)).isEqualTo(bytes("A"));

            // Every row's runtime future completed normally.
            assertThat(putFut.isDone()).isTrue();
            assertThat(delFut.isDone()).isTrue();
            assertThat(mergeFut.isDone()).isTrue();
        }
    }

    // -----------------------------------------------------------------
    // 2. Round-trip equality vs the multi-call path (REAL ENGINE)
    // -----------------------------------------------------------------

    @Test
    void mixedBatchRoundTripsEqualToMultiCallPath() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb mixedDb = linker.dbOpenMemory(arena);
                    FrsCfHandle mixedCf = linker.dbDefaultCf(mixedDb, arena);
                    FrsDb plainDb = linker.dbOpenMemory(arena);
                    FrsCfHandle plainCf = linker.dbDefaultCf(plainDb, arena)) {

                // Pre-existing key that the batch deletes on both paths.
                linker.put(mixedDb, mixedCf, bytes("k2"), bytes("dead"));
                linker.put(plainDb, plainCf, bytes("k2"), bytes("dead"));

                runWriteBatch(linker, mixedDb, mixedCf, arena, /* mixed= */ true);
                runWriteBatch(linker, plainDb, plainCf, arena, /* mixed= */ false);

                for (String k : new String[] {"k1", "k2", "k3"}) {
                    assertThat(linker.get(mixedDb, mixedCf, bytes(k)))
                            .as("key %s: mixed path == multi-call path", k)
                            .isEqualTo(linker.get(plainDb, plainCf, bytes(k)));
                }
                // Absolute expectations (mirror the engine unit test
                // vec_batch_mixed_put_delete_merge_round_trip).
                assertThat(linker.get(mixedDb, mixedCf, bytes("k1"))).isEqualTo(bytes("v1"));
                assertThat(linker.get(mixedDb, mixedCf, bytes("k2"))).isNull();
                assertThat(linker.get(mixedDb, mixedCf, bytes("k3")))
                        .as("merge operands concatenate in row order")
                        .isEqualTo(bytes("AB"));
            }
        }
    }

    /** Offers put(k1=v1) + CLEAR(k2) + LIST_ADD(k3 += A) + LIST_ADD(k3 += B) and executes. */
    private static void runWriteBatch(
            ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena, boolean mixed) {
        VectorizedExecutor exec = new VectorizedExecutor(linker, db, cf, arena);
        exec.setMixedBatchEnabledForTests(mixed);
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container = exec.createRequestContainer();
        assertThat(((VectorizedClassifier) container).isMixedStaging()).isEqualTo(mixed);
        container.offer(
                BatchedFailurePropagationTestHelpers.newRequest(
                        StateRequestType.VALUE_UPDATE,
                        "v",
                        bytes("k1"),
                        bytes("v1"),
                        new RecordingFuture<>()));
        container.offer(
                BatchedFailurePropagationTestHelpers.newRequest(
                        StateRequestType.CLEAR, null, bytes("k2"), null, new RecordingFuture<>()));
        container.offer(newListAddRequest(bytes("k3"), bytes("A"), new RecordingFuture<>()));
        container.offer(newListAddRequest(bytes("k3"), bytes("B"), new RecordingFuture<>()));
        assertThat(exec.executeBatchRequests(container)).isCompleted();
    }

    // -----------------------------------------------------------------
    // 3. Engine ordering contract (REAL ENGINE, linker-level)
    // -----------------------------------------------------------------

    /**
     * Delete-then-put of the SAME key in one mixed batch yields the put: rows apply in array
     * order under one atomic sequence range — the later row wins. (At the executor level a
     * same-key delete+put batch routes to offer-order sync dispatch and never reaches the mixed
     * call; this pins the ENGINE contract the executor relies on.)
     */
    @Test
    void mixedBatchDeleteThenPutOfSameKeyYieldsThePut() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                // [Delete k, Put k=v] → k == v.
                ColumnarBatchBuffer keys = new ColumnarBatchBuffer(arena);
                ColumnarBatchBuffer vals = new ColumnarBatchBuffer(arena);
                keys.append(bytes("ord-k"));
                vals.appendEmpty(); // delete row: empty value slice
                keys.append(bytes("ord-k"));
                vals.append(bytes("v-after-delete"));
                MemorySegment kinds = arena.allocate(2);
                kinds.set(ValueLayout.JAVA_BYTE, 0, (byte) 0); // Delete
                kinds.set(ValueLayout.JAVA_BYTE, 1, (byte) 1); // Put
                linker.vectorizedBatchMixed(
                        db,
                        cf,
                        kinds,
                        keys.offsetsSegment(),
                        keys.dataSegment(),
                        vals.offsetsSegment(),
                        vals.dataSegment(),
                        2);
                assertThat(linker.get(db, cf, bytes("ord-k")))
                        .as("later same-key row wins: delete-then-put yields the put")
                        .isEqualTo(bytes("v-after-delete"));

                // [Put k2=v, Delete k2] → k2 absent (the symmetric direction).
                ColumnarBatchBuffer keys2 = new ColumnarBatchBuffer(arena);
                ColumnarBatchBuffer vals2 = new ColumnarBatchBuffer(arena);
                keys2.append(bytes("ord-k2"));
                vals2.append(bytes("v"));
                keys2.append(bytes("ord-k2"));
                vals2.appendEmpty();
                MemorySegment kinds2 = arena.allocate(2);
                kinds2.set(ValueLayout.JAVA_BYTE, 0, (byte) 1); // Put
                kinds2.set(ValueLayout.JAVA_BYTE, 1, (byte) 0); // Delete
                linker.vectorizedBatchMixed(
                        db,
                        cf,
                        kinds2,
                        keys2.offsetsSegment(),
                        keys2.dataSegment(),
                        vals2.offsetsSegment(),
                        vals2.dataSegment(),
                        2);
                assertThat(linker.get(db, cf, bytes("ord-k2")))
                        .as("put-then-delete yields the delete")
                        .isNull();
            }
        }
    }

    // -----------------------------------------------------------------
    // 4. Hazards still route ordered under mixed staging (seam-stubbed)
    // -----------------------------------------------------------------

    @Test
    void mixedStagingSameKeyGetThenPutStillRoutesOrdered() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            RecordingExecutor exec =
                    new RecordingExecutor(
                            linker,
                            BatchedFailurePropagationTestHelpers.stubDb(),
                            BatchedFailurePropagationTestHelpers.stubCf(),
                            arena);
            exec.setMixedBatchEnabledForTests(true);

            byte[] key = bytes("same-key");
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_GET, null, key, null, new RecordingFuture<>()));
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_UPDATE,
                            "v",
                            key,
                            bytes("v"),
                            new RecordingFuture<>()));

            assertThat(exec.executeBatchRequests(container)).isCompleted();
            // Offer-order sync dispatch — the mixed crossing must NOT fire; the
            // sync path keeps the per-kind (non-mixed) staging by design.
            assertThat(exec.calls).containsExactly("GET", "PUT");
        }
    }

    @Test
    void mixedStagingDeletePrefixVsPutStillRoutesOrdered() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            RecordingExecutor exec =
                    new RecordingExecutor(
                            linker,
                            BatchedFailurePropagationTestHelpers.stubDb(),
                            BatchedFailurePropagationTestHelpers.stubCf(),
                            arena);
            exec.setMixedBatchEnabledForTests(true);

            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            // CLEAR's key is a PREFIX of the put key → delete-ordering hazard.
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_UPDATE,
                            "v",
                            bytes("pfx-key-suffix"),
                            bytes("v"),
                            new RecordingFuture<>()));
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.CLEAR,
                            null,
                            bytes("pfx-key"),
                            null,
                            new RecordingFuture<>()));

            assertThat(exec.executeBatchRequests(container)).isCompleted();
            assertThat(exec.calls).containsExactly("PUT", "DELETE");
            assertThat(exec.calls).doesNotContain("MIXED");
        }
    }

    // -----------------------------------------------------------------
    // 5. Default-off: the multi-call path is untouched (seam-stubbed)
    // -----------------------------------------------------------------

    @Test
    void flagOffKeepsSeparatePutAndDeleteCrossings() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            RecordingExecutor exec =
                    new RecordingExecutor(
                            linker,
                            BatchedFailurePropagationTestHelpers.stubDb(),
                            BatchedFailurePropagationTestHelpers.stubCf(),
                            arena);
            // NOT enabled — mirrors the FRS_RS_MIXED_BATCH default (false).

            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container =
                    exec.createRequestContainer();
            assertThat(((VectorizedClassifier) container).isMixedStaging()).isFalse();
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_UPDATE,
                            "v",
                            bytes("off-k1"),
                            bytes("v1"),
                            new RecordingFuture<>()));
            container.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.CLEAR,
                            null,
                            bytes("off-k2"),
                            null,
                            new RecordingFuture<>()));

            assertThat(exec.executeBatchRequests(container)).isCompleted();
            assertThat(exec.calls).containsExactly("PUT", "DELETE");
        }
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte kindAt(MemorySegment kinds, int row) {
        return kinds.get(ValueLayout.JAVA_BYTE, row);
    }

    private static byte[] sliceBytes(ColumnarBatchBuffer buf, int row) {
        int start = buf.offsetsSegment().get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int end = buf.offsetsSegment().get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        byte[] out = new byte[end - start];
        MemorySegment.copy(buf.dataSegment(), ValueLayout.JAVA_BYTE, start, out, 0, out.length);
        return out;
    }

    /** LIST_ADD request on a list-state stub so the classifier routes it to APPEND_MERGE. */
    private static StateRequest<?, ?, ?, ?> newListAddRequest(
            byte[] key, byte[] operand, RecordingFuture<Object> future) {
        StubListState state = new StubListState(key, operand);
        RecordContext<Object> ctx =
                new RecordContext<>(
                        /* record */ null,
                        /* key */ null,
                        /* disposer */ rc -> {},
                        /* keyGroup */ 0,
                        /* epoch */ new EpochManager.Epoch(0L),
                        new AtomicReferenceArray<>(0),
                        /* priority */ 0);
        return new StateRequest<>(
                state, StateRequestType.LIST_ADD, /* sync */ false, "payload", future, ctx);
    }

    /**
     * Minimal list-state table: {@code isListState() == true} routes LIST_ADD to APPEND_MERGE;
     * key/operand bytes are fixed (the helpers' StubState is final, so this is a sibling).
     */
    private static final class StubListState
            implements State,
                    InternalPartitionedState<Object>,
                    ForStRsInnerTable<Object, Object, Object> {
        private final byte[] key;
        private final byte[] operand;

        StubListState(byte[] key, byte[] operand) {
            this.key = key;
            this.operand = operand;
        }

        @Override
        public byte[] serializeKey(StateRequest<Object, Object, ?, ?> request) {
            return key;
        }

        @Override
        public byte[] serializeValue(Object v) {
            return operand;
        }

        @Override
        public Object deserializeValue(byte[] raw) {
            return raw;
        }

        @Override
        public boolean isListState() {
            return true;
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

    /** Seam-recording executor: captures the mixed crossing and tallies per-kind crossings. */
    private static final class RecordingExecutor extends VectorizedExecutor {
        private final List<String> calls = new ArrayList<>();
        private MemorySegment capturedKinds;
        private MemorySegment capturedKeyOffsets;
        private MemorySegment capturedKeyData;
        private MemorySegment capturedValOffsets;
        private MemorySegment capturedValData;
        private long capturedCount;

        private RecordingExecutor(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
            super(linker, db, cf, arena);
        }

        @Override
        protected void invokeVectorizedBatchMixed(
                MemorySegment kindsSeg,
                MemorySegment keyOffsetsSeg,
                MemorySegment keyDataSeg,
                MemorySegment valOffsetsSeg,
                MemorySegment valDataSeg,
                long count) {
            calls.add("MIXED");
            this.capturedKinds = kindsSeg;
            this.capturedKeyOffsets = keyOffsetsSeg;
            this.capturedKeyData = keyDataSeg;
            this.capturedValOffsets = valOffsetsSeg;
            this.capturedValData = valDataSeg;
            this.capturedCount = count;
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
        protected void invokeVectorizedBatchDelete(
                MemorySegment keyOffsetsSeg, MemorySegment keyDataSeg, long count) {
            calls.add("DELETE");
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

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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;

import org.forstdb.RocksDB;
import org.forstdb.RocksIterator;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.state.forst.ForStIterateOperation.CACHE_SIZE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Test for {@link ForStIterateOperation}. */
class ForStDBIterateOperationTest extends ForStDBOperationTestBase {

    @Test
    void testIterateValues() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(10, mapState, db);
        TestAsyncFuture<StateIterator<String>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        ForStDBIterRequest<Integer, VoidNamespace, String, String, String> request1 =
                new ForStDBMapValueIterRequest<>(contextKey, mapState, null, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<String> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                val -> {
                    assertThat(val).isEqualTo("val-" + count.getAndIncrement());
                });
        assertThat(count.get()).isEqualTo(10);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIterateKeys() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(13, mapState, db);
        TestAsyncFuture<StateIterator<String>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        ForStDBIterRequest<Integer, VoidNamespace, String, String, String> request1 =
                new ForStDBMapKeyIterRequest<>(contextKey, mapState, null, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<String> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                val -> {
                    assertThat(val).isEqualTo("uk-" + count.getAndIncrement());
                });
        assertThat(count.get()).isEqualTo(13);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIterateEntries() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(3, mapState, db);
        TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
                request1 =
                        new ForStDBMapEntryIterRequest<>(contextKey, mapState, null, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<Map.Entry<String, String>> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                entry -> {
                    int cnt = count.getAndIncrement();
                    assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                    assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                });
        assertThat(count.get()).isEqualTo(3);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIteratorLoading() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter");
        prepareData(200, mapState, db);
        TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        MockStateRequestHandler stateRequestHandler = new MockStateRequestHandler();
        ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
                request1 =
                        new ForStDBMapEntryIterRequest<>(
                                contextKey, mapState, stateRequestHandler, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<Map.Entry<String, String>> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        try {
            iterator.onNext(
                    entry -> {
                        int cnt = count.getAndIncrement();
                        assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                        assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                    });
            fail("should throw NPE");
        } catch (NullPointerException npe) {
            assertThat(stateRequestHandler.payload).isNotNull();
            assertThat(count.get()).isEqualTo(CACHE_SIZE_LIMIT);
            Tuple2<StateRequestType, ?> tuple =
                    (Tuple2<StateRequestType, ?>) stateRequestHandler.payload;
            assertThat(tuple.f0).isEqualTo(StateRequestType.MAP_ITER);
            TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future2 =
                    new TestAsyncFuture<>();
            ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
                    request2 = buildEntryIterRequest(contextKey, mapState, tuple.f1, future2);
            batchIterRequest.clear();
            batchIterRequest.add(request2);
            ForStIterateOperation iterOperation2 =
                    new ForStIterateOperation(db, batchIterRequest, executor);
            iterOperation2.process().get();
            StateIterator<Map.Entry<String, String>> iterator2 = future2.getCompletedResult();
            iterator2.onNext(
                    entry -> {
                        int cnt = count.getAndIncrement();
                        assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                        assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                    });
            assertThat(count.get()).isEqualTo(200);
            assertThat(iterator2.isEmpty()).isFalse();
        }
    }

    @Test
    void testForStRsPrefixScanFastPathIteratorLoading() throws Exception {
        assumeTrue(ForStRsLibPrefixScanNative.isAvailable());
        ForStRsLibPrefixScanNative.resetOpenFirstChunkCallsForTesting();
        ForStRsLibPrefixScanNative.resetNextChunkCallsForTesting();
        ForStPrefixScanCursor.resetBufferPoolCountersForTesting();

        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter-fast-path");
        prepareData(200, mapState, db);
        TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        MockStateRequestHandler stateRequestHandler = new MockStateRequestHandler();
        ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
                request1 =
                        new ForStDBMapEntryIterRequest<>(
                                contextKey, mapState, stateRequestHandler, null, future);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<Map.Entry<String, String>> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        try {
            iterator.onNext(
                    entry -> {
                        int cnt = count.getAndIncrement();
                        assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                        assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                    });
            fail("should throw NPE");
        } catch (NullPointerException npe) {
            assertThat(stateRequestHandler.payload).isNotNull();
            assertThat(count.get()).isEqualTo(CACHE_SIZE_LIMIT);
            Tuple2<StateRequestType, ?> tuple =
                    (Tuple2<StateRequestType, ?>) stateRequestHandler.payload;
            assertThat(tuple.f0).isEqualTo(StateRequestType.MAP_ITER);
            assertThat(tuple.f1).isInstanceOf(ForStMapIteratorContinuation.class);

            TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future2 =
                    new TestAsyncFuture<>();
            ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
                    request2 = buildEntryIterRequest(contextKey, mapState, tuple.f1, future2);
            batchIterRequest.clear();
            batchIterRequest.add(request2);
            ForStIterateOperation iterOperation2 =
                    new ForStIterateOperation(db, batchIterRequest, executor);
            iterOperation2.process().get();
            StateIterator<Map.Entry<String, String>> iterator2 = future2.getCompletedResult();
            iterator2.onNext(
                    entry -> {
                        int cnt = count.getAndIncrement();
                        assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                        assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                    });
            assertThat(count.get()).isEqualTo(200);
            assertThat(iterator2.isEmpty()).isFalse();
        }
        assertThat(ForStRsLibPrefixScanNative.getOpenFirstChunkCallsForTesting()).isEqualTo(1);
        assertThat(ForStRsLibPrefixScanNative.getNextChunkCallsForTesting()).isEqualTo(1);
    }

    @Test
    void testForStRsPrefixScanFastPathSmallPrefixManyProbes() throws Exception {
        assumeTrue(ForStRsLibPrefixScanNative.isAvailable());
        ForStRsLibPrefixScanNative.resetOpenFirstChunkCallsForTesting();
        ForStRsLibPrefixScanNative.resetNextChunkCallsForTesting();

        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter-fast-path-many-probes");
        int probeCount = 16;
        int rowsPerProbe = 4;
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        List<TestAsyncFuture<StateIterator<Map.Entry<String, String>>>> futures = new ArrayList<>();

        for (int probe = 0; probe < probeCount; probe++) {
            prepareDataForContext(probe, rowsPerProbe, mapState, db);
            TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future =
                    new TestAsyncFuture<>();
            futures.add(future);
            batchIterRequest.add(
                    new ForStDBMapEntryIterRequest<>(
                            buildContextKey(probe), mapState, null, null, future));
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            new ForStIterateOperation(db, batchIterRequest, executor).process().get();
        } finally {
            executor.shutdownNow();
        }

        for (TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future : futures) {
            assertEntryIterator(future.getCompletedResult(), rowsPerProbe);
        }
        assertThat(ForStRsLibPrefixScanNative.getOpenFirstChunkCallsForTesting())
                .isEqualTo(probeCount);
        assertThat(ForStRsLibPrefixScanNative.getNextChunkCallsForTesting()).isZero();
    }

    @Test
    void testForStRsPrefixScanFastPathReusesDirectBuffersAcrossManySmallProbes() throws Exception {
        assumeTrue(ForStRsLibPrefixScanNative.isAvailable());
        ForStRsLibPrefixScanNative.resetOpenFirstChunkCallsForTesting();
        ForStRsLibPrefixScanNative.resetNextChunkCallsForTesting();

        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter-fast-path-pooled-small-probes");
        int probeCount = 64;
        int rowsPerProbe = 4;
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        List<TestAsyncFuture<StateIterator<Map.Entry<String, String>>>> futures = new ArrayList<>();

        for (int probe = 0; probe < probeCount; probe++) {
            prepareDataForContext(probe, rowsPerProbe, mapState, db);
            TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future =
                    new TestAsyncFuture<>();
            futures.add(future);
            batchIterRequest.add(
                    new ForStDBMapEntryIterRequest<>(
                            buildContextKey(probe), mapState, null, null, future));
        }

        long directBufferCountBefore = directBufferCount();
        long allocatedBefore = ForStPrefixScanCursor.allocatedBufferSetsForTesting();
        long reusedBefore = ForStPrefixScanCursor.reusedBufferSetsForTesting();
        long returnedBefore = ForStPrefixScanCursor.returnedBufferSetsForTesting();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            new ForStIterateOperation(db, batchIterRequest, executor).process().get();
        } finally {
            executor.shutdownNow();
        }
        long directBufferCountAfter = directBufferCount();
        long allocatedDelta =
                ForStPrefixScanCursor.allocatedBufferSetsForTesting() - allocatedBefore;
        long reusedDelta = ForStPrefixScanCursor.reusedBufferSetsForTesting() - reusedBefore;
        long returnedDelta = ForStPrefixScanCursor.returnedBufferSetsForTesting() - returnedBefore;

        for (TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future : futures) {
            assertEntryIterator(future.getCompletedResult(), rowsPerProbe);
        }
        assertThat(ForStRsLibPrefixScanNative.getOpenFirstChunkCallsForTesting())
                .isEqualTo(probeCount);
        assertThat(ForStRsLibPrefixScanNative.getNextChunkCallsForTesting()).isZero();
        assertThat(directBufferCountAfter - directBufferCountBefore)
                .as("single-threaded many small probes should reuse one direct-buffer cursor set")
                .isLessThanOrEqualTo(6);
        assertThat(allocatedDelta).isLessThanOrEqualTo(6);
        assertThat(reusedDelta).isGreaterThanOrEqualTo(probeCount - allocatedDelta);
        assertThat(returnedDelta).isGreaterThanOrEqualTo(probeCount);
        assertThat(ForStPrefixScanCursor.outstandingBufferSetsForTesting()).isZero();
        assertThat(ForStPrefixScanCursor.retainedBufferSetsForTesting()).isLessThanOrEqualTo(6);
    }

    @Test
    void testForStRsPrefixScanFastPathAdjacentPrefixStop() throws Exception {
        assumeTrue(ForStRsLibPrefixScanNative.isAvailable());
        ForStRsLibPrefixScanNative.resetOpenFirstChunkCallsForTesting();
        ForStRsLibPrefixScanNative.resetNextChunkCallsForTesting();

        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter-fast-path-adjacent");
        prepareDataForContext(1, 4, mapState, db);
        prepareDataForContext(2, 4, mapState, db);

        TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        batchIterRequest.add(
                new ForStDBMapEntryIterRequest<>(buildContextKey(1), mapState, null, null, future));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            new ForStIterateOperation(db, batchIterRequest, executor).process().get();
        } finally {
            executor.shutdownNow();
        }

        assertEntryIterator(future.getCompletedResult(), 4);
        assertThat(ForStRsLibPrefixScanNative.getOpenFirstChunkCallsForTesting()).isEqualTo(1);
        assertThat(ForStRsLibPrefixScanNative.getNextChunkCallsForTesting()).isZero();
    }

    @Test
    void testForStRsPrefixScanFastPathExactlyCacheLimit() throws Exception {
        assumeTrue(ForStRsLibPrefixScanNative.isAvailable());
        ForStRsLibPrefixScanNative.resetOpenFirstChunkCallsForTesting();
        ForStRsLibPrefixScanNative.resetNextChunkCallsForTesting();

        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-iter-fast-path-cache-limit");
        prepareData(CACHE_SIZE_LIMIT, mapState, db);
        TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        MockStateRequestHandler stateRequestHandler = new MockStateRequestHandler();
        batchIterRequest.add(
                new ForStDBMapEntryIterRequest<>(
                        contextKey, mapState, stateRequestHandler, null, future));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            new ForStIterateOperation(db, batchIterRequest, executor).process().get();
        } finally {
            executor.shutdownNow();
        }

        assertEntryIterator(future.getCompletedResult(), CACHE_SIZE_LIMIT);
        assertThat(stateRequestHandler.payload).isNull();
        assertThat(ForStRsLibPrefixScanNative.getOpenFirstChunkCallsForTesting()).isEqualTo(1);
        assertThat(ForStRsLibPrefixScanNative.getNextChunkCallsForTesting()).isZero();
    }

    private void assertEntryIterator(
            StateIterator<Map.Entry<String, String>> iterator, int expectedEntries) {
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                entry -> {
                    int cnt = count.getAndIncrement();
                    assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                    assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                });
        assertThat(count.get()).isEqualTo(expectedEntries);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ForStDBIterRequest<Integer, VoidNamespace, String, String, Map.Entry<String, String>>
            buildEntryIterRequest(
                    ContextKey<Integer, VoidNamespace> contextKey,
                    ForStMapState<Integer, VoidNamespace, String, String> mapState,
                    Object continuationPayload,
                    TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future) {
        if (continuationPayload instanceof ForStMapIteratorContinuation) {
            return new ForStDBMapEntryIterRequest<>(
                    contextKey,
                    mapState,
                    null,
                    (ForStMapIteratorContinuation) continuationPayload,
                    true,
                    future);
        }
        return new ForStDBMapEntryIterRequest(
                contextKey, mapState, null, (RocksIterator) continuationPayload, future);
    }

    private void prepareData(
            int num, ForStMapState<Integer, VoidNamespace, String, String> mapState, RocksDB db)
            throws Exception {
        prepareDataForContext(1, num, mapState, db);
    }

    private static long directBufferCount() {
        return ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                .filter(pool -> "direct".equals(pool.getName()))
                .findFirst()
                .map(BufferPoolMXBean::getCount)
                .orElse(0L);
    }

    private void prepareDataForContext(
            int contextKeyId,
            int num,
            ForStMapState<Integer, VoidNamespace, String, String> mapState,
            RocksDB db)
            throws Exception {
        for (int i = 0; i < num; i++) {
            ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(contextKeyId);
            contextKey.setUserKey("uk-" + i);
            String value = "val-" + i;
            byte[] keyBytes = mapState.serializeKey(contextKey);
            byte[] valueBytes = mapState.serializeValue(value);
            db.put(mapState.getColumnFamilyHandle(), keyBytes, valueBytes);
        }
    }

    private static class MockStateRequestHandler implements StateRequestHandler {
        Object payload = null;

        @Override
        public <IN, OUT> InternalAsyncFuture<OUT> handleRequest(
                @Nullable State state, StateRequestType type, @Nullable IN payload) {
            assertThat(type).isEqualTo(StateRequestType.ITERATOR_LOADING);
            this.payload = payload;
            return null;
        }

        @Override
        public <IN, OUT> OUT handleRequestSync(
                State state, StateRequestType type, @Nullable IN payload) {
            return null;
        }

        @Override
        public <N> void setCurrentNamespaceForState(
                @Nonnull InternalPartitionedState<N> state, N namespace) {
            state.setCurrentNamespace(namespace);
        }
    }
}

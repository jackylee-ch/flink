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

import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.runtime.state.VoidNamespace;

import org.forstdb.RocksDB;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Tests for {@link ForStPrefixScanCursor}. */
class ForStPrefixScanCursorTest extends ForStDBOperationTestBase {

    @Test
    void testForStRsPrefixScanFastPathGrowsForLargeValue() throws Exception {
        assumeTrue(ForStRsLibPrefixScanNative.isAvailable());
        ForStRsLibPrefixScanNative.resetNextChunkCallsForTesting();

        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-prefix-large-value");
        String largeValue = "v".repeat(1024 * 1024 + 1024);
        putMapEntry(mapState, db, "large-key", largeValue);

        TestAsyncFuture<StateIterator<Map.Entry<String, String>>> future = new TestAsyncFuture<>();
        List<ForStDBIterRequest<?, ?, ?, ?, ?>> batchIterRequest = new ArrayList<>();
        batchIterRequest.add(
                new ForStDBMapEntryIterRequest<>(
                        buildContextKey(1), mapState, stateRequestHandler, null, future));

        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            new ForStIterateOperation(db, batchIterRequest, executor).process().get();
        } finally {
            executor.shutdownNow();
        }

        StateIterator<Map.Entry<String, String>> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger();
        iterator.onNext(
                entry -> {
                    count.incrementAndGet();
                    assertThat(entry.getKey()).isEqualTo("large-key");
                    assertThat(entry.getValue()).isEqualTo(largeValue);
                });
        assertThat(count.get()).isEqualTo(1);
        assertThat(ForStRsLibPrefixScanNative.getNextChunkCallsForTesting()).isGreaterThan(1);
    }

    private void putMapEntry(
            ForStMapState<Integer, VoidNamespace, String, String> mapState,
            RocksDB db,
            String userKey,
            String value)
            throws Exception {
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        contextKey.setUserKey(userKey);
        db.put(
                mapState.getColumnFamilyHandle(),
                mapState.serializeKey(contextKey),
                mapState.serializeValue(value));
    }
}

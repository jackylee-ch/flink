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

package org.apache.flink.state.forstrs.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-A2 (S1-4 / E2-CRIT-1) regression: every V2 state class must encode the request namespace as
 * the trailing component of the storage composite key. Same (operatorKey, stateName) under
 * different namespaces must produce distinct byte[]s — otherwise window-keyed state silently
 * collides across windows.
 *
 * <p>These tests bypass the full async-state machinery (StateRequestHandler / AEC) and verify the
 * pure serializeKey() byte-level contract: distinct namespace -> distinct composite key bytes.
 */
class V2NamespaceEncodingTest {

    /**
     * Build a minimal RecordContext with a single state→namespace mapping pre-set. The
     * StateRequest constructor pulls the namespace from the context, so this is the hook we use to
     * pin a specific namespace per request.
     */
    private static <K, N> RecordContext<K> contextWithNamespace(
            K key, InternalPartitionedState<N> state, N namespace) {
        RecordContext<K> ctx =
                new RecordContext<>(
                        new Object(),
                        key,
                        c -> {},
                        0 /* keyGroup */,
                        new EpochManager.Epoch(0L),
                        4 /* variableCount */);
        ctx.setNamespace(state, namespace);
        return ctx;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <K, N, IN, OUT> StateRequest<K, N, IN, OUT> request(
            InternalPartitionedState<N> state,
            StateRequestType type,
            IN payload,
            RecordContext<K> ctx) {
        return new StateRequest<>(
                (org.apache.flink.api.common.state.v2.State) state,
                type,
                false,
                payload,
                null,
                ctx);
    }

    // ------------------------------------------------------------------
    // ValueStateV2
    // ------------------------------------------------------------------

    @Test
    void valueStateV2DistinguishesNamespaces() {
        ForStRsValueStateV2<Long, Integer, String> state =
                new ForStRsValueStateV2<>(
                        null /* StateRequestHandler not used by serializeKey */,
                        "myValue",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        // Same operatorKey=42L, two different namespaces.
        RecordContext<Long> ctx1 = contextWithNamespace(42L, state, 1);
        RecordContext<Long> ctx2 = contextWithNamespace(42L, state, 2);

        byte[] k1 = state.serializeKey(request(state, StateRequestType.VALUE_GET, null, ctx1));
        byte[] k2 = state.serializeKey(request(state, StateRequestType.VALUE_GET, null, ctx2));

        assertNotNull(k1);
        assertNotNull(k2);
        assertFalse(
                java.util.Arrays.equals(k1, k2),
                "ValueStateV2: different namespaces must produce distinct composite keys");
        // Pre-A2 the keys would be byte-identical — assert the suffix differs.
        assertNotEquals(k1[k1.length - 1], k2[k2.length - 1]);
    }

    @Test
    void valueStateV2SameNamespaceReusesCachedKey() {
        ForStRsValueStateV2<Long, Integer, String> state =
                new ForStRsValueStateV2<>(
                        null,
                        "myValue",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        // Same context, two requests in same namespace — cache slot keyed by
        // stateName::identityHashCode(namespace) must hit on second call.
        Integer ns = 7;
        RecordContext<Long> ctx = contextWithNamespace(42L, state, ns);

        byte[] k1 = state.serializeKey(request(state, StateRequestType.VALUE_GET, null, ctx));
        byte[] k2 = state.serializeKey(request(state, StateRequestType.VALUE_UPDATE, "x", ctx));

        // Cache hit returns the SAME byte[] reference, not just equal content.
        assertTrue(
                k1 == k2,
                "ValueStateV2: same namespace must reuse cached composite key (identity ref)");
    }

    // ------------------------------------------------------------------
    // MapStateV2 (the V2 async MapState)
    // ------------------------------------------------------------------

    @Test
    void mapStateV2DistinguishesNamespaces() {
        ForStRsMapStateV2<Long, Integer, String, String> state =
                new ForStRsMapStateV2<>(
                        null,
                        "myMap",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        RecordContext<Long> ctx1 = contextWithNamespace(42L, state, 1);
        RecordContext<Long> ctx2 = contextWithNamespace(42L, state, 2);

        // Same userKey under two namespaces — bytes must differ at the namespace position.
        byte[] k1 = state.serializeKey(request(state, StateRequestType.MAP_GET, "uk", ctx1));
        byte[] k2 = state.serializeKey(request(state, StateRequestType.MAP_GET, "uk", ctx2));

        assertFalse(
                java.util.Arrays.equals(k1, k2),
                "MapStateV2: different namespaces must produce distinct composite keys");

        // Also verify the iter-prefix path encodes namespace (so entries() scans stay
        // namespace-local rather than leaking across windows).
        byte[] p1 = state.getIterPrefix(request(state, StateRequestType.MAP_ITER, null, ctx1));
        byte[] p2 = state.getIterPrefix(request(state, StateRequestType.MAP_ITER, null, ctx2));
        assertFalse(
                java.util.Arrays.equals(p1, p2),
                "MapStateV2: iter prefix must be namespace-distinct");
    }

    // ------------------------------------------------------------------
    // ListStateV2 (async)
    // ------------------------------------------------------------------

    @Test
    void listStateV2DistinguishesNamespaces() {
        org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2<Long, Integer, String> state =
                new org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2<>(
                        null,
                        "myList",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        RecordContext<Long> ctx1 = contextWithNamespace(42L, state, 10);
        RecordContext<Long> ctx2 = contextWithNamespace(42L, state, 20);

        byte[] k1 = state.serializeKey(request(state, StateRequestType.LIST_GET, null, ctx1));
        byte[] k2 = state.serializeKey(request(state, StateRequestType.LIST_GET, null, ctx2));

        assertFalse(
                java.util.Arrays.equals(k1, k2),
                "ListStateV2: different namespaces must produce distinct composite keys");
    }

    // ------------------------------------------------------------------
    // ReducingStateV2 (async)
    // ------------------------------------------------------------------

    @Test
    void reducingStateV2DistinguishesNamespaces() {
        ReduceFunction<Long> sum = Long::sum;
        ForStRsAsyncReducingStateV2<Long, Integer, Long> state =
                new ForStRsAsyncReducingStateV2<>(
                        null,
                        "myReducing",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        LongSerializer.INSTANCE,
                        sum);

        RecordContext<Long> ctx1 = contextWithNamespace(42L, state, 100);
        RecordContext<Long> ctx2 = contextWithNamespace(42L, state, 200);

        byte[] k1 = state.serializeKey(request(state, StateRequestType.REDUCING_GET, null, ctx1));
        byte[] k2 = state.serializeKey(request(state, StateRequestType.REDUCING_GET, null, ctx2));

        assertFalse(
                java.util.Arrays.equals(k1, k2),
                "ReducingStateV2: different namespaces must produce distinct composite keys");
    }

    // ------------------------------------------------------------------
    // AggregatingStateV2 (async)
    // ------------------------------------------------------------------

    @Test
    void aggregatingStateV2DistinguishesNamespaces() {
        AggregateFunction<Long, Long, Long> count =
                new AggregateFunction<Long, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Long v, Long acc) {
                        return acc + 1;
                    }

                    @Override
                    public Long getResult(Long acc) {
                        return acc;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                };
        ForStRsAsyncAggregatingStateV2<Long, Integer, Long, Long, Long> state =
                new ForStRsAsyncAggregatingStateV2<>(
                        null,
                        "myAgg",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        LongSerializer.INSTANCE,
                        count);

        RecordContext<Long> ctx1 = contextWithNamespace(42L, state, 1000);
        RecordContext<Long> ctx2 = contextWithNamespace(42L, state, 2000);

        byte[] k1 =
                state.serializeKey(request(state, StateRequestType.AGGREGATING_GET, null, ctx1));
        byte[] k2 =
                state.serializeKey(request(state, StateRequestType.AGGREGATING_GET, null, ctx2));

        assertFalse(
                java.util.Arrays.equals(k1, k2),
                "AggregatingStateV2: different namespaces must produce distinct composite keys");
    }
}

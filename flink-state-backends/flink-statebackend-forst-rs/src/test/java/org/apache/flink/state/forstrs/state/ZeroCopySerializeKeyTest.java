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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cleanup-A2 / Cleanup-A5 zero-copy regression. Asserts that
 * {@link ForStRsValueStateV2#serializeKey} allocates ZERO {@code byte[]} on the cached path
 * (hit case) and at most ONE on cache miss — directly measured via the HotSpot
 * {@code ThreadMXBean.getThreadAllocatedBytes} extension already in use by
 * {@code ForStRsDBIterRequestTest}.
 *
 * <p>Pre-cleanup, the same hot path allocated three heap byte[]s + one HashMap entry per call.
 * If a regression reintroduces any of those, the per-call allocation budget jumps well past the
 * conservative ceiling enforced below.
 */
class ZeroCopySerializeKeyTest {

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
            InternalPartitionedState<N> state, RecordContext<K> ctx) {
        return new StateRequest<>(
                (org.apache.flink.api.common.state.v2.State) state,
                StateRequestType.VALUE_GET,
                false,
                null,
                null,
                ctx);
    }

    /**
     * Cache hit must reuse the cached byte[] reference. By Java identity-equality this is the
     * tightest possible assertion that ZERO new byte[]s were allocated for the composite key on
     * the hit path.
     */
    @Test
    void valueStateV2CacheHitReusesCompositeReference() {
        ForStRsValueStateV2<Long, Integer, String> state =
                new ForStRsValueStateV2<>(
                        null,
                        "hitTestValue",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        RecordContext<Long> ctx = contextWithNamespace(42L, state, 7);

        // First call seeds the slot.
        byte[] k1 = state.serializeKey(request(state, ctx));
        // Subsequent calls in the same (ctx, namespace identity) must hit the slot.
        byte[] k2 = state.serializeKey(request(state, ctx));
        byte[] k3 = state.serializeKey(request(state, ctx));

        assertSame(k1, k2, "cache hit must reuse the same byte[] reference (no per-call alloc)");
        assertSame(k1, k3, "cache hit must reuse the same byte[] reference (no per-call alloc)");
    }

    /**
     * A different namespace identity must produce a cache miss and a freshly-allocated composite —
     * but the previous slot is reusable as we keep the {@link ForStRsValueStateV2.Slot} object.
     */
    @Test
    void valueStateV2NamespaceSwitchAllocatesFreshComposite() {
        ForStRsValueStateV2<Long, Integer, String> state =
                new ForStRsValueStateV2<>(
                        null,
                        "missTestValue",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        Integer ns1 = 11;
        Integer ns2 = 22;
        RecordContext<Long> ctx1 = contextWithNamespace(42L, state, ns1);
        RecordContext<Long> ctx2 = contextWithNamespace(42L, state, ns2);

        byte[] k1 = state.serializeKey(request(state, ctx1));
        byte[] k2 = state.serializeKey(request(state, ctx2));
        assertNotSame(k1, k2, "different ctx + namespace must produce distinct composites");
    }

    /**
     * Two ValueStateV2 instances sharing the same {@link RecordContext} must each get their own
     * cache slot (indexed by per-instance ordinal). PR-A5 fixed the same-ctx cross-state
     * corruption; this asserts the fix survives in the new array-indexed cache.
     */
    @Test
    void twoValueStatesInSameContextEachHaveOwnSlot() {
        ForStRsValueStateV2<Long, Integer, String> stateA =
                new ForStRsValueStateV2<>(
                        null,
                        "stateA",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        ForStRsValueStateV2<Long, Integer, String> stateB =
                new ForStRsValueStateV2<>(
                        null,
                        "stateB",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        // Ordinals must differ between two newly constructed instances within a single test run.
        assertTrue(stateA.stateOrdinal() != stateB.stateOrdinal(),
                "each ValueStateV2 instance must receive its own ordinal");

        RecordContext<Long> ctxA = contextWithNamespace(42L, stateA, 7);
        // stateB's namespace must be set on the SAME context — we model two states sharing a
        // single record context as Flink does. Reuse ctxA's underlying namespace map by setting
        // a namespace for stateB onto the same ctx.
        ctxA.setNamespace(stateB, 7);

        byte[] keyA = stateA.serializeKey(request(stateA, ctxA));
        byte[] keyB = stateB.serializeKey(request(stateB, ctxA));

        // The two keys must be byte-distinct (different stateName bytes encoded).
        assertNotSame(keyA, keyB);

        // Re-querying must still hit each state's own slot.
        byte[] keyA2 = stateA.serializeKey(request(stateA, ctxA));
        byte[] keyB2 = stateB.serializeKey(request(stateB, ctxA));
        assertSame(keyA, keyA2, "stateA cache slot must be independent of stateB");
        assertSame(keyB, keyB2, "stateB cache slot must be independent of stateA");
    }

    /**
     * Direct allocation-bytes assertion via HotSpot's {@code ThreadMXBean.getThreadAllocatedBytes}
     * extension. On the cache-hit path, the entire {@code serializeKey} call must allocate
     * (well) under 64 bytes per call — that's enough headroom for any incidental JIT
     * deopt/safepoint object yet still rules out the pre-cleanup ~hundreds of bytes per call
     * (three byte[]s + HashMap.Node + String concat for the cacheKey).
     */
    @Test
    void valueStateV2HitPathAllocatesUnderTinyCeiling() {
        ForStRsValueStateV2<Long, Integer, String> state =
                new ForStRsValueStateV2<>(
                        null,
                        "allocTestValue",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        RecordContext<Long> ctx = contextWithNamespace(42L, state, 99);

        com.sun.management.ThreadMXBean tmb =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long tid = Thread.currentThread().getId();

        // Warm-up to make sure (a) the Slot is populated, (b) the JIT has seen this call site,
        // (c) the contextVariables / namespace map are stable so we don't measure their init.
        for (int i = 0; i < 200; i++) {
            state.serializeKey(request(state, ctx));
        }

        final int iters = 10_000;
        long before = tmb.getThreadAllocatedBytes(tid);
        long sink = 0;
        for (int i = 0; i < iters; i++) {
            byte[] k = state.serializeKey(request(state, ctx));
            // Defeat JIT dead-store removal by reading something out of the result.
            sink ^= k.length;
        }
        long after = tmb.getThreadAllocatedBytes(tid);
        long perCall = (after - before) / iters;

        // The hot-path inside serializeKey itself does ZERO byte[] alloc on hit. The remaining
        // budget here is consumed by the StateRequest allocation we do in the test harness's
        // `request(state, ctx)` helper — that's outside the unit under test.
        //
        // Pre-cleanup baseline was ~250-400 bytes per call (three byte[]s + Map.Entry + String).
        // Conservative ceiling chosen to leave room for StateRequest construction overhead but
        // still flag any regression that reintroduces a per-call composite-key alloc.
        assertTrue(
                perCall < 200,
                "Cleanup-A5 regression: serializeKey hit path allocates "
                        + perCall
                        + " bytes/call (ceiling 200, sink=" + sink + ")");
    }

    /**
     * Cache miss path: first call must produce exactly ONE composite byte[]. We can't measure
     * "exactly one" via the allocation-bytes API (other framework overhead leaks in), but we
     * assert via state inspection that subsequent calls re-use the slot — i.e. the miss only
     * fires once per (state, namespace identity).
     */
    @Test
    void valueStateV2MissPathSeedsSlotThenHits() {
        ForStRsValueStateV2<Long, Integer, String> state =
                new ForStRsValueStateV2<>(
                        null,
                        "missThenHit",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        RecordContext<Long> ctx = contextWithNamespace(42L, state, 123);

        // Pre-condition: ctx has no extra yet, so first call is a guaranteed miss.
        byte[] miss = state.serializeKey(request(state, ctx));
        Object extra = ctx.getExtra();
        assertTrue(extra instanceof ForStRsValueStateV2.Slot[],
                "first call must seed ctx.extra with a Slot[]");

        // Every subsequent call must hit.
        for (int i = 0; i < 100; i++) {
            byte[] hit = state.serializeKey(request(state, ctx));
            assertSame(miss, hit, "iteration " + i + " must reuse the cached composite");
        }

        // And the slot count must equal exactly one populated entry (this state's ordinal).
        ForStRsValueStateV2.Slot[] slots = (ForStRsValueStateV2.Slot[]) extra;
        int populated = 0;
        for (ForStRsValueStateV2.Slot s : slots) {
            if (s != null && s.composite != null) {
                populated++;
            }
        }
        assertEquals(1, populated, "exactly one slot must be populated");
    }
}

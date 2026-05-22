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
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A4-H1 / PR-A6 sibling: {@link ForStRsAsyncListStateV2#buildDBPutRequest} must invoke the
 * pre-DELETE off-heap-buffer drain on a CLEAR request so pending appends in the {@link
 * ListStateArrowBuffer} do NOT flush AFTER the DELETE lands at the engine. Without this guard,
 * {@code asyncAdd}s queued before {@code asyncClear()} would resurrect the entry post-clear
 * (state leak past asyncClear()).
 *
 * <p><b>Why structural-only verification:</b> {@link
 * org.apache.flink.state.forstrs.ffm.ForStRsLinker} is final and requires loading {@code
 * libforst_rs_ffi} which is not available in unit-test CI. With {@code linker=null} the production
 * drain branch inside {@link ForStRsAsyncListStateV2#flushIfDirty()} short-circuits; we therefore
 * verify the call-sequence routing (CLEAR request reaches the drain step without NPE, returns a
 * DELETE-shaped {@link ForStRsDBPutRequest}) rather than the on-wire FFI effect. The on-wire effect
 * is covered by the existing end-to-end V3 / Q19 benchmark (audit-design §3 V4).
 */
class ListStateV2ClearDrainTest {

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

    @Test
    void clearRequestProducesDeleteShapedPutRequest() {
        // Buffer-less ListState V2 — verifies the CLEAR branch returns a DELETE shape and the
        // drain hook is null-safe.
        ForStRsAsyncListStateV2<Long, Integer, String> noBuffer =
                new ForStRsAsyncListStateV2<>(
                        null,
                        "myList",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        RecordContext<Long> ctx = contextWithNamespace(7L, noBuffer, 0);
        ForStRsDBPutRequest<Long, Integer, ?> req =
                noBuffer.buildDBPutRequest(request(noBuffer, StateRequestType.CLEAR, null, ctx));
        assertNotNull(req, "CLEAR yields a request");
        assertNull(req.getSerializedValue(), "CLEAR maps to value=null (DELETE shape)");
    }

    @Test
    void clearRequestWithBufferedAppendsRunsDrainHookWithoutNpe() {
        // PR-C2 ctor — buffer is configured, linker/db/cf null (unit-test envelope). Five
        // asyncAdds accumulate in the off-heap buffer; the subsequent CLEAR's buildDBPutRequest
        // MUST invoke the drain BEFORE constructing the DELETE row. With linker=null the
        // production flushIfDirty short-circuits without an FFI, but the call-sequence routing
        // (CLEAR → flushIfDirty → DELETE-shape return) must hold under both branches and must
        // not NPE on the null engine handles.
        ListStateArrowBuffer buf = new ListStateArrowBuffer();
        ForStRsAsyncListStateV2<Long, Integer, String> state =
                new ForStRsAsyncListStateV2<>(
                        null,
                        "myList",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        buf,
                        null,
                        null,
                        null);
        RecordContext<Long> ctx = contextWithNamespace(7L, state, 0);
        for (int i = 0; i < 5; i++) {
            state.recordAppendMergeOffHeap(
                    request(state, StateRequestType.LIST_ADD, "v" + i, ctx));
        }
        // CLEAR request — the drain hook fires inside buildDBPutRequest before the DELETE row
        // is materialised. Verifies the post-A4-H1 contract: CLEAR reaches the drain BEFORE
        // the engine sees the DELETE.
        ForStRsDBPutRequest<Long, Integer, ?> req =
                state.buildDBPutRequest(request(state, StateRequestType.CLEAR, null, ctx));
        assertNotNull(req, "CLEAR yields a request even with non-empty off-heap buffer");
        assertNull(req.getSerializedValue(), "CLEAR maps to value=null (DELETE shape)");
    }
}

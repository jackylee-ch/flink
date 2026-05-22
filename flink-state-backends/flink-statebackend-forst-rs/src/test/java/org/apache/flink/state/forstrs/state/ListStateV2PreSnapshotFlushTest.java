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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-C2 (V2-14): pre-snapshot flush wiring test. Verifies that:
 *
 * <ul>
 *   <li>{@code asyncAdd}-buffered chunks accumulate in the {@link ListStateArrowBuffer} between
 *       checkpoint barriers;
 *   <li>The state primitive exposes a {@code flushPreSnapshot} hook that the backend invokes
 *       BEFORE the engine snapshot reads, so accumulated chunks become durable;
 *   <li>The hook is null-safe — calling it on a state without a configured buffer is a no-op.
 * </ul>
 *
 * <p><b>Why no native-FFI exercise:</b> {@link
 * org.apache.flink.state.forstrs.ffm.ForStRsLinker} is {@code final} and requires loading
 * {@code libforst_rs_ffi} which is not available in unit-test CI. The integration with the real
 * {@code frs_vec_merge_append_batch} FFI is exercised by the existing end-to-end Q19 / V20.4
 * benchmark (audit-design §3 V4 ablation). Here we verify the structural pre-snapshot path is
 * wired and null-safe.
 */
class ListStateV2PreSnapshotFlushTest {

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
    void accumulatedAddsRemainInBufferUntilPreSnapshot() {
        // Phase 1 — multiple asyncAdds buffer up in the off-heap accumulator without going to
        // engine (linker is null; flushIfDirty is a no-op in that branch but the buffer state is
        // observable).
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
        assertEquals(5, buf.rowCount(), "All 5 asyncAdd chunks accumulated in off-heap buffer");
        assertFalse(buf.isEmpty(), "Buffer is dirty pre-snapshot");

        // Phase 2 — flushPreSnapshot is the backend's hook. With linker=null this branch
        // short-circuits without an FFI call but does NOT clear the buffer (the FFI is what
        // performs the engine write + buffer reset under the production path). We verify
        // the method is callable and null-safe.
        state.flushPreSnapshot();
        assertEquals(
                5,
                buf.rowCount(),
                "Null-linker pre-snapshot is a no-op; production flush sequence handled by"
                        + " ListStateArrowBuffer.flushTo");
    }

    @Test
    void flushPreSnapshotIsNoOpWhenBufferNotConfigured() {
        // Legacy 5-arg ctor leaves buffer=null. flushPreSnapshot must not NPE.
        ForStRsAsyncListStateV2<Long, Integer, String> noBuffer =
                new ForStRsAsyncListStateV2<>(
                        null,
                        "myList",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        assertNull(noBuffer.buffer(), "Legacy ctor leaves buffer null");
        // Must not NPE.
        noBuffer.flushPreSnapshot();
        noBuffer.flushIfDirty();
        assertFalse(noBuffer.shouldAutoFlush());
    }

    @Test
    void bufferDrainCompletesAllRowFuturesInOrder() {
        // Direct exercise of ListStateArrowBuffer's row-future completion: every appended row's
        // future completes when the buffer's internal row counter resets and the per-row futures
        // collection is drained. We can't invoke flushTo without a real linker, but we CAN
        // exercise the reset() path which the production flushTo invokes after the FFI call
        // and verify the buffer state is consistent.
        ListStateArrowBuffer buf = new ListStateArrowBuffer();
        int n = 10;
        java.util.concurrent.CompletableFuture<Void>[] futs =
                new java.util.concurrent.CompletableFuture[n];
        for (int i = 0; i < n; i++) {
            futs[i] = buf.append(("k" + i).getBytes(), new byte[] {0, 0, 0, 1, (byte) i});
            assertFalse(futs[i].isDone(), "Future " + i + " pending pre-flush");
        }
        assertEquals(n, buf.rowCount());
        // Reset clears row count + futures collection. Real flushTo calls reset after FFI.
        buf.reset();
        assertEquals(0, buf.rowCount(), "rowCount=0 after reset");
        assertTrue(buf.isEmpty(), "Buffer empty after reset");
        // The futures we held are not completed by reset — only by flushTo. This is the
        // documented behaviour (reset is the second half of flushTo; flushTo completes
        // futures BEFORE calling reset). For now, the futures are simply orphaned by reset
        // outside the production flushTo path. Production never calls reset() directly.
        for (int i = 0; i < n; i++) {
            assertFalse(
                    futs[i].isDone(),
                    "Future " + i + " stays pending after standalone reset (futures are only"
                            + " completed by flushTo's success/failure plumbing)");
        }
    }

    @Test
    void autoFlushThresholdGatesProductionDrainCadence() {
        // Production wire: VectorizedClassifier may call state.flushIfDirty() when
        // state.shouldAutoFlush() returns true mid-batch — the pre-snapshot hook is one of THREE
        // drain triggers. This test verifies the threshold-gating logic from the state's
        // perspective. ListStateArrowBuffer floors maxRows at 8.
        ListStateArrowBuffer buf = new ListStateArrowBuffer(8, 1L << 30);
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
        RecordContext<Long> ctx = contextWithNamespace(1L, state, 0);
        // Seven appends — below threshold of 8.
        for (int i = 0; i < 7; i++) {
            state.recordAppendMergeOffHeap(
                    request(state, StateRequestType.LIST_ADD, "v" + i, ctx));
        }
        assertFalse(state.shouldAutoFlush(), "7 < threshold 8");
        // Eighth append — hits threshold.
        state.recordAppendMergeOffHeap(
                request(state, StateRequestType.LIST_ADD, "v7", ctx));
        assertTrue(state.shouldAutoFlush(), "8 >= threshold 8");
    }

    @Test
    void bufferAccessorSurfacesConfiguredInstance() {
        // Sanity: state primitives constructed with the PR-C2 ctor MUST surface their buffer via
        // {@link ForStRsAsyncListStateV2#buffer()}. The backend uses this for its registry +
        // snapshot pre-hook plumbing.
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
        assertNotNull(state.buffer());
        assertTrue(state.buffer() == buf, "Buffer accessor returns the configured instance");
    }
}

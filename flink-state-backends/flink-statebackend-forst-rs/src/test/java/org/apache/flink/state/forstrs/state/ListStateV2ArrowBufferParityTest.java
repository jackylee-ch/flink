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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * PR-C2 (V2-14) parity test — the off-heap path's per-row {@code (composite_key, chunk_bytes)}
 * MUST be byte-identical to what the heap path would have produced via the legacy {@code
 * VectorizedClassifier.recordAppendMerge → AppendMergeBatchBuffer} chain.
 *
 * <p>Strategy: instead of standing up the full classifier + executor + FFI (which requires the
 * native lib loaded), we directly drive {@link
 * ForStRsAsyncListStateV2#recordAppendMergeOffHeap(StateRequest)} for one ListState that has the
 * off-heap buffer and a sibling ListState that doesn't (so the parity check goes through the
 * existing {@code serializeKey} + {@code serializeValue} which is what the heap path uses).
 *
 * <p>If these are byte-equal, the engine FFI sees byte-identical operands either way — the only
 * difference is allocation (heap vs off-heap), which is the PR-C2 win.
 */
class ListStateV2ArrowBufferParityTest {

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
    void offHeapRowMatchesHeapPathBytes_singleAdd() {
        // Off-heap state (with buffer).
        ListStateArrowBuffer buf = new ListStateArrowBuffer();
        ForStRsAsyncListStateV2<Long, Integer, String> offHeap =
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

        // Heap-path state (no buffer — same shape recordAppendMerge uses).
        ForStRsAsyncListStateV2<Long, Integer, String> heapPath =
                new ForStRsAsyncListStateV2<>(
                        null,
                        "myList",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        RecordContext<Long> ctxOff = contextWithNamespace(42L, offHeap, 7);
        RecordContext<Long> ctxHeap = contextWithNamespace(42L, heapPath, 7);

        StateRequest<Long, Integer, String, Void> reqOff =
                request(offHeap, StateRequestType.LIST_ADD, "elem-1", ctxOff);
        StateRequest<Long, Integer, String, Void> reqHeap =
                request(heapPath, StateRequestType.LIST_ADD, "elem-1", ctxHeap);

        CompletableFuture<Void> fut = offHeap.recordAppendMergeOffHeap(reqOff);
        assertNotNull(fut, "Off-heap path must return a future");
        assertEquals(1, buf.rowCount(), "One row buffered");

        // Heap-path equivalent — what the legacy recordAppendMerge would push into
        // AppendMergeBatchBuffer (key from serializeKey, value from serializeValue).
        byte[] heapKey = heapPath.serializeKey(reqHeap);
        byte[] heapChunk = heapPath.serializeValue("elem-1"); // single-elem -> [count=1][elem]

        // Parity: the off-heap row's accumulated bytes equal the heap-path's bytes.
        assertArrayEquals(heapKey, buf.copyKeyAt(0), "Composite key bytes parity");
        assertArrayEquals(heapChunk, buf.copyChunkAt(0), "Chunk bytes parity (Format B)");
    }

    @Test
    void offHeapRowMatchesHeapPathBytes_addAllMultiElement() {
        // LIST_ADD_ALL path — payload is List<V>, chunk format is [count=N][elem0]...[elem(N-1)].
        ListStateArrowBuffer buf = new ListStateArrowBuffer();
        ForStRsAsyncListStateV2<Long, Integer, String> offHeap =
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
        ForStRsAsyncListStateV2<Long, Integer, String> heapPath =
                new ForStRsAsyncListStateV2<>(
                        null,
                        "myList",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        List<String> values = List.of("a", "b", "c", "d");
        RecordContext<Long> ctxOff = contextWithNamespace(99L, offHeap, 3);
        RecordContext<Long> ctxHeap = contextWithNamespace(99L, heapPath, 3);

        StateRequest<Long, Integer, List<String>, Void> reqOff =
                request(offHeap, StateRequestType.LIST_ADD_ALL, values, ctxOff);
        StateRequest<Long, Integer, List<String>, Void> reqHeap =
                request(heapPath, StateRequestType.LIST_ADD_ALL, values, ctxHeap);

        offHeap.recordAppendMergeOffHeap(reqOff);
        assertEquals(1, buf.rowCount(), "ADD_ALL is one row containing the multi-element chunk");

        byte[] heapKey = heapPath.serializeKey(reqHeap);
        byte[] heapChunk = heapPath.serializeValue(values);

        assertArrayEquals(heapKey, buf.copyKeyAt(0));
        assertArrayEquals(heapChunk, buf.copyChunkAt(0));
    }

    @Test
    void randomSequenceMatchesHeapBytesAndPreservesOrder() {
        // Mixed LIST_ADD / LIST_ADD_ALL stream with shared keys — verify per-row parity AND that
        // the buffer preserves submit order (Format B / V20 §7.4: engine merge operator
        // concatenates operands in submit order).
        ListStateArrowBuffer buf = new ListStateArrowBuffer();
        ForStRsAsyncListStateV2<Long, Integer, String> offHeap =
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
        ForStRsAsyncListStateV2<Long, Integer, String> heapPath =
                new ForStRsAsyncListStateV2<>(
                        null,
                        "myList",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);

        Random rng = new Random(0xC2L);
        int n = 40;
        // Track expected bytes per row for the parity comparison.
        List<byte[]> expectedKeys = new ArrayList<>();
        List<byte[]> expectedChunks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            long key = rng.nextInt(5); // small key space → repeats hit same composite key
            int ns = rng.nextInt(3);
            if (rng.nextBoolean()) {
                String v = "v" + i;
                RecordContext<Long> ctxOff = contextWithNamespace(key, offHeap, ns);
                RecordContext<Long> ctxHeap = contextWithNamespace(key, heapPath, ns);
                StateRequest<Long, Integer, String, Void> reqOff =
                        request(offHeap, StateRequestType.LIST_ADD, v, ctxOff);
                StateRequest<Long, Integer, String, Void> reqHeap =
                        request(heapPath, StateRequestType.LIST_ADD, v, ctxHeap);
                offHeap.recordAppendMergeOffHeap(reqOff);
                expectedKeys.add(heapPath.serializeKey(reqHeap));
                expectedChunks.add(heapPath.serializeValue(v));
            } else {
                int len = 1 + rng.nextInt(4);
                List<String> vs = new ArrayList<>();
                for (int j = 0; j < len; j++) {
                    vs.add("vv" + i + "_" + j);
                }
                RecordContext<Long> ctxOff = contextWithNamespace(key, offHeap, ns);
                RecordContext<Long> ctxHeap = contextWithNamespace(key, heapPath, ns);
                StateRequest<Long, Integer, List<String>, Void> reqOff =
                        request(offHeap, StateRequestType.LIST_ADD_ALL, vs, ctxOff);
                StateRequest<Long, Integer, List<String>, Void> reqHeap =
                        request(heapPath, StateRequestType.LIST_ADD_ALL, vs, ctxHeap);
                offHeap.recordAppendMergeOffHeap(reqOff);
                expectedKeys.add(heapPath.serializeKey(reqHeap));
                expectedChunks.add(heapPath.serializeValue(vs));
            }
        }
        assertEquals(n, buf.rowCount(), "Buffer row count matches submit count");
        for (int i = 0; i < n; i++) {
            assertArrayEquals(
                    expectedKeys.get(i),
                    buf.copyKeyAt(i),
                    "Row " + i + " key bytes parity (order-preserved)");
            assertArrayEquals(
                    expectedChunks.get(i),
                    buf.copyChunkAt(i),
                    "Row " + i + " chunk bytes parity (order-preserved)");
        }
    }

    @Test
    void chunkBytesDecodeAsFormatBChunks() {
        // Cross-check: the chunk bytes the off-heap path stores MUST round-trip through the same
        // Format-B decoder that {@code deserializeValue} uses. A LIST_ADD chunk decodes to a
        // single-element list; a LIST_ADD_ALL chunk decodes to the full list.
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
        RecordContext<Long> ctx = contextWithNamespace(1L, state, 0);

        state.recordAppendMergeOffHeap(
                request(state, StateRequestType.LIST_ADD, "x", ctx));
        state.recordAppendMergeOffHeap(
                request(state, StateRequestType.LIST_ADD_ALL, List.of("y", "z"), ctx));

        // Decode row-0 chunk as a Format-B segment: [count=1]["x"].
        byte[] r0 = buf.copyChunkAt(0);
        DataInputDeserializer in0 = new DataInputDeserializer(r0);
        try {
            assertEquals(1, in0.readInt(), "LIST_ADD chunk count=1");
            assertEquals("x", StringSerializer.INSTANCE.deserialize(in0));
            assertEquals(0, in0.available(), "Chunk fully consumed");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Decode row-1 chunk: [count=2]["y"]["z"].
        byte[] r1 = buf.copyChunkAt(1);
        DataInputDeserializer in1 = new DataInputDeserializer(r1);
        try {
            assertEquals(2, in1.readInt(), "LIST_ADD_ALL chunk count=2");
            assertEquals("y", StringSerializer.INSTANCE.deserialize(in1));
            assertEquals("z", StringSerializer.INSTANCE.deserialize(in1));
            assertEquals(0, in1.available());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void nullPayloadReturnsNullForFallback() {
        // recordAppendMergeOffHeap returns null on null payload — caller (classifier) must fall
        // back to recordDelete. The classifier's outer switch already routes null to delete; this
        // tests the defensive null-return on the off-heap path.
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
        RecordContext<Long> ctx = contextWithNamespace(1L, state, 0);
        CompletableFuture<Void> fut =
                state.recordAppendMergeOffHeap(
                        request(state, StateRequestType.LIST_ADD, null, ctx));
        assertNull(fut, "Null payload returns null for fallback");
        assertEquals(0, buf.rowCount(), "No row added on null payload");
    }

    @Test
    void noBufferConfiguredReturnsNullForFallback() {
        // The legacy 5-arg ctor leaves buffer=null. recordAppendMergeOffHeap must return null so
        // the classifier knows to take the heap path.
        ForStRsAsyncListStateV2<Long, Integer, String> noBuffer =
                new ForStRsAsyncListStateV2<>(
                        null,
                        "myList",
                        LongSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        RecordContext<Long> ctx = contextWithNamespace(1L, noBuffer, 0);
        CompletableFuture<Void> fut =
                noBuffer.recordAppendMergeOffHeap(
                        request(noBuffer, StateRequestType.LIST_ADD, "x", ctx));
        assertNull(fut, "No buffer configured returns null for fallback");
        assertNull(noBuffer.buffer(), "Legacy ctor leaves buffer null");
    }
}

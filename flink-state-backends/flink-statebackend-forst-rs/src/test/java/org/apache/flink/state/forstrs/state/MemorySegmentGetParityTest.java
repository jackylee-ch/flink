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
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.v2.adaptor.CompleteStateIterator;
import org.apache.flink.state.forstrs.ForStRsInnerTable;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-B1 (V2-6, C-H1, C-H6) parity test: for every V2 state class on the GET hot path, the new
 * {@code deserializeValue(MemorySegment, long, int)} overload must return a value byte-equal /
 * structurally-equal to what {@code deserializeValue(byte[])} produced from the same payload
 * bytes. Together with the {@code VectorizedExecutor.executeGets} call-site change, this proves
 * the zero-copy GET decode path is correctness-equivalent to the legacy byte[] decode.
 *
 * <p>The test fabricates a {@code byte[]} payload by running the same TypeSerializer the state
 * class would have used on writes, allocates a native {@link MemorySegment} populated with those
 * bytes, then cross-verifies the two decode entry points produce equal results.
 */
class MemorySegmentGetParityTest {

    /** Allocate a confined Arena segment populated with {@code bytes} at offset {@code off}. */
    private static MemorySegment segOf(Arena arena, byte[] bytes, int off) {
        MemorySegment seg = arena.allocate(off + bytes.length + 16);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, off, bytes.length);
        return seg;
    }

    // ------------------------------------------------------------------
    // ValueStateV2 — Long values
    // ------------------------------------------------------------------

    @Test
    void valueStateV2_segmentDecode_matchesByteArrayDecode() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsInnerTable<Long, Integer, Long> state =
                    new ForStRsValueStateV2<>(
                            null,
                            "v",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            LongSerializer.INSTANCE);

            Random rnd = new Random(1L);
            for (int i = 0; i < 32; i++) {
                Long original = rnd.nextLong();
                byte[] bytes = state.serializeValue(original);
                assertNotNull(bytes);
                Object viaArr = state.deserializeValue(bytes);
                MemorySegment seg = segOf(arena, bytes, 7); // non-zero offset
                Object viaSeg = state.deserializeValue(seg, 7L, bytes.length);
                assertEquals(viaArr, viaSeg, "row " + i + " mismatch");
                assertEquals(original, viaSeg, "row " + i + " roundtrip");
            }

            // null-len behavior: both paths must produce null.
            assertNull(state.deserializeValue(new byte[0]));
            assertNull(state.deserializeValue(arena.allocate(0), 0L, 0));
        }
    }

    // ------------------------------------------------------------------
    // MapStateV2 — String userValue
    // ------------------------------------------------------------------

    @Test
    void mapStateV2_segmentDecode_matchesByteArrayDecode() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsInnerTable<Long, Integer, String> state =
                    new ForStRsMapStateV2<>(
                            null,
                            "m",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            StringSerializer.INSTANCE,
                            StringSerializer.INSTANCE);

            String[] samples = {"", "hello", "流处理", "x".repeat(257), "with spaces and 中文"};
            for (int i = 0; i < samples.length; i++) {
                byte[] bytes = state.serializeValue(samples[i]);
                assertNotNull(bytes);
                Object viaArr = state.deserializeValue(bytes);
                MemorySegment seg = segOf(arena, bytes, 3);
                Object viaSeg = state.deserializeValue(seg, 3L, bytes.length);
                assertEquals(viaArr, viaSeg, "row " + i + " mismatch");
                assertEquals(samples[i], viaSeg);
            }
            assertNull(state.deserializeValue(arena.allocate(0), 0L, 0));
        }
    }

    // ------------------------------------------------------------------
    // ListStateV2 — multi-element list
    // ------------------------------------------------------------------

    @Test
    void listStateV2_segmentDecode_matchesByteArrayDecode() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsAsyncListStateV2<Long, Integer, String> state =
                    new ForStRsAsyncListStateV2<>(
                            null,
                            "l",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            StringSerializer.INSTANCE);

            // Build a multi-element list payload manually: [count=3]["a"]["bb"]["流"]
            List<String> expected = List.of("a", "bb", "流");
            org.apache.flink.core.memory.DataOutputSerializer out =
                    new org.apache.flink.core.memory.DataOutputSerializer(32);
            out.writeInt(expected.size());
            for (String s : expected) {
                StringSerializer.INSTANCE.serialize(s, out);
            }
            byte[] bytes = out.getCopyOfBuffer();

            Object viaArr = state.deserializeValue(bytes);
            MemorySegment seg = segOf(arena, bytes, 5);
            Object viaSeg = state.deserializeValue(seg, 5L, bytes.length);
            assertNotNull(viaArr);
            assertNotNull(viaSeg);
            assertTrue(viaArr instanceof CompleteStateIterator);
            assertTrue(viaSeg instanceof CompleteStateIterator);

            List<String> arrList = drain((StateIterator<String>) viaArr);
            List<String> segList = drain((StateIterator<String>) viaSeg);
            assertEquals(expected, arrList);
            assertEquals(expected, segList);

            // empty payload: both yield an empty iterator.
            Object emptyArr = state.deserializeValue(new byte[0]);
            Object emptySeg = state.deserializeValue(arena.allocate(0), 0L, 0);
            assertTrue(drain((StateIterator<String>) emptyArr).isEmpty());
            assertTrue(drain((StateIterator<String>) emptySeg).isEmpty());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> drain(StateIterator<T> it) {
        // CompleteStateIterator.getCurrentCache() returns the backing Iterable<T>.
        Iterable<T> iter = ((CompleteStateIterator<T>) it).getCurrentCache();
        List<T> out = new ArrayList<>();
        iter.forEach(out::add);
        return out;
    }

    // ------------------------------------------------------------------
    // ReducingStateV2 — Long accumulator
    // ------------------------------------------------------------------

    @Test
    void reducingStateV2_segmentDecode_matchesByteArrayDecode() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            ReduceFunction<Long> sum = Long::sum;
            ForStRsInnerTable<Long, Integer, Long> state =
                    new ForStRsAsyncReducingStateV2<>(
                            null,
                            "r",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            LongSerializer.INSTANCE,
                            sum);

            Random rnd = new Random(2L);
            for (int i = 0; i < 16; i++) {
                Long v = rnd.nextLong();
                byte[] bytes = state.serializeValue(v);
                Object viaArr = state.deserializeValue(bytes);
                MemorySegment seg = segOf(arena, bytes, 0);
                Object viaSeg = state.deserializeValue(seg, 0L, bytes.length);
                assertEquals(viaArr, viaSeg);
                assertEquals(v, viaSeg);
            }
            assertNull(state.deserializeValue(arena.allocate(0), 0L, 0));
        }
    }

    // ------------------------------------------------------------------
    // AggregatingStateV2 — Long accumulator
    // ------------------------------------------------------------------

    @Test
    void aggregatingStateV2_segmentDecode_matchesByteArrayDecode() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            AggregateFunction<Long, Long, Long> agg =
                    new AggregateFunction<Long, Long, Long>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }

                        @Override
                        public Long add(Long v, Long acc) {
                            return acc + v;
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
            ForStRsInnerTable<Long, Integer, Long> state =
                    new ForStRsAsyncAggregatingStateV2<>(
                            null,
                            "a",
                            LongSerializer.INSTANCE,
                            IntSerializer.INSTANCE,
                            LongSerializer.INSTANCE,
                            agg);

            Random rnd = new Random(3L);
            for (int i = 0; i < 16; i++) {
                Long v = rnd.nextLong();
                byte[] bytes = state.serializeValue(v);
                Object viaArr = state.deserializeValue(bytes);
                MemorySegment seg = segOf(arena, bytes, 11);
                Object viaSeg = state.deserializeValue(seg, 11L, bytes.length);
                assertEquals(viaArr, viaSeg);
                assertEquals(v, viaSeg);
            }
            assertNull(state.deserializeValue(arena.allocate(0), 0L, 0));
        }
    }

    // ------------------------------------------------------------------
    // Default-fallback contract: ForStRsInnerTable's default
    // deserializeValue(MemorySegment, long, int) must delegate to the byte[]
    // overload. Verified by a minimal anonymous impl that only implements byte[]
    // — the segment overload should still return the correct decoded value.
    // ------------------------------------------------------------------

    @Test
    void defaultFallback_delegatesToByteArrayDecode() {
        ForStRsInnerTable<Object, Object, Object> stub =
                new ForStRsInnerTable<>() {
                    @Override
                    public byte[] serializeKey(
                            org.apache.flink.runtime.asyncprocessing.StateRequest<
                                            Object, Object, ?, ?>
                                    r) {
                        return new byte[0];
                    }

                    @Override
                    public byte[] serializeValue(Object value) {
                        return (byte[]) value;
                    }

                    @Override
                    public Object deserializeValue(byte[] raw) {
                        return "DECODED:" + raw.length;
                    }

                    @Override
                    public org.apache.flink.state.forstrs.ForStRsDBGetRequest<Object, Object, ?>
                            buildDBGetRequest(
                                    org.apache.flink.runtime.asyncprocessing.StateRequest<
                                                    Object, Object, ?, ?>
                                            r) {
                        return null;
                    }

                    @Override
                    public org.apache.flink.state.forstrs.ForStRsDBPutRequest<Object, Object, ?>
                            buildDBPutRequest(
                                    org.apache.flink.runtime.asyncprocessing.StateRequest<
                                                    Object, Object, ?, ?>
                                            r) {
                        return null;
                    }
                };

        try (Arena arena = Arena.ofConfined()) {
            byte[] bytes = {1, 2, 3, 4, 5};
            MemorySegment seg = segOf(arena, bytes, 4);
            Object result = stub.deserializeValue(seg, 4L, bytes.length);
            assertEquals("DECODED:" + bytes.length, result);
        }
    }
}

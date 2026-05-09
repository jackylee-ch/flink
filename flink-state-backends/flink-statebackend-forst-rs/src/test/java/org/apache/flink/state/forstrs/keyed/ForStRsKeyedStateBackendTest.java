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

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.state.forstrs.ForStRsStateBackend;
import org.apache.flink.state.forstrs.state.ForStRsAggregatingState;
import org.apache.flink.state.forstrs.state.ForStRsListState;
import org.apache.flink.state.forstrs.state.ForStRsMapState;
import org.apache.flink.state.forstrs.state.ForStRsReducingState;
import org.apache.flink.state.forstrs.state.ForStRsValueState;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link ForStRsKeyedStateBackend} — exercises the "current key + state-id →
 * state object" pattern across the three committed state types (Value, List, Map). Reducing /
 * Aggregating tests will be added once the sibling agent's classes land.
 */
class ForStRsKeyedStateBackendTest {

    private static <T> List<T> drain(Iterable<T> iter) {
        List<T> out = new ArrayList<>();
        iter.forEach(out::add);
        return out;
    }

    @Test
    void testCreateValueState() throws Exception {
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {

            backend.setCurrentKey("a");
            ForStRsValueState<Integer> stateA =
                    backend.getValueState("counter", IntSerializer.INSTANCE);
            stateA.update(1);
            assertEquals(Integer.valueOf(1), stateA.value());

            backend.setCurrentKey("b");
            ForStRsValueState<Integer> stateB =
                    backend.getValueState("counter", IntSerializer.INSTANCE);
            assertNull(stateB.value(), "key 'b' must not see key 'a' state");
        }
    }

    @Test
    void testCreateListState() throws Exception {
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {

            backend.setCurrentKey("a");
            ForStRsListState<Integer> state =
                    backend.getListState("buffer", IntSerializer.INSTANCE);
            state.add(1);
            state.add(2);
            state.add(3);

            assertEquals(List.of(1, 2, 3), drain(state.get()));
        }
    }

    @Test
    void testCreateMapState() throws Exception {
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {

            backend.setCurrentKey("a");
            ForStRsMapState<String, String> state =
                    backend.getMapState("attrs", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
            state.put("k", "v");

            assertEquals("v", state.get("k"));
            assertTrue(state.contains("k"));
            List<Map.Entry<String, String>> entries = drain(state.entries());
            assertEquals(1, entries.size());
            assertEquals("k", entries.get(0).getKey());
            assertEquals("v", entries.get(0).getValue());
        }
    }

    @Test
    void testKeyIsolationValueState() throws Exception {
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {

            backend.setCurrentKey("a");
            backend.getValueState("v", IntSerializer.INSTANCE).update(100);

            backend.setCurrentKey("b");
            backend.getValueState("v", IntSerializer.INSTANCE).update(200);

            backend.setCurrentKey("a");
            assertEquals(
                    Integer.valueOf(100),
                    backend.getValueState("v", IntSerializer.INSTANCE).value(),
                    "switching back to 'a' must restore its prior value");

            backend.setCurrentKey("b");
            assertEquals(
                    Integer.valueOf(200),
                    backend.getValueState("v", IntSerializer.INSTANCE).value());

            backend.setCurrentKey("c");
            assertNull(
                    backend.getValueState("v", IntSerializer.INSTANCE).value(),
                    "previously-unwritten key 'c' must read null");
        }
    }

    @Test
    void testKeyIsolationListState() throws Exception {
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {

            backend.setCurrentKey("a");
            ForStRsListState<String> aState =
                    backend.getListState("l", StringSerializer.INSTANCE);
            aState.add("a-1");
            aState.add("a-2");

            backend.setCurrentKey("b");
            ForStRsListState<String> bState =
                    backend.getListState("l", StringSerializer.INSTANCE);
            assertTrue(drain(bState.get()).isEmpty(), "key 'b' must start empty");
            bState.add("b-1");

            backend.setCurrentKey("a");
            assertEquals(
                    List.of("a-1", "a-2"),
                    drain(backend.getListState("l", StringSerializer.INSTANCE).get()));
        }
    }

    @Test
    void testStateNameIsolation() throws Exception {
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {

            backend.setCurrentKey("a");
            backend.getValueState("first", IntSerializer.INSTANCE).update(11);
            backend.getValueState("second", IntSerializer.INSTANCE).update(22);

            assertEquals(
                    Integer.valueOf(11),
                    backend.getValueState("first", IntSerializer.INSTANCE).value());
            assertEquals(
                    Integer.valueOf(22),
                    backend.getValueState("second", IntSerializer.INSTANCE).value());
        }
    }

    @Test
    void testDispose() {
        ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE);
        backend.setCurrentKey("a");
        backend.getValueState("v", IntSerializer.INSTANCE);
        backend.dispose();
        // After dispose, subsequent operations must surface a clear error rather than crash
        // the JVM with a use-after-close native pointer.
        assertThrows(IllegalStateException.class, () -> backend.setCurrentKey("b"));
    }

    @Test
    void testDoubleCloseIsNoOp() throws Exception {
        ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE);
        backend.close();
        // Second close must be a silent no-op (not a double-free of the FFM resources).
        backend.close();
    }

    @Test
    void testStateBackendCreateBasicKeyedBackendSmoke() throws Exception {
        // End-to-end smoke that mirrors what a Flink job would do via the StateBackend factory:
        // construct the backend, set keys, exercise all three state types, close cleanly.
        ForStRsStateBackend sb = new ForStRsStateBackend();
        try (ForStRsKeyedStateBackend<String> backend =
                sb.createBasicKeyedBackend(StringSerializer.INSTANCE)) {
            assertNotNull(backend);
            backend.setCurrentKey("alice");

            backend.getValueState("v", IntSerializer.INSTANCE).update(42);
            backend.getListState("l", StringSerializer.INSTANCE).add("x");
            backend
                    .getMapState("m", StringSerializer.INSTANCE, IntSerializer.INSTANCE)
                    .put("hits", 7);

            assertEquals(
                    Integer.valueOf(42),
                    backend.getValueState("v", IntSerializer.INSTANCE).value());
            assertEquals(
                    List.of("x"),
                    drain(backend.getListState("l", StringSerializer.INSTANCE).get()));
            assertEquals(
                    Integer.valueOf(7),
                    backend
                            .getMapState("m", StringSerializer.INSTANCE, IntSerializer.INSTANCE)
                            .get("hits"));

            assertTrue(backend.numKeyValueStateEntries() >= 3);
        }
    }

    @Test
    void testCreateKeyedStateBackendThrowsUOE() {
        // Until Phase-D L5/L6 wires the full CheckpointableKeyedStateBackend interface, the
        // canonical Flink entry-point must surface a clear error rather than silently no-op.
        ForStRsStateBackend sb = new ForStRsStateBackend();
        UnsupportedOperationException e =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> sb.createKeyedStateBackend(null));
        assertTrue(
                e.getMessage().contains("createBasicKeyedBackend"),
                "UOE message should point users at the stepping-stone factory");
    }

    @Test
    void testGetValueStateRequiresCurrentKey() {
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {
            assertThrows(
                    IllegalStateException.class,
                    () -> backend.getValueState("v", IntSerializer.INSTANCE));
        } catch (Exception ignored) {
            // close() may throw if internal state is bad — surfaced by the assertion above.
        }
    }

    @Test
    void testCreateReducingState() throws Exception {
        ReduceFunction<Integer> sum = Integer::sum;
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {

            backend.setCurrentKey("a");
            ForStRsReducingState<Integer> state =
                    backend.getReducingState("sum", IntSerializer.INSTANCE, sum);
            state.add(1);
            state.add(2);
            state.add(3);
            assertEquals(Integer.valueOf(6), state.get());

            backend.setCurrentKey("b");
            ForStRsReducingState<Integer> stateB =
                    backend.getReducingState("sum", IntSerializer.INSTANCE, sum);
            assertNull(stateB.get(), "key 'b' must not see key 'a' reducing state");
        }
    }

    @Test
    void testCreateAggregatingState() throws Exception {
        // Mean aggregator: ACC = (sum, count), OUT = double mean.
        AggregateFunction<Integer, long[], Double> mean =
                new AggregateFunction<>() {
                    @Override
                    public long[] createAccumulator() {
                        return new long[2];
                    }

                    @Override
                    public long[] add(Integer value, long[] accumulator) {
                        accumulator[0] += value;
                        accumulator[1] += 1;
                        return accumulator;
                    }

                    @Override
                    public Double getResult(long[] accumulator) {
                        return accumulator[1] == 0 ? 0.0 : (double) accumulator[0] / accumulator[1];
                    }

                    @Override
                    public long[] merge(long[] a, long[] b) {
                        a[0] += b[0];
                        a[1] += b[1];
                        return a;
                    }
                };

        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {

            backend.setCurrentKey("a");
            ForStRsAggregatingState<Integer, long[], Double> state =
                    backend.getAggregatingState(
                            "avg",
                            org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer
                                    .INSTANCE,
                            mean);
            state.add(2);
            state.add(4);
            state.add(6);
            assertEquals(Double.valueOf(4.0), state.get());
        }
    }

    @Test
    void testCachedStateInstanceReused() throws Exception {
        try (ForStRsKeyedStateBackend<String> backend =
                new ForStRsStateBackend().createBasicKeyedBackend(StringSerializer.INSTANCE)) {
            backend.setCurrentKey("a");
            ForStRsValueState<Integer> first =
                    backend.getValueState("v", IntSerializer.INSTANCE);
            ForStRsValueState<Integer> second =
                    backend.getValueState("v", IntSerializer.INSTANCE);
            assertTrue(first == second, "same key + state-name must return the cached instance");

            backend.setCurrentKey("b");
            ForStRsValueState<Integer> afterRekey =
                    backend.getValueState("v", IntSerializer.INSTANCE);
            assertFalse(
                    first == afterRekey,
                    "key change must invalidate the cache so the new instance binds the new key");
        }
    }
}

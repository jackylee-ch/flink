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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips a {@link ForStRsAggregatingState} backed by an in-memory ForSt-RS engine, exercising
 * add/clear with a list-accumulating {@link AggregateFunction} that stringifies on getResult.
 */
class ForStRsAggregatingStateTest {

    /**
     * Aggregator: accumulates {@link Integer} inputs into a {@link List}, returning a bracketed,
     * comma-separated string on {@link AggregateFunction#getResult(Object) getResult}. Tracks
     * whether {@link AggregateFunction#createAccumulator() createAccumulator} has been invoked so
     * tests can assert the first add() bootstraps a fresh accumulator.
     */
    private static final class ListAggregator
            implements AggregateFunction<Integer, List<Integer>, String> {
        private static final long serialVersionUID = 1L;
        int createAccumulatorCalls;

        @Override
        public List<Integer> createAccumulator() {
            createAccumulatorCalls++;
            return new ArrayList<>();
        }

        @Override
        public List<Integer> add(Integer value, List<Integer> accumulator) {
            accumulator.add(value);
            return accumulator;
        }

        @Override
        public String getResult(List<Integer> accumulator) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < accumulator.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(accumulator.get(i));
            }
            return sb.append(']').toString();
        }

        @Override
        public List<Integer> merge(List<Integer> a, List<Integer> b) {
            a.addAll(b);
            return a;
        }
    }

    private static byte[] prefix(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ListSerializer<Integer> accSerializer() {
        return new ListSerializer<>(IntSerializer.INSTANCE);
    }

    @Test
    void testAddSingle() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsAggregatingState<Integer, List<Integer>, String> state =
                        new ForStRsAggregatingState<>(
                                linker,
                                db,
                                cf,
                                prefix("agg-1"),
                                accSerializer(),
                                new ListAggregator());

                state.add(5);
                assertEquals("[5]", state.get());
            }
        }
    }

    @Test
    void testAddMultiple() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsAggregatingState<Integer, List<Integer>, String> state =
                        new ForStRsAggregatingState<>(
                                linker,
                                db,
                                cf,
                                prefix("agg-2"),
                                accSerializer(),
                                new ListAggregator());

                state.add(1);
                state.add(2);
                state.add(3);
                state.add(4);

                assertEquals("[1,2,3,4]", state.get());
            }
        }
    }

    @Test
    void testClear() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsAggregatingState<Integer, List<Integer>, String> state =
                        new ForStRsAggregatingState<>(
                                linker,
                                db,
                                cf,
                                prefix("agg-3"),
                                accSerializer(),
                                new ListAggregator());

                state.add(7);
                state.add(11);
                assertEquals("[7,11]", state.get());

                state.clear();
                assertNull(state.get());
            }
        }
    }

    @Test
    void testInitialAccumulator() throws Exception {
        // Verifies that the first add() against an empty state bootstraps via createAccumulator(),
        // and that a subsequent add() reuses the persisted accumulator without bootstrapping again.
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ListAggregator agg = new ListAggregator();
                ForStRsAggregatingState<Integer, List<Integer>, String> state =
                        new ForStRsAggregatingState<>(
                                linker, db, cf, prefix("agg-4"), accSerializer(), agg);

                assertEquals(0, agg.createAccumulatorCalls);

                state.add(42);
                assertEquals(1, agg.createAccumulatorCalls, "first add must bootstrap accumulator");
                assertEquals("[42]", state.get());

                state.add(43);
                assertEquals(
                        1,
                        agg.createAccumulatorCalls,
                        "subsequent add must not re-bootstrap accumulator");
                assertEquals("[42,43]", state.get());

                // Cross-check that getResult is stable when called repeatedly without mutation.
                assertTrue(state.get().startsWith("[42,43"));
            }
        }
    }
}

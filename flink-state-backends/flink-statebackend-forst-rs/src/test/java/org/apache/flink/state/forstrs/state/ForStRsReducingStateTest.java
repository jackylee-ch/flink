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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Round-trips a {@link ForStRsReducingState} backed by an in-memory ForSt-RS engine, exercising
 * add/clear with a sum {@link ReduceFunction} over {@link Long}.
 */
class ForStRsReducingStateTest {

    /** Sum reducer: combines two longs into their sum. */
    private static final ReduceFunction<Long> SUM = (a, b) -> a + b;

    private static byte[] prefix(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void testAddSingle() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsReducingState<Long> state =
                        new ForStRsReducingState<>(
                                linker,
                                db,
                                cf,
                                prefix("reduce-1"),
                                LongSerializer.INSTANCE,
                                SUM);

                state.add(5L);
                assertEquals(5L, state.get());
            }
        }
    }

    @Test
    void testAddMultiple() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsReducingState<Long> state =
                        new ForStRsReducingState<>(
                                linker,
                                db,
                                cf,
                                prefix("reduce-2"),
                                LongSerializer.INSTANCE,
                                SUM);

                state.add(3L);
                state.add(4L);
                state.add(5L);

                assertEquals(12L, state.get());
            }
        }
    }

    @Test
    void testClear() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsReducingState<Long> state =
                        new ForStRsReducingState<>(
                                linker,
                                db,
                                cf,
                                prefix("reduce-3"),
                                LongSerializer.INSTANCE,
                                SUM);

                state.add(3L);
                assertEquals(3L, state.get());

                state.clear();
                assertNull(state.get());
            }
        }
    }

    @Test
    void testReducerWithNullStart() throws Exception {
        // The first add against an empty state must be stored verbatim — the reducer must NOT be
        // invoked with a null operand. We verify by using a reducer that would NPE on null inputs.
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ReduceFunction<Long> npeOnNull =
                        (a, b) -> {
                            if (a == null || b == null) {
                                throw new IllegalStateException(
                                        "reducer must not be called on first add()");
                            }
                            return a + b;
                        };
                ForStRsReducingState<Long> state =
                        new ForStRsReducingState<>(
                                linker,
                                db,
                                cf,
                                prefix("reduce-4"),
                                LongSerializer.INSTANCE,
                                npeOnNull);

                state.add(7L);
                assertEquals(7L, state.get());
            }
        }
    }
}

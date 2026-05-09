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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips a {@link ForStRsListState} backed by an in-memory ForSt-RS engine, exercising
 * add/update/addAll/clear with {@link StringSerializer}.
 */
class ForStRsListStateTest {

    private static byte[] prefix(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static <T> List<T> drain(Iterable<T> iter) {
        List<T> out = new ArrayList<>();
        iter.forEach(out::add);
        return out;
    }

    @Test
    void testAddRetrieve() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsListState<String> state =
                        new ForStRsListState<>(
                                linker, db, cf, prefix("list-1"), StringSerializer.INSTANCE);

                state.add("a");
                state.add("b");
                state.add("c");

                assertEquals(List.of("a", "b", "c"), drain(state.get()));
            }
        }
    }

    @Test
    void testUpdate() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsListState<String> state =
                        new ForStRsListState<>(
                                linker, db, cf, prefix("list-2"), StringSerializer.INSTANCE);

                state.add("orig-1");
                state.add("orig-2");
                assertEquals(List.of("orig-1", "orig-2"), drain(state.get()));

                state.update(List.of("new-1", "new-2", "new-3"));
                assertEquals(List.of("new-1", "new-2", "new-3"), drain(state.get()));
            }
        }
    }

    @Test
    void testAddAll() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsListState<String> state =
                        new ForStRsListState<>(
                                linker, db, cf, prefix("list-3"), StringSerializer.INSTANCE);

                state.add("x");
                state.add("y");
                state.addAll(List.of("p", "q", "r"));

                assertEquals(List.of("x", "y", "p", "q", "r"), drain(state.get()));
            }
        }
    }

    @Test
    void testClear() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsListState<String> state =
                        new ForStRsListState<>(
                                linker, db, cf, prefix("list-4"), StringSerializer.INSTANCE);

                state.add("alpha");
                state.add("beta");
                state.add("gamma");
                assertFalse(drain(state.get()).isEmpty());

                state.clear();
                assertTrue(drain(state.get()).isEmpty());
            }
        }
    }

    @Test
    void testEmpty() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsListState<String> state =
                        new ForStRsListState<>(
                                linker, db, cf, prefix("list-5"), StringSerializer.INSTANCE);

                assertTrue(drain(state.get()).isEmpty());
            }
        }
    }

    @Test
    void testNullUpdateClears() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsListState<String> state =
                        new ForStRsListState<>(
                                linker, db, cf, prefix("list-6"), StringSerializer.INSTANCE);

                state.add("present");
                assertFalse(drain(state.get()).isEmpty());

                state.update(null);
                assertTrue(drain(state.get()).isEmpty());
            }
        }
    }

    @Test
    void testUpdateEmptyClears() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsListState<String> state =
                        new ForStRsListState<>(
                                linker, db, cf, prefix("list-7"), StringSerializer.INSTANCE);

                state.add("present");
                assertFalse(drain(state.get()).isEmpty());

                state.update(Collections.emptyList());
                assertTrue(drain(state.get()).isEmpty());
            }
        }
    }
}

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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips a {@link ForStRsMapState} backed by an in-memory ForSt-RS engine, exercising
 * put/get/remove/contains/entries/keys/values/isEmpty/clear/putAll. Keys are {@link String}, values
 * are {@link Integer}.
 */
class ForStRsMapStateTest {

    private static byte[] prefix(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static ForStRsMapState<String, Integer> newState(
            ForStRsLinker linker, FrsDb db, FrsCfHandle cf, String prefix) {
        return new ForStRsMapState<>(
                linker,
                db,
                cf,
                prefix(prefix),
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE);
    }

    @Test
    void testPutGet() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-1");

                state.put("alice", 1);
                state.put("bob", 2);
                state.put("carol", 3);

                assertEquals(1, state.get("alice"));
                assertEquals(2, state.get("bob"));
                assertEquals(3, state.get("carol"));
                assertNull(state.get("missing"));
            }
        }
    }

    @Test
    void testRemove() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-2");

                state.put("alice", 1);
                state.put("bob", 2);
                assertTrue(state.contains("alice"));

                state.remove("alice");
                assertFalse(state.contains("alice"));
                assertNull(state.get("alice"));
                assertEquals(2, state.get("bob"));
            }
        }
    }

    @Test
    void testEntries() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-3");

                state.put("a", 10);
                state.put("b", 20);
                state.put("c", 30);

                Map<String, Integer> seen = new HashMap<>();
                int count = 0;
                for (Map.Entry<String, Integer> e : state.entries()) {
                    seen.put(e.getKey(), e.getValue());
                    count++;
                }
                assertEquals(3, count);
                assertEquals(Map.of("a", 10, "b", 20, "c", 30), seen);
            }
        }
    }

    @Test
    void testKeys() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-4");

                state.put("a", 1);
                state.put("b", 2);
                state.put("c", 3);

                Set<String> keys = new HashSet<>();
                state.keys().forEach(keys::add);
                assertEquals(Set.of("a", "b", "c"), keys);
            }
        }
    }

    @Test
    void testValues() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-5");

                state.put("a", 11);
                state.put("b", 22);
                state.put("c", 33);

                Set<Integer> values = new HashSet<>();
                state.values().forEach(values::add);
                assertEquals(Set.of(11, 22, 33), values);
            }
        }
    }

    @Test
    void testIsEmpty() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-6");

                assertTrue(state.isEmpty());

                state.put("k", 1);
                assertFalse(state.isEmpty());

                state.clear();
                assertTrue(state.isEmpty());
            }
        }
    }

    @Test
    void testClear() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-7");

                state.put("x", 1);
                state.put("y", 2);
                state.put("z", 3);
                assertFalse(state.isEmpty());

                state.clear();
                assertTrue(state.isEmpty());
                assertNull(state.get("x"));
            }
        }
    }

    @Test
    void testContains() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-8");

                state.put("here", 42);
                assertTrue(state.contains("here"));
                assertFalse(state.contains("there"));
            }
        }
    }

    @Test
    void testPutAll() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                ForStRsMapState<String, Integer> state = newState(linker, db, cf, "map-9");

                Map<String, Integer> bulk = new LinkedHashMap<>();
                bulk.put("one", 1);
                bulk.put("two", 2);
                bulk.put("three", 3);
                state.putAll(bulk);

                assertEquals(1, state.get("one"));
                assertEquals(2, state.get("two"));
                assertEquals(3, state.get("three"));

                int count = 0;
                Iterator<Map.Entry<String, Integer>> it = state.iterator();
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
                assertEquals(3, count);
            }
        }
    }
}

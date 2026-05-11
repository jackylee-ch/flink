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
import org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trips a {@link ForStRsMapState} constructed via the spec §6 kg-prefixed ctor: the
 * composite ForSt keys are recomputed per call via {@code encodeForMap}; the iteration prefix is
 * recomputed via {@code encodeForState}.
 */
class ForStRsMapStateKgTest {

    @Test
    void putGetUnderKgPrefixThenSwitchKey() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                ForStRsKeyGroupedSerializer<String> kgSer =
                        new ForStRsKeyGroupedSerializer<>(StringSerializer.INSTANCE);
                AtomicReference<String> currentKey = new AtomicReference<>("alice");
                int kg = 7;

                ForStRsMapState<String, Integer> state =
                        new ForStRsMapState<>(
                                linker,
                                db,
                                cf,
                                StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE,
                                () -> kgSer.encodeForState(kg, currentKey.get(), "myMap"),
                                uk ->
                                        kgSer.encodeForMap(
                                                kg,
                                                currentKey.get(),
                                                "myMap",
                                                StringSerializer.INSTANCE,
                                                uk));

                state.put("a", 1);
                state.put("b", 2);
                assertEquals(1, state.get("a"));
                assertEquals(2, state.get("b"));
                assertTrue(state.contains("a"));
                assertFalse(state.isEmpty());

                Map<String, Integer> seen = new HashMap<>();
                state.entries().forEach(e -> seen.put(e.getKey(), e.getValue()));
                assertEquals(Map.of("a", 1, "b", 2), seen);

                // Switch user-key — entries above are isolated and should not be visible.
                currentKey.set("bob");
                assertNull(state.get("a"));
                assertTrue(state.isEmpty());

                state.put("a", 10);
                assertEquals(10, state.get("a"));

                // Switching back recovers alice's entries.
                currentKey.set("alice");
                assertEquals(1, state.get("a"));
                assertEquals(2, state.get("b"));
            }
        }
    }
}

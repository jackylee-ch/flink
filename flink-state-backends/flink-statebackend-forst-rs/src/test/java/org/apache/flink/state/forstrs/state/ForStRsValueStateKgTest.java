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
import org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Round-trips a {@link ForStRsValueState} constructed via the spec §6 kg-prefixed ctor: the
 * composite ForSt key is recomputed per call from currentKg + currentKey + stateName.
 */
class ForStRsValueStateKgTest {

    @Test
    void kgPrefixedKeySwitchesIsolatesEntries() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                ForStRsKeyGroupedSerializer<String> kgSer =
                        new ForStRsKeyGroupedSerializer<>(StringSerializer.INSTANCE);
                AtomicReference<String> currentKey = new AtomicReference<>("alice");
                AtomicReference<Integer> currentKg = new AtomicReference<>(7);

                ForStRsValueState<String> state =
                        new ForStRsValueState<>(
                                linker,
                                db,
                                cf,
                                StringSerializer.INSTANCE,
                                () ->
                                        kgSer.encodeForState(
                                                currentKg.get(), currentKey.get(), "myState"));

                state.update("hello");
                assertEquals("hello", state.value());

                // Switching the user-key changes the underlying ForSt key — the prior write must
                // not be visible.
                currentKey.set("bob");
                assertNull(state.value());

                state.update("world");
                assertEquals("world", state.value());

                // Switching back recovers alice's value.
                currentKey.set("alice");
                assertEquals("hello", state.value());

                // Switching the kg also isolates entries.
                currentKg.set(8);
                assertNull(state.value());
            }
        }
    }
}

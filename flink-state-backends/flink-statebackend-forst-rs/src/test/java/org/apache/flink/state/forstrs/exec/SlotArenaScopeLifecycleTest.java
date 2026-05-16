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

package org.apache.flink.state.forstrs.exec;

import org.apache.flink.state.forstrs.keyed.ForStRsAsyncKeyedStateBackend;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Shape test verifying that both keyed-state backends declare a {@link SlotArenaScope} field and
 * that the field is wired in correctly. Real lifecycle behavior (init → dispose) is exercised in
 * higher-level integration tests in P5+.
 */
class SlotArenaScopeLifecycleTest {

    @Test
    void asyncBackendDeclaresSlotArenaScopeField() {
        Field scopeField = null;
        try {
            scopeField = ForStRsAsyncKeyedStateBackend.class.getDeclaredField("slotArenaScope");
        } catch (NoSuchFieldException nsfe) {
            fail("ForStRsAsyncKeyedStateBackend must declare a SlotArenaScope field");
        }
        assertEquals(
                SlotArenaScope.class,
                scopeField.getType(),
                "slotArenaScope field must be of type SlotArenaScope");
    }

    @Test
    void syncBackendDeclaresSlotArenaScopeField() {
        Field scopeField = null;
        try {
            scopeField = ForStRsKeyedStateBackend.class.getDeclaredField("slotArenaScope");
        } catch (NoSuchFieldException nsfe) {
            fail("ForStRsKeyedStateBackend must declare a SlotArenaScope field");
        }
        assertEquals(
                SlotArenaScope.class,
                scopeField.getType(),
                "slotArenaScope field must be of type SlotArenaScope");
    }
}

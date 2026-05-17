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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P6-B structural tests for {@link ForStRsListStateV2}.
 *
 * <p>Verifies that the class exists in the expected package and exposes the required APPEND_MERGE
 * methods. Full end-to-end integration tests against a running ForSt-RS engine are deferred to P11
 * (integration test suite).
 */
class ListStateV2DispatchTest {

    @Test
    void listStateV2ClassExists() {
        assertNotNull(
                ForStRsListStateV2.class, "ForStRsListStateV2 must exist in the state package");
    }

    @Test
    void addMethodExists() {
        boolean found = false;
        for (var m : ForStRsListStateV2.class.getMethods()) {
            if (m.getName().equals("add")) {
                found = true;
                break;
            }
        }
        assertTrue(found, "ForStRsListStateV2 must declare add()");
    }

    @Test
    void addAllMethodExists() {
        boolean found = false;
        for (var m : ForStRsListStateV2.class.getMethods()) {
            if (m.getName().equals("addAll")) {
                found = true;
                break;
            }
        }
        assertTrue(found, "ForStRsListStateV2 must declare addAll()");
    }

    @Test
    void getMethodExists() {
        boolean found = false;
        for (var m : ForStRsListStateV2.class.getMethods()) {
            if (m.getName().equals("get")) {
                found = true;
                break;
            }
        }
        assertTrue(found, "ForStRsListStateV2 must declare get()");
    }

    @Test
    void encodeMethodExists() {
        boolean found = false;
        for (var m : ForStRsListStateV2.class.getMethods()) {
            if (m.getName().equals("encode")) {
                found = true;
                break;
            }
        }
        assertTrue(found, "ForStRsListStateV2 must declare encode() for update() semantics");
    }
}

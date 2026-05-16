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

class ReducingStateV2DispatchTest {

    @Test
    void reducingStateV2ClassExists() {
        assertNotNull(ForStRsReducingStateV2.class);
    }

    @Test
    void addMethodAcceptsByteKeyAndValue() {
        boolean found = false;
        for (var m : ForStRsReducingStateV2.class.getMethods()) {
            if (m.getName().equals("add") && m.getParameterCount() == 2) {
                found = true;
                break;
            }
        }
        assertTrue(found, "add(byte[], T) must exist");
    }

    @Test
    void flushOnBarrierMethodExists() {
        boolean found = false;
        for (var m : ForStRsReducingStateV2.class.getMethods()) {
            if (m.getName().equals("flushOnBarrier") && m.getParameterCount() == 0) {
                found = true;
                break;
            }
        }
        assertTrue(found, "flushOnBarrier() must exist");
    }

    @Test
    void getMethodExists() {
        boolean found = false;
        for (var m : ForStRsReducingStateV2.class.getMethods()) {
            if (m.getName().equals("get") && m.getParameterCount() == 1) {
                found = true;
                break;
            }
        }
        assertTrue(found, "get(byte[]) must exist");
    }
}

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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structural smoke-tests for the Trace E barrier drain registry in {@link
 * ForStRsAsyncKeyedStateBackend} (umbrella spec §3 Trace E).
 */
class BarrierDrainTest {

    @Test
    void asyncKeyedStateBackendHasRmwStateRegisters() {
        boolean reducing = false, aggregating = false;
        for (var m : ForStRsAsyncKeyedStateBackend.class.getMethods()) {
            if (m.getName().equals("registerReducingState")) {
                reducing = true;
            }
            if (m.getName().equals("registerAggregatingState")) {
                aggregating = true;
            }
        }
        assertTrue(reducing, "ForStRsAsyncKeyedStateBackend.registerReducingState must exist");
        assertTrue(
                aggregating, "ForStRsAsyncKeyedStateBackend.registerAggregatingState must exist");
    }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Structural smoke-tests for {@link ForStRsAggregatingStateV2} (umbrella spec §2 component 13). */
class AggregatingStateV2DispatchTest {

    @Test
    void aggregatingStateV2ClassExists() {
        assertNotNull(ForStRsAggregatingStateV2.class);
    }

    @Test
    void hasAddGetClearFlushOnBarrier() {
        boolean add = false, get = false, clear = false, flush = false;
        for (var m : ForStRsAggregatingStateV2.class.getMethods()) {
            if (m.getName().equals("add")) {
                add = true;
            }
            if (m.getName().equals("get")) {
                get = true;
            }
            if (m.getName().equals("clear")) {
                clear = true;
            }
            if (m.getName().equals("flushOnBarrier")) {
                flush = true;
            }
        }
        assertTrue(add, "ForStRsAggregatingStateV2 must have add()");
        assertTrue(get, "ForStRsAggregatingStateV2 must have get()");
        assertTrue(clear, "ForStRsAggregatingStateV2 must have clear()");
        assertTrue(flush, "ForStRsAggregatingStateV2 must have flushOnBarrier()");
    }

    @Test
    void hasThreeTypeParams() {
        var params = ForStRsAggregatingStateV2.class.getTypeParameters();
        assertEquals(3, params.length, "AggregatingState<IN, ACC, OUT> — 3 type params");
    }
}

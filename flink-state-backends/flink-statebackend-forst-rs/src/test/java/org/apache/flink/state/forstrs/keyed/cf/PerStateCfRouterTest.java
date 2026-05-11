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

package org.apache.flink.state.forstrs.keyed.cf;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class PerStateCfRouterTest {

    @Test
    void distinctStatesGetDistinctCfs() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                try (PerStateCfRouter router = new PerStateCfRouter(linker, db, arena)) {
                    FrsCfHandle a = router.getCfForState("stateA");
                    FrsCfHandle b = router.getCfForState("stateB");
                    assertNotSame(a, b);
                    assertEquals(2, router.allCfs().size());
                    assertEquals("stateA", router.stateNameForCf(a));
                    assertEquals("stateB", router.stateNameForCf(b));
                    assertEquals(false, router.isSingleCf());
                }
            }
        }
    }

    @Test
    void repeatedGetReturnsSameCf() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                try (PerStateCfRouter router = new PerStateCfRouter(linker, db, arena)) {
                    FrsCfHandle first = router.getCfForState("s");
                    FrsCfHandle second = router.getCfForState("s");
                    assertEquals(first, second);
                    assertEquals(1, router.allCfs().size());
                }
            }
        }
    }
}

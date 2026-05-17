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

package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FrsAbiVerifyTest {

    @Test
    void verifyAbiPassesWhenVersionsMatch() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            // Pure static helper — does not require full backend instantiation
            assertDoesNotThrow(() -> FrsAbi.verifyAgainst(() -> linker.frsAbiVersion()));
        }
    }

    @Test
    void verifyAbiThrowsOnMismatch() {
        // Simulate mismatch via a supplier that returns a wrong version
        FrsAbiMismatchException ex =
                assertThrows(FrsAbiMismatchException.class, () -> FrsAbi.verifyAgainst(() -> 999));
        assertEquals(999, ex.getActualVersion());
        assertEquals(FrsAbi.EXPECTED_ABI_VERSION, ex.getExpectedVersion());
    }
}

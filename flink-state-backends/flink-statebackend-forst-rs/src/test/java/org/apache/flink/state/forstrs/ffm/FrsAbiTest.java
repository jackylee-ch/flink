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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FrsAbiTest {

    @Test
    void expectedVersionIsOne() {
        assertEquals(1, FrsAbi.EXPECTED_ABI_VERSION);
    }

    @Test
    void linkerReportsMatchingVersion() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            assertEquals(FrsAbi.EXPECTED_ABI_VERSION, linker.frsAbiVersion());
        }
    }

    @Test
    void mismatchExceptionCarriesBothVersions() {
        FrsAbiMismatchException ex = new FrsAbiMismatchException(2, 1);
        assertEquals(2, ex.getActualVersion());
        assertEquals(1, ex.getExpectedVersion());
        assertTrue(ex.getMessage().contains("version 2"));
        assertTrue(ex.getMessage().contains("expects version 1"));
    }
}

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SlotArenaScopeTest {

    SlotArenaScope scope;

    @BeforeEach
    void newScope() {
        scope = SlotArenaScope.openForSlot(8L * 1024 * 1024, 64L * 1024 * 1024);
    }

    @AfterEach
    void close() {
        if (scope != null) {
            scope.closeSlot();
        }
    }

    @Test
    void turnRegionBumpResetReusesSpace() {
        scope.enterTurn();
        MemorySegment seg1 = scope.allocateTurn(256, 64);
        long addr1 = seg1.address();
        scope.exitTurn();

        scope.enterTurn();
        MemorySegment seg2 = scope.allocateTurn(256, 64);
        assertEquals(addr1, seg2.address(), "exitTurn must restore bump offset");
        scope.exitTurn();
    }

    @Test
    void enterTurnAssertsIterRegistryEmpty() {
        scope.enterTurn();
        scope.exitTurn();
        // Second turn must not throw
        scope.enterTurn();
        scope.exitTurn();
    }

    @Test
    void overflowCreatesPerTurnArenaAndReleasesOnExit() {
        scope.enterTurn();
        // Allocate beyond turnRegion bound — single huge alloc forces overflow
        MemorySegment huge = scope.allocateTurn(9L * 1024 * 1024, 64);
        assertNotNull(huge);
        assertEquals(9L * 1024 * 1024, huge.byteSize());
        assertEquals(1, scope.overflowArenaCountForCurrentTurn());
        scope.exitTurn();

        scope.enterTurn();
        assertEquals(
                0, scope.overflowArenaCountForCurrentTurn(), "exit must close overflow arenas");
        scope.exitTurn();
    }

    @Test
    void cacheRegionSurvivesTurnBoundary() {
        scope.enterTurn();
        MemorySegment cacheSeg = scope.allocateCache(1024, 64);
        cacheSeg.set(ValueLayout.JAVA_BYTE, 0, (byte) 42);
        scope.exitTurn();
        // cacheRegion not affected by bump-reset — segment still valid
        assertEquals(42, cacheSeg.get(ValueLayout.JAVA_BYTE, 0));
    }

    @Test
    void allocateTurnRespectsAlignment() {
        scope.enterTurn();
        // First alloc may be at offset 0 (already 64-aligned); force misalignment
        MemorySegment a = scope.allocateTurn(7, 64); // 7B unaligned tail
        MemorySegment b = scope.allocateTurn(8, 8); // next should align to 8
        assertEquals(0L, b.address() & 7L, "8-byte aligned");
        scope.exitTurn();
    }

    @Test
    void closeSlotIsIdempotent() {
        scope.closeSlot();
        // Second close must not throw
        scope.closeSlot();
        scope = null; // suppress @AfterEach
    }

    @Test
    void allocateOnClosedSlotThrows() {
        scope.closeSlot();
        // enterTurn also throws on closed scope; test the allocation path directly
        assertThrows(IllegalStateException.class, () -> scope.allocateTurn(64, 8));
        scope = null;
    }
}

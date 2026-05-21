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

class ArrowBinaryBufferAutoTunerTest {

    @Test
    void growsCapacityOnHighHitRateAndHighOccupancy() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(1024);
        // Feed 1024 reads, all hits; size==cap so occupancy=1.0.
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true, 1024, 1024);
        }
        int newCap = t.shouldResizeTo(1024);
        assertEquals(2048, newCap, "≥80% hit AND ≥70% occupancy must trigger 2x grow");
    }

    @Test
    void shrinksCapacityOnLowHitRateAndLowOccupancy() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(8192);
        // ~10 % hit, ~10 % occupancy (size=800, cap=8192).
        for (int i = 0; i < 1024; i++) {
            t.observeRead(i < 100, 800, 8192);
        }
        int newCap = t.shouldResizeTo(8192);
        assertEquals(4096, newCap, "<30% hit AND <20% occupancy must trigger 0.5x shrink");
    }

    @Test
    void noChangeInMiddleZone() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(4096);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(i < 500, 4096, 4096); // ~49% hit, full
        }
        int newCap = t.shouldResizeTo(4096);
        assertEquals(4096, newCap, "30-80% hit rate must be in hysteresis zone");
    }

    @Test
    void respectsMaxCapacity() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MAX_CAPACITY);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true, ArrowBinaryBuffer.MAX_CAPACITY, ArrowBinaryBuffer.MAX_CAPACITY);
        }
        int newCap = t.shouldResizeTo(ArrowBinaryBuffer.MAX_CAPACITY);
        assertEquals(ArrowBinaryBuffer.MAX_CAPACITY, newCap, "must not exceed MAX_CAPACITY");
    }

    @Test
    void respectsMinCapacity() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(false, 0, ArrowBinaryBuffer.MIN_CAPACITY); // 0% hit, 0% occupancy
        }
        int newCap = t.shouldResizeTo(ArrowBinaryBuffer.MIN_CAPACITY);
        assertEquals(ArrowBinaryBuffer.MIN_CAPACITY, newCap, "must not shrink below MIN_CAPACITY");
    }

    @Test
    void resetsSampleAfterDecision() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(2048);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true, 2048, 2048);
        }
        t.shouldResizeTo(2048); // consumes the window
        // Next 100 reads with 0% hit — window not yet full so no decision.
        for (int i = 0; i < 100; i++) {
            t.observeRead(false, 2048, 2048);
        }
        assertEquals(2048, t.shouldResizeTo(2048), "sample window not full → no decision");
    }

    // --- Size-aware (dual-gate) tests added 2026-05-21 -----------------------

    @Test
    void growIsGatedByOccupancy() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(1024);
        // High hit rate but low occupancy (size=100, cap=1024 → 0.10) — must NOT grow.
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true, 100, 1024);
        }
        int newCap = t.shouldResizeTo(1024);
        assertEquals(1024, newCap, "high hit + low occupancy must NOT grow (Q11 pattern)");
    }

    @Test
    void growWhenBothGatesPass() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(1024);
        // High hit + occupancy 900/1024 = 0.88 → grow.
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true, 900, 1024);
        }
        int newCap = t.shouldResizeTo(1024);
        assertEquals(2048, newCap, "high hit + ≥70% occupancy → 2x grow");
    }

    @Test
    void shrinkIsGatedByOccupancy() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(1024);
        // Low hit rate but high occupancy (size=900, cap=1024 → 0.88) — must NOT shrink:
        // workload is churning but the buffer is right-sized for the working set.
        for (int i = 0; i < 1024; i++) {
            t.observeRead(i < 100, 900, 1024);
        }
        int newCap = t.shouldResizeTo(1024);
        assertEquals(1024, newCap, "low hit + high occupancy must NOT shrink (churn pattern)");
    }

    @Test
    void shrinkWhenBothGatesPass() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(2048);
        // Low hit + low occupancy (size=100, cap=2048 → 0.05) → shrink approved.
        for (int i = 0; i < 1024; i++) {
            t.observeRead(i < 100, 100, 2048);
        }
        int newCap = t.shouldResizeTo(2048);
        assertEquals(1024, newCap, "low hit + ≤20% occupancy → 0.5x shrink");
    }

    @Test
    void respectsNewMaxCapacity() {
        // At 524 288, full + 100% hit → grow to 1 048 576 (the new MAX_CAPACITY).
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(524288);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true, 524288, 524288);
        }
        int newCap = t.shouldResizeTo(524288);
        assertEquals(1_048_576, newCap, "must grow to new MAX_CAPACITY=1M from 512K");
        // At 1 048 576, no further growth allowed.
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true, 1_048_576, 1_048_576);
        }
        assertEquals(
                1_048_576,
                t.shouldResizeTo(1_048_576),
                "must clamp at new MAX_CAPACITY=1M");
    }
}

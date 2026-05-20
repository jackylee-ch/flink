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
    void growsCapacityOnHighHitRate() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(1024);
        // Feed 1024 reads, all hits.
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true);
        }
        int newCap = t.shouldResizeTo(1024);
        assertEquals(2048, newCap, "≥80% hit rate must trigger 2x grow");
    }

    @Test
    void shrinksCapacityOnLowHitRate() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(8192);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(i < 100); // ~10% hit
        }
        int newCap = t.shouldResizeTo(8192);
        assertEquals(4096, newCap, "<30% hit rate must trigger 0.5x shrink");
    }

    @Test
    void noChangeInMiddleZone() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(4096);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(i < 500); // ~49% hit
        }
        int newCap = t.shouldResizeTo(4096);
        assertEquals(4096, newCap, "30-80% hit rate must be in hysteresis zone");
    }

    @Test
    void respectsMaxCapacity() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MAX_CAPACITY);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true);
        }
        int newCap = t.shouldResizeTo(ArrowBinaryBuffer.MAX_CAPACITY);
        assertEquals(ArrowBinaryBuffer.MAX_CAPACITY, newCap, "must not exceed MAX_CAPACITY");
    }

    @Test
    void respectsMinCapacity() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(false); // 0% hit
        }
        int newCap = t.shouldResizeTo(ArrowBinaryBuffer.MIN_CAPACITY);
        assertEquals(ArrowBinaryBuffer.MIN_CAPACITY, newCap, "must not shrink below MIN_CAPACITY");
    }

    @Test
    void resetsSampleAfterDecision() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(2048);
        for (int i = 0; i < 1024; i++) {
            t.observeRead(true);
        }
        t.shouldResizeTo(2048); // consumes the window
        // Next 100 reads with 0% hit — window not yet full so no decision.
        for (int i = 0; i < 100; i++) {
            t.observeRead(false);
        }
        assertEquals(2048, t.shouldResizeTo(2048),
                "sample window not full → no decision");
    }
}

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

import org.apache.flink.api.common.typeutils.base.LongSerializer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Parity tests for {@link ForStRsKeyGroupedSerializer#encodeForStateOffheap}. */
class KeyGroupedSerializerOffheapParityTest {

    @Test
    void offheapAndOnheapProduceSameBytes() {
        ForStRsKeyGroupedSerializer<Long> ser =
                new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        String stateName = "myState";
        byte[] stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        long userKey = 12345L;
        int keyGroup = 7;

        byte[] expected = ser.encodeForState(keyGroup, userKey, stateName);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scratch = arena.allocate(256);
            long packed = ser.encodeForStateOffheap(keyGroup, userKey, stateNameBytes, scratch, 0);
            int offset = (int) (packed >>> 32);
            int length = (int) (packed & 0xFFFFFFFFL);
            byte[] actual = new byte[length];
            MemorySegment.copy(scratch, ValueLayout.JAVA_BYTE, offset, actual, 0, length);
            assertArrayEquals(
                    expected,
                    actual,
                    "off-heap encoding must produce byte-identical output to on-heap");
        }
    }

    @Test
    void offheapPreservesStartOffset() {
        ForStRsKeyGroupedSerializer<Long> ser =
                new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        byte[] stateNameBytes = "x".getBytes(StandardCharsets.UTF_8);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scratch = arena.allocate(256);
            long packed = ser.encodeForStateOffheap(3, 99L, stateNameBytes, scratch, 100);
            int offset = (int) (packed >>> 32);
            assertEquals(100, offset, "returned offset must equal startOffset passed in");
        }
    }
}

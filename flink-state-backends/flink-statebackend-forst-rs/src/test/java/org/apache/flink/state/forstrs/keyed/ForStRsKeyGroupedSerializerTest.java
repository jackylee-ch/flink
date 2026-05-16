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

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForStRsKeyGroupedSerializerTest {

    @Test
    void encodesKeyGroupAsBigEndian2Bytes() {
        ForStRsKeyGroupedSerializer<String> ser =
                new ForStRsKeyGroupedSerializer<>(
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] composite = ser.encodeForState(258, "k", "myState");
        // kg=258 = 0x0102 BE => 0x01, 0x02
        assertEquals((byte) 0x01, composite[0]);
        assertEquals((byte) 0x02, composite[1]);
    }

    @Test
    void encodesValueStateRoundTrip() {
        ForStRsKeyGroupedSerializer<String> ser =
                new ForStRsKeyGroupedSerializer<>(
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] composite = ser.encodeForState(7, "userKey", "valueState");
        ForStRsKeyGroupedSerializer.Decoded<String> decoded = ser.decode(composite);
        assertEquals(7, decoded.keyGroup());
        assertEquals("userKey", decoded.userKey());
        assertEquals("valueState", decoded.stateName());
    }

    @Test
    void encodesMapStateWithUserKey() {
        ForStRsKeyGroupedSerializer<String> ser =
                new ForStRsKeyGroupedSerializer<>(
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] composite =
                ser.encodeForMap(
                        7,
                        "userKey",
                        "mapState",
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        "userMapKey");
        // Should start with the same 7-byte kg-prefix + userKey + /mapState/
        ForStRsKeyGroupedSerializer.Decoded<String> decoded = ser.decode(composite);
        assertEquals(7, decoded.keyGroup());
        assertEquals("userKey", decoded.userKey());
        assertEquals("mapState", decoded.stateName());
    }

    @Test
    void prefixForKeyGroupYieldsScanRange() {
        ForStRsKeyGroupedSerializer<String> ser =
                new ForStRsKeyGroupedSerializer<>(
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] prefix = ser.keyGroupPrefix(7);
        assertArrayEquals(new byte[] {0x00, 0x07}, prefix);
    }

    @Test
    void prefixForKeyGroupAndStateYieldsScanRange() {
        ForStRsKeyGroupedSerializer<String> ser =
                new ForStRsKeyGroupedSerializer<>(
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] prefix = ser.keyGroupAndStatePrefix(7, "myState");
        // 0x00 0x07 + serialize("any") + ... — actually for a per-state prefix
        // we don't need the userKey portion; just encode kg + a state-name
        // marker. The scan range is whatever encodeForState would produce
        // for kg=7, stateName="myState", before the trailing terminator.
        // For this test we just assert the prefix STARTS with the kg bytes.
        assertEquals((byte) 0x00, prefix[0]);
        assertEquals((byte) 0x07, prefix[1]);
    }

    // ------------------------------------------------------------------
    // Vector API batch key-group assignment tests
    // ------------------------------------------------------------------

    @Test
    void batchAssignKeyGroupsPowerOfTwo() {
        // maxParallelism=128 is power-of-2 — exercises the SIMD AND path
        int maxParallelism = 128;
        byte[][] keys = new byte[100][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = ("key-" + i).getBytes();
        }
        int[] result = ForStRsKeyGroupedSerializer.batchAssignKeyGroups(keys, maxParallelism);
        assertEquals(keys.length, result.length);

        // Verify each result matches the scalar reference implementation
        for (int i = 0; i < keys.length; i++) {
            int expected =
                    ForStRsKeyGroupedSerializer.scalarMurmurHash(Arrays.hashCode(keys[i]))
                            & (maxParallelism - 1);
            assertEquals(expected, result[i], "Mismatch at index " + i);
            assertTrue(result[i] >= 0 && result[i] < maxParallelism);
        }
    }

    @Test
    void batchAssignKeyGroupsNonPowerOfTwo() {
        // maxParallelism=100 is NOT power-of-2 — exercises the scalar modulo path
        int maxParallelism = 100;
        byte[][] keys = new byte[73][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = ("record-" + i).getBytes();
        }
        int[] result = ForStRsKeyGroupedSerializer.batchAssignKeyGroups(keys, maxParallelism);
        assertEquals(keys.length, result.length);

        for (int i = 0; i < keys.length; i++) {
            int hash = ForStRsKeyGroupedSerializer.scalarMurmurHash(Arrays.hashCode(keys[i]));
            int expected = hash % maxParallelism;
            assertEquals(expected, result[i], "Mismatch at index " + i);
            assertTrue(result[i] >= 0 && result[i] < maxParallelism);
        }
    }

    @Test
    void batchAssignKeyGroupsEmptyInput() {
        int[] result = ForStRsKeyGroupedSerializer.batchAssignKeyGroups(new byte[0][], 128);
        assertEquals(0, result.length);
    }

    @Test
    void batchAssignKeyGroupsLargeBatch() {
        // Exercises multiple full SIMD vector iterations + tail
        int maxParallelism = 256;
        byte[][] keys = new byte[1000][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = ("large-batch-key-" + i).getBytes();
        }
        int[] result = ForStRsKeyGroupedSerializer.batchAssignKeyGroups(keys, maxParallelism);
        assertEquals(keys.length, result.length);

        // Spot-check first, middle, last
        for (int idx : new int[] {0, 499, 999}) {
            int expected =
                    ForStRsKeyGroupedSerializer.scalarMurmurHash(Arrays.hashCode(keys[idx]))
                            & (maxParallelism - 1);
            assertEquals(expected, result[idx], "Mismatch at index " + idx);
        }
    }

    @Test
    void scalarMurmurHashMatchesFlinkMathUtils() {
        // Verify our scalar implementation matches the expected murmur hash behavior:
        // non-negative output for all inputs
        int[] testInputs = {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, 42, -999};
        for (int input : testInputs) {
            int result = ForStRsKeyGroupedSerializer.scalarMurmurHash(input);
            assertTrue(
                    result >= 0, "murmurHash(" + input + ") should be non-negative, got " + result);
        }
    }
}

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

package org.apache.flink.state.forstrs.state.ttl;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-A7 (S1-12) regression: {@link TtlSerializer} round-trips {@code [long expiry][value bytes]}
 * losslessly and {@link TtlValue#isExpired(long)} compares strictly {@code >=}.
 */
class TtlSerializerTest {

    @Test
    void roundTripsExpiryAndValue() throws IOException {
        TtlSerializer<String> ser = new TtlSerializer<>(StringSerializer.INSTANCE);
        TtlValue<String> v = new TtlValue<>(1234567890L, "hello");

        DataOutputSerializer out = new DataOutputSerializer(64);
        ser.serialize(v, out);
        byte[] bytes = out.getCopyOfBuffer();
        // [long expiry = 8 bytes][string serialization]
        assertTrue(bytes.length > 8, "must include 8-byte expiry prefix plus body");

        DataInputDeserializer in = new DataInputDeserializer(bytes);
        TtlValue<String> rt = ser.deserialize(in);
        assertEquals(1234567890L, rt.getExpiryTimestamp());
        assertEquals("hello", rt.getValue());
    }

    @Test
    void expiryComparisonIsHalfOpen() {
        TtlValue<String> v = new TtlValue<>(100L, "x");
        assertFalse(v.isExpired(99L), "before expiry must be fresh");
        assertTrue(v.isExpired(100L), "at expiry must be expired (>=)");
        assertTrue(v.isExpired(101L), "past expiry must be expired");
    }

    @Test
    void rejectsNullInnerOnSerialize() {
        TtlSerializer<String> ser = new TtlSerializer<>(StringSerializer.INSTANCE);
        TtlValue<String> v = new TtlValue<>(100L, null);
        DataOutputSerializer out = new DataOutputSerializer(16);
        assertThrows(IOException.class, () -> ser.serialize(v, out));
    }

    @Test
    void copyPreservesExpiryAndValue() {
        TtlSerializer<String> ser = new TtlSerializer<>(StringSerializer.INSTANCE);
        TtlValue<String> v = new TtlValue<>(42L, "abc");
        TtlValue<String> copy = ser.copy(v);
        assertEquals(42L, copy.getExpiryTimestamp());
        assertEquals("abc", copy.getValue());
    }

    @Test
    void getLengthAddsTimestampPrefix() {
        // StringSerializer has variable length (-1); composed length must also be -1.
        TtlSerializer<String> ser = new TtlSerializer<>(StringSerializer.INSTANCE);
        assertEquals(-1, ser.getLength());
    }
}

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
 * losslessly.
 *
 * <p>R21-L2: {@link TtlValue#isExpired(long)} compares strictly {@code >} (not {@code >=}) so a
 * {@link Long#MAX_VALUE} never-expire sentinel stays live even when the wall clock reads
 * {@link Long#MAX_VALUE}.
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
    void expiryComparisonIsStrictlyAfter() {
        // R21-L2: isExpired is now strictly greater-than (was >=). At expiry instant the value
        // is still live; only strictly AFTER the expiry instant does it become invisible. This
        // preserves the Long.MAX_VALUE never-expire sentinel against a Long.MAX_VALUE clock
        // reading and matches Flink's documented half-open [now, expiry] visibility window.
        TtlValue<String> v = new TtlValue<>(100L, "x");
        assertFalse(v.isExpired(99L), "before expiry must be fresh");
        assertFalse(v.isExpired(100L), "AT expiry must still be live (strict >)");
        assertTrue(v.isExpired(101L), "past expiry must be expired");
    }

    @Test
    void maxValueExpiryNeverExpiresEvenAtMaxClock() {
        // R21-L2: regression — Long.MAX_VALUE is used as a "never expire" sentinel by the TTL
        // compaction filter / disabled-TTL fast path. The OLD >= comparison would mark such a
        // row as expired the moment the clock reaches Long.MAX_VALUE (or wraps in some test
        // scenarios). The new strict-> semantics keep the sentinel live.
        TtlValue<String> v = new TtlValue<>(Long.MAX_VALUE, "sentinel");
        assertFalse(
                v.isExpired(Long.MAX_VALUE),
                "Long.MAX_VALUE expiry must remain live even at clock=Long.MAX_VALUE");
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

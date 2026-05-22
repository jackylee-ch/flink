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

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Regression test for PR-B3: V1-sync heap-path {@code update()} / {@code getAndUpdate()} reuse
 * the {@code DataOutputSerializer}'s shared internal buffer instead of allocating a fresh byte[]
 * per call. Verifies no aliasing bug occurs when consecutive {@code update(v1); update(v2);
 * read()} calls share the same backing buffer slot — the engine must capture each value's bytes
 * synchronously inside the critical-mode FFM call.
 */
class ValueStateUpdateNoSharedBufferAlias {

    private Arena linkerArena;
    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;

    @BeforeEach
    void setUp() {
        linkerArena = Arena.ofShared();
        linker = new ForStRsLinker(linkerArena);
        db = linker.dbOpenMemory(linkerArena);
        cf = linker.dbDefaultCf(db, linkerArena);
    }

    @AfterEach
    void tearDown() {
        cf.close();
        db.close();
        linkerArena.close();
    }

    /**
     * Two consecutive {@code update()} calls writing distinct values to the same key followed by a
     * read must return the second value. If the engine accidentally captured a pointer (rather than
     * the bytes) of the shared internal buffer, the first read would see the second value's bytes
     * partially overwriting the first row, or vice versa.
     */
    @Test
    void consecutiveUpdatesWithSharedBufferPreserveCorrectness() throws Exception {
        byte[] keyPrefix = new byte[] {1, 2, 3, 4};
        ForStRsValueState<Long> state =
                new ForStRsValueState<>(linker, db, cf, keyPrefix, LongSerializer.INSTANCE);

        state.update(1234567890L);
        // Second update on the same key — shares the same outputBuffer.getSharedBuffer() slot.
        state.update(9876543210L);

        assertEquals(9876543210L, state.value());
    }

    /**
     * Update-read-update-read sequence forces the read to interleave with writes. Each read MUST
     * see the most recent value: if the shared buffer aliased a still-pending write, the
     * deserialized value would carry stale or partial bytes.
     */
    @Test
    void updateReadInterleavedPreservesValues() throws Exception {
        byte[] keyPrefix = new byte[] {9, 8, 7, 6};
        ForStRsValueState<Long> state =
                new ForStRsValueState<>(linker, db, cf, keyPrefix, LongSerializer.INSTANCE);

        for (long v = 1; v <= 32; v++) {
            state.update(v);
            assertEquals(v, state.value(), "round " + v);
        }
    }

    /**
     * Variable-length values (string serializer) are the strongest stress on shared-buffer reuse:
     * a longer value followed by a shorter value would leave trailing bytes in the shared buffer.
     * The engine must use {@code outputBuffer.length()} (not {@code buffer.length}) to capture
     * only the valid prefix.
     */
    @Test
    void variableLengthValuesDoNotLeakTrailingBytes() throws Exception {
        byte[] keyPrefix = new byte[] {5, 5, 5, 5};
        ForStRsValueState<String> state =
                new ForStRsValueState<>(linker, db, cf, keyPrefix, StringSerializer.INSTANCE);

        state.update("a_very_long_string_value_to_grow_the_shared_buffer");
        assertEquals("a_very_long_string_value_to_grow_the_shared_buffer", state.value());

        // Shorter value: if the engine read past length(), it would include stale bytes from the
        // previous write and the string deserializer would either crash or return wrong bytes.
        state.update("short");
        assertEquals("short", state.value());

        // And back to long: re-grows correctly.
        state.update("medium_length_after_short");
        assertEquals("medium_length_after_short", state.value());
    }

    /**
     * {@code getAndUpdate} also runs through the shared-buffer path. The returned "old value" must
     * not be affected by the new value's bytes still residing in the shared buffer (the engine
     * copies the OLD value into a heap byte[] before returning, so the shared buffer is safe to
     * reuse on the next call).
     */
    @Test
    void getAndUpdateReturnsCorrectOldValueAcrossCalls() throws Exception {
        byte[] keyPrefix = new byte[] {7, 7, 7, 7};
        ForStRsValueState<Long> state =
                new ForStRsValueState<>(linker, db, cf, keyPrefix, LongSerializer.INSTANCE);

        // First call: no prior value.
        assertNull(state.getAndUpdate(111L));
        // Second call: should return the FIRST value, not the second.
        assertEquals(111L, state.getAndUpdate(222L));
        // Third call: should return 222L.
        assertEquals(222L, state.getAndUpdate(333L));
        // Final read confirms last write.
        assertEquals(333L, state.value());
    }
}

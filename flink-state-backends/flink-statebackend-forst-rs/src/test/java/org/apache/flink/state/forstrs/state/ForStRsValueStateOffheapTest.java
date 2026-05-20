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
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit tests for 1b.1 off-heap mode of {@link ForStRsValueState}. */
class ForStRsValueStateOffheapTest {

    private Arena linkerArena;
    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;
    private Arena scratchArena;
    private MemorySegment scratch;
    private long currentKey;
    private int currentKeyGroup;

    @BeforeEach
    void setUp() {
        linkerArena = Arena.ofShared();
        linker = new ForStRsLinker(linkerArena);
        db = linker.dbOpenMemory(linkerArena);
        cf = linker.dbDefaultCf(db, linkerArena);
        scratchArena = Arena.ofConfined();
        scratch = scratchArena.allocate(4096);
        currentKey = 42L;
        currentKeyGroup = 0;
    }

    @AfterEach
    void tearDown() {
        scratchArena.close();
        cf.close();
        db.close();
        linkerArena.close();
    }

    private ForStRsValueState<Long> newState(ArrowBinaryBuffer buf) {
        ForStRsKeyGroupedSerializer<Long> kgSer =
                new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        ArrowBinaryBufferAutoTuner tuner = new ArrowBinaryBufferAutoTuner(1024);
        return new ForStRsValueState<>(
                linker,
                db,
                cf,
                LongSerializer.INSTANCE,
                () -> scratch,
                kgSer,
                "myState",
                () -> currentKeyGroup,
                () -> currentKey,
                buf,
                tuner);
    }

    @Test
    void valueAfterUpdateReturnsValue() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsValueState<Long> state = newState(buf);
            state.update(100L);
            assertEquals(100L, state.value());
        } finally {
            buf.close();
        }
    }

    @Test
    void valueReturnsNullForUnseenKey() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsValueState<Long> state = newState(buf);
            assertNull(state.value());
        } finally {
            buf.close();
        }
    }

    @Test
    void clearRemovesValue() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsValueState<Long> state = newState(buf);
            state.update(5L);
            state.clear();
            assertNull(state.value());
        } finally {
            buf.close();
        }
    }

    @Test
    void valueAcrossDifferentKeysReturnsDifferentValues() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsValueState<Long> state = newState(buf);
            currentKey = 1L;
            state.update(100L);
            currentKey = 2L;
            state.update(200L);
            currentKey = 1L;
            assertEquals(100L, state.value());
            currentKey = 2L;
            assertEquals(200L, state.value());
        } finally {
            buf.close();
        }
    }
}

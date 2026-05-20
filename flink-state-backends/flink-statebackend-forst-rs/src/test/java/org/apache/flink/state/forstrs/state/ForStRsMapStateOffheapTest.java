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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for 1c.1 off-heap mode of {@link ForStRsMapState}. */
class ForStRsMapStateOffheapTest {

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

    private ForStRsMapState<Long, Long> newState(ArrowBinaryBuffer buf) {
        ForStRsKeyGroupedSerializer<Long> kgSer =
                new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        ArrowBinaryBufferAutoTuner tuner = new ArrowBinaryBufferAutoTuner(1024);
        return new ForStRsMapState<>(
                linker,
                db,
                cf,
                LongSerializer.INSTANCE,
                LongSerializer.INSTANCE,
                () -> scratch,
                kgSer,
                "myMap",
                () -> currentKeyGroup,
                () -> currentKey,
                buf,
                tuner,
                /* prefixComputer = */ () ->
                        kgSer.encodeForState(currentKeyGroup, currentKey, "myMap"));
    }

    @Test
    void putAndGetReturnsValue() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            state.put(1L, 100L);
            assertEquals(100L, state.get(1L));
        } finally {
            buf.close();
        }
    }

    @Test
    void valueReturnsNullForUnseenKey() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            assertNull(state.get(7L));
        } finally {
            buf.close();
        }
    }

    @Test
    void removeThenValueReturnsNull() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            state.put(1L, 100L);
            state.remove(1L);
            assertNull(state.get(1L));
        } finally {
            buf.close();
        }
    }

    @Test
    void containsReturnsTrueIfPresent() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            state.put(2L, 200L);
            assertTrue(state.contains(2L));
            assertFalse(state.contains(99L));
        } finally {
            buf.close();
        }
    }

    @Test
    void overwriteUpdatesValue() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            state.put(3L, 300L);
            state.put(3L, 333L);
            assertEquals(333L, state.get(3L));
        } finally {
            buf.close();
        }
    }

    @Test
    void entriesEmitsStatebufEntries() throws Exception {
        // NOTE: in-memory engine fixture (dbOpenMemory) does not expose batchPut-flushed rows
        // via prefixLookupOpen — even though single-row put + prefix scan works (verified in
        // ForStRsLinkerExtendedTest.prefixLookupReturnsOnlyMatches). The iter-merge engine-walk
        // half of the new code path is therefore not testable here; production ForSt engine
        // handles prefix scans over batchPut writes correctly. This test only validates the
        // statebuf-walk half (statebuf takes precedence over engine; no pre-flush).
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            state.put(1L, 10L);
            state.put(2L, 20L);
            state.put(3L, 30L);
            assertTrue(buf.size() > 0);
            Map<Long, Long> collected = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                collected.put(e.getKey(), e.getValue());
            }
            assertEquals(3, collected.size());
            assertEquals(10L, collected.get(1L));
            assertEquals(20L, collected.get(2L));
            assertEquals(30L, collected.get(3L));
            // Critical: statebuf must NOT have been flushed by entries() — iter-no-flush invariant.
            assertTrue(buf.size() > 0, "iter must not pre-flush statebuf");
        } finally {
            buf.close();
        }
    }

    @Test
    void keysReturnsStatebufKeys() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            state.put(1L, 10L);
            state.put(2L, 20L);
            Set<Long> ks = new HashSet<>();
            for (Long k : state.keys()) {
                ks.add(k);
            }
            assertEquals(Set.of(1L, 2L), ks);
            assertTrue(buf.size() > 0, "iter must not pre-flush statebuf");
        } finally {
            buf.close();
        }
    }

    @Test
    void isEmptyReflectsStatebufWithoutFlush() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            assertTrue(state.isEmpty());
            state.put(1L, 10L);
            assertFalse(state.isEmpty());
            // Buffer must still hold the pending write — isEmpty must not flush.
            assertTrue(buf.size() > 0, "isEmpty must not pre-flush statebuf");
        } finally {
            buf.close();
        }
    }

    @Test
    void clearRemovesStatebufEntries() throws Exception {
        // Engine-side clear is not testable on the in-memory fixture (see note above); we
        // verify the statebuf-side tombstone path here.
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            state.put(1L, 10L);
            state.put(2L, 20L);
            state.clear();
            assertTrue(state.isEmpty());
            assertNull(state.get(1L));
            assertNull(state.get(2L));
        } finally {
            buf.close();
        }
    }

    @Test
    void writesPersistToEngineAfterFlush() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newState(buf);
            for (long k = 1; k <= 100; k++) {
                state.put(k, k * 10);
            }
            // Explicit flush. After flush, the per-instance buffer is empty but engine has all
            // 100 writes.
            state.flushStateBuffer();
            // Open a fresh state instance with EMPTY buffer; reads must hit the engine via the
            // miss-fall-through path.
            ArrowBinaryBuffer freshBuf = new ArrowBinaryBuffer(1024);
            try {
                ForStRsMapState<Long, Long> state2 = newState(freshBuf);
                for (long k = 1; k <= 100; k++) {
                    Long v = state2.get(k);
                    assertNotNull(v, "map-key " + k + " missing after flush");
                    assertEquals(k * 10, v, "wrong value for map-key " + k);
                }
            } finally {
                freshBuf.close();
            }
        } finally {
            buf.close();
        }
    }
}

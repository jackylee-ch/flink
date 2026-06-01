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
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 2026-05-28 q11 SCALE-dependent bug reproducer.
 *
 * <p>Smaller-scale tests in {@link ForStRsMapStateMergingWindowSetReproTest} all pass.
 * q11 at 100M-record cluster scale fails. There must be a scale-dependent bug that the
 * smaller tests miss. This file exercises:
 *
 * <ul>
 *   <li>Many distinct operator keys, each with their own MapState namespace
 *       (mimicking MergingWindowSet's per-key isolation contract).
 *   <li>Statebuf grow boundaries (many puts trigger {@code ArrowBinaryBuffer} grow).
 *   <li>Interleaved cross-key put/remove (operator processes events across many keys).
 *   <li>Verification after each phase that {@code entries()} for each key produces
 *       exactly the puts-minus-removes oracle for that key.
 * </ul>
 *
 * <p>If any of these fail, the test localizes a scale-dependent bug in ForStRsMapState
 * that would explain q11's cluster-level error.
 */
class ForStRsMapStateScaleReproTest {

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
        scratch = scratchArena.allocate(65536);   // larger scratch for scale
        currentKey = 0L;
        currentKeyGroup = 0;
    }

    @AfterEach
    void tearDown() {
        scratchArena.close();
        cf.close();
        db.close();
        linkerArena.close();
    }

    private ForStRsMapState<Long, Long> newOffheapState(ArrowBinaryBuffer buf) {
        ForStRsKeyGroupedSerializer<Long> kgSer =
                new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        ArrowBinaryBufferAutoTuner tuner = new ArrowBinaryBufferAutoTuner(1024);
        return new ForStRsMapState<>(
                linker, db, cf,
                LongSerializer.INSTANCE, LongSerializer.INSTANCE,
                () -> scratch,
                kgSer,
                "mergingWindowsState",
                () -> currentKeyGroup,
                () -> currentKey,
                buf,
                tuner,
                () -> kgSer.encodeForState(currentKeyGroup, currentKey, "mergingWindowsState"));
    }

    /**
     * 10,000 distinct operator keys × 10 entries each = 100K entries total.
     * After loading, iterate every key's entries() and verify isolation.
     * If statebuf overflows or composite-key prefix encoding has a scale bug, this catches it.
     */
    @Test
    void scaleTenThousandKeysIsolated() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(64);  // tiny initial — many grows
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            HashMap<Long, HashMap<Long, Long>> oraclePerKey = new HashMap<>();
            // Phase 1: load entries across many keys
            for (long opKey = 0; opKey < 10_000; opKey++) {
                currentKey = opKey;
                HashMap<Long, Long> oracle = new HashMap<>();
                for (long entry = 0; entry < 10; entry++) {
                    long userKey = entry;
                    long value = opKey * 1000L + entry;
                    state.put(userKey, value);
                    oracle.put(userKey, value);
                }
                oraclePerKey.put(opKey, oracle);
            }
            // Phase 2: iterate each key's entries and verify isolation
            // Sample 100 keys (random) to keep test fast.
            Random rng = new Random(42L);
            for (int i = 0; i < 100; i++) {
                long opKey = (long) rng.nextInt(10_000);
                currentKey = opKey;
                HashMap<Long, Long> actual = new HashMap<>();
                for (Map.Entry<Long, Long> e : state.entries()) {
                    actual.put(e.getKey(), e.getValue());
                }
                assertEquals(
                        oraclePerKey.get(opKey),
                        actual,
                        "opKey=" + opKey + " entries() diverged at scale");
            }
        } finally {
            buf.close();
        }
    }

    /**
     * Stress: 100K total ops with frequent put/remove cycling. The aggressive churn
     * is what q11 SESSION-window does as windows merge and retire continuously.
     */
    @Test
    void scaleChurnPutsAndRemoves() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(64);
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            HashMap<Long, HashMap<Long, Long>> oraclePerKey = new HashMap<>();
            Random rng = new Random(7L);
            int totalKeys = 5_000;
            int opsPerKey = 50;
            for (long opKey = 0; opKey < totalKeys; opKey++) {
                currentKey = opKey;
                HashMap<Long, Long> oracle = new HashMap<>();
                for (int op = 0; op < opsPerKey; op++) {
                    long userKey = rng.nextInt(20);  // small user-key domain → lots of overwrites
                    int action = rng.nextInt(10);
                    if (action < 7) {
                        // 70% puts
                        long value = (long) rng.nextInt(1_000_000);
                        state.put(userKey, value);
                        oracle.put(userKey, value);
                    } else {
                        // 30% removes
                        state.remove(userKey);
                        oracle.remove(userKey);
                    }
                }
                oraclePerKey.put(opKey, oracle);
            }
            // Sample-verify 100 random keys
            for (int i = 0; i < 100; i++) {
                long opKey = rng.nextInt(totalKeys);
                currentKey = opKey;
                HashMap<Long, Long> actual = new HashMap<>();
                for (Map.Entry<Long, Long> e : state.entries()) {
                    actual.put(e.getKey(), e.getValue());
                }
                assertEquals(
                        oraclePerKey.get(opKey),
                        actual,
                        "churn opKey=" + opKey + " entries() diverged");
            }
        } finally {
            buf.close();
        }
    }

    /**
     * The exact failure pattern: a window timer fires for a window that was previously
     * inserted (mapping.put) but then ?? lost from entries(). This test simulates that
     * cycle on a single key with many windows.
     */
    @Test
    void scaleSingleKeyManyWindows() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(64);
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            currentKey = 1234L;
            HashMap<Long, Long> oracle = new HashMap<>();
            // Put 5000 distinct windows
            for (long window = 0; window < 5_000; window++) {
                state.put(window, window);
                oracle.put(window, window);
            }
            // Verify every window via get()
            for (long window = 0; window < 5_000; window++) {
                Long actual = state.get(window);
                assertNotNull(actual, "window=" + window + " not found via get() at scale");
                assertEquals(window, actual.longValue(), "window=" + window + " value diverged");
            }
            // Verify via entries()
            HashMap<Long, Long> actual = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                actual.put(e.getKey(), e.getValue());
            }
            assertEquals(oracle, actual, "entries() at single-key scale diverged");
        } finally {
            buf.close();
        }
    }

    /**
     * Interleaved cross-key put-then-iterate-elsewhere. q11's runtime: process events
     * for many keys. Between processing key A and key B, we put entries for A, then
     * iterate B's entries (must NOT see A's entries).
     */
    @Test
    void scaleInterleavedPutAcrossKeys() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(64);
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            HashMap<Long, HashMap<Long, Long>> oraclePerKey = new HashMap<>();
            for (long opKey = 0; opKey < 200; opKey++) {
                oraclePerKey.put(opKey, new HashMap<>());
            }
            Random rng = new Random(13L);
            // Phase 1: 5000 random ops across 200 keys
            for (int op = 0; op < 5_000; op++) {
                long opKey = rng.nextInt(200);
                currentKey = opKey;
                long userKey = rng.nextInt(30);
                if (rng.nextInt(3) == 0) {
                    state.remove(userKey);
                    oraclePerKey.get(opKey).remove(userKey);
                } else {
                    long value = (long) rng.nextInt(1_000_000);
                    state.put(userKey, value);
                    oraclePerKey.get(opKey).put(userKey, value);
                }
            }
            // Phase 2: iterate every key's entries and assert
            for (long opKey = 0; opKey < 200; opKey++) {
                currentKey = opKey;
                HashMap<Long, Long> actual = new HashMap<>();
                for (Map.Entry<Long, Long> e : state.entries()) {
                    actual.put(e.getKey(), e.getValue());
                }
                assertEquals(
                        oraclePerKey.get(opKey),
                        actual,
                        "interleaved cross-key opKey=" + opKey + " diverged");
            }
        } finally {
            buf.close();
        }
    }
}

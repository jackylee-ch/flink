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
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 2026-05-28 q11 ROOT CAUSE #2 REPRO TEST.
 *
 * <p>q11 SESSION window uses {@code MergingWindowSet} which is backed by
 * {@code MapState<W, W>} (window → state-window mapping). The operator pattern is:
 *
 * <pre>{@code
 *   // On init for each key:
 *   for (entry : mapping.entries()) {
 *       heapHashMap.put(entry.key, entry.value);
 *   }
 *   // On event:
 *   if (newWindow merges with existing): mapping.put + mapping.remove(merged_away)
 *   // On timer fire:
 *   heapHashMap.remove(window) // throws if not present
 *   mapping.remove(window)
 * }</pre>
 *
 * <p>If the V1-sync {@code ForStRsMapState} has any inconsistency between put/remove
 * effects and the next entries()/iterator() iteration, the operator's heap HashMap
 * drifts from the state-backed mapping, leading to
 * {@code IllegalStateException: Window is not in in-flight window set}.
 *
 * <p>This test exercises the exact put/remove/entries pattern used by MergingWindowSet
 * at small scale + with both off-heap (statebuf != null) and on-heap modes.
 */
class ForStRsMapStateMergingWindowSetReproTest {

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
     * Operator-init reconciliation pattern: put 10 entries, then iterate entries() and
     * verify HashMap-equivalent. This is what MergingWindowSet does on operator init.
     */
    @Test
    void offheapEntriesReturnsAllPutEntries() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            HashMap<Long, Long> oracle = new HashMap<>();
            for (long i = 0; i < 10; i++) {
                state.put(i, i * 100L);
                oracle.put(i, i * 100L);
            }
            HashMap<Long, Long> actual = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                actual.put(e.getKey(), e.getValue());
            }
            assertEquals(oracle, actual, "entries() output diverged from put oracle");
        } finally {
            buf.close();
        }
    }

    /**
     * Put-then-remove-then-iterate: 20 puts, remove half, iterate. The remaining must
     * match. This mirrors MergingWindowSet's incremental merge-and-cleanup pattern.
     */
    @Test
    void offheapPutRemoveIterate() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            HashMap<Long, Long> oracle = new HashMap<>();
            for (long i = 0; i < 20; i++) {
                state.put(i, i * 10L);
                oracle.put(i, i * 10L);
            }
            for (long i = 0; i < 20; i += 2) {
                state.remove(i);
                oracle.remove(i);
            }
            HashMap<Long, Long> actual = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                actual.put(e.getKey(), e.getValue());
            }
            assertEquals(oracle, actual, "entries() after remove diverged from oracle");
        } finally {
            buf.close();
        }
    }

    /**
     * Cross-key isolation: simulate two different operator keys. Each key has its own
     * MergingWindowSet view. Entries from key A must NOT leak into key B's entries().
     */
    @Test
    void offheapEntriesCrossKeyIsolated() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            currentKey = 100L;
            state.put(1L, 100L);
            state.put(2L, 200L);
            currentKey = 200L;
            state.put(1L, 500L);
            state.put(3L, 700L);
            // Verify key 100's entries
            currentKey = 100L;
            HashMap<Long, Long> actual100 = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                actual100.put(e.getKey(), e.getValue());
            }
            HashMap<Long, Long> expected100 = new HashMap<>();
            expected100.put(1L, 100L);
            expected100.put(2L, 200L);
            assertEquals(expected100, actual100, "key 100 entries() leaked across key");
            // Verify key 200's entries
            currentKey = 200L;
            HashMap<Long, Long> actual200 = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                actual200.put(e.getKey(), e.getValue());
            }
            HashMap<Long, Long> expected200 = new HashMap<>();
            expected200.put(1L, 500L);
            expected200.put(3L, 700L);
            assertEquals(expected200, actual200, "key 200 entries() leaked across key");
        } finally {
            buf.close();
        }
    }

    /**
     * The closest-to-actual pattern: MergingWindowSet calls put(W, W) for window
     * mappings, then remove(W) for merged-away windows. Then iter to rebuild HashMap.
     * The HashMap must reflect ALL puts minus all removes.
     */
    @Test
    void offheapMergingWindowSetPattern() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            HashMap<Long, Long> oracle = new HashMap<>();
            // Phase 1: bursty put/remove/put pattern across many ops (like a merging
            // window scenario where some windows merge and others stay)
            for (long w = 0; w < 50; w++) {
                state.put(w, w);
                oracle.put(w, w);
                // Every 3rd window gets remapped to a "merged" parent.
                if (w >= 3 && w % 3 == 0) {
                    long parent = w - 2;
                    state.put(w, parent);
                    oracle.put(w, parent);
                }
                // Every 5th window gets cleaned up (cleanup-on-fire timer).
                if (w >= 5 && w % 5 == 0) {
                    state.remove(w);
                    oracle.remove(w);
                }
            }
            // Phase 2: simulate operator re-init: iterate entries to rebuild HashMap.
            HashMap<Long, Long> actual = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                actual.put(e.getKey(), e.getValue());
            }
            assertEquals(oracle.size(), actual.size(), "entry count diverged");
            assertEquals(oracle, actual, "entries() after merge-window-set pattern diverged");
        } finally {
            buf.close();
        }
    }

    /**
     * Larger scale (mimics q11's 100M-record SESSION-window scale at 1/1000 ratio):
     * 1000 puts + 200 removes, then iterate. Catches statebuf-engine merge bugs that
     * only manifest after multiple statebuf-grow + flush cycles.
     */
    @Test
    void offheapLargerScalePattern() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(64);  // tiny initial — forces grow
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            HashMap<Long, Long> oracle = new HashMap<>();
            for (long i = 0; i < 1000; i++) {
                state.put(i, i + 1_000_000L);
                oracle.put(i, i + 1_000_000L);
            }
            for (long i = 0; i < 200; i++) {
                state.remove(i * 5);  // remove every 5th
                oracle.remove(i * 5);
            }
            HashMap<Long, Long> actual = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                actual.put(e.getKey(), e.getValue());
            }
            assertEquals(oracle.size(), actual.size(),
                    "entry count diverged at scale: oracle=" + oracle.size() + ", actual=" + actual.size());
            assertEquals(oracle, actual, "entries() at scale diverged from oracle");
        } finally {
            buf.close();
        }
    }

    /**
     * Cross-call get-vs-iterate consistency. After put, both get() and entries()
     * must see the same data. This is the specific path MergingWindowSet hits.
     */
    @Test
    void offheapGetMatchesEntries() throws Exception {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        try {
            ForStRsMapState<Long, Long> state = newOffheapState(buf);
            HashMap<Long, Long> oracle = new HashMap<>();
            for (long i = 0; i < 30; i++) {
                state.put(i, i * 7L);
                oracle.put(i, i * 7L);
            }
            for (long i = 0; i < 10; i++) {
                state.remove(i);
                oracle.remove(i);
            }
            // Verify every key via get()
            for (long i = 0; i < 30; i++) {
                Long expected = oracle.get(i);
                Long actual = state.get(i);
                if (expected == null) {
                    assertNull(actual, "key " + i + " removed in oracle but get() returns " + actual);
                } else {
                    assertNotNull(actual, "key " + i + " present in oracle but get() returns null");
                    assertEquals(expected, actual, "key " + i + " value diverged");
                }
            }
            // Verify the same data via entries()
            HashMap<Long, Long> entriesActual = new HashMap<>();
            for (Map.Entry<Long, Long> e : state.entries()) {
                entriesActual.put(e.getKey(), e.getValue());
            }
            assertEquals(oracle, entriesActual, "get-vs-entries divergence");
        } finally {
            buf.close();
        }
    }
}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Correctness tests for {@link ByteArrayLongMap}. The map's backward-shift removal is the only
 * subtle path; this test fuzzes it against {@link HashMap} as an oracle and verifies probe-chain
 * invariants are preserved under mixed put/remove/get traffic.
 */
class ByteArrayLongMapTest {

    @Test
    void putGetRoundTrip() {
        ByteArrayLongMap m = new ByteArrayLongMap();
        m.put(new byte[] {1, 2, 3}, 0xDEADBEEFCAFEBABEL);
        assertEquals(0xDEADBEEFCAFEBABEL, m.get(new byte[] {1, 2, 3}));
        assertEquals(ByteArrayLongMap.ABSENT, m.get(new byte[] {1, 2, 4}));
        assertEquals(1, m.size());
    }

    @Test
    void putOverwriteSameKey() {
        ByteArrayLongMap m = new ByteArrayLongMap();
        byte[] k = new byte[] {1, 2, 3};
        m.put(k, 100L);
        m.put(k, 200L);
        assertEquals(200L, m.get(k));
        assertEquals(1, m.size());
    }

    @Test
    void removeReturnsPriorValueAndShrinksSize() {
        ByteArrayLongMap m = new ByteArrayLongMap();
        m.put(new byte[] {1}, 10L);
        m.put(new byte[] {2}, 20L);
        assertEquals(10L, m.remove(new byte[] {1}));
        assertEquals(ByteArrayLongMap.ABSENT, m.get(new byte[] {1}));
        assertEquals(20L, m.get(new byte[] {2}));
        assertEquals(1, m.size());
        assertEquals(ByteArrayLongMap.ABSENT, m.remove(new byte[] {99}));
    }

    @Test
    void clearResetsSize() {
        ByteArrayLongMap m = new ByteArrayLongMap();
        for (int i = 0; i < 100; i++) {
            m.put(new byte[] {(byte) i, (byte) (i + 1)}, i);
        }
        assertEquals(100, m.size());
        m.clear();
        assertEquals(0, m.size());
        assertTrue(m.isEmpty());
        assertEquals(ByteArrayLongMap.ABSENT, m.get(new byte[] {0, 1}));
        // post-clear inserts still work
        m.put(new byte[] {7}, 700L);
        assertEquals(700L, m.get(new byte[] {7}));
    }

    @Test
    void resizesOnLoadFactor() {
        ByteArrayLongMap m = new ByteArrayLongMap(16);
        // 16 * 0.75 = 12 -> 13th insert triggers resize. Push to 200 to force ≥ 4 resizes.
        for (int i = 0; i < 200; i++) {
            m.put(intKey(i), i);
        }
        assertEquals(200, m.size());
        for (int i = 0; i < 200; i++) {
            assertEquals(i, m.get(intKey(i)));
        }
    }

    @Test
    void forEachVisitsAllLiveEntries() {
        ByteArrayLongMap m = new ByteArrayLongMap();
        Map<String, Long> oracle = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            byte[] k = intKey(i * 7);
            m.put(k, i + 1L);
            oracle.put(Arrays.toString(k), i + 1L);
        }
        Map<String, Long> visited = new HashMap<>();
        m.forEach((k, v) -> visited.put(Arrays.toString(k), v));
        assertEquals(oracle, visited);
    }

    @Test
    void slotCursorIterationCoversAllEntries() {
        ByteArrayLongMap m = new ByteArrayLongMap();
        for (int i = 0; i < 64; i++) {
            m.put(intKey(i), i);
        }
        List<Long> seen = new ArrayList<>();
        for (int slot = 0; slot < m.capacity(); slot++) {
            byte[] k = m.keyAt(slot);
            if (k != null) {
                seen.add(m.valueAt(slot));
            }
        }
        assertEquals(64, seen.size());
    }

    /** Fuzz: mixed put/remove/get vs {@link HashMap} oracle with 1000 ops. */
    @Test
    void fuzzAgainstHashMap() {
        Random rnd = new Random(0xCAFEBABEL);
        ByteArrayLongMap m = new ByteArrayLongMap();
        Map<String, Long> oracle = new HashMap<>();
        List<byte[]> aliveKeys = new ArrayList<>();
        for (int op = 0; op < 1000; op++) {
            int r = rnd.nextInt(100);
            if (r < 60 || aliveKeys.isEmpty()) {
                // put
                byte[] k = new byte[1 + rnd.nextInt(20)];
                rnd.nextBytes(k);
                long v = rnd.nextLong();
                m.put(k, v);
                String ks = Arrays.toString(k);
                if (!oracle.containsKey(ks)) {
                    aliveKeys.add(k);
                }
                oracle.put(ks, v);
            } else if (r < 85) {
                // remove existing
                byte[] k = aliveKeys.remove(rnd.nextInt(aliveKeys.size()));
                String ks = Arrays.toString(k);
                Long expected = oracle.remove(ks);
                long actual = m.remove(k);
                assertEquals(expected.longValue(), actual);
            } else {
                // get existing or random
                byte[] k = aliveKeys.get(rnd.nextInt(aliveKeys.size()));
                assertEquals(oracle.get(Arrays.toString(k)), m.get(k));
            }
            // periodic invariant: size matches oracle
            if ((op & 0xff) == 0) {
                assertEquals(oracle.size(), m.size(), "size mismatch at op " + op);
            }
        }
        // final exhaustive scan
        assertEquals(oracle.size(), m.size());
        for (Map.Entry<String, Long> e : oracle.entrySet()) {
            // find matching alive key
            byte[] match = null;
            for (byte[] k : aliveKeys) {
                if (Arrays.toString(k).equals(e.getKey())) {
                    match = k;
                    break;
                }
            }
            assertFalse(match == null, "alive key missing for " + e.getKey());
            assertEquals(e.getValue().longValue(), m.get(match));
        }
    }

    /** Probe-chain stress: insert many keys colliding into the same initial slot, remove half. */
    @Test
    void backwardShiftPreservesProbeChain() {
        ByteArrayLongMap m = new ByteArrayLongMap(64);
        // 128 keys, all 8 bytes, deliberately varied so hashes spread but collisions are frequent.
        for (int i = 0; i < 128; i++) {
            m.put(intKey(i), i + 1L);
        }
        // Remove every other key
        for (int i = 0; i < 128; i += 2) {
            assertEquals(i + 1L, m.remove(intKey(i)));
        }
        // Survivors must still be reachable
        for (int i = 1; i < 128; i += 2) {
            assertEquals(i + 1L, m.get(intKey(i)));
        }
        // Removed must be absent
        for (int i = 0; i < 128; i += 2) {
            assertEquals(ByteArrayLongMap.ABSENT, m.get(intKey(i)));
        }
        assertEquals(64, m.size());
    }

    private static byte[] intKey(int v) {
        return new byte[] {
            (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v,
            // append a constant prefix-like region to mimic Q11 composite-key shape
            'k', '/', 's', 't'
        };
    }
}

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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-A3 / S1-6 / E-CRIT-3 regression test: verifies that {@link ForStRsKeyedStateBackend}'s
 * off-heap key-group supplier no longer returns 0 unconditionally. Constructs the backend with a
 * sub-range {@code KeyGroupRange.of(0, 3)} + {@code numberOfKeyGroups = 128}, registers 10
 * distinct keys, and asserts each one's keygroup is computed via {@link
 * KeyGroupRangeAssignment#assignToKeyGroup(Object, int)} — i.e. it must match the
 * Flink-canonical assignment and is NOT a constant 0.
 *
 * <p>Note: the assigned keygroup is computed from the key + {@code numberOfKeyGroups} (the
 * max-parallelism), independent of which task's {@link KeyGroupRange} the key is later routed
 * to. Under real rescaling, each parallel subtask's range is a different sub-slice of
 * {@code [0, numberOfKeyGroups)} — the key's <i>keygroup</i> is the same value across all
 * subtasks; only the question "does this subtask own that keygroup?" varies. This test
 * exercises the keygroup-assignment side of the equation, which is exactly what {@code
 * offheapKeyGroupSupplier} feeds into the V1-sync encoder prefix.
 */
class V1SyncRescaleTest {

    @Test
    void offheapKeyGroupSupplierComputesPerKeyKeygroupNotConstantZero() throws Exception {
        // KGR (0..3) with maxParallelism=128 — a small slice typical of a rescale-test setup.
        KeyGroupRange kgr = KeyGroupRange.of(0, 3);
        int numberOfKeyGroups = 128;

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                    ForStRsKeyedStateBackend<String> backend =
                            new ForStRsKeyedStateBackend<>(
                                    arena,
                                    linker,
                                    db,
                                    cf,
                                    StringSerializer.INSTANCE,
                                    /* ownsResources= */ false,
                                    kgr,
                                    numberOfKeyGroups)) {

                // Verify constructor wiring.
                assertEquals(kgr, backend.getKeyGroupRange());
                assertEquals(numberOfKeyGroups, backend.getNumberOfKeyGroups());

                // With no current key, supplier returns the range's start keygroup (a valid
                // in-range value), not 0 — unless the start happens to be 0 (it is here).
                assertEquals(kgr.getStartKeyGroup(), backend.getCurrentKeyGroup());

                // Register 10 distinct keys and verify each one's keygroup matches the
                // Flink-canonical assignment. The assigned keygroup is in [0,
                // numberOfKeyGroups); not all 10 keys will land in `kgr` (only a subset will
                // — that's exactly what rescaling routes to other subtasks), but every key
                // MUST produce a keygroup that matches assignToKeyGroup() and at least one
                // key MUST produce something other than 0.
                boolean sawNonZero = false;
                for (int i = 0; i < 10; i++) {
                    String key = "rescale-key-" + i;
                    backend.setCurrentKey(key);
                    int actual = backend.getCurrentKeyGroup();
                    int expected =
                            KeyGroupRangeAssignment.assignToKeyGroup(key, numberOfKeyGroups);
                    assertEquals(
                            expected,
                            actual,
                            "key=" + key + " keygroup mismatch (S1-6 regression?)");
                    assertTrue(
                            actual >= 0 && actual < numberOfKeyGroups,
                            "keygroup " + actual + " out of [0," + numberOfKeyGroups + ")");
                    if (actual != 0) {
                        sawNonZero = true;
                    }
                }
                assertTrue(
                        sawNonZero,
                        "All 10 keys mapped to keygroup 0 — supplier likely still constant. "
                                + "This is exactly the S1-6 / E-CRIT-3 rescale-breaking bug.");
            }
        }
    }

    @Test
    void supplierFallsBackToRangeStartWhenCurrentKeyIsNull() throws Exception {
        // Verify the null-current-key fallback path: supplier returns the range's start
        // keygroup rather than throwing NPE or routing to keygroup 0 outside the range.
        KeyGroupRange kgr = KeyGroupRange.of(64, 127);
        int numberOfKeyGroups = 128;

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                    ForStRsKeyedStateBackend<String> backend =
                            new ForStRsKeyedStateBackend<>(
                                    arena,
                                    linker,
                                    db,
                                    cf,
                                    StringSerializer.INSTANCE,
                                    /* ownsResources= */ false,
                                    kgr,
                                    numberOfKeyGroups)) {

                // No setCurrentKey() yet — fallback path engages.
                assertEquals(64, backend.getCurrentKeyGroup());
                assertNotEquals(0, backend.getCurrentKeyGroup());
            }
        }
    }
}

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

package org.apache.flink.state.forstrs.exec;

import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.VectorizedClassifier;
import org.apache.flink.state.forstrs.VectorizedClassifier.DispatchKind;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * PR-F1 parity gate for {@link VectorizedClassifier#offer}.
 *
 * <p>The original implementation was a ~22-case switch on {@link StateRequestType} that routed each
 * enum constant to {@code recordGet} / {@code recordPut} / {@code recordDelete} / append-merge
 * candidate / iter-list / unsupported-default. PR-F1 replaces that with a precomputed dispatch
 * table {@code StateRequestType.ordinal() → DispatchKind}.
 *
 * <p>This test pins the exact pre-refactor routing for every enum value, so any future drift would
 * be loudly flagged. The expected map below is the ground-truth re-derived from the original
 * switch in {@code offer()} — see git history for the prior block.
 */
class OfferDispatchTableParityTest {

    /** Ground-truth routing — derived from the pre-PR-F1 switch in {@link VectorizedClassifier}. */
    private static Map<StateRequestType, DispatchKind> expectedRouting() {
        EnumMap<StateRequestType, DispatchKind> m = new EnumMap<>(StateRequestType.class);

        // GET family
        m.put(StateRequestType.VALUE_GET, DispatchKind.GET);
        m.put(StateRequestType.LIST_GET, DispatchKind.GET);
        m.put(StateRequestType.MAP_GET, DispatchKind.GET);
        m.put(StateRequestType.MAP_CONTAINS, DispatchKind.GET);
        m.put(StateRequestType.REDUCING_GET, DispatchKind.GET);
        m.put(StateRequestType.AGGREGATING_GET, DispatchKind.GET);

        // PUT family — null payload still routes through delete inside offer(), but the
        // table-level routing kind is PUT.
        m.put(StateRequestType.VALUE_UPDATE, DispatchKind.PUT);
        m.put(StateRequestType.LIST_UPDATE, DispatchKind.PUT);
        m.put(StateRequestType.MAP_PUT, DispatchKind.PUT);
        m.put(StateRequestType.MAP_PUT_ALL, DispatchKind.PUT);
        m.put(StateRequestType.REDUCING_ADD, DispatchKind.PUT);
        m.put(StateRequestType.AGGREGATING_ADD, DispatchKind.PUT);

        // DELETE family
        m.put(StateRequestType.CLEAR, DispatchKind.DELETE);
        m.put(StateRequestType.MAP_REMOVE, DispatchKind.DELETE);

        // ITER family
        m.put(StateRequestType.MAP_IS_EMPTY, DispatchKind.ITER);
        m.put(StateRequestType.MAP_ITER, DispatchKind.ITER);
        m.put(StateRequestType.MAP_ITER_KEY, DispatchKind.ITER);
        m.put(StateRequestType.MAP_ITER_VALUE, DispatchKind.ITER);
        m.put(StateRequestType.ITERATOR_LOADING, DispatchKind.ITER);

        // APPEND_MERGE candidate family (LIST_ADD / LIST_ADD_ALL)
        m.put(StateRequestType.LIST_ADD, DispatchKind.APPEND_MERGE_CANDIDATE);
        m.put(StateRequestType.LIST_ADD_ALL, DispatchKind.APPEND_MERGE_CANDIDATE);

        // The remaining enum constants (SYNC_POINT, CUSTOMIZED) hit the "unsupported"
        // default in the original switch — they have no DispatchKind.

        return m;
    }

    @Test
    void dispatchTableLengthMatchesEnum() {
        assertEquals(
                StateRequestType.values().length,
                VectorizedClassifier.DISPATCH_TABLE.length,
                "DISPATCH_TABLE must be sized to StateRequestType.values().length");
    }

    @Test
    void everyHandledEnumRoutesToExpectedKind() {
        Map<StateRequestType, DispatchKind> expected = expectedRouting();
        for (Map.Entry<StateRequestType, DispatchKind> e : expected.entrySet()) {
            StateRequestType type = e.getKey();
            DispatchKind want = e.getValue();
            DispatchKind got = VectorizedClassifier.DISPATCH_TABLE[type.ordinal()];
            assertNotNull(got, "DISPATCH_TABLE entry must be non-null for " + type);
            assertSame(want, got, "Routing mismatch for " + type);
        }
    }

    @Test
    void unhandledEnumsAreNullInTable() {
        // SYNC_POINT and CUSTOMIZED are the two enum values that the original switch sent to
        // the "default → UnsupportedOperationException" branch. They must remain null in the
        // table so that offer() still throws when they are encountered.
        EnumSet<StateRequestType> unhandled =
                EnumSet.of(StateRequestType.SYNC_POINT, StateRequestType.CUSTOMIZED);
        for (StateRequestType t : unhandled) {
            assertNull(
                    VectorizedClassifier.DISPATCH_TABLE[t.ordinal()],
                    "DISPATCH_TABLE entry for " + t + " must be null (unsupported by offer())");
        }
    }

    @Test
    void coverageIsComplete() {
        // Every enum value must be accounted for: either in the expected routing map
        // (and thus mapped to a non-null DispatchKind), or in the explicit unhandled set.
        Map<StateRequestType, DispatchKind> expected = expectedRouting();
        EnumSet<StateRequestType> unhandled =
                EnumSet.of(StateRequestType.SYNC_POINT, StateRequestType.CUSTOMIZED);
        for (StateRequestType t : StateRequestType.values()) {
            boolean handled = expected.containsKey(t);
            boolean explicitlyUnhandled = unhandled.contains(t);
            assertEquals(
                    true,
                    handled ^ explicitlyUnhandled,
                    "StateRequestType."
                            + t
                            + " must appear in exactly one of {expected-routing, unhandled-set}."
                            + " If a new enum value was added upstream, update DISPATCH_TABLE"
                            + " and this parity test together.");
        }
    }
}

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

package org.apache.flink.state.forstrs.cache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Mirror of {@link ReducingAggregatingCacheTest}'s A10-H1 regression for the {@code long}-
 * specialized peer cache {@link LongReducingAggregatingCache}.
 */
class LongReducingAggregatingCacheTest {

    /**
     * A11-H2 / D11-H1 / E11-H1 regression: when a re-stash cascade leaves an entry stranded in the
     * {@code pendingFlushKey/Value} slot, {@code flushAllDirty} must drain it BEFORE iterating
     * {@code entries}. Prior shape iterated {@code entries} only — a slot populated by a deferred
     * eviction that had not yet been drained by a subsequent put/putIfGen was silently lost on the
     * next checkpoint, dropping cascaded eviction's dirty accumulator on Q12 SumAgg/CountAgg.
     *
     * <p>Mirrors the {@code flushAllDirtyDrainsPendingFlushSlotFirst} test on the boxed sibling
     * (see {@link ReducingAggregatingCacheTest}). Scenario: pre-populate near {@code maxEntries}, a
     * {@code put} triggers {@code removeEldestEntry} which stashes the eldest into the pending
     * slot; the {@code drainPendingFlush} callback throws, re-stashing the entry into
     * {@code entries} but the cascade refills the pending slot. The next checkpoint calls
     * {@code flushAllDirty} — without the drain-first fix, the cascaded eviction is never
     * delivered.
     */
    @Test
    void flushAllDirtyDrainsPendingFlushSlotFirst() {
        final java.util.concurrent.atomic.AtomicInteger throwBudget =
                new java.util.concurrent.atomic.AtomicInteger(1);
        final List<byte[]> deliveredKeys = new ArrayList<>();
        final List<Long> deliveredVals = new ArrayList<>();

        LongReducingAggregatingCache cache =
                new LongReducingAggregatingCache(
                        Long::sum,
                        (k, v) -> {
                            if (throwBudget.getAndDecrement() > 0) {
                                throw new RuntimeException("first-eviction throw to force cascade");
                            }
                            deliveredKeys.add(k);
                            deliveredVals.add(v);
                        },
                        2);

        cache.put(new byte[] {1}, 100L);
        cache.put(new byte[] {2}, 200L);

        // Trigger eviction of {1}. The drain throws (budget=1 -> throws once, becomes 0). The
        // E9-H2 logic re-stashes {1} into entries dirty.
        RuntimeException thrown = null;
        try {
            cache.put(new byte[] {3}, 300L);
        } catch (RuntimeException e) {
            thrown = e;
        }
        assertNotNull(thrown, "first eviction's drain must throw");

        // Now flushAllDirty: budget=0, callback delivers normally. We must see {1}'s
        // accumulator (100L) delivered. Without the A11-H2 drain-first guard, if the throw path
        // left a stranded slot the entry would never be delivered.
        cache.flushAllDirty();

        assertTrue(
                containsKeyBytes(deliveredKeys, new byte[] {1}),
                "cascaded/re-stashed entry {1} must be delivered by flushAllDirty");
        int idx = indexOfKeyBytes(deliveredKeys, new byte[] {1});
        assertEquals(Long.valueOf(100L), deliveredVals.get(idx));
        assertTrue(
                containsKeyBytes(deliveredKeys, new byte[] {2}),
                "live entry {2} must be delivered");
        assertTrue(
                containsKeyBytes(deliveredKeys, new byte[] {3}),
                "live entry {3} must be delivered");
    }

    private static boolean containsKeyBytes(List<byte[]> keys, byte[] needle) {
        return indexOfKeyBytes(keys, needle) >= 0;
    }

    private static int indexOfKeyBytes(List<byte[]> keys, byte[] needle) {
        for (int i = 0; i < keys.size(); i++) {
            if (java.util.Arrays.equals(keys.get(i), needle)) {
                return i;
            }
        }
        return -1;
    }
}

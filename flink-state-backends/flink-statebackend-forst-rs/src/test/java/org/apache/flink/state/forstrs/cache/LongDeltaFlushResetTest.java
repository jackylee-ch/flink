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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OPT-N04 (A2 / J3) regression for {@link LongReducingAggregatingCache#flushDeltasAndReset}.
 *
 * <p>In delta mode the cache entry holds a PENDING delta. The barrier flush must emit that delta AND
 * reset the entry to {@code 0}, otherwise the already-flushed delta is re-folded on the next barrier
 * and the accumulator is double-counted. This test pins the difference between {@code
 * flushAllDirty()} (absolute-value semantics — value retained) and {@code flushDeltasAndReset()}
 * (delta semantics — value zeroed).
 */
class LongDeltaFlushResetTest {

    private static byte[] k(int i) {
        return new byte[] {(byte) i};
    }

    @Test
    void deltaIsZeroedAfterFlushSoSecondBarrierDoesNotDoubleCount() {
        List<Long> flushed = new ArrayList<>();
        LongReducingAggregatingCache cache =
                ReducingAggregatingCache.forLong(Long::sum, (kb, v) -> flushed.add(v));

        // Barrier 1: seed +5, fold +3 -> pending delta 8.
        cache.put(k(1), 5L);
        cache.tryFold(k(1), 3L);
        cache.flushDeltasAndReset();
        assertEquals(List.of(8L), flushed, "barrier-1 emits the accumulated delta 8");

        // Barrier 2 with NO new adds: entry is clean (delta reset to 0) -> nothing emitted.
        cache.flushDeltasAndReset();
        assertEquals(List.of(8L), flushed, "no new adds -> no re-emit (delta was reset to 0)");

        // Barrier 3: fold +2 onto the reset entry -> emits exactly 2 (NOT 10).
        assertTrue(cache.tryFold(k(1), 2L), "entry is still resident for the alloc-free fold");
        cache.flushDeltasAndReset();
        assertEquals(
                List.of(8L, 2L),
                flushed,
                "post-reset fold emits only the NEW delta 2 (no double-count of the flushed 8)");
    }

    @Test
    void flushAllDirtyRetainsValue_contrastWithDeltaReset() {
        // The legacy absolute-value semantic: flushAllDirty keeps the value, so a re-flush after a
        // fresh fold emits the running absolute total. This is why delta mode needs its own method.
        List<Long> flushed = new ArrayList<>();
        LongReducingAggregatingCache cache =
                ReducingAggregatingCache.forLong(Long::sum, (kb, v) -> flushed.add(v));
        cache.put(k(2), 10L);
        cache.flushAllDirty();
        cache.tryFold(k(2), 5L);
        cache.flushAllDirty();
        assertEquals(
                List.of(10L, 15L),
                flushed,
                "flushAllDirty retains the absolute value (10 then 15), unlike the delta reset");
    }
}

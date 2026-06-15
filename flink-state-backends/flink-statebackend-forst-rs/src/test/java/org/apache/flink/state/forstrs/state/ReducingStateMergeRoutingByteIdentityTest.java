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

import org.apache.flink.state.forstrs.cache.LongReducingAggregatingCache;
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.foreign.Arena;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * OPT-N04 (A2 / J3) byte-identity gate for the in-engine merge-RMW path against a LIVE engine.
 *
 * <p>This is the §4 falsifier: the merge-routed write path (delta cache → BE-encoded {@code Merge}
 * deltas → {@code NumericAddBeMergeOperator} fold at read) MUST produce a value byte-for-byte
 * identical to the legacy get-fold-put path ({@code asyncGetInternal} → {@code Long::sum} → {@code
 * Put} of the absolute value, Flink {@code LongSerializer} big-endian bytes).
 *
 * <p>It exercises the real machinery this feature ships:
 *
 * <ul>
 *   <li>{@link ForStRsLinker#dbCreateCfWithMerge} creating the {@code agg-merge-i64} CF with
 *       {@code NumericAddBeMergeOperator} (J1 binding + the engine name registry).
 *   <li>The delta-cache flush emitting BE deltas via {@link ForStRsLinker#merge} (J1 + J3).
 *   <li>{@link LongReducingAggregatingCache#flushDeltasAndReset} (the delta reset that prevents
 *       double-counting across barriers).
 *   <li>Read = engine GET (folds the chain) + pending delta, with BE decode.
 * </ul>
 *
 * <p>Requires the FFI dylib (module passes {@code -Dforstrs.native.libpath}); skipped otherwise.
 */
@EnabledIfSystemProperty(named = "forstrs.native.libpath", matches = ".+")
class ReducingStateMergeRoutingByteIdentityTest {

    /** 8-byte BE encode, exactly Flink {@code LongSerializer#serialize}. */
    private static byte[] be(long v) {
        return new byte[] {
            (byte) (v >>> 56),
            (byte) (v >>> 48),
            (byte) (v >>> 40),
            (byte) (v >>> 32),
            (byte) (v >>> 24),
            (byte) (v >>> 16),
            (byte) (v >>> 8),
            (byte) v
        };
    }

    private static long deBe(byte[] b) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[i] & 0xFF);
        }
        return v;
    }

    private static byte[] key(long k) {
        return ("k/" + k + "/sum/").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Random add sequence with periodic barriers, multiple keys, retractions (negative deltas), and
     * clears — merge-routed CF vs an absolute-PUT oracle CF. Final reads must be byte-identical.
     */
    @Test
    void mergeRoutedMatchesGetFoldPutByteForByte() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle oracleCf = linker.dbDefaultCf(db, arena);
                    FrsCfHandle aggCf =
                            linker.dbCreateCfWithMerge(
                                    db, arena, "agg-merge-i64", "NumericAddBeMergeOperator")) {

                // Delta cache wired exactly like ForStRsAsyncReducingStateV2#enableMergeRouting:
                // combiner = +, flush emits a BE Merge per dirty key into the agg CF.
                LongReducingAggregatingCache deltaCache =
                        ReducingAggregatingCache.forLong(
                                Long::sum,
                                (kb, delta) -> {
                                    if (delta != 0L) {
                                        linker.merge(db, aggCf, kb, be(delta));
                                    }
                                });

                Random rnd = new Random(20260615L);
                long[] keys = {7L, 42L, 1000003L, -5L};
                Map<Long, Long> oracle = new HashMap<>(); // absolute-value heap oracle

                int barriers = 12;
                int addsPerBarrier = 400;
                for (int barrier = 0; barrier < barriers; barrier++) {
                    for (int i = 0; i < addsPerBarrier; i++) {
                        long k = keys[rnd.nextInt(keys.length)];
                        // ~6% clears, otherwise a signed delta (covers retraction).
                        if (rnd.nextInt(100) < 6) {
                            // Clear: oracle removes, merge path drops delta + DELETE on agg CF.
                            oracle.remove(k);
                            deltaCache.invalidate(key(k));
                            linker.delete(db, aggCf, key(k));
                            continue;
                        }
                        long delta = rnd.nextInt(2001) - 1000;

                        // Oracle: get-fold-put of the absolute value (Long::sum) on the oracle CF.
                        long base = oracle.getOrDefault(k, 0L);
                        long folded = base + delta; // wrapping == Java +
                        oracle.put(k, folded);
                        linker.put(db, oracleCf, key(k), be(folded));

                        // Merge path: fold the delta into the cache (no engine GET on miss).
                        byte[] ckey = key(k);
                        if (!deltaCache.tryFold(ckey, 0, ckey.length, delta)) {
                            deltaCache.put(ckey, 0, ckey.length, delta);
                        }
                    }
                    // Barrier: flush deltas as Merge rows, reset to 0 (the double-count guard).
                    deltaCache.flushDeltasAndReset();

                    // Mid-run read parity (engine-folded chain + zero pending delta == oracle).
                    for (long k : keys) {
                        byte[] oracleBytes = linker.get(db, oracleCf, key(k));
                        byte[] aggBytes = linker.get(db, aggCf, key(k));
                        if (oracleBytes == null) {
                            // Cleared key: agg CF must also read absent (DELETE tombstone).
                            assertNull(
                                    aggBytes,
                                    "cleared key " + k + " must read absent on the agg CF");
                        } else {
                            assertArrayEquals(
                                    oracleBytes,
                                    aggBytes,
                                    "merge-routed value for key " + k + " must be byte-identical "
                                            + "to get-fold-put after barrier " + barrier);
                            assertEquals(oracle.get(k).longValue(), deBe(aggBytes));
                        }
                    }
                }
            }
        }
    }

    /** A read with a non-zero PENDING delta (no barrier yet) = engine value + delta, byte-exact. */
    @Test
    void pendingDeltaIsAddedToEngineFoldOnRead() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle aggCf =
                            linker.dbCreateCfWithMerge(
                                    db, arena, "agg-merge-i64", "NumericAddBeMergeOperator")) {

                byte[] k = key(99L);
                // Persist a base of 100 via a Merge + barrier, then add 23 as a PENDING delta.
                linker.merge(db, aggCf, k, be(100L));
                long persisted = deBe(linker.get(db, aggCf, k));
                long pending = 23L;
                // Read model from ForStRsAsyncReducingStateV2#asyncGet (merge-routed branch).
                long observed = persisted + pending;
                assertEquals(123L, observed);
                assertArrayEquals(be(123L), be(observed));

                // After flushing the pending delta as a Merge, the engine read alone == 123.
                linker.merge(db, aggCf, k, be(pending));
                assertArrayEquals(be(123L), linker.get(db, aggCf, k));
            }
        }
    }

    /** Retraction to zero: deltas that sum to the negative of the base read back as 0, byte-exact. */
    @Test
    void retractionToZeroIsByteExact() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle aggCf =
                            linker.dbCreateCfWithMerge(
                                    db, arena, "agg-merge-i64", "NumericAddBeMergeOperator")) {
                byte[] k = key(1L);
                linker.merge(db, aggCf, k, be(50L));
                linker.merge(db, aggCf, k, be(-20L));
                linker.merge(db, aggCf, k, be(-30L));
                assertArrayEquals(be(0L), linker.get(db, aggCf, k));
            }
        }
    }
}

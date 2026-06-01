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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 2026-05-28 REGRESSION REPRO TEST for q11 root cause #2 (V1-sync ReducingState RMW
 * read-after-write divergence).
 *
 * <p>q11 SESSION-window uses {@code mergingWindowsState} (ReducingState) to track time-window
 * merges. The operator calls {@code add(window)} repeatedly which triggers RMW:
 * {@code readValue() → reduce → writeValue()}. If the engine breaks read-your-own-write,
 * the cumulative reduce result diverges from what the operator's heap HashMap expects,
 * leading to {@code IllegalStateException: Window is not in in-flight window set} on
 * timer fire (see project_q11_correctness_regression_2026-05-28).
 *
 * <p>This test exercises ForStRsReducingState with a SUM reducer over many adds at the
 * same key and verifies the final value matches the arithmetic sum. If the test fails,
 * the engine's lookupKv-after-putSegment is broken (the suspected root cause).
 */
class ForStRsReducingStateRmwRepoTest {

    private Arena linkerArena;
    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;

    @BeforeEach
    void setUp() {
        linkerArena = Arena.ofShared();
        linker = new ForStRsLinker(linkerArena);
        db = linker.dbOpenMemory(linkerArena);
        cf = linker.dbDefaultCf(db, linkerArena);
    }

    @AfterEach
    void tearDown() {
        cf.close();
        db.close();
        linkerArena.close();
    }

    private ForStRsReducingState<Long> newSumState(byte[] keyPrefix) {
        // Long sum reducer — minimal stand-in for q11's mergingWindowsState reducer.
        return new ForStRsReducingState<>(
                linker, db, cf, keyPrefix, LongSerializer.INSTANCE, Long::sum);
    }

    /**
     * Core repro: 100 sequential {@code add(1L)} calls at the same key must produce
     * final value 100. If RMW read-after-write is broken, the cumulative sum will
     * diverge (typically returns 1L every time because the read sees no prior write).
     */
    @Test
    void rmwSumOf100AddsEqualsHundred() throws Exception {
        byte[] key = new byte[] {0x01, 0x02, 0x03, 0x04};
        ForStRsReducingState<Long> state = newSumState(key);
        for (int i = 0; i < 100; i++) {
            state.add(1L);
        }
        Long actual = state.get();
        assertNotNull(actual, "state has no value after 100 adds");
        assertEquals(100L, actual.longValue(), "cumulative sum diverged — engine RMW read-after-write inconsistency");
    }

    /**
     * Same key, ascending sequence — final value must equal arithmetic sum.
     */
    @Test
    void rmwSumOfArithmeticSeries() throws Exception {
        byte[] key = new byte[] {0x10, 0x11, 0x12, 0x13};
        ForStRsReducingState<Long> state = newSumState(key);
        long expected = 0L;
        for (int i = 1; i <= 50; i++) {
            state.add((long) i);
            expected += i;
        }
        Long actual = state.get();
        assertNotNull(actual, "state has no value after 50 adds");
        assertEquals(expected, actual.longValue(), "arithmetic series sum diverged — engine RMW read-after-write inconsistency");
    }

    /**
     * Interleaved RMW across two distinct keys — verify isolation. Even if RMW is broken
     * within a single key, this should still pass IF cross-key isolation is correct.
     */
    @Test
    void rmwTwoKeysIsolated() throws Exception {
        byte[] keyA = new byte[] {0x20};
        byte[] keyB = new byte[] {0x21};
        ForStRsReducingState<Long> stateA = newSumState(keyA);
        ForStRsReducingState<Long> stateB = newSumState(keyB);
        for (int i = 0; i < 10; i++) {
            stateA.add(1L);
            stateB.add(2L);
        }
        assertEquals(10L, stateA.get().longValue(), "keyA cumulative sum diverged");
        assertEquals(20L, stateB.get().longValue(), "keyB cumulative sum diverged");
    }

    /**
     * Verify single-write-then-read works. If THIS fails, the bug is in basic
     * put-then-get (not RMW specifically) — would narrow root cause further.
     */
    @Test
    void singleWriteThenReadReturnsValue() throws Exception {
        byte[] key = new byte[] {0x30};
        ForStRsReducingState<Long> state = newSumState(key);
        state.add(42L);
        Long actual = state.get();
        assertNotNull(actual, "state has no value after single add — write didn't persist");
        assertEquals(42L, actual.longValue(), "single put-then-get value diverged");
    }

    /**
     * Verify clear after adds resets the state.
     */
    @Test
    void clearAfterAddsReturnsNull() throws Exception {
        byte[] key = new byte[] {0x40};
        ForStRsReducingState<Long> state = newSumState(key);
        for (int i = 0; i < 10; i++) {
            state.add(1L);
        }
        state.clear();
        Long actual = state.get();
        // After clear, state should return null (no value).
        // If RMW read sees torn data, this might NOT return null — secondary signal.
        assertEquals(null, actual, "clear didn't remove value — read-after-delete inconsistency");
    }
}

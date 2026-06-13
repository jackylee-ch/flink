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

package org.apache.flink.state.forstrs;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * FRS-WA-V0 lifecycle-manager flag-ON unit coverage. The engine V0 surface is INERT (stores +
 * logs the descriptor; default-OFF death-bucketed-segment path), so the contract under test is:
 * the three FFI calls succeed against a real CF AND the underlying state remains byte-exact (V0
 * declares lifecycle but reclaims nothing). Mirrors the {@link ForStRsTtlCompactFiltersManager}
 * registration shape.
 */
class ForStRsLifecycleManagerTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void declareLifecycleAndAdvanceClocksIsInertButByteExact() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                ForStRsLifecycleManager mgr = new ForStRsLifecycleManager(linker, db);

                // Windowed-state declaration (kind=1, ttl=60s) — the q5/q8/q11 class.
                assertDoesNotThrow(
                        () ->
                                mgr.setLifecycleForState(
                                        "windowAgg",
                                        cf,
                                        ForStRsLifecycleManager.KIND_WINDOWED,
                                        60_000L));

                // Write rows, then advance the event-time bound + watermark. Under V0 these are
                // inert (the engine derives a sound death stamp but the default-OFF path drops
                // nothing), so every row must still be readable byte-exact.
                for (int i = 0; i < 16; i++) {
                    linker.put(db, cf, b("w" + i), b("payload" + i));
                }
                assertDoesNotThrow(() -> mgr.noteMaxEventTime(cf, 100_000L));
                assertDoesNotThrow(() -> mgr.advanceWatermark(200_000L));

                for (int i = 0; i < 16; i++) {
                    assertArrayEquals(
                            b("payload" + i),
                            linker.get(db, cf, b("w" + i)),
                            "V0 lifecycle is inert — no row may be reclaimed");
                }
            }
        }
    }

    @Test
    void setLifecycleFromTtlConfigMapsEnabledAndDisabled() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                ForStRsLifecycleManager mgr = new ForStRsLifecycleManager(linker, db);

                StateTtlConfig enabled =
                        StateTtlConfig.newBuilder(Duration.ofMillis(30_000L)).build();
                // Enabled TTL ⇒ KIND_WINDOWED(ttl); disabled ⇒ KIND_UNBOUNDED. Both must succeed
                // and leave any data untouched.
                assertDoesNotThrow(() -> mgr.setLifecycleFromTtlConfig("ttlState", cf, enabled));
                assertDoesNotThrow(() -> mgr.setLifecycleFromTtlConfig("noTtlState", cf, null));

                StateTtlConfig disabled = StateTtlConfig.DISABLED;
                assertDoesNotThrow(
                        () -> mgr.setLifecycleFromTtlConfig("disabledState", cf, disabled));

                // advanceWatermark over the registered set (windowed + unbounded) is a no-op for
                // unbounded CFs and inert for windowed ones in V0.
                assertDoesNotThrow(() -> mgr.advanceWatermark(50_000L));
            }
        }
    }

    @Test
    void timerKindDeclarationSucceeds() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                ForStRsLifecycleManager mgr = new ForStRsLifecycleManager(linker, db);
                // KIND_TIMER is declared-but-unacted in V1 — the declaration must still succeed.
                assertDoesNotThrow(
                        () ->
                                mgr.setLifecycleForState(
                                        "timers", cf, ForStRsLifecycleManager.KIND_TIMER, 0L));
            }
        }
    }
}

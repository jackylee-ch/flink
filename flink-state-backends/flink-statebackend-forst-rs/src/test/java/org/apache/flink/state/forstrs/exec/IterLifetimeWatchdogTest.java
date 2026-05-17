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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link IterLifetimeWatchdog}.
 *
 * <p>Real behavioral tests (handle idle-detection, max-lifetime breach) land in P5 when handles can
 * be opened against a real engine. Here we verify:
 *
 * <ul>
 *   <li>Watchdog is constructible, startable, and stoppable.
 *   <li>With no registered handles, no timeouts fire.
 *   <li>Stop is idempotent (no exception on second call).
 * </ul>
 */
class IterLifetimeWatchdogTest {

    @Test
    void watchdogConstructibleAndStartable() throws Exception {
        SlotArenaScope scope = SlotArenaScope.openForSlot(1 << 20, 1 << 20);
        IterLifetimeWatchdog wd = new IterLifetimeWatchdog(scope, 30_000, 300_000);
        wd.start();
        Thread.sleep(100); // let one sweep run
        assertEquals(0, wd.idleTimeoutsCount(), "no handles — no idle timeouts expected");
        assertEquals(
                0, wd.maxLifetimeAbortsCount(), "no handles — no max-lifetime aborts expected");
        wd.stop();
        scope.closeSlot();
    }

    @Test
    void watchdogStopIsIdempotent() throws Exception {
        SlotArenaScope scope = SlotArenaScope.openForSlot(1 << 20, 1 << 20);
        IterLifetimeWatchdog wd = new IterLifetimeWatchdog(scope, 30_000, 300_000);
        wd.start();
        wd.stop();
        wd.stop(); // second stop must not throw
        scope.closeSlot();
    }

    @Test
    void watchdogExposesConfiguredThresholds() {
        SlotArenaScope scope = SlotArenaScope.openForSlot(1 << 20, 1 << 20);
        IterLifetimeWatchdog wd = new IterLifetimeWatchdog(scope, 12_345L, 67_890L);
        assertEquals(12_345L, wd.idleTimeoutMs());
        assertEquals(67_890L, wd.maxLifetimeMs());
        scope.closeSlot();
    }

    @Test
    void defaultConstructorUsesDefaultThresholds() {
        SlotArenaScope scope = SlotArenaScope.openForSlot(1 << 20, 1 << 20);
        IterLifetimeWatchdog wd = new IterLifetimeWatchdog(scope);
        assertEquals(IterLifetimeWatchdog.DEFAULT_IDLE_TIMEOUT_MS, wd.idleTimeoutMs());
        assertEquals(IterLifetimeWatchdog.DEFAULT_MAX_LIFETIME_MS, wd.maxLifetimeMs());
        scope.closeSlot();
    }

    @Test
    void startIsIdempotent() throws Exception {
        SlotArenaScope scope = SlotArenaScope.openForSlot(1 << 20, 1 << 20);
        IterLifetimeWatchdog wd = new IterLifetimeWatchdog(scope, 30_000, 300_000);
        wd.start();
        wd.start(); // second start must not throw or create a second thread
        wd.stop();
        scope.closeSlot();
    }
}

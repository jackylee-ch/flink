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

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * STAGE-1 contract tests for the two-regime dispatch policy at the {@link RegimeSwitch} level
 * (the executor-level dispatch itself is covered by {@code RoutingStateExecutorAsyncTest}'s
 * stub-worker scaffolding plus the q8 e2e gates; these tests pin the policy invariants the
 * dispatch relies on).
 */
class TwoRegimeDispatchTest {

    @Test
    void heavyEntryAndExitTrackOutstandingExactly() {
        RegimeSwitch rs = new RegimeSwitch();
        assertThat(rs.isLight()).isTrue();
        rs.batchDispatched();
        rs.batchDispatched();
        assertThat(rs.isLight()).isFalse();
        assertThat(rs.outstanding()).isEqualTo(2);
        rs.batchSettled();
        assertThat(rs.isLight()).isFalse();
        rs.batchSettled();
        assertThat(rs.isLight()).isTrue();
    }

    @Test
    void transitionHookSealsExactlyOncePerHeavyEntry() {
        RegimeSwitch rs = new RegimeSwitch();
        AtomicInteger seals = new AtomicInteger();
        rs.setOnHeavyTransition(seals::incrementAndGet);
        // L→H: seal fires before the first heavy batch only.
        rs.batchDispatched();
        rs.batchDispatched();
        rs.batchDispatched();
        assertThat(seals.get()).isEqualTo(1);
        rs.batchSettled();
        rs.batchSettled();
        rs.batchSettled();
        // Back to LIGHT, then H again: a fresh seal.
        rs.batchDispatched();
        assertThat(seals.get()).isEqualTo(2);
    }

    @Test
    void settleFromForeignThreadIsVisibleToMailboxReads() throws Exception {
        RegimeSwitch rs = new RegimeSwitch();
        rs.batchDispatched();
        Thread worker = new Thread(rs::batchSettled, "worker");
        worker.start();
        worker.join();
        // Mailbox-side read observes the worker's settle (AtomicInteger visibility).
        assertThat(rs.isLight()).isTrue();
    }
}

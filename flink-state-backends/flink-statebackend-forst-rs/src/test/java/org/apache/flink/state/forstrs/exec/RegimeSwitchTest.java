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

/** Contract for {@link RegimeSwitch}: light iff outstanding == 0; L→H hook fires once per transition. */
class RegimeSwitchTest {

    @Test
    void lightIffOutstandingZero() {
        RegimeSwitch rs = new RegimeSwitch();
        assertThat(rs.isLight()).isTrue();
        rs.batchDispatched();
        assertThat(rs.isLight()).isFalse();
        rs.batchSettled();
        assertThat(rs.isLight()).isTrue();
    }

    @Test
    void transitionHookFiresExactlyOncePerLtoH() {
        RegimeSwitch rs = new RegimeSwitch();
        AtomicInteger seals = new AtomicInteger();
        rs.setOnHeavyTransition(seals::incrementAndGet);
        rs.batchDispatched(); // L→H: hook fires
        rs.batchDispatched(); // already H: no hook
        assertThat(seals.get()).isEqualTo(1);
        rs.batchSettled();
        rs.batchSettled(); // back to L
        rs.batchDispatched(); // L→H again
        assertThat(seals.get()).isEqualTo(2);
    }
}

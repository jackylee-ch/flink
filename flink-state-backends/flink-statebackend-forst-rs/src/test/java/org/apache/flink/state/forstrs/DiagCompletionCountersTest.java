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

import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit contract for {@link DiagCompletionCounters}: offered/completed/failed tallies + report. */
class DiagCompletionCountersTest {

    @Test
    void talliesAndReportsImbalance() {
        DiagCompletionCounters.resetForTests();
        DiagCompletionCounters.offered(StateRequestType.LIST_ADD);
        DiagCompletionCounters.offered(StateRequestType.LIST_ADD);
        DiagCompletionCounters.offered(StateRequestType.LIST_GET);
        DiagCompletionCounters.completed(StateRequestType.LIST_ADD);
        DiagCompletionCounters.failed(StateRequestType.LIST_GET);
        String report = DiagCompletionCounters.report();
        // LIST_ADD: offered 2, completed 1 → imbalance flagged with a leading '!'
        assertThat(report).contains("!LIST_ADD off=2 done=1 fail=0");
        // LIST_GET: offered 1, failed 1 → balanced (done+fail == off), no '!'
        assertThat(report).contains(" LIST_GET off=1 done=0 fail=1");
    }

    @Test
    void balancedTypeHasNoImbalanceMarker() {
        DiagCompletionCounters.resetForTests();
        DiagCompletionCounters.offered(StateRequestType.MAP_PUT);
        DiagCompletionCounters.completed(StateRequestType.MAP_PUT);
        assertThat(DiagCompletionCounters.report()).contains(" MAP_PUT off=1 done=1 fail=0");
        assertThat(DiagCompletionCounters.report()).doesNotContain("!MAP_PUT");
    }
}

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

package org.apache.flink.state.forstrs.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.state.forstrs.VectorizedStateRequest;
import org.apache.flink.state.forstrs.ffm.FrsErrorCode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DispatchMetricsTest {

    @Test
    void recordDispatchIncrementsPerKindCounters() {
        MetricGroup root = new UnregisteredMetricsGroup();
        DispatchMetrics m = new DispatchMetrics(root);
        m.recordDispatch(VectorizedStateRequest.Kind.GET, "myState", 32, 1024, 500_000);
        m.recordDispatch(VectorizedStateRequest.Kind.GET, "myState", 16, 512, 300_000);
        assertEquals(2, m.dispatchCountFor(VectorizedStateRequest.Kind.GET, "myState"));
        assertEquals(48, m.dispatchRowsFor(VectorizedStateRequest.Kind.GET, "myState"));
    }

    @Test
    void cardinalityCappedAt128() {
        MetricGroup root = new UnregisteredMetricsGroup();
        DispatchMetrics m = new DispatchMetrics(root);
        // Push past the 128 cap
        for (int i = 0; i < 130; i++) {
            m.recordDispatch(VectorizedStateRequest.Kind.GET, "state-" + i, 1, 100, 1000);
        }
        assertTrue(m.cardinalityCappedCount() >= 1, "cardinality_capped must fire above 128");
        // The 130th state-name should land in the overflow bucket
        assertTrue(m.overflowCountFor(VectorizedStateRequest.Kind.GET) >= 1);
    }

    @Test
    void recordFfiErrorIncrementsErrorCounter() {
        MetricGroup root = new UnregisteredMetricsGroup();
        DispatchMetrics m = new DispatchMetrics(root);
        m.recordFfiError(VectorizedStateRequest.Kind.PUT, "putState", FrsErrorCode.ENGINE_IO);
        m.recordFfiError(VectorizedStateRequest.Kind.PUT, "putState", FrsErrorCode.KEY_TOO_LARGE);
        assertEquals(2, m.ffiErrorsFor(VectorizedStateRequest.Kind.PUT, "putState"));
    }

    @Test
    void iterMetricsRegistered() {
        MetricGroup root = new UnregisteredMetricsGroup();
        DispatchMetrics m = new DispatchMetrics(root);
        m.recordIterHandlesLeaked(3);
        m.recordIdleTimeout();
        m.recordIdleTimeout();
        m.recordMaxLifetimeAbort();
        assertEquals(3, m.iterHandlesLeakedCount());
        assertEquals(2, m.idleTimeoutsCount());
        assertEquals(1, m.maxLifetimeAbortsCount());
    }
}

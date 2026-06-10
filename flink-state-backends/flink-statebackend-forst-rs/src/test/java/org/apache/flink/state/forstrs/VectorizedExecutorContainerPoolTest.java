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

import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PR-1 (coordinated executor) prerequisite: two OUTSTANDING request containers must be distinct
 * classifier instances with distinct fill-side buffers, so the mailbox can fill batch N+1 while
 * batch N executes on a worker thread (the depth&gt;1 blocker documented in {@link
 * VectorizedExecutor#executeBatchRequests}). A released container is pooled and reused so the
 * steady state allocates nothing per batch (PERF-RESTORE-#0 preserved).
 */
class VectorizedExecutorContainerPoolTest {

    @Test
    void twoOutstandingContainersAreDistinct() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();
            VectorizedExecutor exec = new VectorizedExecutor(linker, db, cf, arena);

            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> a = exec.createRequestContainer();
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> b = exec.createRequestContainer();
            assertThat(b)
                    .as("second container created while the first is outstanding must be a new instance")
                    .isNotSameAs(a);
        }
    }

    @Test
    void releasedContainerIsReused() {
        try (Arena arena = Arena.ofConfined()) {
            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();
            VectorizedExecutor exec = new VectorizedExecutor(linker, db, cf, arena);

            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> a = exec.createRequestContainer();
            exec.releaseRequestContainer(a);
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> b = exec.createRequestContainer();
            assertThat(b).as("released container must be pooled and reused").isSameAs(a);
        }
    }
}

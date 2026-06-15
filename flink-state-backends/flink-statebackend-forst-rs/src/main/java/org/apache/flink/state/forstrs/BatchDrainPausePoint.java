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

import java.util.function.Consumer;

/**
 * TEST-ONLY deterministic pause-point seam for the worker-drain path (Stage-0 §4, two-regime
 * executor design 2026-06-11 §3.3). Lets a unit/integration test force the exact
 * {@code LIST_ADD-vs-ITER-vs-CLEAR} interleaving across the mailbox→worker boundary WITHOUT
 * relying on timing — the same shape as the {@code MapStateCacheConcurrentCorruptionTest}
 * {@code CyclicBarrier} seam, lifted one layer into the executor.
 *
 * <p><b>Zero production effect when not engaged.</b> {@link VectorizedExecutor#executeBatchRequests}
 * calls {@link #beforeApply(VectorizedClassifier)} exactly once per batch, right BEFORE it applies
 * the batch's writes to the engine (the point where a worker thread is about to drain queued
 * LIST_ADD / PUT / DELETE rows). The default {@link #HOOK} is {@code null}, so the call is a single
 * volatile read + null check on the hot path — no allocation, no lock, no behavioral change. A test
 * arms a hook that blocks the worker at this point, then releases it after it has set up the racing
 * mailbox-side read; the production path never arms a hook.
 *
 * <p>The seam is intentionally placed before write-application (not after) so a test can observe the
 * window where the writes are <em>queued/in-flight on the worker</em> but <em>not yet visible to the
 * engine</em> — exactly the read-your-writes hazard the q8 residual op-mix race lives in.
 */
final class BatchDrainPausePoint {

    private BatchDrainPausePoint() {}

    /**
     * Armed by a test via {@link #arm(Consumer)}; {@code null} on the production path. {@code
     * volatile} so a worker thread observes the test-thread's arm/disarm without extra
     * synchronization. The consumer receives the classifier whose batch is about to be applied so a
     * test can gate only the batch it cares about (e.g., the one carrying its LIST_ADD rows).
     */
    private static volatile Consumer<VectorizedClassifier> hook;

    /** Invoked once per batch immediately before its writes are applied. No-op unless armed. */
    static void beforeApply(VectorizedClassifier classifier) {
        Consumer<VectorizedClassifier> h = hook;
        if (h != null) {
            h.accept(classifier);
        }
    }

    /** TEST-ONLY: install the pause-point hook. Pass {@code null} to disarm. */
    static void arm(Consumer<VectorizedClassifier> h) {
        hook = h;
    }

    /** TEST-ONLY: remove the pause-point hook (production no-op state). */
    static void disarm() {
        hook = null;
    }
}

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

import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers;
import org.apache.flink.state.forstrs.BatchedFailurePropagationTestHelpers.RecordingFuture;
import org.apache.flink.state.forstrs.VectorizedExecutor;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * R2a executor batch micro-bench (NOT NexMark) — isolates the ONE thing R2a changes: the
 * mailbox→worker→mailbox thread handoff per batch. The engine cost is held constant by a stub
 * worker that does a fixed amount of work per batch, so the delta between arms is PURELY the
 * dispatch mechanism:
 *
 * <ul>
 *   <li><b>iter-batch routed</b> — every batch fans out to the worker threads (the q11 lever; the
 *       baseline R2a uses for ITER batches);
 *   <li><b>iter-free inline (R2a carve-out)</b> — every batch runs inline on the caller thread,
 *       zero handoff (the q17 carve-out R2a uses for ITER-FREE batches).
 * </ul>
 *
 * <p>Each measured iteration is a full batch round-trip; the routed arm pays a real cross-thread
 * submit+block on a warm single-thread executor (the handoff R2a removes for iter-free batches),
 * the inline arm a direct same-thread call. Reports the MIN of several passes (least-noise). No
 * native FFI, so it runs anywhere. Run with: {@code java ... RoutingAdaptiveR2aMicroBench [iters]}
 * (a {@code main}); it has no JUnit annotations so it never runs in the normal {@code mvn test}
 * gate.
 *
 * <p><b>Interpretation caveat:</b> with a do-nothing stub the per-batch dispatch cost is only a few
 * microseconds and sits close to thread-scheduling jitter, so the absolute numbers are indicative,
 * not precise. The DETERMINISTIC, load-bearing evidence for the carve-out is the dispatch-count
 * assertions in {@link RoutingAdaptiveR2aTest} (iter-free ⇒ ZERO worker dispatch; iter ⇒ N worker
 * dispatches) plus the prior e2e measurements (q17 271s offloaded vs 77s inline; q11 318.9→135.7s
 * routed) — the engine cost that makes the handoff matter is absent from this pure-Java harness.
 */
public final class RoutingAdaptiveR2aMicroBench {

    private RoutingAdaptiveR2aMicroBench() {}

    private static final int WORKERS = 3;

    // The q17 hot shape is a SINGLE key-group per batch of CHEAP point-RMW ops (cache-resident
    // get+put). Modelling it as one key-group with a near-zero-cost stub isolates the ONE thing R2a
    // changes: the mailbox→worker→mailbox thread handoff. A multi-kg batch would confound the
    // measurement with parallel-vs-serial execution of the per-batch work (not the q17 regime,
    // where the work is too small to amortize a 3-thread fan-out — the handoff dominates).
    private static final int KEY_GROUPS_PER_BATCH = 1;

    /** Stub worker doing the minimum a cache-resident point batch would: nothing measurable. */
    private static final class SpinWorker extends VectorizedExecutor {
        SpinWorker(ForStRsLinker linker, Arena arena) {
            super(
                    linker,
                    BatchedFailurePropagationTestHelpers.stubDb(),
                    BatchedFailurePropagationTestHelpers.stubCf(),
                    arena);
        }

        @Override
        public CompletableFuture<Void> executeBatchRequests(
                AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
            // Near-zero work: the q17 point-RMW batch is cache-resident, so the handoff cost is the
            // dominant per-batch term R2a's carve-out removes.
            return CompletableFuture.completedFuture(null);
        }
    }

    private static SpinWorker[] newWorkers(Arena arena) {
        ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
        SpinWorker[] ws = new SpinWorker[WORKERS];
        for (int i = 0; i < WORKERS; i++) {
            ws[i] = new SpinWorker(linker, arena);
        }
        return ws;
    }

    private static AsyncRequestContainer<StateRequest<?, ?, ?, ?>> batch(RoutingStateExecutor ex) {
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> c = ex.createRequestContainer();
        for (int kg = 0; kg < KEY_GROUPS_PER_BATCH; kg++) {
            c.offer(
                    BatchedFailurePropagationTestHelpers.newRequest(
                            StateRequestType.VALUE_GET,
                            null,
                            ("k" + kg).getBytes(StandardCharsets.UTF_8),
                            null,
                            new RecordingFuture<>(),
                            kg));
        }
        return c;
    }

    /** Runs {@code iters} batch round-trips with the iter verdict forced to {@code iters}-mode. */
    private static long timeArm(boolean forceIters, int iters) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            SpinWorker[] ws = newWorkers(arena);
            RoutingStateExecutor ex =
                    new RoutingStateExecutor(ws, false, false, /* routingAdaptive= */ true);
            ex.setIterVerdictOverrideForTest(forceIters);
            // warm-up (a full iters pass so the worker threads and JIT are hot/steady)
            for (int i = 0; i < iters; i++) {
                ex.executeBatchRequests(batch(ex)).get(30, TimeUnit.SECONDS);
            }
            long t0 = System.nanoTime();
            for (int i = 0; i < iters; i++) {
                ex.executeBatchRequests(batch(ex)).get(30, TimeUnit.SECONDS);
            }
            long elapsed = System.nanoTime() - t0;
            ex.shutdown();
            return elapsed;
        }
    }

    public static void main(String[] args) throws Exception {
        int iters = args.length > 0 ? Integer.parseInt(args[0]) : 200_000;
        // Two passes each to expose variance; report the better (least-noisy) of each arm.
        long routedBest = Long.MAX_VALUE;
        long inlineBest = Long.MAX_VALUE;
        for (int pass = 0; pass < 3; pass++) {
            long routed = timeArm(/* forceIters= */ true, iters); // iter batch → worker fan-out
            long inline = timeArm(/* forceIters= */ false, iters); // iter-free → inline carve-out
            routedBest = Math.min(routedBest, routed);
            inlineBest = Math.min(inlineBest, inline);
            System.out.printf(
                    "pass %d: routed(fan-out)=%.2f us/batch  inline(carve-out)=%.2f us/batch%n",
                    pass, routed / 1e3 / iters, inline / 1e3 / iters);
        }
        double routedUs = routedBest / 1e3 / iters;
        double inlineUs = inlineBest / 1e3 / iters;
        System.out.printf("%n=== R2a executor batch micro-bench (%d iters/arm) ===%n", iters);
        System.out.printf("iter-batch ROUTED (worker fan-out, q11 path): %.3f us/batch%n", routedUs);
        System.out.printf("iter-free INLINE (R2a q17 carve-out):         %.3f us/batch%n", inlineUs);
        System.out.printf(
                "handoff tax removed by the carve-out:         %.3f us/batch (%.1f%%)%n",
                routedUs - inlineUs, 100.0 * (routedUs - inlineUs) / routedUs);
    }
}

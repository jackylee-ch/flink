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
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.state.forstrs.VectorizedExecutor;
import org.apache.flink.state.forstrs.metrics.DispatchMetrics;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * M1 (async-state parallel executor) — routes each async-state batch to one of {@code N}
 * independent single-thread {@link VectorizedExecutor} workers so disjoint-key batches execute
 * concurrently across cores, with a deferred future so the AEC mailbox overlaps the next batch.
 *
 * <p><b>Why this is correct.</b> The {@code AsyncExecutionController} (1) enforces per-key ordering
 * — it will not hand a key's next record to a new batch until the prior batch's future completes —
 * so concurrent batches hold <em>disjoint</em> keys, and (2) drains all in-flight batch futures
 * before a checkpoint snapshot, so the deferred futures returned here preserve checkpoint
 * consistency. Each worker owns its own {@link Arena} + reused Arrow/columnar buffers, and the
 * lease guarantees at most one batch per worker at a time, so no worker's buffers are touched
 * concurrently. The mailbox→worker handoff is safe because the worker arenas are
 * {@link Arena#ofShared()} and the executor submit establishes happens-before.
 *
 * <p>This replaces the prior single-{@code VectorizedExecutor}-per-subtask model (which ran every
 * batch inline on the mailbox thread, returning an already-completed future — the documented
 * "in-flight depth == 1" constraint). With this router the in-flight depth reaches {@code N}.
 *
 * <p>Worker count: {@code FRS_RS_READ_IO_PARALLELISM} env (default {@code min(cores,4)}).
 */
public final class RoutingStateExecutor implements StateExecutor {

    private final VectorizedExecutor[] workers;
    private final ExecutorService[] workerThreads;
    private final Arena[] workerArenas;

    /** Free worker ids; guarded by {@code lock}. */
    private final ArrayDeque<Integer> free;

    private final Object lock = new Object();

    /** container identity → leased worker id (set at createRequestContainer, removed at dispatch). */
    private final Map<AsyncRequestContainer<StateRequest<?, ?, ?, ?>>, Integer> leased =
            new IdentityHashMap<>();

    /** In-flight batch futures, for drain-on-sync. */
    private final java.util.Set<CompletableFuture<Void>> inflight = ConcurrentHashMap.newKeySet();

    private volatile boolean shutdown = false;

    public static int workerCount() {
        // OPT-01: default worker count = 3 to MATCH ForSt's read-io-parallelism default
        // (ForStOptions read-io-parallelism=3) per the "config must match ForSt" constraint.
        // Overridable via FRS_RS_READ_IO_PARALLELISM (the spike used 6).
        int dflt = 3;
        String s = System.getenv("FRS_RS_READ_IO_PARALLELISM");
        if (s != null) {
            try {
                int v = Integer.parseInt(s.trim());
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException ignore) {
                // fall through to default
            }
        }
        return dflt;
    }

    /**
     * Backpressure cap for the non-blocking mode: once this many batches are dispatched and
     * unfinished, {@link #fullyLoaded()} reports {@code true} and the AEC stops triggering new
     * batches until workers catch up. Default {@code 2 × workerCount()} — one executing + one
     * queued per worker — keeps the classifier pool bounded while letting every worker stay busy.
     */
    public static int maxInFlightBatches(int workerCount) {
        String s = System.getenv("FRS_RS_MAX_INFLIGHT_BATCHES");
        if (s != null) {
            try {
                int v = Integer.parseInt(s.trim());
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException ignore) {
                // fall through to default
            }
        }
        return 2 * workerCount;
    }

    /** True when the backend selected the STAGE-1 two-regime executor mode. */
    static boolean twoRegimeMode() {
        String m = System.getenv("FRS_RS_EXECUTOR");
        return m != null && m.trim().equals("two-regime");
    }

    /** Env-tunable LIGHT-inline ceiling (FRS_RS_INLINE_MAX, default 256 = the AEC batch size). */
    private static int inlineMaxBatch() {
        String s = System.getenv("FRS_RS_INLINE_MAX");
        if (s != null) {
            try {
                int v = Integer.parseInt(s.trim());
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException ignore) {
                // fall through to default
            }
        }
        return 256;
    }

    /** Test/backend accessor for the two-regime switch (null in other modes). */
    public RegimeSwitch regimeSwitch() {
        return regime;
    }

    /**
     * PR-1 adaptive dispatch: when {@code true}, ITER-FREE batches (point gets/puts/deletes only)
     * execute INLINE on the calling (mailbox) thread instead of being dispatched to workers — the
     * mailbox→worker→mailbox handoff costs more than it saves for cheap high-rate batches (q17
     * measured: 271.8s offloaded ≈ ForSt's own 253s, vs 77s inline — the 3.3× frs-over-ForSt q17
     * advantage IS the inline path). Iter-containing batches still fan out to the worker pool
     * (q11 2.35×, q20 DNF→finish, q7 1441→1052s measured). Both paths complete synchronously
     * before returning, so at most one batch is ever in flight — the lockstep visibility
     * semantics that depth-1 and the blocking router are proven correct under is preserved by
     * construction (the non-blocking pipelined mode corrupted q8 by −18%: mailbox-direct statebuf
     * writes overtook queued reads — see the 2026-06-10 bisect in the sweep doc).
     */
    private final boolean adaptiveInline;

    /**
     * R2a (read-side lever 2a, 2026-06-14) — CONTENT-ADAPTIVE EXECUTOR DEPTH, the SAFE realization.
     *
     * <p>When {@code true} ({@code FRS_RS_EXECUTOR=routing-adaptive}) every batch stays kg-affine
     * (one MapStateCache per key-group — the proven windowed-join read-your-writes invariant, never
     * the cross-cache mailbox-vs-worker split that made the experimental modes flaky), but the
     * EXECUTION mechanism is chosen per batch by content:
     *
     * <ul>
     *   <li><b>ITER-bearing batches</b> fan out to the kg-affine worker threads so the deep
     *       per-probe I/O of different key-groups overlaps (the routing path that measured q11
     *       318.9→135.7s, exact 92M rows). Cross-probe overlap is the q11/q7 lever.
     *   <li><b>ITER-FREE batches (q17 carve-out)</b> run INLINE on the calling (mailbox) thread on
     *       their kg workers' executors with ZERO worker-thread handoff and ZERO latch — the q17
     *       tight point-RMW loop REGRESSES under any mailbox→worker handoff (271.8s offloaded ≈
     *       ForSt's own 253s, vs the 76.7s inline path that BEATS ForSt 3.3×). Because the inline
     *       path still uses the same {@code workers[kg]} executor as the iter path would, the
     *       per-key-group cache stays consistent — there is NO mailbox-vs-worker cross-cache hazard
     *       (the q8-flaky ingredient is absent by construction).
     * </ul>
     *
     * <p>This is byte-identical in OUTPUT to {@code routing} (same kg-affine caches, same
     * synchronous completion before return) — it only removes the worker-thread handoff for the
     * iter-free carve-out. Default stays {@code inline}; R2a is opt-in until the q8 band + q17
     * no-regress NexMark gates pass under one uniform config.
     */
    private final boolean routingAdaptive;

    /**
     * Diagnostic counters (R2a microbench / falsifier gate): how many sub-batches were executed
     * inline on the caller (mailbox) thread vs dispatched to a worker thread. Lets a test assert
     * the q17 carve-out incurs ZERO worker dispatch and the iter path DOES dispatch. Cheap relaxed
     * counters; never read on the production hot path.
     */
    private final AtomicInteger inlineSubBatches = new AtomicInteger();

    private final AtomicInteger workerDispatchedSubBatches = new AtomicInteger();

    /**
     * FRS-ROUTING-ASYNC (2026-06-11): when {@code true}, {@link #executeBatchRequests} dispatches
     * each non-empty per-worker sub-batch to its worker FIFO and returns IMMEDIATELY with a
     * truthful aggregate future (completed when every sub-batch actually finishes) — the mailbox
     * thread is never blocked on engine work, so the AEC pipelines the next batch's offer phase
     * while workers execute (in-flight depth &gt; 1).
     *
     * <p><b>⚠ EXPERIMENTAL — FAILED the q8@100M exactness canary (2026-06-11). DO NOT enable in
     * production.</b> Run 1: permanent wedge after source completion (windows never fired,
     * MAXSEC@600). Run 2: finished 36.7s but emitted 1,285,415 rows vs the 3,064,4xx reference
     * band (−58% under-emit). Root cause (mechanism, post-mortem): the request-routing here is
     * sound — classifier buffers ARE per-batch private (pool + worker-side self-release), and
     * per-key-group FIFO ordering holds — but the PER-STATE off-heap staging buffers
     * (MapStateV2 Arrow buffer, ListStateArrowBuffer, value statebufs) are long-lived and shared
     * per state object: the mailbox APPENDS to them at offer time while a worker DRAINS them at
     * batch-execution time. Every proven mode is lockstep (offer N+1 strictly after execute N),
     * so those buffers see one thread at a time; any non-blocking mode overlaps the phases and
     * tears them. This is the same mechanism as the 2026-06-10 coordinated-mode corruption — it
     * was never specific to inline execution. Prerequisite for enabling this mode: per-batch
     * ownership of the per-state staging buffers (seal/swap at dispatch — the "per-batch buffer
     * ownership refactor of the C1 design" deferred on
     * {@code VectorizedExecutor#executeBatchRequests}).
     *
     * <p>What WAS validated: the AEC contract supports incomplete container futures +
     * {@link #fullyLoaded()} backpressure (job ran end-to-end at 10M and at 100M run 2), and the
     * q9@100M profile proves the blocking latch caps the TM at 1.7/8 cores — the motivation
     * stands; the per-state buffer refactor is the remaining blocker.
     */
    private final boolean nonBlocking;

    /** Outstanding dispatched-but-unfinished batches; drives {@link #fullyLoaded()}. */
    private final AtomicInteger outstanding = new AtomicInteger();

    /** Backpressure cap for {@link #fullyLoaded()} (env FRS_RS_MAX_INFLIGHT_BATCHES). */
    private final int maxOutstanding;

    /**
     * STAGE-1 two-regime mode (design 2026-06-11 §3): non-null only for
     * FRS_RS_EXECUTOR=two-regime. LIGHT (pipeline empty + small iter-free batch) executes
     * inline on the mailbox — today's proven depth-1 semantics, the measured q17-class win;
     * HEAVY dispatches to the kg-affine worker FIFOs non-blocking. The dispatch predicate
     * (outstanding == 0) and the no-overtaking safety invariant coincide by construction.
     */
    final RegimeSwitch regime;

    /** LIGHT-inline ceiling: batches above this request count dispatch HEAVY. */
    private final int inlineMax;

    /**
     * STAGE-0 sync-direct experiment: a DEDICATED executor (own arena + buffers) for
     * mailbox-inline sync requests, so direct execution never shares scratch with a worker
     * thread mid-batch.
     *
     * <p><b>RETIRED under the non-blocking executor by the Stage-0 §6.4 fix (2026-06-15).</b> The
     * mailbox-direct bypass it provided is the read-your-writes hazard on the window-fire path (it
     * jumps the kg worker FIFO and can read state before a queued LIST_ADD has been applied — the q8
     * −77% under-emit). It is therefore no longer constructed under {@code nonBlocking}; only the
     * blocking/inline modes — where batches complete synchronously so there is no queued write to
     * overtake — could ever take the bypass, and they never set this field either (it was only ever
     * built when {@code nonBlocking}). The field is kept null-valued for ABI/forward compatibility.
     */
    private final VectorizedExecutor syncDirectWorker;
    private final Arena syncDirectArena;

    public RoutingStateExecutor(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            DispatchMetrics metrics,
            java.util.function.Consumer<VectorizedExecutor> register) {
        this(linker, db, cf, metrics, register, false, false);
    }

    public RoutingStateExecutor(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            DispatchMetrics metrics,
            java.util.function.Consumer<VectorizedExecutor> register,
            boolean adaptiveInline) {
        this(linker, db, cf, metrics, register, adaptiveInline, false);
    }

    public RoutingStateExecutor(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            DispatchMetrics metrics,
            java.util.function.Consumer<VectorizedExecutor> register,
            boolean adaptiveInline,
            boolean nonBlocking) {
        this(linker, db, cf, metrics, register, adaptiveInline, nonBlocking, false);
    }

    public RoutingStateExecutor(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            DispatchMetrics metrics,
            java.util.function.Consumer<VectorizedExecutor> register,
            boolean adaptiveInline,
            boolean nonBlocking,
            boolean routingAdaptive) {
        this.adaptiveInline = adaptiveInline;
        this.nonBlocking = nonBlocking;
        this.routingAdaptive = routingAdaptive;
        int n = workerCount();
        this.maxOutstanding = maxInFlightBatches(n);
        this.regime = twoRegimeMode() ? new RegimeSwitch() : null;
        this.inlineMax = inlineMaxBatch();
        this.workers = new VectorizedExecutor[n];
        this.workerThreads = new ExecutorService[n];
        this.workerArenas = new Arena[n];
        this.free = new ArrayDeque<>(n);
        AtomicInteger seq = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            // Shared arena: the mailbox thread fills the worker's classifier buffers, the worker
            // thread reads them — cross-thread access requires a shared (not confined) arena.
            Arena a = Arena.ofShared();
            VectorizedExecutor w = new VectorizedExecutor(linker, db, cf, a);
            if (metrics != null) {
                w.setDispatchMetrics(metrics);
            }
            if (register != null) {
                register.accept(w); // add to backend managedExecutors for snapshot/flush/shutdown
            }
            final int id = i;
            ThreadFactory tf =
                    r -> {
                        Thread t = new Thread(r, "forst-rs-state-worker-" + id + "-" + seq.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    };
            this.workers[i] = w;
            this.workerArenas[i] = a;
            this.workerThreads[i] = Executors.newSingleThreadExecutor(tf);
            this.free.addLast(i);
        }
        // STAGE-0 §6.4 fix (2026-06-15): the FRS_RS_SYNC_DIRECT mailbox-direct bypass is RETIRED
        // under the non-blocking executor (it is the fire-path read-your-writes hazard). Never
        // construct it — the sync path always funnels through the kg worker FIFO.
        this.syncDirectArena = null;
        this.syncDirectWorker = null;
    }

    /** Test-only: wrap pre-built (stub) workers; no native linker/db touched. */
    RoutingStateExecutor(VectorizedExecutor[] testWorkers, boolean adaptiveInline, boolean nonBlocking) {
        this(testWorkers, adaptiveInline, nonBlocking, false);
    }

    /** Test-only: wrap pre-built (stub) workers with the R2a routing-adaptive flag. */
    RoutingStateExecutor(
            VectorizedExecutor[] testWorkers,
            boolean adaptiveInline,
            boolean nonBlocking,
            boolean routingAdaptive) {
        this.adaptiveInline = adaptiveInline;
        this.nonBlocking = nonBlocking;
        this.routingAdaptive = routingAdaptive;
        int n = testWorkers.length;
        this.maxOutstanding = maxInFlightBatches(n);
        this.regime = twoRegimeMode() ? new RegimeSwitch() : null;
        this.inlineMax = inlineMaxBatch();
        this.workers = testWorkers;
        this.workerThreads = new ExecutorService[n];
        this.workerArenas = new Arena[n];
        this.free = new ArrayDeque<>(n);
        AtomicInteger seq = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            final int id = i;
            ThreadFactory tf =
                    r -> {
                        Thread t = new Thread(r, "forst-rs-state-worker-" + id + "-" + seq.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    };
            this.workerThreads[i] = Executors.newSingleThreadExecutor(tf);
            this.free.addLast(i);
        }
        this.syncDirectArena = null;
        this.syncDirectWorker = null;
    }

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        // ADAPTIVE (split-iters-only): cheap requests (gets/puts/deletes/appends) go into ONE
        // classifier executed INLINE on the mailbox — zero handoff, zero kg-splitting (full-batch
        // splitting cost q17 2×: 148.8s vs 77s). Only ITERATOR requests route by key-group for
        // parallel fan-out. ROUTING (non-adaptive): everything routes by key-group as before.
        // ADAPTIVE uses the SAME offer-time routing container as ROUTING. Deferred-offer
        // containers were REFUTED (q8 −40%, 2026-06-10): VectorizedClassifier.offer serializes
        // keys/namespaces from the STATE OBJECT's live context (window states setCurrentNamespace
        // before request creation) — deferring serialization to dispatch time stamps requests
        // with a LATER record's namespace. Serialization must happen at offer time, always.
        // KEY-GROUP-AFFINE ROUTING: the returned container routes each offered request to a
        // per-worker sub-container by keyGroup % N, so a given key-group's state always lands in ONE
        // worker's MapStateCache (read-your-writes + per-key ordering — the windowed-join correctness
        // fix). executeBatchRequests then runs the non-empty sub-containers in PARALLEL and completes
        // SYNCHRONOUSLY (no incomplete-future / fullyLoaded coordination → deadlock-free).
        return new RoutingRequestContainer();
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
        // DEADLOCK-FREE SYNCHRONOUS design: the prior async-offload version (incomplete future +
        // busyWorkers/fullyLoaded gate) deadlocked the AEC mailbox. Here we dispatch each non-empty
        // per-worker sub-container to its worker thread, run them concurrently (intra-batch
        // parallelism across disjoint key-groups), BLOCK until all finish, then return an
        // already-completed future — the same contract as the inline VectorizedExecutor, so there is
        // no async completion reordering to deadlock. Because we block until done, the per-worker
        // pooled classifiers are free again before the next batch's createRequestContainer (no
        // cross-batch reuse hazard). Cross-batch pipelining (depth>1) is a later optimization (OPT-02
        // / double-buffering); this delivers correctness + intra-batch parallelism safely.
        final AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs =
                ((RoutingRequestContainer) container).subs;
        java.util.List<Integer> busy = new java.util.ArrayList<>(workers.length);
        for (int i = 0; i < subs.length; i++) {
            if (subs[i] != null && !subs[i].isEmpty()) {
                busy.add(i);
            }
        }
        if (busy.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (routingAdaptive) {
            // R2a — CONTENT-ADAPTIVE EXECUTOR DEPTH (the SAFE realization, 2026-06-14).
            // Decide per batch by content; everything stays kg-affine (one cache/key-group), so
            // there is no mailbox-vs-worker cross-cache hazard regardless of branch.
            if (!anySubHasItersForBatch(subs, busy)) {
                // q17 CARVE-OUT: iter-free batch → run each kg sub-batch INLINE on THIS (mailbox)
                // thread, on its own worker's executor. ZERO worker-thread handoff, ZERO latch —
                // the tight point-RMW path that BEATS ForSt 3.3× (76.7s vs 255.7s); any handoff
                // regresses it to ~271s. Synchronous: the inner executors complete before return.
                try {
                    for (int id : busy) {
                        inlineSubBatches.incrementAndGet();
                        CompletableFuture<Void> inner = workers[id].executeBatchRequests(subs[id]);
                        Throwable t = inner.handle((v, ex) -> ex).getNow(null);
                        if (t != null) {
                            return CompletableFuture.failedFuture(t);
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                } catch (Throwable t) {
                    return CompletableFuture.failedFuture(t);
                }
            }
            // ITER batch → fan out to the kg-affine worker threads so deep per-probe I/O of
            // different key-groups OVERLAPS (q11 318.9→135.7s, exact 92M). Blocking + synchronous
            // (same completion contract as routing): wait for every worker, then return a
            // completed future — no async reordering, byte-identical OUTPUT to routing.
            return runIterBatchOnWorkers(subs, busy);
        }
        if (nonBlocking) {
            if (regime != null
                    && regime.isLight()
                    && !anySubHasIters(subs, busy)
                    && totalRequests(subs, busy) <= inlineMax) {
                // STAGE-1 LIGHT: inline on the mailbox — zero handoff (q17-class win). Safe:
                // nothing outstanding ⇒ nothing can be overtaken; inner executors are
                // synchronous so the future is complete at return.
                try {
                    for (int id : busy) {
                        CompletableFuture<Void> inner = workers[id].executeBatchRequests(subs[id]);
                        Throwable t = inner.handle((v, e) -> e).getNow(null);
                        if (t != null) {
                            return CompletableFuture.failedFuture(t);
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                } catch (Throwable t) {
                    return CompletableFuture.failedFuture(t);
                }
            }
            if (regime != null) {
                regime.batchDispatched();
                CompletableFuture<Void> agg = dispatchNonBlocking(subs, busy);
                agg.whenComplete((v, e) -> regime.batchSettled());
                return agg;
            }
            return dispatchNonBlocking(subs, busy);
        }
        if (adaptiveInline && !anySubHasIters(subs, busy)) {
            // ADAPTIVE fast path (the leak-fixed configuration that measured q8 IN BAND
            // 3,064,493 / q17 148.8s): iter-free batch → execute each kg sub-batch INLINE
            // sequentially on this (mailbox) thread, each on its own worker's executor. No
            // worker handoff. Iter batches fall through to the parallel latch dispatch below.
            try {
                for (int id : busy) {
                    CompletableFuture<Void> inner = workers[id].executeBatchRequests(subs[id]);
                    Throwable t = inner.handle((v, ex) -> ex).getNow(null);
                    if (t != null) {
                        return CompletableFuture.failedFuture(t);
                    }
                }
                return CompletableFuture.completedFuture(null);
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(t);
            }
        }
        final java.util.concurrent.atomic.AtomicReference<Throwable> err =
                new java.util.concurrent.atomic.AtomicReference<>();
        if (busy.size() == 1) {
            // Common case (batch touched one worker's key-groups): run inline-on-worker, no latch.
            int id = busy.get(0);
            runSubBatchAndWait(id, subs[id], err);
        } else {
            final java.util.concurrent.CountDownLatch latch =
                    new java.util.concurrent.CountDownLatch(busy.size());
            for (int wi : busy) {
                final int id = wi;
                final AsyncRequestContainer<StateRequest<?, ?, ?, ?>> sub = subs[id];
                workerThreads[id].execute(
                        () -> {
                            try {
                                CompletableFuture<Void> inner = workers[id].executeBatchRequests(sub);
                                Throwable t = inner.handle((v, e) -> e).getNow(null);
                                if (t != null) {
                                    err.compareAndSet(null, t);
                                }
                            } catch (Throwable t) {
                                err.compareAndSet(null, t);
                            } finally {
                                latch.countDown();
                            }
                        });
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                err.compareAndSet(null, e);
            }
        }
        Throwable e = err.get();
        return (e != null)
                ? CompletableFuture.failedFuture(e)
                : CompletableFuture.completedFuture(null);
    }

    /**
     * FRS-ROUTING-ASYNC dispatch: enqueue each busy sub-batch on its worker's FIFO and return a
     * truthful aggregate future without waiting. The mailbox thread is free to build the next
     * batch; same-key-group ordering across batches is preserved by the per-worker single-thread
     * FIFO (a later batch's sub enqueues strictly after an earlier batch's sub for that worker).
     * Inner futures are read with {@code getNow} only AFTER the worker task returns — the wrapped
     * {@link VectorizedExecutor#executeBatchRequests} is synchronous (always returns an
     * already-completed future), so the read is exact, and the aggregate completes only when
     * every sub-batch has actually finished (no early-success discard).
     */
    private CompletableFuture<Void> dispatchNonBlocking(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs, java.util.List<Integer> busy) {
        final CompletableFuture<Void> agg = new CompletableFuture<>();
        final AtomicInteger pending = new AtomicInteger(busy.size());
        final java.util.concurrent.atomic.AtomicReference<Throwable> err =
                new java.util.concurrent.atomic.AtomicReference<>();
        outstanding.incrementAndGet();
        final Runnable settleOne =
                () -> {
                    if (pending.decrementAndGet() == 0) {
                        outstanding.decrementAndGet();
                        Throwable e = err.get();
                        if (e != null) {
                            agg.completeExceptionally(e);
                        } else {
                            agg.complete(null);
                        }
                    }
                };
        for (int wi : busy) {
            final int id = wi;
            final AsyncRequestContainer<StateRequest<?, ?, ?, ?>> sub = subs[id];
            try {
                workerThreads[id].execute(
                        () -> {
                            try {
                                CompletableFuture<Void> inner =
                                        workers[id].executeBatchRequests(sub);
                                Throwable t = inner.handle((v, e) -> e).getNow(null);
                                if (t != null) {
                                    err.compareAndSet(null, t);
                                }
                            } catch (Throwable t) {
                                err.compareAndSet(null, t);
                            } finally {
                                settleOne.run();
                            }
                        });
            } catch (java.util.concurrent.RejectedExecutionException rex) {
                // Shutdown raced the dispatch: the worker pool is closed. Per-row futures of this
                // sub were never started; fail the batch so nothing waits forever.
                err.compareAndSet(null, rex);
                settleOne.run();
            }
        }
        return agg;
    }

    /**
     * R2a iter-batch fan-out: dispatch each busy sub-batch to its kg-affine worker thread and BLOCK
     * until all finish (cross-key-group overlap, synchronous completion = byte-identical OUTPUT to
     * routing). A single-worker batch runs inline-on-its-worker (no latch); multi-worker fans out
     * across worker threads under a latch. Worker dispatches are counted for the R2a gate.
     */
    private CompletableFuture<Void> runIterBatchOnWorkers(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs, java.util.List<Integer> busy) {
        final java.util.concurrent.atomic.AtomicReference<Throwable> err =
                new java.util.concurrent.atomic.AtomicReference<>();
        if (busy.size() == 1) {
            int id = busy.get(0);
            workerDispatchedSubBatches.incrementAndGet();
            runSubBatchAndWait(id, subs[id], err);
        } else {
            final java.util.concurrent.CountDownLatch latch =
                    new java.util.concurrent.CountDownLatch(busy.size());
            for (int wi : busy) {
                final int id = wi;
                final AsyncRequestContainer<StateRequest<?, ?, ?, ?>> sub = subs[id];
                workerDispatchedSubBatches.incrementAndGet();
                workerThreads[id].execute(
                        () -> {
                            try {
                                CompletableFuture<Void> inner = workers[id].executeBatchRequests(sub);
                                Throwable t = inner.handle((v, e) -> e).getNow(null);
                                if (t != null) {
                                    err.compareAndSet(null, t);
                                }
                            } catch (Throwable t) {
                                err.compareAndSet(null, t);
                            } finally {
                                latch.countDown();
                            }
                        });
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                err.compareAndSet(null, e);
            }
        }
        Throwable e = err.get();
        return (e != null)
                ? CompletableFuture.failedFuture(e)
                : CompletableFuture.completedFuture(null);
    }

    /** R2a gate accessor: sub-batches executed inline on the caller (mailbox) thread. */
    public int inlineSubBatchCount() {
        return inlineSubBatches.get();
    }

    /** R2a gate accessor: sub-batches dispatched to a worker thread. */
    public int workerDispatchedSubBatchCount() {
        return workerDispatchedSubBatches.get();
    }

    /** STAGE-1: total requests across busy sub-batches (LIGHT-inline ceiling check). */
    private static int totalRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs, java.util.List<Integer> busy) {
        int total = 0;
        for (int id : busy) {
            if (subs[id] instanceof org.apache.flink.state.forstrs.VectorizedClassifier vc) {
                total += vc.totalRequestCount();
            }
        }
        return total;
    }

    /**
     * Test-only override of the R2a iter verdict. The real
     * {@link org.apache.flink.state.forstrs.VectorizedClassifier#hasIterRequests()} signal is only
     * raised at FFI dispatch-build time (not exercised by the stub-worker harness), so the R2a
     * falsifier injects a {@code Boolean} here to drive the iter / iter-free branches
     * deterministically. {@code null} = use the real classifier flag (production path, unchanged).
     */
    private volatile Boolean iterVerdictOverrideForTest;

    /** Test-only: force {@link #anySubHasItersForBatch} to return {@code value}. */
    void setIterVerdictOverrideForTest(Boolean value) {
        this.iterVerdictOverrideForTest = value;
    }

    /**
     * Adaptive dispatch: whether any non-empty sub-batch contains iterator requests. The default
     * reads the real classifier flag; a test override (if set) short-circuits it so the R2a
     * branches can be driven without FFI.
     */
    boolean anySubHasItersForBatch(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs, java.util.List<Integer> busy) {
        Boolean override = iterVerdictOverrideForTest;
        if (override != null) {
            return override;
        }
        return anySubHasIters(subs, busy);
    }

    /** Adaptive dispatch: whether any non-empty sub-batch contains iterator requests. */
    private static boolean anySubHasIters(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs, java.util.List<Integer> busy) {
        for (int id : busy) {
            if (subs[id] instanceof org.apache.flink.state.forstrs.VectorizedClassifier vc
                    && vc.hasIterRequests()) {
                return true;
            }
        }
        return false;
    }

    /** Runs one worker's sub-batch on its worker thread and blocks until it completes. */
    private void runSubBatchAndWait(
            int id,
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> sub,
            java.util.concurrent.atomic.AtomicReference<Throwable> err) {
        try {
            workerThreads[id]
                    .submit(
                            () -> {
                                CompletableFuture<Void> inner = workers[id].executeBatchRequests(sub);
                                Throwable t = inner.handle((v, e) -> e).getNow(null);
                                if (t != null) {
                                    err.compareAndSet(null, t);
                                }
                            })
                    .get();
        } catch (java.util.concurrent.ExecutionException ex) {
            err.compareAndSet(null, ex.getCause() != null ? ex.getCause() : ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            err.compareAndSet(null, ex);
        }
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
        // STAGE-0 §6.4 FIX (2026-06-15) — option A "route ALL fire-path engine ops through the kg
        // worker FIFO". The window-fire trigger issues its reads via this sync path under overdraft
        // (InternalTimerServiceAsyncImpl.maintainContextAndProcess → syncPointRequestWithCallback,
        // allowOverdraft=true), and overdraft seizeCapacity does NOT drain the AEC. If such a
        // fire-path read takes a mailbox-direct route — exactly the FRS_RS_SYNC_DIRECT experiment's
        // dedicated syncDirectWorker, which shares the engine's backing store but NOT the kg worker
        // FIFO — it can run while that key-group's LIST_ADD is still queued/in-flight on the worker,
        // reading the pre-write state: the q8 windowed-join under-emit (−77%, root-caused +
        // deterministically reproduced in Q8OpMixBoundaryRaceTest). Under the NON-BLOCKING executor
        // the mailbox-direct bypass is therefore UNSAFE and is disabled: the sync request MUST
        // funnel onto its key-group worker's FIFO TAIL (below), behind any queued write, so
        // read-your-writes holds by FIFO construction — the property the proven blocking `routing`
        // mode already has. The bypass remains available ONLY under the blocking/inline modes, where
        // batches complete synchronously so there is never a queued worker write to overtake.
        if (syncDirectWorker != null && !nonBlocking) {
            syncDirectWorker.executeRequestSync(request);
            return;
        }
        // Route the sync request to ITS key-group's worker so it observes that key-group's cached
        // writes (read-your-writes). Blocking modes: batches complete synchronously, nothing to
        // drain. Non-blocking mode: any in-flight batch for this key-group sits in the SAME
        // worker's FIFO ahead of this submission, so the .get() below transitively drains it.
        int kg = request.getRecordContext().getKeyGroup();
        int id = Math.floorMod(kg, workers.length);
        try {
            workerThreads[id].submit(() -> workers[id].executeRequestSync(request)).get();
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof RuntimeException re) {
                throw re;
            }
            if (c instanceof Error er) {
                throw er;
            }
            throw new RuntimeException(c);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted during sync state request", e);
        }
    }

    @Override
    public boolean fullyLoaded() {
        // Blocking modes: executeBatchRequests blocks until the batch completes and returns an
        // already-completed future, so there is never an outstanding async batch → never "loaded"
        // (outstanding stays 0). Non-blocking mode: report loaded once the dispatched-unfinished
        // batch count reaches the cap, so the AEC stops triggering and the classifier pool stays
        // bounded (the documented fullyLoaded()-always-false unbounded-pipelining hazard).
        return (regime != null ? regime.outstanding() : outstanding.get()) >= maxOutstanding;
    }

    /**
     * Container that routes each offered request to a per-worker sub-container by key-group, so a
     * given key-group's requests always go to the same worker (consistent MapStateCache + per-key
     * ordering — the windowed-join correctness fix). Sub-containers are the workers' pooled
     * classifiers, acquired lazily on first offer; the worker arenas are {@link Arena#ofShared()} so
     * cross-thread fill is safe.
     */
    private final class RoutingRequestContainer
            implements AsyncRequestContainer<StateRequest<?, ?, ?, ?>> {
        @SuppressWarnings("unchecked")
        final AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs =
                (AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[])
                        new AsyncRequestContainer<?>[workers.length];

        @Override
        public void offer(StateRequest<?, ?, ?, ?> request) {
            int kg = request.getRecordContext().getKeyGroup();
            int w = Math.floorMod(kg, workers.length);
            if (subs[w] == null) {
                subs[w] = workers[w].createRequestContainer();
            }
            subs[w].offer(request);
        }

        @Override
        public boolean isEmpty() {
            for (AsyncRequestContainer<StateRequest<?, ?, ?, ?>> s : subs) {
                if (s != null && !s.isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
        synchronized (lock) {
            lock.notifyAll(); // wake any leaseWorker() waiter so it observes shutdown
        }
        for (VectorizedExecutor w : workers) {
            w.shutdown(); // flip each worker's rejection gate
        }
        for (ExecutorService t : workerThreads) {
            t.shutdown();
        }
        // Await worker threads so NO in-flight FFI call can touch a worker arena, THEN close the
        // arenas (no native leak, no use-after-free).
        for (ExecutorService t : workerThreads) {
            try {
                t.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (Arena a : workerArenas) {
            try {
                a.close();
            } catch (Throwable ignore) {
                // best-effort; a racing close is benign at teardown
            }
        }
        if (syncDirectWorker != null) {
            syncDirectWorker.shutdown();
            try {
                syncDirectArena.close();
            } catch (Throwable ignore) {
                // best-effort at teardown
            }
        }
    }

    /** Awaits all currently in-flight batch futures (used before a sync request). */
    private void drainInflight() {
        for (CompletableFuture<Void> f : inflight) {
            try {
                f.join();
            } catch (Throwable ignore) {
                // failures are surfaced to the owning batch's future; drain just waits for settle
            }
        }
    }

    private int leaseWorker() {
        synchronized (lock) {
            while (free.isEmpty() && !shutdown) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted leasing state worker", e);
                }
            }
            if (shutdown) {
                throw new IllegalStateException("RoutingStateExecutor shut down");
            }
            return free.removeFirst();
        }
    }

    private void releaseWorker(int id) {
        synchronized (lock) {
            free.addLast(id);
            lock.notifyAll();
        }
    }

    /** Test accessor: the per-worker {@link Arena}s (for backend-managed close). */
    public Arena[] workerArenas() {
        return workerArenas;
    }
}

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
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.metrics.DispatchMetrics;

import java.lang.foreign.Arena;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * PR-1 coordinated executor — the ForSt {@code ForStStateExecutor} model adapted to the FFM
 * vectorized engine: key-group-affine routing (same container scheme as {@link
 * RoutingStateExecutor}) with <b>non-blocking</b> dispatch. {@link #executeBatchRequests} returns
 * an INCOMPLETE future immediately and the AEC mailbox thread never waits on state I/O; {@link
 * #fullyLoaded()} does real outstanding-batch accounting so the AEC stops admitting records when
 * every worker is busy (bounded in-flight, the backpressure ForSt gets from {@code ongoing >=
 * readThreadCount}).
 *
 * <p><b>Why this cannot deadlock where the 2026-06-09 async-offload attempt did.</b> That version
 * blocked the mailbox inside {@code leaseWorker()} when no worker was free — mailbox waits for a
 * worker, the worker's completion needs the mailbox, cycle. Here NOTHING on the mailbox path
 * blocks: container creation grows the per-worker classifier pool instead of waiting (each pooled
 * classifier owns a private buffer quartet — see {@code VectorizedExecutor.createRequestContainer}),
 * and admission is bounded by {@link #fullyLoaded()}, which the AEC polls without blocking.
 * Batch-future completion happens on the worker thread; per-row callbacks already hop to the
 * mailbox via {@code CallbackRunnerWrapper}.
 *
 * <p><b>Why parallel workers preserve correctness.</b> (1) The AEC enforces per-key ordering
 * across batches (a key's next record is not released until its prior request completes). (2) A
 * key-group always routes to the SAME worker ({@code keyGroup % N}), and each worker executes its
 * sub-batches on a single thread in submission order — so per-key-group effects are totally
 * ordered. (3) Worker arenas are {@link Arena#ofShared()}: the mailbox fills a classifier's
 * buffers, the worker reads them, with happens-before established by the executor submit.
 */
public final class CoordinatedStateExecutor implements StateExecutor {

    private final VectorizedExecutor[] workers;
    private final ExecutorService[] workerThreads;
    private final Arena[] workerArenas;

    /** Outstanding sub-batches across all workers ({@link #fullyLoaded()} accounting). */
    private final AtomicInteger ongoing = new AtomicInteger();

    /** Worker count: FRS_RS_READ_IO_PARALLELISM env, default 3 (matches ForSt). */
    public static int workerCount() {
        return RoutingStateExecutor.workerCount();
    }

    /** Production factory: builds {@link #workerCount()} workers, each with a shared arena. */
    public static CoordinatedStateExecutor create(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            DispatchMetrics metrics,
            java.util.function.Consumer<VectorizedExecutor> register) {
        int n = workerCount();
        VectorizedExecutor[] ws = new VectorizedExecutor[n];
        Arena[] arenas = new Arena[n];
        for (int i = 0; i < n; i++) {
            // Shared arena: the mailbox thread fills the worker's classifier buffers, the worker
            // thread reads them — cross-thread access requires a shared (not confined) arena.
            Arena a = Arena.ofShared();
            VectorizedExecutor w = new VectorizedExecutor(linker, db, cf, a);
            if (metrics != null) {
                w.setDispatchMetrics(metrics);
            }
            if (register != null) {
                register.accept(w);
            }
            ws[i] = w;
            arenas[i] = a;
        }
        return new CoordinatedStateExecutor(ws, arenas);
    }

    /**
     * Test-visible constructor: takes pre-built workers (tests pass latch-instrumented {@link
     * VectorizedExecutor} subclasses so no native FFI is touched) and, optionally, the arenas to
     * close at shutdown ({@code null} entries are skipped).
     */
    CoordinatedStateExecutor(VectorizedExecutor[] workers, Arena[] arenas) {
        this.workers = workers;
        int n = workers.length;
        this.workerThreads = new ExecutorService[n];
        this.workerArenas = (arenas != null) ? arenas : new Arena[n];
        AtomicInteger seq = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            final int id = i;
            ThreadFactory tf =
                    r -> {
                        Thread t =
                                new Thread(
                                        r,
                                        "forst-rs-state-worker-" + id + "-" + seq.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    };
            this.workerThreads[i] = Executors.newSingleThreadExecutor(tf);
        }
    }

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        return new RoutingRequestContainer();
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
        RoutingRequestContainer rc = (RoutingRequestContainer) container;
        final AsyncRequestContainer<StateRequest<?, ?, ?, ?>>[] subs = rc.subs;
        int n = 0;
        for (AsyncRequestContainer<StateRequest<?, ?, ?, ?>> s : subs) {
            if (s != null && !s.isEmpty()) {
                n++;
            }
        }
        if (n == 0) {
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final AtomicInteger remaining = new AtomicInteger(n);
        final AtomicReference<Throwable> err = new AtomicReference<>();
        for (int i = 0; i < subs.length; i++) {
            final AsyncRequestContainer<StateRequest<?, ?, ?, ?>> sub = subs[i];
            if (sub == null || sub.isEmpty()) {
                continue;
            }
            final int id = i;
            ongoing.incrementAndGet();
            workerThreads[id].execute(
                    () -> {
                        try {
                            // The worker's FFI is synchronous: the inner future is complete when
                            // this call returns.
                            CompletableFuture<Void> inner = workers[id].executeBatchRequests(sub);
                            Throwable t = inner.handle((v, e) -> e).getNow(null);
                            if (t != null) {
                                err.compareAndSet(null, t);
                            }
                        } catch (Throwable t) {
                            err.compareAndSet(null, t);
                        } finally {
                            // Execution fully done → classifier (and its private buffers) can be
                            // refilled by the mailbox for a future batch.
                            workers[id].releaseRequestContainer(sub);
                            ongoing.decrementAndGet();
                            if (remaining.decrementAndGet() == 0) {
                                Throwable e = err.get();
                                if (e != null) {
                                    result.completeExceptionally(e);
                                } else {
                                    result.complete(null);
                                }
                            }
                        }
                    });
        }
        return result; // INCOMPLETE — the mailbox continues immediately.
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
        // Route to ITS key-group's worker so it observes that key-group's prior batches (the
        // worker's single thread serializes them ahead of this submit). Sync requests are rare
        // (timer/migration paths); blocking the caller here is the contract.
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
        return ongoing.get() >= workers.length;
    }

    @Override
    public void shutdown() {
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
                if (a != null) {
                    a.close();
                }
            } catch (Throwable ignore) {
                // best-effort; a racing close is benign at teardown
            }
        }
    }

    /** Test hook: submit a runnable to worker {@code id}'s thread (latch-blocking in tests). */
    void submitToWorkerForTest(int id, Runnable r) {
        workerThreads[id].execute(r);
    }

    /** Test hook: number of workers this instance routes across. */
    int workerCountInstance() {
        return workers.length;
    }

    /**
     * Routes each offered request to a per-worker sub-container by key-group, so a given
     * key-group's requests always go to the same worker (consistent per-key-group ordering).
     * Sub-containers are pooled classifiers with private buffers (PR-1), acquired lazily on first
     * offer; worker arenas are shared so cross-thread fill is safe.
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
}

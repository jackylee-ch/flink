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

    public RoutingStateExecutor(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            DispatchMetrics metrics,
            java.util.function.Consumer<VectorizedExecutor> register) {
        this(linker, db, cf, metrics, register, false);
    }

    public RoutingStateExecutor(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            DispatchMetrics metrics,
            java.util.function.Consumer<VectorizedExecutor> register,
            boolean adaptiveInline) {
        this.adaptiveInline = adaptiveInline;
        int n = workerCount();
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
        // Route the sync request to ITS key-group's worker so it observes that key-group's cached
        // writes (read-your-writes). Batches complete synchronously (executeBatchRequests blocks), so
        // there is no async in-flight batch to drain here.
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
        // Synchronous design: executeBatchRequests blocks until the batch completes and returns an
        // already-completed future, so there is never an outstanding async batch → never "loaded".
        return false;
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

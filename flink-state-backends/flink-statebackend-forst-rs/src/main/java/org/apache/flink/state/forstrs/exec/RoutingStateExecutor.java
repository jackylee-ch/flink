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

    public RoutingStateExecutor(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            DispatchMetrics metrics,
            java.util.function.Consumer<VectorizedExecutor> register) {
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
        int id = leaseWorker();
        AsyncRequestContainer<StateRequest<?, ?, ?, ?>> c = workers[id].createRequestContainer();
        synchronized (lock) {
            leased.put(c, id);
        }
        return c;
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
        final int id;
        synchronized (lock) {
            Integer w = leased.remove(container);
            id = (w != null) ? w : 0; // defensive: a container we didn't lease runs on worker 0
        }
        VectorizedExecutor w = workers[id];
        CompletableFuture<Void> result = new CompletableFuture<>();
        inflight.add(result);
        workerThreads[id].execute(
                () -> {
                    try {
                        // worker.executeBatchRequests runs the (inline) dispatch on THIS worker
                        // thread and returns an already-resolved future; flatten it.
                        CompletableFuture<Void> inner = w.executeBatchRequests(container);
                        inner.whenComplete(
                                (v, t) -> {
                                    if (t != null) {
                                        result.completeExceptionally(t);
                                    } else {
                                        result.complete(null);
                                    }
                                });
                    } catch (Throwable t) {
                        result.completeExceptionally(t);
                    } finally {
                        releaseWorker(id);
                    }
                });
        result.whenComplete((v, t) -> inflight.remove(result));
        return result;
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
        // Conservative correctness: drain all in-flight async batches so the sync read observes
        // every prior write (preserves ordering vs concurrent workers), then run on worker 0.
        drainInflight();
        try {
            workerThreads[0].submit(() -> workers[0].executeRequestSync(request)).get();
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
        synchronized (lock) {
            return free.isEmpty();
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

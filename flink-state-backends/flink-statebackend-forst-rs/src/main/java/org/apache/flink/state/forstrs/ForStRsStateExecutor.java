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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Internal
public class ForStRsStateExecutor implements StateExecutor {

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final Arena arena;

    public ForStRsStateExecutor(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.arena = arena;
    }

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        return new ForStRsStateRequestClassifier();
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
        ForStRsStateRequestClassifier classifier = (ForStRsStateRequestClassifier) container;
        // E8-H3 / E9-H1: track which phase failed and how many requests inside that phase
        // already completed successfully BEFORE the throw, so the drain only completes tails
        // that were left dangling. Calling completeExceptionally on an already-completed
        // Flink InternalAsyncFuture invokes the framework exceptionHandler — that would raise
        // a spurious task fail. AsyncExecutionController.executeBatchRequests discards the
        // returned failedFuture, so the per-StateRequest tail futures are what the runtime
        // mailbox joins on. Pattern mirrors VectorizedExecutor.dispatchAppendMergeBatch.
        //
        // E9-H1: progress counters live in int[1] sentinels so a row-level throw inside an
        // execute*() helper leaves the partial count visible to the catch block here. The
        // prior signature returned an int on success only — if {@code gets.get(i).complete()}
        // itself threw at row k, the value never made it back, getsCompleted stayed at 0, and
        // the drain double-completed rows [0, k-1].
        List<ForStRsDBGetRequest<?, ?, ?>> gets = classifier.getGetRequests();
        List<ForStRsDBPutRequest<?, ?, ?>> puts = classifier.getPutRequests();
        List<ForStRsDBIterRequest<?, ?, ?, ?>> iters = classifier.getIterRequests();
        int[] getsProgress = {0};
        int[] putsProgress = {0};
        int[] itersProgress = {0};
        try {
            executeGets(gets, getsProgress);
            executePuts(puts, putsProgress);
            executeIters(iters, itersProgress);
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            drainTailExceptionally(
                    gets,
                    getsProgress[0],
                    puts,
                    putsProgress[0],
                    iters,
                    itersProgress[0],
                    t);
            if (t instanceof Error) {
                throw (Error) t;
            }
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
        ForStRsStateRequestClassifier single = new ForStRsStateRequestClassifier();
        single.offer(request);
        // executeRequestSync has only one request in each phase and propagates exceptions
        // directly; no drain logic, so the progress counters here are write-only sentinels.
        int[] progress = {0};
        executeGets(single.getGetRequests(), progress);
        executePuts(single.getPutRequests(), progress);
        executeIters(single.getIterRequests(), progress);
    }

    @Override
    public boolean fullyLoaded() {
        return false;
    }

    @Override
    public void shutdown() {}

    public void flushDirty() {}

    /**
     * E8-H3: drain only the requests whose tail futures were NOT yet completed when the
     * phase threw. Requests already completed by an earlier phase (or by the earlier rows
     * of the failing phase) are skipped so we don't double-complete and trigger the
     * framework exceptionHandler twice.
     */
    private void drainTailExceptionally(
            List<ForStRsDBGetRequest<?, ?, ?>> gets,
            int getsCompleted,
            List<ForStRsDBPutRequest<?, ?, ?>> puts,
            int putsCompleted,
            List<ForStRsDBIterRequest<?, ?, ?, ?>> iters,
            int itersCompleted,
            Throwable t) {
        for (int i = getsCompleted; i < gets.size(); i++) {
            try {
                gets.get(i).completeExceptionally(t);
            } catch (RuntimeException ignored) {
                // exceptionHandler.handleException may throw; swallow secondary so the
                // original cause propagates.
            }
        }
        for (int i = putsCompleted; i < puts.size(); i++) {
            try {
                puts.get(i).completeExceptionally(t);
            } catch (RuntimeException ignored) {
                // see above
            }
        }
        for (int i = itersCompleted; i < iters.size(); i++) {
            try {
                iters.get(i).completeExceptionally(t);
            } catch (RuntimeException ignored) {
                // see above
            }
        }
    }

    /**
     * E9-H1: writes the count of requests whose tail future was successfully completed into
     * {@code progressOut[0]}. The counter advances AFTER each successful
     * {@code complete()} call, so if {@code complete()} itself throws at row k, the catch
     * block sees {@code progressOut[0] == k} and the drain skips rows [0, k). The prior
     * shape returned the count on the success path only — a throw never reached the return
     * statement, the outer counter stayed at 0, and the drain double-completed rows
     * [0, k-1].
     */
    private void executeGets(List<ForStRsDBGetRequest<?, ?, ?>> gets, int[] progressOut) {
        progressOut[0] = 0;
        if (gets.isEmpty()) {
            return;
        }
        int count = gets.size();
        byte[][] keys = new byte[count][];
        for (int i = 0; i < count; i++) {
            keys[i] = gets.get(i).getSerializedKey();
        }
        byte[][] results = invokeBatchGet(keys);
        // E8-H3 / E9-H1: complete row-by-row so a deserialization throw mid-loop leaves a
        // precise completed-count for the drain. progressOut[0] is updated AFTER each
        // successful complete() so a throw from complete(i) leaves progressOut[0] == i and
        // the drain skips rows [0, i).
        for (int i = 0; i < count; i++) {
            gets.get(i).complete(results[i]);
            progressOut[0] = i + 1;
        }
    }

    private void executePuts(List<ForStRsDBPutRequest<?, ?, ?>> puts, int[] progressOut) {
        progressOut[0] = 0;
        if (puts.isEmpty()) {
            return;
        }
        List<byte[]> putKeys = new ArrayList<>();
        List<byte[]> putValues = new ArrayList<>();
        for (ForStRsDBPutRequest<?, ?, ?> req : puts) {
            byte[] val = req.getSerializedValue();
            if (val != null) {
                putKeys.add(req.getSerializedKey());
                putValues.add(val);
            } else {
                invokeDelete(req.getSerializedKey());
            }
        }
        if (!putKeys.isEmpty()) {
            invokeBatchPut(
                    putKeys.toArray(new byte[0][]), putValues.toArray(new byte[0][]));
        }
        // E9-H1: same progress invariant as executeGets — update AFTER each successful
        // complete(). If complete() throws at row k, progressOut[0] == k and the drain
        // completes rows [k, size) exceptionally.
        for (int i = 0; i < puts.size(); i++) {
            puts.get(i).complete();
            progressOut[0] = i + 1;
        }
    }

    private void executeIters(List<ForStRsDBIterRequest<?, ?, ?, ?>> iters, int[] progressOut) {
        progressOut[0] = 0;
        if (iters.isEmpty()) {
            return;
        }
        // E8-H3 / E9-H1: process row-by-row; if iter.process throws on row k, the drain
        // completes rows [k, size) exceptionally. Rows [0, k) are already completed inside
        // process. progressOut[0] is updated AFTER each successful process() so a throw
        // leaves the counter precise.
        for (int i = 0; i < iters.size(); i++) {
            iters.get(i).process(linker, db, cf, arena);
            progressOut[0] = i + 1;
        }
    }

    // ----------------- Protected FFI seams (test override points) -----------------
    // E9-H1 regression test injects throws here without needing native libs. Production
    // wiring delegates directly to {@link ForStRsLinker}. These seams add no overhead — the
    // JIT inlines them after class-loading because there are no production subclasses.

    /** Delegates to {@link ForStRsLinker#batchGetArrow}. Test override point only. */
    protected byte[][] invokeBatchGet(byte[][] keys) {
        return linker.batchGetArrow(db, cf, keys);
    }

    /** Delegates to {@link ForStRsLinker#batchPut}. Test override point only. */
    protected void invokeBatchPut(byte[][] keys, byte[][] values) {
        linker.batchPut(db, cf, keys, values);
    }

    /** Delegates to {@link ForStRsLinker#delete}. Test override point only. */
    protected void invokeDelete(byte[] key) {
        linker.delete(db, cf, key);
    }
}

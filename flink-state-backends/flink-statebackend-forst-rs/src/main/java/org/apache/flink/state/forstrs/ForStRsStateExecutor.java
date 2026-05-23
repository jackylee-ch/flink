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
        // E8-H3: track which phase failed and how many requests inside that phase already
        // completed successfully BEFORE the throw, so the drain only completes tails that
        // were left dangling. Calling completeExceptionally on an already-completed Flink
        // InternalAsyncFuture invokes the framework exceptionHandler — that would raise a
        // spurious task fail. AsyncExecutionController.executeBatchRequests discards the
        // returned failedFuture, so the per-StateRequest tail futures are what the runtime
        // mailbox joins on. Pattern mirrors VectorizedExecutor.dispatchAppendMergeBatch.
        List<ForStRsDBGetRequest<?, ?, ?>> gets = classifier.getGetRequests();
        List<ForStRsDBPutRequest<?, ?, ?>> puts = classifier.getPutRequests();
        List<ForStRsDBIterRequest<?, ?, ?, ?>> iters = classifier.getIterRequests();
        int getsCompleted = 0;
        int putsCompleted = 0;
        int itersCompleted = 0;
        try {
            getsCompleted = executeGets(gets);
            putsCompleted = executePuts(puts);
            itersCompleted = executeIters(iters);
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            drainTailExceptionally(
                    gets,
                    getsCompleted,
                    puts,
                    putsCompleted,
                    iters,
                    itersCompleted,
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
        executeGets(single.getGetRequests());
        executePuts(single.getPutRequests());
        executeIters(single.getIterRequests());
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
     * Returns the number of requests whose tail future was successfully completed. If the
     * FFI call throws before completion, returns 0 (no per-row completion happened). If a
     * later phase throws, the caller skips the first {@code getsCompleted} rows during
     * drain so they are not double-completed.
     */
    private int executeGets(List<ForStRsDBGetRequest<?, ?, ?>> gets) {
        if (gets.isEmpty()) {
            return 0;
        }
        int count = gets.size();
        byte[][] keys = new byte[count][];
        for (int i = 0; i < count; i++) {
            keys[i] = gets.get(i).getSerializedKey();
        }
        byte[][] results = linker.batchGetArrow(db, cf, keys);
        // E8-H3: complete row-by-row so a deserialization throw mid-loop leaves a precise
        // completed-count for the drain.
        for (int i = 0; i < count; i++) {
            gets.get(i).complete(results[i]);
        }
        return count;
    }

    private int executePuts(List<ForStRsDBPutRequest<?, ?, ?>> puts) {
        if (puts.isEmpty()) {
            return 0;
        }
        List<byte[]> putKeys = new ArrayList<>();
        List<byte[]> putValues = new ArrayList<>();
        for (ForStRsDBPutRequest<?, ?, ?> req : puts) {
            byte[] val = req.getSerializedValue();
            if (val != null) {
                putKeys.add(req.getSerializedKey());
                putValues.add(val);
            } else {
                linker.delete(db, cf, req.getSerializedKey());
            }
        }
        if (!putKeys.isEmpty()) {
            linker.batchPut(
                    db, cf, putKeys.toArray(new byte[0][]), putValues.toArray(new byte[0][]));
        }
        for (int i = 0; i < puts.size(); i++) {
            puts.get(i).complete();
        }
        return puts.size();
    }

    private int executeIters(List<ForStRsDBIterRequest<?, ?, ?, ?>> iters) {
        if (iters.isEmpty()) {
            return 0;
        }
        // E8-H3: process row-by-row; if iter.process throws on row k, the drain completes
        // rows [k, size) exceptionally. Rows [0, k) are already completed inside process.
        int completed = 0;
        for (ForStRsDBIterRequest<?, ?, ?, ?> iter : iters) {
            iter.process(linker, db, cf, arena);
            completed++;
        }
        return completed;
    }
}

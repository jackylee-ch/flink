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
        try {
            executeGets(classifier.getGetRequests());
            executePuts(classifier.getPutRequests());
            executeIters(classifier.getIterRequests());
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
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

    private void executeGets(List<ForStRsDBGetRequest<?, ?, ?>> gets) {
        if (gets.isEmpty()) {
            return;
        }
        int count = gets.size();
        byte[][] keys = new byte[count][];
        for (int i = 0; i < count; i++) {
            keys[i] = gets.get(i).getSerializedKey();
        }
        byte[][] results = linker.batchGetArrow(db, cf, keys);
        for (int i = 0; i < count; i++) {
            gets.get(i).complete(results[i]);
        }
    }

    private void executePuts(List<ForStRsDBPutRequest<?, ?, ?>> puts) {
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
    }

    private void executeIters(List<ForStRsDBIterRequest<?, ?, ?, ?>> iters) {
        if (iters.isEmpty()) {
            return;
        }
        for (ForStRsDBIterRequest<?, ?, ?, ?> iter : iters) {
            iter.process(linker, db, cf, arena);
        }
    }
}

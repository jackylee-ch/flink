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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.ffm.FrsIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Classifies incoming state requests into get and put lists for batch execution. Implements {@link
 * AsyncRequestContainer} so the Flink async execution controller can accumulate requests before
 * dispatching them to the executor.
 */
@Internal
public class ForStRsStateRequestClassifier
        implements AsyncRequestContainer<StateRequest<?, ?, ?, ?>> {

    private final List<ForStRsDBGetRequest<?, ?, ?>> getRequests = new ArrayList<>();
    private final List<ForStRsDBPutRequest<?, ?, ?>> putRequests = new ArrayList<>();
    private final List<ForStRsDBIterRequest<?, ?, ?, ?>> iterRequests = new ArrayList<>();

    @Override
    @SuppressWarnings("unchecked")
    public void offer(StateRequest<?, ?, ?, ?> stateRequest) {
        Object state = stateRequest.getState();
        if (!(state instanceof ForStRsInnerTable)) {
            throw new IllegalArgumentException(
                    "State "
                            + state.getClass().getName()
                            + " does not implement ForStRsInnerTable");
        }
        ForStRsInnerTable<?, ?, ?> table = (ForStRsInnerTable<?, ?, ?>) state;
        StateRequestType type = stateRequest.getRequestType();
        switch (type) {
            case VALUE_GET:
            case LIST_GET:
            case MAP_GET:
            case MAP_CONTAINS:
            case REDUCING_GET:
            case AGGREGATING_GET:
                getRequests.add(buildGetRequest(table, stateRequest));
                break;
            case VALUE_UPDATE:
            case CLEAR:
            case LIST_ADD:
            case LIST_UPDATE:
            case LIST_ADD_ALL:
            case MAP_PUT:
            case MAP_PUT_ALL:
            case MAP_REMOVE:
            case REDUCING_ADD:
            case AGGREGATING_ADD:
                putRequests.add(buildPutRequest(table, stateRequest));
                break;
            case MAP_IS_EMPTY:
            case MAP_ITER:
            case MAP_ITER_KEY:
            case MAP_ITER_VALUE:
            case ITERATOR_LOADING:
                iterRequests.add(buildIterRequest(table, stateRequest));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported state request type: " + type);
        }
    }

    @Override
    public boolean isEmpty() {
        return getRequests.isEmpty() && putRequests.isEmpty() && iterRequests.isEmpty();
    }

    public List<ForStRsDBGetRequest<?, ?, ?>> getGetRequests() {
        return getRequests;
    }

    public List<ForStRsDBPutRequest<?, ?, ?>> getPutRequests() {
        return putRequests;
    }

    public List<ForStRsDBIterRequest<?, ?, ?, ?>> getIterRequests() {
        return iterRequests;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ForStRsDBGetRequest<?, ?, ?> buildGetRequest(
            ForStRsInnerTable table, StateRequest request) {
        return table.buildDBGetRequest(request);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ForStRsDBPutRequest<?, ?, ?> buildPutRequest(
            ForStRsInnerTable table, StateRequest request) {
        return table.buildDBPutRequest(request);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ForStRsDBIterRequest<?, ?, ?, ?> buildIterRequest(
            ForStRsInnerTable table, StateRequest request) {
        ForStRsIterableState iterableState = (ForStRsIterableState) table;
        StateRequestType type = request.getRequestType();
        StateRequestType originalType = type;
        FrsIterator existingIter = null;
        long existingVecHandle = 0L;
        if (type == StateRequestType.ITERATOR_LOADING) {
            Tuple2<StateRequestType, ForStRsMapIterator.IterContinuation> payload =
                    (Tuple2<StateRequestType, ForStRsMapIterator.IterContinuation>)
                            request.getPayload();
            originalType = payload.f0;
            ForStRsMapIterator.IterContinuation continuation = payload.f1;
            if (continuation != null) {
                existingIter = continuation.iter;
                existingVecHandle = continuation.vecHandle;
            }
        }
        byte[] prefix = iterableState.getIterPrefix(request);
        ForStRsDBIterRequest<?, ?, ?, ?> req =
                new ForStRsDBIterRequest<>(
                        prefix, request, originalType, iterableState, existingIter);
        if (existingVecHandle != 0L) {
            req.setExistingVecHandle(existingVecHandle);
        }
        return req;
    }
}

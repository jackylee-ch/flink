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
 * Classifies incoming state requests into op-type-partitioned columnar buffers, replacing the
 * legacy {@code ForStRsStateRequestClassifier}'s per-request {@code byte[]} + wrapper-object
 * allocation.
 *
 * <p>Three parallel buffer sets, one per op type, so each set is contiguous and can be passed
 * directly to the vectorized FFI without sub-buffer building:
 *
 * <ul>
 *   <li>GET path — {@link #getKeys} + parallel {@link #getRequests} / {@link #getTables}
 *   <li>PUT path — {@link #putKeys} + {@link #putValues} + parallel {@link #putRequests}
 *   <li>DELETE path — {@link #deleteKeys} + parallel {@link #deleteRequests}
 * </ul>
 *
 * <p>Iterator requests can not be batched by columnar dispatch (each prefix produces its own row
 * set, and existing {@link FrsIterator} state must be preserved across calls), so they remain on a
 * separate list and are executed sequentially by the executor.
 *
 * <p>Correctness: Flink's async-v2 framework guarantees that all requests for the same record-key
 * are serialized via the RecordContext lock, so a single batch contains at most one in-flight op
 * per logical state key. Within a batch the executor may reorder by op type freely (spec §
 * Correctness Invariants).
 */
@Internal
public class VectorizedClassifier
        implements AsyncRequestContainer<StateRequest<?, ?, ?, ?>> {

    private static final int INIT_SLOTS = 256;

    private final ColumnarBatchBuffer getKeys;
    private final ColumnarBatchBuffer putKeys;
    private final ColumnarBatchBuffer putValues;
    private final ColumnarBatchBuffer deleteKeys;

    private StateRequest<?, ?, ?, ?>[] getRequests;
    private ForStRsInnerTable<?, ?, ?>[] getTables;
    private int getCount;

    private StateRequest<?, ?, ?, ?>[] putRequests;
    private int putCount;

    private StateRequest<?, ?, ?, ?>[] deleteRequests;
    private int deleteCount;

    private final List<ForStRsDBIterRequest<?, ?, ?, ?>> iterRequests = new ArrayList<>();

    public VectorizedClassifier(
            ColumnarBatchBuffer getKeys,
            ColumnarBatchBuffer putKeys,
            ColumnarBatchBuffer putValues,
            ColumnarBatchBuffer deleteKeys) {
        this.getKeys = getKeys;
        this.putKeys = putKeys;
        this.putValues = putValues;
        this.deleteKeys = deleteKeys;
        this.getRequests = new StateRequest<?, ?, ?, ?>[INIT_SLOTS];
        this.getTables = new ForStRsInnerTable<?, ?, ?>[INIT_SLOTS];
        this.putRequests = new StateRequest<?, ?, ?, ?>[INIT_SLOTS];
        this.deleteRequests = new StateRequest<?, ?, ?, ?>[INIT_SLOTS];
    }

    public void reset() {
        getKeys.reset();
        putKeys.reset();
        putValues.reset();
        deleteKeys.reset();
        getCount = 0;
        putCount = 0;
        deleteCount = 0;
        iterRequests.clear();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
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
                recordGet(table, (StateRequest) stateRequest);
                break;
            case VALUE_UPDATE:
            case LIST_ADD:
            case LIST_UPDATE:
            case LIST_ADD_ALL:
            case MAP_PUT:
            case MAP_PUT_ALL:
            case REDUCING_ADD:
            case AGGREGATING_ADD:
                // A null payload on an UPDATE/ADD is the canonical Flink idiom
                // for "clear this entry" (matches the legacy path's
                // `serializedValue == null → delete` behaviour).
                if (stateRequest.getPayload() == null) {
                    recordDelete(table, (StateRequest) stateRequest);
                } else {
                    recordPut(table, (StateRequest) stateRequest);
                }
                break;
            case CLEAR:
            case MAP_REMOVE:
                recordDelete(table, (StateRequest) stateRequest);
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
        return getCount == 0 && putCount == 0 && deleteCount == 0 && iterRequests.isEmpty();
    }

    public int getCount() {
        return getCount;
    }

    public int putCount() {
        return putCount;
    }

    public int deleteCount() {
        return deleteCount;
    }

    public ColumnarBatchBuffer getKeys() {
        return getKeys;
    }

    public ColumnarBatchBuffer putKeys() {
        return putKeys;
    }

    public ColumnarBatchBuffer putValues() {
        return putValues;
    }

    public ColumnarBatchBuffer deleteKeys() {
        return deleteKeys;
    }

    public StateRequest<?, ?, ?, ?>[] getRequests() {
        return getRequests;
    }

    public ForStRsInnerTable<?, ?, ?>[] getTables() {
        return getTables;
    }

    public StateRequest<?, ?, ?, ?>[] putRequests() {
        return putRequests;
    }

    public StateRequest<?, ?, ?, ?>[] deleteRequests() {
        return deleteRequests;
    }

    public List<ForStRsDBIterRequest<?, ?, ?, ?>> iterRequests() {
        return iterRequests;
    }

    private <K, N, V> void recordGet(
            ForStRsInnerTable<K, N, V> table, StateRequest<K, N, ?, ?> request) {
        ensureGetCapacity();
        table.serializeKeyInto(request, getKeys);
        getRequests[getCount] = request;
        getTables[getCount] = table;
        getCount++;
    }

    private <K, N, V> void recordPut(
            ForStRsInnerTable<K, N, V> table, StateRequest<K, N, ?, ?> request) {
        ensurePutCapacity();
        table.serializeKeyInto(request, putKeys);
        table.serializeValueInto(request, putValues);
        putRequests[putCount] = request;
        putCount++;
    }

    private <K, N, V> void recordDelete(
            ForStRsInnerTable<K, N, V> table, StateRequest<K, N, ?, ?> request) {
        ensureDeleteCapacity();
        table.serializeKeyInto(request, deleteKeys);
        deleteRequests[deleteCount] = request;
        deleteCount++;
    }

    private void ensureGetCapacity() {
        if (getCount < getRequests.length) {
            return;
        }
        int newCap = getRequests.length << 1;
        StateRequest<?, ?, ?, ?>[] r = new StateRequest<?, ?, ?, ?>[newCap];
        System.arraycopy(getRequests, 0, r, 0, getRequests.length);
        getRequests = r;
        ForStRsInnerTable<?, ?, ?>[] t = new ForStRsInnerTable<?, ?, ?>[newCap];
        System.arraycopy(getTables, 0, t, 0, getTables.length);
        getTables = t;
    }

    private void ensurePutCapacity() {
        if (putCount < putRequests.length) {
            return;
        }
        int newCap = putRequests.length << 1;
        StateRequest<?, ?, ?, ?>[] r = new StateRequest<?, ?, ?, ?>[newCap];
        System.arraycopy(putRequests, 0, r, 0, putRequests.length);
        putRequests = r;
    }

    private void ensureDeleteCapacity() {
        if (deleteCount < deleteRequests.length) {
            return;
        }
        int newCap = deleteRequests.length << 1;
        StateRequest<?, ?, ?, ?>[] r = new StateRequest<?, ?, ?, ?>[newCap];
        System.arraycopy(deleteRequests, 0, r, 0, deleteRequests.length);
        deleteRequests = r;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ForStRsDBIterRequest<?, ?, ?, ?> buildIterRequest(
            ForStRsInnerTable table, StateRequest request) {
        ForStRsIterableState iterableState = (ForStRsIterableState) table;
        StateRequestType type = request.getRequestType();
        StateRequestType originalType = type;
        FrsIterator existingIter = null;
        if (type == StateRequestType.ITERATOR_LOADING) {
            Tuple2<StateRequestType, FrsIterator> payload =
                    (Tuple2<StateRequestType, FrsIterator>) request.getPayload();
            originalType = payload.f0;
            existingIter = payload.f1;
        }
        byte[] prefix = iterableState.getIterPrefix(request);
        return new ForStRsDBIterRequest<>(
                prefix, request, originalType, iterableState, existingIter);
    }
}

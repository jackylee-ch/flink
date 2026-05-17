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

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
 *
 * <p><b>Extension (P2 Batch C):</b> {@link #submitVectorized(VectorizedStateRequest)} accepts the
 * sealed {@link VectorizedStateRequest} hierarchy directly (the off-heap, new-style path). The
 * three new kinds are routed to {@link AppendMergeBatchBuffer}, {@link IterPrefixBatchBuffer}, and
 * {@link IterRangeBatchBuffer} respectively. The APPEND_MERGE-ListState-only guard (spec §1 §a) is
 * enforced at submission time: callers must first register list-state names via {@link
 * #registerListState(String)}.
 */
@Internal
public class VectorizedClassifier implements AsyncRequestContainer<StateRequest<?, ?, ?, ?>> {

    private static final int INIT_SLOTS = 256;

    private final ColumnarBatchBuffer getKeys;
    private final ColumnarBatchBuffer putKeys;
    private final ColumnarBatchBuffer putValues;
    private final ColumnarBatchBuffer deleteKeys;

    // -- New-style (VectorizedStateRequest / off-heap) batch buffers for P2 Batch C kinds --

    /** Buffer for APPEND_MERGE requests (ListState-only, spec §1 §a). Wired to FFI in P6. */
    private AppendMergeBatchBuffer appendMergeBuffer;

    /** Buffer for ITER_PREFIX requests. Wired to FFI in P3. */
    private IterPrefixBatchBuffer iterPrefixBuffer;

    /** Buffer for ITER_RANGE requests. Wired to FFI in P9. */
    private IterRangeBatchBuffer iterRangeBuffer;

    /**
     * Registry of state names that belong to a {@code ListState}. Used by the APPEND_MERGE guard
     * (spec §1 §a). Thread-safe; populated once per state primitive registration, not on the hot
     * path.
     */
    private final Set<String> listStateNames = ConcurrentHashMap.newKeySet();

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
        // appendMergeBuffer / iterPrefixBuffer / iterRangeBuffer are lazy-initialized by
        // initNewKindBuffers(Arena) to avoid requiring an Arena here.
    }

    /**
     * Initialises the three new-kind buffers (APPEND_MERGE, ITER_PREFIX, ITER_RANGE). Must be
     * called before {@link #submitVectorized(VectorizedStateRequest)} is used. Idempotent — safe to
     * call multiple times with the same arena.
     */
    public void initNewKindBuffers(Arena arena) {
        if (appendMergeBuffer == null) {
            appendMergeBuffer = new AppendMergeBatchBuffer(arena);
        }
        if (iterPrefixBuffer == null) {
            iterPrefixBuffer = new IterPrefixBatchBuffer();
        }
        if (iterRangeBuffer == null) {
            iterRangeBuffer = new IterRangeBatchBuffer();
        }
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
        if (appendMergeBuffer != null) {
            appendMergeBuffer.reset();
        }
        if (iterPrefixBuffer != null) {
            iterPrefixBuffer.reset();
        }
        if (iterRangeBuffer != null) {
            iterRangeBuffer.reset();
        }
    }

    // -----------------------------------------------------------------
    // New-style (VectorizedStateRequest / off-heap) submission path
    // -----------------------------------------------------------------

    /**
     * Submits a {@link VectorizedStateRequest} via the off-heap dispatch path.
     *
     * <p>This is the entry point for the sealed interface hierarchy (post-P2.4). It coexists with
     * the existing {@link #offer(StateRequest)} Flink-runtime path; both may be used in the same
     * batch.
     *
     * <p><b>Spec §1 §a guard:</b> APPEND_MERGE is ListState-only. If the request's {@link
     * VectorizedStateRequest#stateName()} is not registered via {@link #registerListState(String)},
     * an {@link IllegalArgumentException} is thrown at classification time.
     *
     * @throws IllegalStateException if the new-kind buffers have not been initialised via {@link
     *     #initNewKindBuffers(Arena)}
     * @throws IllegalArgumentException if an APPEND_MERGE request targets a non-list state
     */
    public void submitVectorized(VectorizedStateRequest req) {
        switch (req.kind()) {
            case APPEND_MERGE:
                // §1 §a guard: APPEND_MERGE is ListState-only.
                if (!isListStateName(req.stateName())) {
                    throw new IllegalArgumentException(
                            "APPEND_MERGE is ListState-only per spec §1 §a — got stateName="
                                    + req.stateName()
                                    + ". Reducing/Aggregating state must use the RMW cache path"
                                    + " (GET + combine + PUT, spec §3 Trace A).");
                }
                ensureAppendMergeBuffer();
                appendMergeBuffer.append((AppendMergeRequest) req);
                break;
            case ITER_PREFIX:
                ensureIterPrefixBuffer();
                iterPrefixBuffer.append((IterPrefixRequest) req);
                break;
            case ITER_RANGE:
                ensureIterRangeBuffer();
                iterRangeBuffer.append((IterRangeRequest) req);
                break;
            case GET:
            case PUT:
            case DELETE:
                // These kinds are handled by the existing Flink-runtime offer() path.
                // If callers submit new-style GET/PUT/DELETE via submitVectorized(), they
                // must first wrap them appropriately. For now, reject with a clear message
                // to avoid silent mis-routing until P5 wires the full new-style path.
                throw new UnsupportedOperationException(
                        "GET/PUT/DELETE via submitVectorized() is not yet supported. "
                                + "Use the existing offer(StateRequest) Flink-runtime path. "
                                + "Full new-style GET/PUT/DELETE routing lands in P5.");
            default:
                throw new UnsupportedOperationException("Unknown kind: " + req.kind());
        }
    }

    // -----------------------------------------------------------------
    // ListState registry (§1 §a guard)
    // -----------------------------------------------------------------

    /**
     * Registers {@code stateName} as belonging to a {@code ListState}. After registration,
     * APPEND_MERGE requests for this name are accepted. Call once when the ListState primitive is
     * created.
     */
    public void registerListState(String stateName) {
        listStateNames.add(stateName);
    }

    /**
     * Removes {@code stateName} from the list-state registry. Call when the ListState primitive is
     * closed/destroyed.
     */
    public void unregisterListState(String stateName) {
        listStateNames.remove(stateName);
    }

    /** Returns {@code true} if {@code stateName} is registered as a ListState. */
    public boolean isListStateName(String stateName) {
        return listStateNames.contains(stateName);
    }

    // -----------------------------------------------------------------
    // Accessors for new-kind buffers (used by VectorizedExecutor)
    // -----------------------------------------------------------------

    /** Returns the APPEND_MERGE buffer, or {@code null} if not yet initialised. */
    public AppendMergeBatchBuffer appendMergeBuffer() {
        return appendMergeBuffer;
    }

    /** Returns the ITER_PREFIX buffer, or {@code null} if not yet initialised. */
    public IterPrefixBatchBuffer iterPrefixBuffer() {
        return iterPrefixBuffer;
    }

    /** Returns the ITER_RANGE buffer, or {@code null} if not yet initialised. */
    public IterRangeBatchBuffer iterRangeBuffer() {
        return iterRangeBuffer;
    }

    // -----------------------------------------------------------------
    // Lazy init helpers (fail-fast when buffers not configured)
    // -----------------------------------------------------------------

    private void ensureAppendMergeBuffer() {
        if (appendMergeBuffer == null) {
            throw new IllegalStateException(
                    "AppendMerge buffer not initialised — call initNewKindBuffers(Arena) first");
        }
    }

    private void ensureIterPrefixBuffer() {
        if (iterPrefixBuffer == null) {
            throw new IllegalStateException(
                    "IterPrefix buffer not initialised — call initNewKindBuffers(Arena) first");
        }
    }

    private void ensureIterRangeBuffer() {
        if (iterRangeBuffer == null) {
            throw new IllegalStateException(
                    "IterRange buffer not initialised — call initNewKindBuffers(Arena) first");
        }
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

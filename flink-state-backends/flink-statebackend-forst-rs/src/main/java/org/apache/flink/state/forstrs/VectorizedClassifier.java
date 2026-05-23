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

    // V3.1: parallel StateRequest list for APPEND_MERGE so future completion can be
    // plumbed back to the Flink-runtime async future at dispatch end (Option Z, §4.3).
    private StateRequest<?, ?, ?, ?>[] appendMergeRequests;
    private int appendMergeCount;

    // PR-C2: parallel arrays for the off-heap APPEND_MERGE fast path. For each row {@code i}, if
    // {@code offHeapAppendMergeFutures[i] != null} the row went through the per-state
    // {@link org.apache.flink.state.forstrs.state.ListStateArrowBuffer}; otherwise it's a heap-path
    // row sharing {@link #appendMergeBuffer}. The executor uses these to (a) complete the row's
    // {@code AppendMergeRequest}-equivalent future on the off-heap-path, and (b) drain unique
    // state-instance buffers exactly once after dispatch.
    private java.util.concurrent.CompletableFuture<Void>[] offHeapAppendMergeFutures;
    private org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2<?, ?, ?>[]
            offHeapAppendMergeStates;

    private final List<ForStRsDBIterRequest<?, ?, ?, ?>> iterRequests = new ArrayList<>();

    /**
     * A6-H4: poison flag set by {@link #recordDelete} when the {@code onClear} hook throws
     * (typically an FFI {@code linker.batchPut} failure from a per-state list-buffer drain).
     * {@code VectorizedExecutor#executeBatchRequests} consults {@link #batchPoisonCause()}
     * BEFORE dispatching each step and aborts with this throwable as the cause — preventing
     * the partial batch from being silently discarded by the next {@link #reset()}.
     *
     * <p>Cleared in {@link #reset()} so a poisoned batch's torn-down state does not bleed into
     * the next batch.
     */
    private volatile Throwable batchPoisonCause = null;

    /**
     * R20-H1: explicit tracking of which {@link StateRequest}s have already had their future
     * completed exceptionally by {@link #recordDelete}'s onClear-throw handler. Required because
     * {@link org.apache.flink.core.asyncprocessing.AsyncFutureImpl#completeExceptionally(String,
     * Throwable)} in flink-core delegates ONLY to the framework exception handler — it does NOT
     * touch the internal {@code completableFuture}, so {@code getFuture().isDone()} keeps
     * returning {@code false} even after exceptional completion fired. The R19-H1 isDone() guard
     * in {@link VectorizedExecutor#executeRequestSync} was therefore ineffective in production.
     *
     * <p>Set membership is the source of truth: {@code recordDelete} adds the request when it
     * pre-completes the future; {@link VectorizedExecutor#executeRequestSync}'s outer catch
     * checks {@link #wasClassifierCompletedExceptionally(StateRequest)} to decide whether to
     * skip the second {@code completePutExceptionally} call.
     *
     * <p>Membership is bounded by the number of concurrent {@code recordDelete} calls within a
     * single batch (= batch size). Cleared on {@link #reset()}.
     */
    private final Set<StateRequest<?, ?, ?, ?>> classifierCompletedExceptionally =
            ConcurrentHashMap.newKeySet();

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
        this.appendMergeRequests = new StateRequest<?, ?, ?, ?>[INIT_SLOTS];
        @SuppressWarnings({"unchecked", "rawtypes"})
        java.util.concurrent.CompletableFuture<Void>[] futs =
                (java.util.concurrent.CompletableFuture<Void>[])
                        new java.util.concurrent.CompletableFuture[INIT_SLOTS];
        this.offHeapAppendMergeFutures = futs;
        this.offHeapAppendMergeStates =
                new org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2<?, ?, ?>[INIT_SLOTS];
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
        // PR-C2: clear parallel off-heap arrays up to the prior appendMergeCount so stale state
        // refs don't keep ListStateArrowBuffers alive across batch boundaries.
        if (offHeapAppendMergeFutures != null) {
            for (int i = 0; i < appendMergeCount; i++) {
                offHeapAppendMergeFutures[i] = null;
                offHeapAppendMergeStates[i] = null;
            }
        }
        appendMergeCount = 0;
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
        // A6-H4: clear the poison once the batch is fully torn down so the next batch
        // starts in a clean state. Callers must read {@link #batchPoisonCause()} before
        // calling reset() if they need the cause for diagnostics.
        batchPoisonCause = null;
        // R20-H1: clear the explicit "classifier already completed this request exceptionally"
        // tracker so the next batch starts fresh. Membership is meaningful only within a single
        // batch's offer-phase + executeBatchRequests/executeRequestSync window.
        classifierCompletedExceptionally.clear();
    }

    /**
     * R20-H1: returns {@code true} iff {@code request}'s future has already been completed
     * exceptionally by {@link #recordDelete} (typically because the state's {@code onClear} hook
     * threw an FFI failure). The outer catch in {@link VectorizedExecutor#executeRequestSync}
     * (and the batched-path equivalent) uses this to skip the second
     * {@code completePutExceptionally} call that would otherwise double-fire the framework
     * exception handler in production (where
     * {@link org.apache.flink.core.asyncprocessing.AsyncFutureImpl#completeExceptionally} does
     * not change {@code isDone()} state).
     *
     * <p>This method removes the entry on lookup so the membership set stays bounded by the
     * number of concurrent in-flight failures, not by the total number of historical failures.
     * Each request is checked at most once per batch.
     */
    public boolean takeClassifierCompletedExceptionally(StateRequest<?, ?, ?, ?> request) {
        return classifierCompletedExceptionally.remove(request);
    }

    /**
     * R21-H1: record that {@code request}'s future is about to be (or has just been) completed
     * exceptionally on an executor-side per-op catch path (executePuts / executeDeletes /
     * executeGets / dispatchAppendMerge*). Must be called BEFORE the per-op
     * {@code completePutExceptionally} / {@code completeDeleteExceptionally} /
     * {@code completeGetExceptionally} call so that {@link VectorizedExecutor#executeRequestSync}'s
     * outer catch can consult {@link #takeClassifierCompletedExceptionally} and skip the second
     * completion attempt (otherwise the framework {@code AsyncFrameworkExceptionHandler} sees the
     * failure TWICE: once from the per-op catch and once from the outer catch, producing a double
     * task-failure log).
     *
     * <p>Idempotent: re-adding an already-present request is a no-op. Membership is cleared by
     * {@link #reset()}; lookup via {@link #takeClassifierCompletedExceptionally} removes the entry
     * so the set stays bounded by in-flight concurrent failures (= batch size).
     */
    public void markCompletedExceptionally(StateRequest<?, ?, ?, ?> request) {
        if (request != null) {
            classifierCompletedExceptionally.add(request);
        }
    }

    /**
     * A6-H4: returns the throwable that poisoned this batch (or {@code null} if the batch is
     * clean). Set when an {@code onClear} drain throws inside {@link #recordDelete}; cleared by
     * {@link #reset()}. The {@link
     * org.apache.flink.state.forstrs.VectorizedExecutor#executeBatchRequests} loop checks this
     * before each dispatch step and aborts with this cause so the partially-built batch is not
     * silently dropped.
     */
    public Throwable batchPoisonCause() {
        return batchPoisonCause;
    }

    /**
     * R20-M1: drain every per-row future that has already been classified into this batch with
     * the given {@code cause}. Called from {@link #recordDelete}'s onClear-throw handler before
     * the rethrow so rows 0..N-1 don't hang on unresolved futures after
     * {@code executeBatchRequests} bails out via {@link #batchPoisonCause()}.
     *
     * <p>Walks the four classified-row arrays (getRequests, putRequests, deleteRequests,
     * appendMergeRequests) and the iter list. The current request itself is NOT drained here —
     * it was already pre-completed by the caller above.
     *
     * <p>Each drained request is also added to {@link #classifierCompletedExceptionally} so a
     * subsequent {@code executeBatchRequests} path (or {@code executeRequestSync} outer catch)
     * skips re-completion.
     *
     * <p>Best-effort: a throw from any individual {@code completeExceptionally} call is
     * swallowed so the rest of the rows still drain. The set membership marker is set
     * regardless so the outer catch never re-completes.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void drainClassifiedRowsExceptionally(Throwable cause) {
        String prefix = "ForSt-RS onClear flush failed (batched-row drain): ";
        String msg = prefix + cause.getClass().getSimpleName();
        for (int i = 0; i < getCount; i++) {
            failPerRowFuture(getRequests[i], msg, cause);
        }
        for (int i = 0; i < putCount; i++) {
            failPerRowFuture(putRequests[i], msg, cause);
        }
        for (int i = 0; i < deleteCount; i++) {
            failPerRowFuture(deleteRequests[i], msg, cause);
        }
        for (int i = 0; i < appendMergeCount; i++) {
            failPerRowFuture(appendMergeRequests[i], msg, cause);
        }
        // R21-M1: per-iter row may be null (defensive — see R21 review). Skip null entries so
        // we don't NPE on {@code ir.getStateRequest()}.
        for (int i = 0; i < iterRequests.size(); i++) {
            ForStRsDBIterRequest<?, ?, ?, ?> ir = iterRequests.get(i);
            if (ir == null) {
                continue;
            }
            StateRequest<?, ?, ?, ?> sr = ir.getStateRequest();
            if (sr != null) {
                failPerRowFuture(sr, msg, cause);
            }
        }
        // R21-H2: discard off-heap-buffered rows for every list-state-instance that the
        // poisoned batch wrote into. Without this, the per-state {@link ListStateArrowBuffer}
        // retains the buffered rows and the NEXT batch's {@code flushIfDirty} writes them
        // (along with new rows) to the engine — silently breaking exactly-once because the
        // owning StateRequests were already reported as failed above. The design constraint is
        // "off-heap buffered rows for failed StateRequests must NOT be flushed to the engine"
        // across ALL exception paths.
        //
        // Implementation: walk the parallel {@code offHeapAppendMergeStates} array (one slot
        // per APPEND_MERGE row that took the off-heap fast path). De-dup is automatic because
        // {@link ForStRsAsyncListStateV2#discardBufferedRows} clears the buffer on the first
        // call and {@link ListStateArrowBuffer#discardWithCause} short-circuits on an empty
        // buffer for subsequent rows pointing to the same state.
        if (offHeapAppendMergeStates != null) {
            for (int i = 0; i < appendMergeCount; i++) {
                org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2 s =
                        offHeapAppendMergeStates[i];
                if (s != null) {
                    try {
                        s.discardBufferedRows(cause);
                    } catch (Throwable ignore) {
                        // Best-effort: keep draining remaining states even if one buffer
                        // discard throws. The amDirty bit is cleared inside the discard path
                        // even on the empty-buffer short-circuit, so subsequent batches won't
                        // re-walk this state.
                    }
                }
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void failPerRowFuture(StateRequest<?, ?, ?, ?> request, String msg, Throwable cause) {
        if (request == null) {
            return;
        }
        // Mark first so even if the completion call throws, the outer catch still skips.
        classifierCompletedExceptionally.add(request);
        try {
            ((org.apache.flink.core.asyncprocessing.InternalAsyncFuture<Object>)
                            request.getFuture())
                    .completeExceptionally(msg, cause);
        } catch (Throwable ignore) {
            // Best-effort: continue draining the rest. The set membership marker is already in
            // place so executeRequestSync's outer catch won't double-fire on this request.
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

    // -----------------------------------------------------------------
    // PR-F1: precomputed dispatch table for offer().
    //
    // Replaces the previous ~22-case switch on StateRequestType with a single
    // array lookup (StateRequestType.ordinal() → DispatchKind) followed by a
    // small 5-case switch. This is branch-predictor friendly and reduces the
    // instruction-cache footprint on the hot path; closes V2-7 (B-H7).
    // -----------------------------------------------------------------

    /**
     * Coarse-grained routing kind used by {@link #offer(StateRequest)}. The mapping from {@link
     * StateRequestType} → {@code DispatchKind} is precomputed at class-load time into {@link
     * #DISPATCH_TABLE}.
     *
     * <p>Distinct from {@link VectorizedStateRequest.Kind}: that one is the public sealed-request
     * tag; this one is an internal classifier-routing tag.
     */
    public enum DispatchKind {
        /** Read paths → recordGet. */
        GET,
        /** Write paths whose payload may be null (null → delete) → recordPut or recordDelete. */
        PUT,
        /** Pure delete paths → recordDelete. */
        DELETE,
        /** Iterator paths → iterRequests.add(...). */
        ITER,
        /** LIST_ADD / LIST_ADD_ALL: list-state routes to APPEND_MERGE, else falls back to PUT. */
        APPEND_MERGE_CANDIDATE
    }

    /**
     * Precomputed routing table, indexed by {@link StateRequestType#ordinal()}. A {@code null} entry
     * means the corresponding {@code StateRequestType} is not handled by {@link
     * #offer(StateRequest)} and will trigger an {@link UnsupportedOperationException}.
     *
     * <p>Visible for the {@code OfferDispatchTableParityTest} regression gate.
     */
    public static final DispatchKind[] DISPATCH_TABLE;

    static {
        StateRequestType[] all = StateRequestType.values();
        DISPATCH_TABLE = new DispatchKind[all.length];
        // Default: every slot null (= unsupported) until explicitly populated.
        DISPATCH_TABLE[StateRequestType.VALUE_GET.ordinal()] = DispatchKind.GET;
        DISPATCH_TABLE[StateRequestType.LIST_GET.ordinal()] = DispatchKind.GET;
        DISPATCH_TABLE[StateRequestType.MAP_GET.ordinal()] = DispatchKind.GET;
        DISPATCH_TABLE[StateRequestType.MAP_CONTAINS.ordinal()] = DispatchKind.GET;
        DISPATCH_TABLE[StateRequestType.REDUCING_GET.ordinal()] = DispatchKind.GET;
        DISPATCH_TABLE[StateRequestType.AGGREGATING_GET.ordinal()] = DispatchKind.GET;

        DISPATCH_TABLE[StateRequestType.VALUE_UPDATE.ordinal()] = DispatchKind.PUT;
        DISPATCH_TABLE[StateRequestType.LIST_UPDATE.ordinal()] = DispatchKind.PUT;
        DISPATCH_TABLE[StateRequestType.MAP_PUT.ordinal()] = DispatchKind.PUT;
        DISPATCH_TABLE[StateRequestType.MAP_PUT_ALL.ordinal()] = DispatchKind.PUT;
        DISPATCH_TABLE[StateRequestType.REDUCING_ADD.ordinal()] = DispatchKind.PUT;
        DISPATCH_TABLE[StateRequestType.AGGREGATING_ADD.ordinal()] = DispatchKind.PUT;

        DISPATCH_TABLE[StateRequestType.CLEAR.ordinal()] = DispatchKind.DELETE;
        DISPATCH_TABLE[StateRequestType.MAP_REMOVE.ordinal()] = DispatchKind.DELETE;

        DISPATCH_TABLE[StateRequestType.MAP_IS_EMPTY.ordinal()] = DispatchKind.ITER;
        DISPATCH_TABLE[StateRequestType.MAP_ITER.ordinal()] = DispatchKind.ITER;
        DISPATCH_TABLE[StateRequestType.MAP_ITER_KEY.ordinal()] = DispatchKind.ITER;
        DISPATCH_TABLE[StateRequestType.MAP_ITER_VALUE.ordinal()] = DispatchKind.ITER;
        DISPATCH_TABLE[StateRequestType.ITERATOR_LOADING.ordinal()] = DispatchKind.ITER;

        DISPATCH_TABLE[StateRequestType.LIST_ADD.ordinal()] = DispatchKind.APPEND_MERGE_CANDIDATE;
        DISPATCH_TABLE[StateRequestType.LIST_ADD_ALL.ordinal()] =
                DispatchKind.APPEND_MERGE_CANDIDATE;

        // Intentionally left as null (unsupported by offer()):
        //   SYNC_POINT — framework sync only, never reaches the classifier
        //   CUSTOMIZED — backend-defined; not handled by ForSt-RS today
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
        // Single array load → branch-predictor friendly. JIT can lower the
        // small 5-case switch below into a tableswitch / jump table.
        DispatchKind kind = DISPATCH_TABLE[type.ordinal()];
        if (kind == null) {
            throw new UnsupportedOperationException("Unsupported state request type: " + type);
        }
        switch (kind) {
            case GET:
                recordGet(table, (StateRequest) stateRequest);
                break;
            case PUT:
                // A null payload on an UPDATE/ADD is the canonical Flink idiom
                // for "clear this entry" (matches the legacy path's
                // `serializedValue == null → delete` behaviour).
                if (stateRequest.getPayload() == null) {
                    recordDelete(table, (StateRequest) stateRequest);
                } else {
                    recordPut(table, (StateRequest) stateRequest);
                }
                break;
            case DELETE:
                recordDelete(table, (StateRequest) stateRequest);
                break;
            case ITER:
                iterRequests.add(buildIterRequest(table, stateRequest));
                break;
            case APPEND_MERGE_CANDIDATE:
                // V3.1: LIST_ADD / LIST_ADD_ALL on a registered ListState routes to
                // APPEND_MERGE instead of destructive PUT. Falls back to PUT if the
                // state is not registered (shouldn't happen via the public API, but
                // defensive). A null payload still routes to delete.
                //
                // B4-H4 (zero-copy): replaces the previous per-record {@code
                // listStateNames.contains(name)} {@link java.util.Set#contains} + {@code
                // String.hashCode()} with a single per-state-instance boolean read on the
                // table itself ({@link ForStRsInnerTable#isListState()}, default-false,
                // overridden to true by {@code ForStRsAsyncListStateV2}). The
                // {@link #listStateNames} registry is still maintained for inspection / tests,
                // but is no longer consulted on the dispatch hot path.
                if (stateRequest.getPayload() == null) {
                    recordDelete(table, (StateRequest) stateRequest);
                } else if (table.isListState()) {
                    recordAppendMerge(table, (StateRequest) stateRequest);
                } else {
                    recordPut(table, (StateRequest) stateRequest);
                }
                break;
            default:
                // Unreachable: DISPATCH_TABLE only ever stores enum constants above.
                throw new UnsupportedOperationException(
                        "Unsupported dispatch kind: " + kind + " (state request type: " + type + ")");
        }
    }

    @Override
    public boolean isEmpty() {
        return getCount == 0
                && putCount == 0
                && deleteCount == 0
                && appendMergeCount == 0
                && iterRequests.isEmpty();
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <K, N, V> void recordDelete(
            ForStRsInnerTable<K, N, V> table, StateRequest<K, N, ?, ?> request) {
        // A5-H1 / A5-H2: fire the pre-DELETE hook BEFORE the DELETE row is enqueued. State
        // classes use this to drain per-instance write buffers (ListStateArrowBuffer) and
        // invalidate per-instance caches (ReducingAggregatingCache) so that pending APPEND_MERGE
        // rows / dirty cached accumulators do NOT survive the engine-side DELETE.
        //
        // Why here and not in {@link VectorizedExecutor#executeDeletes}: the dispatch order in
        // {@code executeBatchRequests} is PUT → DELETE → GET → ITER → APPEND_MERGE → off-heap
        // drain. If we drained inside the executor we'd have to reorder the loop — but the
        // off-heap APPEND_MERGE drain is bundled with the dispatch step at the END. Calling
        // {@code onClear} at classifier time means the per-state {@code flushIfDirty()} runs
        // SYNCHRONOUSLY (in-mailbox-turn) before {@code executeDeletes} ever sees this row, so
        // pending APPEND_MERGE bytes hit the engine BEFORE the DELETE.
        //
        // A6-H4: {@code onClear} on a list state triggers an FFI {@code linker.batchPut} via
        // {@code ListStateArrowBuffer.flushTo}. If that FFI write throws, the partially-built
        // batch must NOT be silently abandoned by the next {@code reset()} (which is what would
        // happen if we let the exception escape with no rollback). We catch the throwable,
        // (1) complete the current request's future exceptionally so its caller sees the
        // failure, (2) mark the batch poisoned so {@code executeBatchRequests} aborts the
        // remaining dispatch and propagates the error rather than silently dropping the
        // in-progress rows, and (3) rethrow so the mailbox-thread caller sees the failure too.
        try {
            table.onClear(request);
        } catch (Throwable t) {
            // (1) Surface to the request's own future. Cleanup-safe: completeExceptionally is
            // a no-op if the future was already completed by an earlier path.
            //
            // R20-H1: record the request in {@link #classifierCompletedExceptionally} BEFORE
            // calling completeExceptionally. The outer catch in
            // {@code VectorizedExecutor#executeRequestSync} consults this set (not the
            // production-broken {@code isDone()}) to decide whether to skip its own
            // completePutExceptionally call.
            classifierCompletedExceptionally.add(request);
            try {
                ((org.apache.flink.core.asyncprocessing.InternalAsyncFuture<Object>)
                                request.getFuture())
                        .completeExceptionally(
                                "ForSt-RS onClear flush failed: " + t.getClass().getSimpleName(),
                                t);
            } catch (Throwable ignore) {
                // The request may not yet have a future (test path) — swallow so the poison
                // path below still runs and the original cause is re-thrown. The set entry
                // remains so the outer catch still skips a duplicate completion attempt.
            }
            // R20-M1: drain every per-row future from rows already classified into THIS batch
            // before the throw. The batched path ({@link
            // VectorizedExecutor#executeBatchRequests}) reads the poison flag and returns a
            // failed container future WITHOUT entering the per-op executors — those executors
            // are what would otherwise complete getRequests / putRequests / appendMergeRequests
            // / iterRequests futures. Without this drain, rows 0..N-1 sit on unresolved futures
            // forever and AEC hangs waiting on them at the operator level.
            //
            // Each completed future is also marked in {@code classifierCompletedExceptionally}
            // so a subsequent reset()-then-rebuild path cannot re-complete the same future
            // (defensive — reset() clears the set as part of teardown so this is belt-and-
            // suspenders rather than load-bearing).
            drainClassifiedRowsExceptionally(t);
            // (2) Mark the batch poisoned. The executor's executeBatchRequests checks this
            // before each dispatch step and aborts with the captured cause; reset() then clears
            // the flag once the batch is fully torn down.
            this.batchPoisonCause = t;
            // (3) Re-throw so the mailbox-thread caller (AEC dispatch loop) sees the failure
            // and bypasses any further offer() calls on this batch.
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new RuntimeException(
                    "ForSt-RS onClear flush failed (checked exception wrapped)", t);
        }
        ensureDeleteCapacity();
        table.serializeKeyInto(request, deleteKeys);
        deleteRequests[deleteCount] = request;
        deleteCount++;
    }

    /**
     * V3.1: route a LIST_ADD / LIST_ADD_ALL request through APPEND_MERGE rather than the
     * destructive PUT path. Constructs an {@link AppendMergeRequest} whose key + operand are
     * heap-backed {@link MemorySegment}s; {@link VectorizedExecutor#dispatchAppendMergeBatch}
     * copies them into off-heap buffers before the FFI call.
     *
     * <p><b>PR-C2 fast path:</b> when {@code table} is a {@link
     * org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2} with an off-heap accumulator
     * configured, route the chunk bytes directly into the per-state-instance buffer (one off-heap
     * copy) and skip both the heap {@link AppendMergeBatchBuffer} step and the per-row scratch
     * {@link Arena} that the legacy dispatch path allocates. The buffer drains via a single
     * {@code frs_vec_merge_append_batch} call on auto-flush, pre-snapshot, or
     * {@link #flushOffHeapListBuffersIfDirty}.
     *
     * <p>The off-heap path's future (returned by {@code recordAppendMergeOffHeap}) is captured
     * into {@link #offHeapAppendMergeFutures} parallel to {@link #appendMergeRequests} so the
     * executor can plumb completion back to the Flink-runtime async future.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <K, N, V> void recordAppendMerge(
            ForStRsInnerTable<K, N, V> table, StateRequest<K, N, ?, ?> request) {
        // PR-C2: off-heap fast path when the state has a configured ListStateArrowBuffer.
        if (table instanceof org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2) {
            org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2 list =
                    (org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2) table;
            if (list.buffer() != null) {
                java.util.concurrent.CompletableFuture<Void> fut =
                        list.recordAppendMergeOffHeap((StateRequest) request);
                if (fut != null) {
                    ensureAppendMergeCapacity();
                    appendMergeRequests[appendMergeCount] = request;
                    ensureOffHeapAppendMergeCapacity();
                    offHeapAppendMergeFutures[appendMergeCount] = fut;
                    offHeapAppendMergeStates[appendMergeCount] = list;
                    // B6-H3: mark the per-state buffer as dirty so end-of-batch flush walk can
                    // skip the IdentityHashMap + Boolean boxing dedup pass.
                    list.markAmDirty();
                    appendMergeCount++;
                    return;
                }
                // null return means defensive fall-through to recordDelete (null payload) —
                // handle that here so we don't double-count.
                if (request.getPayload() == null) {
                    recordDelete(table, request);
                    return;
                }
                // Otherwise fall through to the heap path below.
            }
        }
        ensureAppendMergeBuffer();
        ensureAppendMergeCapacity();
        // B6-H1: defensive payload-null check using getPayload() rather than allocating a
        // serialized byte[] up-front and inspecting it. serializeValueInto handles non-null
        // payloads only; null payload routes to recordDelete (preserving previous behavior).
        if (request.getPayload() == null) {
            recordDelete(table, request);
            return;
        }
        // B6-H1: flow the heap-path key + value bytes directly through the column buffers via
        // serializeKeyInto / serializeValueInto. Eliminates per-row allocations:
        //   - byte[] keyBytes (replaced by DataOutputSerializer.getSharedBuffer copy)
        //   - byte[] valBytes (same)
        //   - 2× MemorySegment.ofArray(...) wrappers
        //   - new MemorySegment[]{valSlice} 1-element array
        //   - new AppendMergeRequest(...) wrapper
        // The single-operand contract matches dispatchAppendMergeBatch (Phase A.1) which expects
        // exactly 1 pre-encoded operand per row.
        table.serializeKeyInto(request, appendMergeBuffer.keyBuffer());
        table.serializeValueInto(request, appendMergeBuffer.valueBuffer());
        appendMergeBuffer.appendHeapRow(new java.util.concurrent.CompletableFuture<>());
        appendMergeRequests[appendMergeCount] = request;
        ensureOffHeapAppendMergeCapacity();
        // Mark slot as heap-path (null off-heap future + null off-heap state).
        offHeapAppendMergeFutures[appendMergeCount] = null;
        offHeapAppendMergeStates[appendMergeCount] = null;
        appendMergeCount++;
    }

    /**
     * PR-C2: drain any non-empty off-heap ListStateArrowBuffers referenced by this batch's
     * APPEND_MERGE rows. Called by the executor after the per-row {@link
     * AppendMergeBatchBuffer} dispatch so the off-heap-path's per-row futures resolve in the
     * same batch boundary. Idempotent (no-op when nothing was buffered or buffer empty).
     *
     * <p>De-duplicates by state-instance identity — multiple LIST_ADD rows for the same state
     * share one buffer, so a single drain handles all of them.
     */
    public void flushOffHeapListBuffersIfDirty() {
        if (offHeapAppendMergeStates == null) {
            return;
        }
        // B6-H3: dedup via the `amDirty` bit on each ForStRsAsyncListStateV2 instead of the
        // IdentityHashMap + Boolean.TRUE boxing the old pass allocated per batch. Walk the same
        // parallel array, but only call flushIfDirty when the bit is still set — flushIfDirty
        // clears it inside the same call so subsequent rows that reference the same state become
        // no-ops. Net effect: 0 allocation per batch on the dispatch boundary; same engine-side
        // semantics (each unique buffer drained exactly once).
        for (int i = 0; i < appendMergeCount; i++) {
            org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2 s =
                    offHeapAppendMergeStates[i];
            if (s != null && s.isAmDirty()) {
                s.flushIfDirty(); // clears amDirty
            }
        }
    }

    private void ensureAppendMergeCapacity() {
        if (appendMergeCount < appendMergeRequests.length) {
            return;
        }
        int newCap = appendMergeRequests.length << 1;
        StateRequest<?, ?, ?, ?>[] r = new StateRequest<?, ?, ?, ?>[newCap];
        System.arraycopy(appendMergeRequests, 0, r, 0, appendMergeRequests.length);
        appendMergeRequests = r;
    }

    /** PR-C2: keep off-heap parallel arrays in lockstep with {@link #appendMergeRequests}. */
    private void ensureOffHeapAppendMergeCapacity() {
        if (appendMergeCount < offHeapAppendMergeFutures.length) {
            return;
        }
        int newCap = offHeapAppendMergeFutures.length << 1;
        @SuppressWarnings({"unchecked", "rawtypes"})
        java.util.concurrent.CompletableFuture<Void>[] futs =
                (java.util.concurrent.CompletableFuture<Void>[])
                        new java.util.concurrent.CompletableFuture[newCap];
        System.arraycopy(offHeapAppendMergeFutures, 0, futs, 0, offHeapAppendMergeFutures.length);
        offHeapAppendMergeFutures = futs;
        org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2<?, ?, ?>[] sts =
                new org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2<?, ?, ?>[newCap];
        System.arraycopy(offHeapAppendMergeStates, 0, sts, 0, offHeapAppendMergeStates.length);
        offHeapAppendMergeStates = sts;
    }

    /**
     * PR-C2 accessor: per-row off-heap future. Index {@code i} parallels
     * {@link #appendMergeRequests}; {@code null} means the row used the heap fast path
     * ({@link #appendMergeBuffer}).
     */
    public java.util.concurrent.CompletableFuture<Void>[] offHeapAppendMergeFutures() {
        return offHeapAppendMergeFutures;
    }

    /**
     * PR-C2 accessor: per-row state instance for off-heap path. Index {@code i} parallels
     * {@link #appendMergeRequests}; {@code null} for heap-path rows.
     */
    public org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2<?, ?, ?>[]
            offHeapAppendMergeStates() {
        return offHeapAppendMergeStates;
    }

    /** V3.1 accessor — parallel to {@link #putRequests()} / {@link #deleteRequests()}. */
    public StateRequest<?, ?, ?, ?>[] appendMergeRequests() {
        return appendMergeRequests;
    }

    public int appendMergeCount() {
        return appendMergeCount;
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

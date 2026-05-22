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
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.state.forstrs.exec.FrsIterHandle;
import org.apache.flink.state.forstrs.exec.SlotArenaScope;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsEnginePanicError;
import org.apache.flink.state.forstrs.ffm.FrsErrorCode;
import org.apache.flink.state.forstrs.ffm.FrsException;
import org.apache.flink.state.forstrs.metrics.DispatchMetrics;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Vectorized {@link StateExecutor} that dispatches an entire batch of state requests via a single
 * FFM call per op type, using caller-owned Arrow {@link ColumnarBatchBuffer}s. Replaces {@code
 * ForStRsStateExecutor}'s per-request {@code byte[]} + {@code byte[][]} allocations on the hot
 * path.
 *
 * <p>See spec {@code docs/superpowers/specs/2026-05-15-forst-rs-vectorized-executor-design.md} §C3.
 */
@Internal
public class VectorizedExecutor implements StateExecutor {

    /** Initial output buffer capacity for GET values. Grows on BUFFER_TOO_SMALL. */
    private static final int INITIAL_OUT_DATA_CAP = 64 * 1024;

    private static final int FRS_STATUS_OK = 0;
    private static final int FRS_STATUS_BUFFER_TOO_SMALL = 17;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final Arena arena;

    // Optional metrics + fatal-handler — set via setters after construction so that
    // backends that don't yet have a MetricGroup can still instantiate the executor.
    private DispatchMetrics metrics;
    private FatalErrorHandler fatalHandler;

    // Optional SlotArenaScope + monotonic handle ID counter for ITER_PREFIX dispatch.
    // Set via setSlotScope() before any submitVectorized(IterPrefixRequest) calls.
    private SlotArenaScope slotScope;
    private final AtomicLong nextIterHandleId = new AtomicLong(0);

    // Long-lived classifier-side buffers (reused across batches via reset()).
    private final ColumnarBatchBuffer getKeys;
    private final ColumnarBatchBuffer putKeys;
    private final ColumnarBatchBuffer putValues;
    private final ColumnarBatchBuffer deleteKeys;

    // V3.1 (V20 sub-spec §5): long-lived registry of ListState names. A fresh classifier is
    // created per batch by createRequestContainer(); the registry is passed in so APPEND_MERGE
    // routing persists across batches. Backend calls registerListState() once per state primitive
    // at creation time.
    private final java.util.Set<String> listStateNames =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    // Reusable output buffers for the GET path.
    private MemorySegment outOffsets;
    private MemorySegment outValidity;
    private MemorySegment outData;
    private long outDataCap;
    private int outSlotsCap;
    private final MemorySegment outDataLenSeg; // *mut usize scratch

    public VectorizedExecutor(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.arena = arena;
        this.getKeys = new ColumnarBatchBuffer(arena);
        this.putKeys = new ColumnarBatchBuffer(arena);
        this.putValues = new ColumnarBatchBuffer(arena);
        this.deleteKeys = new ColumnarBatchBuffer(arena);
        this.outSlotsCap = 4096;
        this.outOffsets = arena.allocate(ValueLayout.JAVA_INT, (long) outSlotsCap + 1);
        this.outValidity = arena.allocate(outSlotsCap);
        this.outDataCap = INITIAL_OUT_DATA_CAP;
        this.outData = arena.allocate(outDataCap);
        this.outDataLenSeg = arena.allocate(ValueLayout.JAVA_LONG);
    }

    // -----------------------------------------------------------------
    // Optional wiring (P4)
    // -----------------------------------------------------------------

    /**
     * Attach dispatch metrics. Call immediately after construction; thread-safe for single-writer
     * scenarios (the backend thread that owns this executor).
     */
    public void setDispatchMetrics(DispatchMetrics m) {
        this.metrics = m;
    }

    /**
     * Attach a FatalErrorHandler for fail-process error escalation (PANIC_CAUGHT / UNKNOWN). If not
     * set, fail-process errors are still thrown as {@link FrsEnginePanicError} — Flink's default
     * uncaught-exception handler will catch them at the task level.
     */
    public void setFatalHandler(FatalErrorHandler fh) {
        this.fatalHandler = fh;
    }

    /**
     * Attach a {@link SlotArenaScope} for ITER_PREFIX dispatch. Must be set before any {@link
     * IterPrefixRequest} is dispatched; the scope is used to allocate per-iterator Arenas and
     * register handles for turn-boundary lifetime management.
     */
    public void setSlotScope(SlotArenaScope scope) {
        this.slotScope = scope;
    }

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        // Each batch gets its own classifier that wraps the long-lived buffers.
        // The classifier just resets the buffers on construction so the previous
        // batch's data is invalidated. V3.1: also propagates the executor-level
        // listStateNames registry into the classifier so APPEND_MERGE routing
        // survives the per-batch classifier reset.
        VectorizedClassifier classifier =
                new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys);
        classifier.reset();
        for (String name : listStateNames) {
            classifier.registerListState(name);
        }
        // Lazy-init the APPEND_MERGE / ITER buffers using the executor's Arena so the
        // classifier doesn't have to allocate them per-batch.
        classifier.initNewKindBuffers(arena);
        return classifier;
    }

    /**
     * V3.1 (V20 sub-spec §5): register a ListState name so the per-batch classifier's
     * APPEND_MERGE routing recognizes LIST_ADD requests on this state. Idempotent.
     */
    public void registerListState(String stateName) {
        listStateNames.add(stateName);
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
        VectorizedClassifier classifier = (VectorizedClassifier) container;
        try {
            // Spec §Correctness Invariant 2: any deferred / cached writes must be
            // flushed BEFORE iterator ops. Within a single batch the natural
            // ordering of PUT/DELETE before ITER guarantees that.
            executePuts(classifier);
            executeDeletes(classifier);
            executeGets(classifier);
            executeIters(classifier);
            // Dispatch vectorized APPEND_MERGE requests (P6-B, ListState path).
            //
            // PR-C2 split: each row went via either the heap path (appendMergeBuffer) or the
            // off-heap path (per-state ListStateArrowBuffer). The classifier's parallel arrays
            // {@code offHeapAppendMergeFutures} / {@code offHeapAppendMergeStates} mark which
            // path each row took.
            //   - Heap path: dispatchAppendMerge(amBuf) drives futures via amBuf.futures();
            //   - Off-heap path: drain unique state buffers via flushOffHeapListBuffersIfDirty()
            //     and the per-row off-heap futures resolve when their state's buffer flushes.
            AppendMergeBatchBuffer amBuf = classifier.appendMergeBuffer();
            Throwable firstRowFailure = null;
            int amCount = classifier.appendMergeCount();
            if (amCount > 0) {
                // 1) Heap path: dispatch and propagate any heap-row futures.
                if (amBuf != null && !amBuf.isEmpty()) {
                    dispatchAppendMerge(amBuf);
                }
                // 2) Off-heap path: drain unique state-instance buffers exactly once.
                classifier.flushOffHeapListBuffersIfDirty();
                // 3) Plumb completion to the StateRequest's runtime future on a per-row basis.
                StateRequest<?, ?, ?, ?>[] amReqs = classifier.appendMergeRequests();
                CompletableFuture<Void>[] offFutures = classifier.offHeapAppendMergeFutures();
                List<CompletableFuture<Void>> heapFutures =
                        amBuf != null ? amBuf.futures() : null;
                // Track per-path indices: heap rows index into heapFutures in heap-append order.
                int heapIdx = 0;
                for (int i = 0; i < amCount; i++) {
                    CompletableFuture<Void> amFut =
                            offFutures[i] != null
                                    ? offFutures[i]
                                    : (heapFutures != null && heapIdx < heapFutures.size()
                                            ? heapFutures.get(heapIdx++)
                                            : null);
                    if (amFut != null && amFut.isCompletedExceptionally()) {
                        Throwable cause;
                        try {
                            amFut.getNow(null);
                            cause = new RuntimeException(
                                    "AppendMergeRequest future completed exceptionally"
                                            + " but cause unavailable");
                        } catch (Throwable t) {
                            cause = t.getCause() != null ? t.getCause() : t;
                        }
                        completePutExceptionally(amReqs[i], cause);
                        if (firstRowFailure == null) {
                            firstRowFailure = cause;
                        }
                    } else {
                        completePut(amReqs[i]);
                    }
                }
            }
            // Dispatch vectorized ITER_PREFIX requests if the classifier's buffer is
            // non-null/non-empty.
            IterPrefixBatchBuffer ipBuf = classifier.iterPrefixBuffer();
            if (ipBuf != null && !ipBuf.isEmpty()) {
                dispatchIterPrefix(ipBuf);
            }
            // Round-2 fix A2-H3: container future should reflect row failures so the
            // runtime does not schedule the next batch into the failing engine.
            if (firstRowFailure != null) {
                return CompletableFuture.failedFuture(firstRowFailure);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            // Round-3 fix A3-H2: widen from `catch (Exception)` to `catch (Throwable)`.
            // FrsEnginePanicError extends Error, so a panic in executeGets/Puts/etc. would
            // previously escape the outer catch and the container future would never be
            // returned (operator hangs forever on the unresolved CompletableFuture).
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
        VectorizedClassifier single =
                new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys);
        single.reset();
        // Lazy-init the new-kind buffers (matches createRequestContainer behavior).
        for (String name : listStateNames) {
            single.registerListState(name);
        }
        single.initNewKindBuffers(arena);
        single.offer(request);
        // Round-3 fix A3-H1: wrap dispatch in try/catch so an FFI/engine throw on the sync
        // path doesn't leak the StateRequest's future (operator would hang).
        try {
            executeRequestSyncInner(single);
        } catch (Throwable t) {
            completePutExceptionally(request, t);
        }
    }

    private void executeRequestSyncInner(VectorizedClassifier single) {
        executePuts(single);
        executeDeletes(single);
        executeGets(single);
        executeIters(single);
        AppendMergeBatchBuffer amBuf = single.appendMergeBuffer();
        int amCount = single.appendMergeCount();
        if (amCount > 0) {
            // PR-C2: mirror the executeBatchRequests split — drive both heap (amBuf) and
            // off-heap (per-state ListStateArrowBuffer) paths and complete per-row futures
            // in row order.
            if (amBuf != null && !amBuf.isEmpty()) {
                dispatchAppendMerge(amBuf);
            }
            single.flushOffHeapListBuffersIfDirty();
            StateRequest<?, ?, ?, ?>[] amReqs = single.appendMergeRequests();
            CompletableFuture<Void>[] offFutures = single.offHeapAppendMergeFutures();
            List<CompletableFuture<Void>> heapFutures =
                    amBuf != null ? amBuf.futures() : null;
            int heapIdx = 0;
            for (int i = 0; i < amCount; i++) {
                CompletableFuture<Void> amFut =
                        offFutures[i] != null
                                ? offFutures[i]
                                : (heapFutures != null && heapIdx < heapFutures.size()
                                        ? heapFutures.get(heapIdx++)
                                        : null);
                if (amFut != null && amFut.isCompletedExceptionally()) {
                    Throwable cause;
                    try {
                        amFut.getNow(null);
                        cause = new RuntimeException(
                                "AppendMergeRequest future completed exceptionally"
                                        + " but cause unavailable");
                    } catch (Throwable t) {
                        cause = t.getCause() != null ? t.getCause() : t;
                    }
                    completePutExceptionally(amReqs[i], cause);
                } else {
                    completePut(amReqs[i]);
                }
            }
        }
        IterPrefixBatchBuffer ipBuf = single.iterPrefixBuffer();
        if (ipBuf != null && !ipBuf.isEmpty()) {
            dispatchIterPrefix(ipBuf);
        }
    }

    @Override
    public boolean fullyLoaded() {
        return false;
    }

    @Override
    public void shutdown() {}

    /**
     * Pre-snapshot drain hook (PR-A1 wiring).
     *
     * <p>The async-state V2 dispatch loop in {@link #executeBatchRequests} hands every classifier
     * row to the engine within a single mailbox turn: by the time {@code executeBatchRequests}
     * returns, every PUT / DELETE / GET / ITER / APPEND_MERGE row has been submitted via FFM, the
     * engine memtable has applied the mutation, and the per-row futures are resolved. There is no
     * deferred work owned by this {@link VectorizedExecutor} that {@code flushDirty} needs to
     * drain — the work is "dirty" in the engine memtable, not on the Java side.
     *
     * <p>What we still need to do here, however, is force the engine to fold its in-memory
     * memtable down to L0 SSTs <em>before</em> the snapshot strategy enumerates files. The
     * engine's normal flush cadence is asynchronous and L0-rotation-driven; on a checkpoint
     * barrier the strategy expects every committed write to be reachable as an SST file. Calling
     * {@link ForStRsLinker#flush} forces a synchronous memtable → L0 conversion so the snapshot's
     * file enumeration is complete.
     *
     * <p>Called from {@code ForStRsAsyncKeyedStateBackend.snapshot()} (PR-A1) and {@code
     * notifyCheckpointComplete}.
     */
    public void flushDirty() {
        // FFI flush: synchronously fold memtable → L0 SST. Idempotent; engine returns OK if the
        // memtable is empty. Throws on engine-side error (caller is the snapshot path which will
        // surface the failure as a failed RunnableFuture).
        if (linker != null && db != null) {
            try {
                linker.flush(db);
            } catch (RuntimeException re) {
                // Snapshot pre-flush failures must propagate — there is no value to silently
                // continuing because the snapshot strategy would then enumerate an incomplete
                // SST set. Re-throw and let the outer snapshot path mark the future failed.
                throw re;
            }
        }
    }

    // -----------------------------------------------------------------
    // Op-type executors
    // -----------------------------------------------------------------

    /**
     * Aggregate stateName for old-style Flink-runtime batches that mix multiple state names. The
     * new-style VectorizedStateRequest path (P5+) will provide per-state attribution.
     */
    private static final String MIXED_STATE = "_mixed";

    private void executePuts(VectorizedClassifier c) {
        int n = c.putCount();
        if (n == 0) {
            return;
        }
        StateRequest<?, ?, ?, ?>[] reqs = c.putRequests();
        long t0 = System.nanoTime();
        try {
            invokeVectorizedBatchPut(
                    c.putKeys().offsetsSegment(),
                    c.putKeys().dataSegment(),
                    c.putValues().offsetsSegment(),
                    c.putValues().dataSegment(),
                    n);
            long latencyNs = System.nanoTime() - t0;
            if (metrics != null) {
                metrics.recordDispatch(
                        VectorizedStateRequest.Kind.PUT, MIXED_STATE, n, 0L, latencyNs);
            }
            for (int i = 0; i < n; i++) {
                completePut(reqs[i]);
            }
        } catch (Throwable t) {
            // PR-A10 / S1-9: an FFI throw here previously left every per-row
            // StateRequest future unresolved — the outer catch in
            // executeBatchRequests returned a failed container future but the
            // runtime would still wait on each row's future forever. Drain them
            // first so the async-state runtime sees them all resolve, then
            // re-throw so the outer catch can return a failed container future.
            for (int i = 0; i < n; i++) {
                try {
                    completePutExceptionally(reqs[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            throw t;
        }
    }

    private void executeDeletes(VectorizedClassifier c) {
        int n = c.deleteCount();
        if (n == 0) {
            return;
        }
        StateRequest<?, ?, ?, ?>[] reqs = c.deleteRequests();
        try {
            invokeVectorizedBatchDelete(
                    c.deleteKeys().offsetsSegment(), c.deleteKeys().dataSegment(), n);
            for (int i = 0; i < n; i++) {
                completeDelete(reqs[i]);
            }
        } catch (Throwable t) {
            // PR-A10 / S1-9: drain per-row futures so the runtime does not hang
            // on the failing batch's individual StateRequest futures.
            for (int i = 0; i < n; i++) {
                try {
                    completeDeleteExceptionally(reqs[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            throw t;
        }
    }

    private void executeGets(VectorizedClassifier c) {
        int n = c.getCount();
        if (n == 0) {
            return;
        }
        StateRequest<?, ?, ?, ?>[] reqs = c.getRequests();
        ForStRsInnerTable<?, ?, ?>[] tables = c.getTables();
        try {
            ensureOutCapacity(n);

            long t0 = System.nanoTime();
            // Retry-with-growth loop: if out_data buffer is too small, grow and retry.
            while (true) {
                int rc =
                        invokeVectorizedBatchGet(
                                c.getKeys().offsetsSegment(),
                                c.getKeys().dataSegment(),
                                n,
                                outOffsets,
                                outData,
                                outValidity,
                                outDataCap,
                                outDataLenSeg);
                if (rc == FRS_STATUS_OK) {
                    long latencyNs = System.nanoTime() - t0;
                    if (metrics != null) {
                        metrics.recordDispatch(
                                VectorizedStateRequest.Kind.GET, MIXED_STATE, n, 0L, latencyNs);
                    }
                    break;
                }
                if (rc == FRS_STATUS_BUFFER_TOO_SMALL) {
                    long needed = outDataLenSeg.get(ValueLayout.JAVA_LONG, 0L);
                    long newCap = Math.max(outDataCap * 2L, needed);
                    outData = arena.allocate(newCap);
                    outDataCap = newCap;
                    continue;
                }
                // Non-OK, non-BUFFER_TOO_SMALL: classify via FrsErrorCode.
                FrsErrorCode errCode = FrsErrorCode.fromU32(rc);
                if (metrics != null) {
                    metrics.recordFfiError(VectorizedStateRequest.Kind.GET, MIXED_STATE, errCode);
                }
                if (errCode.isFailProcess()) {
                    FrsEnginePanicError panicErr =
                            new FrsEnginePanicError(
                                    errCode, "kind=GET state=" + MIXED_STATE + " rc=" + rc);
                    if (fatalHandler != null) {
                        fatalHandler.onFatalError(panicErr);
                    }
                    throw panicErr;
                }
                throw new FrsBackendException(
                        FrsStatus.fromCode(rc), "frs_vectorized_batch_get rc=" + rc);
            }

            // Decode results: for each slot, read validity byte and (offsets, data) range.
            // PR-B1 (V2-6, C-H1, C-H6): pass the native segment slice directly into the
            // table's MemorySegment overload — eliminates the per-row `new byte[len]`
            // that used to happen here before deserialisation.
            for (int i = 0; i < n; i++) {
                byte vld = outValidity.get(ValueLayout.JAVA_BYTE, i);
                boolean present = vld != 0;
                int len = 0;
                long start = 0L;
                if (present) {
                    int s = outOffsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
                    int e =
                            outOffsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
                    start = s;
                    len = e - s;
                }
                completeGet(reqs[i], tables[i], present, outData, start, len);
            }
        } catch (Throwable t) {
            // PR-A10 / S1-9: drain every pending GET future on FFI / decode
            // failure. Without this, the outer catch in executeBatchRequests
            // returns a failed container future but the per-row StateRequest
            // futures stay unresolved — the operator hangs on the first
            // transient engine error in the GET path.
            for (int i = 0; i < n; i++) {
                try {
                    completeGetExceptionally(reqs[i], tables[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            throw t;
        }
    }

    private void executeIters(VectorizedClassifier c) {
        if (c.iterRequests().isEmpty()) {
            return;
        }
        for (ForStRsDBIterRequest<?, ?, ?, ?> iter : c.iterRequests()) {
            iter.process(linker, db, cf, arena);
        }
    }

    // -----------------------------------------------------------------
    // New-kind dispatch stubs (P2 Batch C) — real FFI wiring in later PRs
    // -----------------------------------------------------------------

    /**
     * Dispatches an APPEND_MERGE batch via {@code frs_vec_merge_append} FFI (P6-B).
     *
     * <p><b>Phase A.1 update (audit-design §3 V4):</b> when each request carries exactly 1 operand
     * slice, this method delegates to {@link #dispatchAppendMergeBatch} for a single batched FFM
     * crossing. Multi-operand-per-row requests still go through the legacy per-row path below
     * (the batched FFI's wire layout is 1 operand per row).
     *
     * <p>Legacy per-request dispatch: for each request in the buffer, allocates a small scratch
     * {@link Arena} to hold the {@code operand_ptrs} and {@code operand_lens} arrays, then calls
     * {@code frs_vec_merge_append} with the key and N operand slices. Each request carries N
     * value slices (one per list element). For {@link
     * org.apache.flink.state.forstrs.state.ForStRsListStateV2#addAll(java.util.List)}, N &gt; 1 so
     * the call is effectively batched at the element level even in this per-request form.
     *
     * @param buffer the APPEND_MERGE batch buffer populated by the classifier
     */
    public void dispatchAppendMerge(AppendMergeBatchBuffer buffer) {
        // Phase A.1 fast path: if every row has exactly 1 operand, use the batched FFI.
        // This is the common case for ListState.asyncAdd (one element per call).
        int count = buffer.count();
        if (count == 0) {
            return;
        }
        boolean allSingleOperand = true;
        for (int row = 0; row < count; row++) {
            if (buffer.valueSliceLists().get(row).length != 1) {
                allSingleOperand = false;
                break;
            }
        }
        if (allSingleOperand) {
            dispatchAppendMergeBatch(buffer);
            return;
        }
        // Fall through to the legacy per-row path for multi-operand-per-row requests.
        dispatchAppendMergePerRow(buffer);
    }

    /** Legacy per-row dispatch path. Kept for multi-operand-per-row requests until V20 closes. */
    private void dispatchAppendMergePerRow(AppendMergeBatchBuffer buffer) {
        int count = buffer.count();
        if (count == 0) {
            return;
        }
        long t0 = System.nanoTime();
        int rowsProcessed = 0;
        long bytesIn = 0;

        ColumnarBatchBuffer keyBuf = buffer.keyBuffer();
        List<MemorySegment[]> valueSliceLists = buffer.valueSliceLists();
        List<CompletableFuture<Void>> futures = buffer.futures();

        for (int row = 0; row < count; row++) {
            MemorySegment[] vs = valueSliceLists.get(row);
            CompletableFuture<Void> future = futures.get(row);

            // Extract key slice from the columnar key buffer.
            int keyStart =
                    keyBuf.offsetsSegment().get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int keyEnd =
                    keyBuf.offsetsSegment()
                            .get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
            int keyLen = keyEnd - keyStart;
            MemorySegment keyPtr = keyBuf.dataSegment().asSlice(keyStart, keyLen);

            bytesIn += keyLen;
            for (MemorySegment v : vs) {
                bytesIn += v.byteSize();
            }

            // Build operand_ptrs and operand_lens arrays in a per-row scratch Arena.
            // Value slices may be heap segments; copy each into native memory so
            // the operand_ptrs array holds stable native addresses for the FFI call.
            // Bounded allocation: typically a handful of operands per call.
            Arena scratch = Arena.ofConfined();
            try {
                MemorySegment ptrs = scratch.allocate(ValueLayout.ADDRESS, vs.length);
                MemorySegment lens = scratch.allocate(ValueLayout.JAVA_INT, vs.length);
                for (int i = 0; i < vs.length; i++) {
                    long vLen = vs[i].byteSize();
                    MemorySegment nativeV = scratch.allocate(vLen);
                    MemorySegment.copy(vs[i], 0L, nativeV, 0L, vLen);
                    ptrs.setAtIndex(ValueLayout.ADDRESS, i, nativeV);
                    lens.setAtIndex(ValueLayout.JAVA_INT, i, (int) vLen);
                }
                int rc =
                        linker.frsVecMergeAppend(
                                db.handle(), cf.handle(), keyPtr, keyLen, ptrs, lens, vs.length);
                FrsErrorCode code = FrsErrorCode.fromU32(rc);
                if (code == FrsErrorCode.OK) {
                    future.complete(null);
                    rowsProcessed += vs.length;
                } else if (code.isFailProcess()) {
                    if (metrics != null) {
                        metrics.recordFfiError(
                                VectorizedStateRequest.Kind.APPEND_MERGE, "_mixed", code);
                    }
                    FrsEnginePanicError panicErr =
                            new FrsEnginePanicError(code, "kind=APPEND_MERGE row=" + row);
                    if (fatalHandler != null) {
                        fatalHandler.onFatalError(panicErr);
                    }
                    future.completeExceptionally(panicErr);
                } else {
                    if (metrics != null) {
                        metrics.recordFfiError(
                                VectorizedStateRequest.Kind.APPEND_MERGE, "_mixed", code);
                    }
                    future.completeExceptionally(new FrsException(code, row, new byte[0]));
                }
            } finally {
                scratch.close();
            }
        }

        if (metrics != null) {
            metrics.recordDispatch(
                    VectorizedStateRequest.Kind.APPEND_MERGE,
                    MIXED_STATE,
                    rowsProcessed,
                    bytesIn,
                    System.nanoTime() - t0);
        }
    }

    /**
     * Phase A.1 (audit-design §3 V4) — batched APPEND_MERGE dispatch.
     *
     * <p>Single FFI crossing for {@code count} rows. Each row's operand is the
     * caller-pre-encoded payload bytes (typically {@code [count=u32 LE][elem_bytes*]}
     * for ListState semantics — see {@link
     * org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2#asyncAdd}).
     *
     * <p>Per-row layout in {@code buffer}: each {@link AppendMergeBatchBuffer#valueSliceLists()}
     * entry must contain exactly ONE {@link MemorySegment} (the pre-encoded operand). The
     * batched FFI expects one operand per row — for multi-element {@code asyncAddAll}, the
     * caller should pre-concatenate elements into a single operand with {@code count=N}.
     *
     * <p>NOT YET WIRED to any call site in Phase A.1 — {@link #dispatchAppendMerge} is still
     * the only caller of the engine FFI. Phase A.2 switches the call site by replacing the
     * per-row {@code dispatchAppendMerge} loop with this batched form.
     *
     * @param buffer the APPEND_MERGE batch buffer populated by the classifier
     * @return native error code (0 = OK)
     */
    public int dispatchAppendMergeBatch(AppendMergeBatchBuffer buffer) {
        int count = buffer.count();
        if (count == 0) {
            return 0;
        }
        long t0 = System.nanoTime();
        long bytesIn = 0;

        ColumnarBatchBuffer keyBuf = buffer.keyBuffer();
        List<MemorySegment[]> valueSliceLists = buffer.valueSliceLists();
        List<CompletableFuture<Void>> futures = buffer.futures();

        // Build packed ops_off / ops_data in a single scratch Arena (one alloc total,
        // not one-per-row like the legacy per-row dispatchAppendMerge).
        Arena scratch = Arena.ofConfined();
        try {
            // Compute total ops byte size and per-row offset.
            int[] opsOffsets = new int[count + 1];
            int opsTotal = 0;
            for (int row = 0; row < count; row++) {
                MemorySegment[] vs = valueSliceLists.get(row);
                // Phase A.1 contract: exactly 1 pre-encoded operand per row.
                if (vs.length != 1) {
                    throw new IllegalArgumentException(
                            "dispatchAppendMergeBatch: row "
                                    + row
                                    + " must carry exactly 1 pre-encoded operand (got "
                                    + vs.length
                                    + "). Multi-element asyncAddAll callers must pre-concat.");
                }
                opsOffsets[row] = opsTotal;
                opsTotal += (int) vs[0].byteSize();
            }
            opsOffsets[count] = opsTotal;

            MemorySegment opsOffSeg = scratch.allocate(ValueLayout.JAVA_INT, count + 1L);
            MemorySegment opsDataSeg = scratch.allocate(opsTotal);
            int writeOff = 0;
            for (int row = 0; row < count; row++) {
                opsOffSeg.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, opsOffsets[row]);
                MemorySegment op = valueSliceLists.get(row)[0];
                long opLen = op.byteSize();
                MemorySegment.copy(op, 0L, opsDataSeg, writeOff, opLen);
                writeOff += (int) opLen;
                bytesIn += opLen;
            }
            opsOffSeg.set(ValueLayout.JAVA_INT, (long) count * Integer.BYTES, opsTotal);

            // Keys are already in the columnar layout of keyBuf — no copy.
            MemorySegment keysOffSeg = keyBuf.offsetsSegment();
            MemorySegment keysDataSeg = keyBuf.dataSegment();
            for (int row = 0; row < count; row++) {
                int kStart = keysOffSeg.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                int kEnd = keysOffSeg.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
                bytesIn += kEnd - kStart;
            }

            int rc =
                    linker.frsVecMergeAppendBatch(
                            db.handle(),
                            cf.handle(),
                            keysOffSeg,
                            keysDataSeg,
                            opsOffSeg,
                            opsDataSeg,
                            count);
            FrsErrorCode code = FrsErrorCode.fromU32(rc);

            if (code == FrsErrorCode.OK) {
                for (CompletableFuture<Void> f : futures) {
                    f.complete(null);
                }
            } else {
                Throwable err =
                        code.isFailProcess()
                                ? new FrsEnginePanicError(code, "kind=APPEND_MERGE_BATCH")
                                : new FrsException(code, -1, new byte[0]);
                if (code.isFailProcess() && fatalHandler != null) {
                    fatalHandler.onFatalError((FrsEnginePanicError) err);
                }
                if (metrics != null) {
                    metrics.recordFfiError(
                            VectorizedStateRequest.Kind.APPEND_MERGE, "_batched", code);
                }
                for (CompletableFuture<Void> f : futures) {
                    f.completeExceptionally(err);
                }
            }

            if (metrics != null) {
                metrics.recordDispatch(
                        VectorizedStateRequest.Kind.APPEND_MERGE,
                        MIXED_STATE,
                        count,
                        bytesIn,
                        System.nanoTime() - t0);
            }

            return rc;
        } finally {
            scratch.close();
        }
    }

    /**
     * Dispatches an ITER_PREFIX batch via {@code frs_vec_iter_prefix_open} FFI (P5).
     *
     * <p>For each request in the buffer, opens a native prefix-bounded iterator, wraps it in an
     * {@link FrsIterHandle}, registers it with the {@link SlotArenaScope}, and completes the
     * request's future with an {@link IterPrefixRequest.IterFirstChunk} carrying the handle and
     * first-chunk row count.
     *
     * <p>Requires {@link #setSlotScope(SlotArenaScope)} to have been called beforehand; throws
     * {@link IllegalStateException} if the scope is not set.
     *
     * @param buffer the ITER_PREFIX batch buffer populated by the classifier
     */
    public void dispatchIterPrefix(IterPrefixBatchBuffer buffer) {
        if (slotScope == null) {
            throw new IllegalStateException(
                    "SlotArenaScope not set on VectorizedExecutor — call setSlotScope() "
                            + "before dispatching ITER_PREFIX requests");
        }
        long t0 = System.nanoTime();
        int rowsTotal = 0;
        long bytesIn = 0;

        List<MemorySegment> prefixSlices = buffer.prefixSlices();
        List<MemorySegment> chunkBufSlices = buffer.chunkBufSlices();
        List<CompletableFuture<IterPrefixRequest.IterFirstChunk>> futures = buffer.futures();

        for (int row = 0; row < buffer.count(); row++) {
            MemorySegment prefix = prefixSlices.get(row);
            MemorySegment chunkBuf = chunkBufSlices.get(row);
            CompletableFuture<IterPrefixRequest.IterFirstChunk> future = futures.get(row);

            // Allocate out-params in a per-iterator Arena; the Arena is closed when the
            // handle is closed (via FrsIterHandle.close() → perIterArena.close()).
            Arena perIterArena = Arena.ofShared();
            MemorySegment outHandle = perIterArena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment outRowCount = perIterArena.allocate(ValueLayout.JAVA_INT);
            MemorySegment outBytesUsed = perIterArena.allocate(ValueLayout.JAVA_INT);

            int rc =
                    linker.frsVecIterPrefixOpen(
                            db.handle(),
                            cf.handle(),
                            prefix,
                            (int) prefix.byteSize(),
                            chunkBuf,
                            (int) chunkBuf.byteSize(),
                            outHandle,
                            outRowCount,
                            outBytesUsed);

            FrsErrorCode code = FrsErrorCode.fromU32(rc);
            if (code != FrsErrorCode.OK) {
                perIterArena.close();
                future.completeExceptionally(new FrsException(code, row, new byte[0]));
                if (metrics != null) {
                    metrics.recordFfiError(VectorizedStateRequest.Kind.ITER_PREFIX, "_mixed", code);
                }
                continue;
            }

            long nativeHandle = outHandle.get(ValueLayout.JAVA_LONG, 0);
            int firstChunkRows = outRowCount.get(ValueLayout.JAVA_INT, 0);
            int firstChunkBytes = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
            rowsTotal += firstChunkRows;
            bytesIn += firstChunkBytes;

            long jHandleId = nextIterHandleId.incrementAndGet();
            FrsIterHandle fh =
                    new FrsIterHandle(jHandleId, nativeHandle, linker, perIterArena, slotScope);
            slotScope.registerIter(fh);
            if (metrics != null) {
                metrics.recordIterHandlesOpened();
            }
            future.complete(new IterPrefixRequest.IterFirstChunk(fh, firstChunkRows));
        }

        if (metrics != null) {
            metrics.recordDispatch(
                    VectorizedStateRequest.Kind.ITER_PREFIX,
                    MIXED_STATE,
                    rowsTotal,
                    bytesIn,
                    System.nanoTime() - t0);
        }
    }

    /**
     * Dispatches an ITER_RANGE batch via {@code frs_vec_iter_range_open} FFI (P9).
     *
     * <p>For each request in the buffer, opens a native range-bounded iterator over [lo, hi), wraps
     * it in an {@link FrsIterHandle}, registers it with the {@link SlotArenaScope}, and completes
     * the request's future with an {@link IterRangeRequest.IterFirstChunk} carrying the handle and
     * first-chunk row count.
     *
     * <p>Requires {@link #setSlotScope(SlotArenaScope)} to have been called beforehand; throws
     * {@link IllegalStateException} if the scope is not set.
     *
     * @param buffer the ITER_RANGE batch buffer populated by the classifier
     */
    public void dispatchIterRange(IterRangeBatchBuffer buffer) {
        if (slotScope == null) {
            throw new IllegalStateException(
                    "SlotArenaScope not set on VectorizedExecutor — call setSlotScope() "
                            + "before dispatching ITER_RANGE requests");
        }
        long t0 = System.nanoTime();
        int rowsTotal = 0;
        long bytesIn = 0;

        List<MemorySegment> loSlices = buffer.loSlices();
        List<MemorySegment> hiSlices = buffer.hiSlices();
        List<MemorySegment> chunkBufSlices = buffer.chunkBufSlices();
        List<CompletableFuture<IterRangeRequest.IterFirstChunk>> futures = buffer.futures();

        for (int row = 0; row < buffer.count(); row++) {
            MemorySegment lo = loSlices.get(row);
            MemorySegment hi = hiSlices.get(row);
            MemorySegment chunkBuf = chunkBufSlices.get(row);
            CompletableFuture<IterRangeRequest.IterFirstChunk> future = futures.get(row);

            // Allocate out-params in a per-iterator Arena; closed when FrsIterHandle.close() fires.
            Arena perIterArena = Arena.ofShared();
            MemorySegment outHandle = perIterArena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment outRowCount = perIterArena.allocate(ValueLayout.JAVA_INT);
            MemorySegment outBytesUsed = perIterArena.allocate(ValueLayout.JAVA_INT);

            int rc =
                    linker.frsVecIterRangeOpen(
                            db.handle(),
                            cf.handle(),
                            lo,
                            (int) lo.byteSize(),
                            hi,
                            (int) hi.byteSize(),
                            chunkBuf,
                            (int) chunkBuf.byteSize(),
                            outHandle,
                            outRowCount,
                            outBytesUsed);

            FrsErrorCode code = FrsErrorCode.fromU32(rc);
            if (code != FrsErrorCode.OK) {
                perIterArena.close();
                future.completeExceptionally(new FrsException(code, row, new byte[0]));
                if (metrics != null) {
                    metrics.recordFfiError(
                            VectorizedStateRequest.Kind.ITER_RANGE, MIXED_STATE, code);
                }
                continue;
            }

            long nativeHandle = outHandle.get(ValueLayout.JAVA_LONG, 0);
            int firstChunkRows = outRowCount.get(ValueLayout.JAVA_INT, 0);
            int firstChunkBytes = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
            rowsTotal += firstChunkRows;
            bytesIn += firstChunkBytes;

            long jHandleId = nextIterHandleId.incrementAndGet();
            FrsIterHandle fh =
                    new FrsIterHandle(jHandleId, nativeHandle, linker, perIterArena, slotScope);
            slotScope.registerIter(fh);
            if (metrics != null) {
                metrics.recordIterHandlesOpened();
            }
            future.complete(new IterRangeRequest.IterFirstChunk(fh, firstChunkRows));
        }

        if (metrics != null) {
            metrics.recordDispatch(
                    VectorizedStateRequest.Kind.ITER_RANGE,
                    MIXED_STATE,
                    rowsTotal,
                    bytesIn,
                    System.nanoTime() - t0);
        }
    }

    // -----------------------------------------------------------------
    // FFI invocation seams — overridable for tests so we can stub the
    // linker's batch entry points without subclassing the final
    // ForStRsLinker class. Production code always calls the real linker.
    // -----------------------------------------------------------------

    /**
     * PR-A10 / S1-9 test seam: invoked from {@link #executePuts(VectorizedClassifier)}. Production
     * impl forwards to {@link ForStRsLinker#vectorizedBatchPut}; tests may override to throw a
     * deterministic exception and verify per-row future propagation.
     */
    protected void invokeVectorizedBatchPut(
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            MemorySegment valOffsetsSeg,
            MemorySegment valDataSeg,
            long count) {
        linker.vectorizedBatchPut(
                db, cf, keyOffsetsSeg, keyDataSeg, valOffsetsSeg, valDataSeg, count);
    }

    /**
     * PR-A10 / S1-9 test seam: invoked from {@link #executeDeletes(VectorizedClassifier)}.
     * Production impl forwards to {@link ForStRsLinker#vectorizedBatchDelete}; tests may override
     * to throw a deterministic exception and verify per-row future propagation.
     */
    protected void invokeVectorizedBatchDelete(
            MemorySegment keyOffsetsSeg, MemorySegment keyDataSeg, long count) {
        linker.vectorizedBatchDelete(db, cf, keyOffsetsSeg, keyDataSeg, count);
    }

    /**
     * PR-A10 / S1-9 test seam: invoked from {@link #executeGets(VectorizedClassifier)}. Production
     * impl forwards to {@link ForStRsLinker#vectorizedBatchGet}; tests may override to throw a
     * deterministic exception and verify per-row future propagation.
     */
    protected int invokeVectorizedBatchGet(
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            long count,
            MemorySegment outOffsetsSeg,
            MemorySegment outDataSeg,
            MemorySegment outValiditySeg,
            long outDataCapArg,
            MemorySegment outDataLenSegArg) {
        return linker.vectorizedBatchGet(
                db,
                cf,
                keyOffsetsSeg,
                keyDataSeg,
                count,
                outOffsetsSeg,
                outDataSeg,
                outValiditySeg,
                outDataCapArg,
                outDataLenSegArg);
    }

    // -----------------------------------------------------------------
    // Future completion
    // -----------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void completePut(StateRequest<?, ?, ?, ?> request) {
        ((InternalAsyncFuture<Object>) request.getFuture()).complete(null);
    }

    /**
     * Round-1 fix A1-H5 + Round-2 fix A2-H2: propagate FFI / engine failures to the
     * Flink-runtime async future. Preserves the original cause type (FrsException /
     * FrsEnginePanicError) so per-row diagnostics — including {@code FrsErrorCode} and
     * row index — are not lost. Does NOT re-invoke the fatal handler when the cause
     * is already a {@link FrsEnginePanicError}: the dispatcher fired it once and
     * downstream layers must not double-escalate.
     */
    @SuppressWarnings("unchecked")
    private static void completePutExceptionally(
            StateRequest<?, ?, ?, ?> request, Throwable cause) {
        // Use the cause's own message if specific (FrsException/FrsEnginePanicError
        // include rc + row index); fall back to a generic label only when message is empty.
        String msg = cause.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = "ForSt-RS dispatch failed: " + cause.getClass().getSimpleName();
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).completeExceptionally(msg, cause);
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result completion. The decoded value is read
     * directly off the native {@code outData} segment via the table's {@link
     * ForStRsInnerTable#deserializeValue(MemorySegment, long, int)} overload — no per-row
     * {@code byte[]} allocation. {@code present} encodes the validity bit (null result vs
     * empty value); MAP_CONTAINS resolves to a boolean derived from {@code present} without
     * touching the bytes.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void completeGet(
            StateRequest<?, ?, ?, ?> request,
            ForStRsInnerTable table,
            boolean present,
            MemorySegment data,
            long offset,
            int len) {
        Object result;
        StateRequestType type = request.getRequestType();
        if (type == StateRequestType.MAP_CONTAINS) {
            result = present;
        } else if (!present) {
            result = null;
        } else {
            result = table.deserializeValue(data, offset, len);
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).complete(result);
    }

    /**
     * PR-A10 / S1-9: propagate a GET-path FFI / decode failure to the per-row StateRequest future.
     * Mirrors {@link #completeGet} but completes the future exceptionally. The {@code table}
     * argument is accepted for symmetry with {@link #completeGet} (and to mirror the call sites)
     * but is currently unused — completion does not need to materialize a value on the failure
     * path.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void completeGetExceptionally(
            StateRequest<?, ?, ?, ?> request, ForStRsInnerTable table, Throwable cause) {
        String msg = cause.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = "ForSt-RS GET dispatch failed: " + cause.getClass().getSimpleName();
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).completeExceptionally(msg, cause);
    }

    /**
     * PR-A10 / S1-9: DELETE futures are {@code <Void>} so completion is a null-result. Distinct
     * from {@link #completePut} only for call-site clarity at the dispatcher.
     */
    @SuppressWarnings("unchecked")
    private static void completeDelete(StateRequest<?, ?, ?, ?> request) {
        ((InternalAsyncFuture<Object>) request.getFuture()).complete(null);
    }

    /**
     * PR-A10 / S1-9: propagate a DELETE-path FFI failure to the per-row StateRequest future. Same
     * shape as {@link #completePutExceptionally} but distinct for call-site clarity.
     */
    @SuppressWarnings("unchecked")
    private static void completeDeleteExceptionally(
            StateRequest<?, ?, ?, ?> request, Throwable cause) {
        String msg = cause.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = "ForSt-RS DELETE dispatch failed: " + cause.getClass().getSimpleName();
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).completeExceptionally(msg, cause);
    }

    // -----------------------------------------------------------------
    // Output-buffer sizing
    // -----------------------------------------------------------------

    private void ensureOutCapacity(int slots) {
        if (slots <= outSlotsCap) {
            return;
        }
        int newCap = outSlotsCap;
        while (newCap < slots) {
            newCap <<= 1;
        }
        outOffsets = arena.allocate(ValueLayout.JAVA_INT, (long) newCap + 1);
        outValidity = arena.allocate(newCap);
        outSlotsCap = newCap;
    }
}

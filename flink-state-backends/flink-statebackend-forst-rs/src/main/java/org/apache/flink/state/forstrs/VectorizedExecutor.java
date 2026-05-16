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
     * Attach a FatalErrorHandler for fail-process error escalation (PANIC_CAUGHT / UNKNOWN).
     * If not set, fail-process errors are still thrown as {@link FrsEnginePanicError} — Flink's
     * default uncaught-exception handler will catch them at the task level.
     */
    public void setFatalHandler(FatalErrorHandler fh) {
        this.fatalHandler = fh;
    }

    /**
     * Attach a {@link SlotArenaScope} for ITER_PREFIX dispatch. Must be set before any
     * {@link IterPrefixRequest} is dispatched; the scope is used to allocate per-iterator Arenas
     * and register handles for turn-boundary lifetime management.
     */
    public void setSlotScope(SlotArenaScope scope) {
        this.slotScope = scope;
    }

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        // Each batch gets its own classifier that wraps the long-lived buffers.
        // The classifier just resets the buffers on construction so the previous
        // batch's data is invalidated.
        VectorizedClassifier classifier =
                new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys);
        classifier.reset();
        return classifier;
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
            AppendMergeBatchBuffer amBuf = classifier.appendMergeBuffer();
            if (amBuf != null && !amBuf.isEmpty()) {
                dispatchAppendMerge(amBuf);
            }
            // Dispatch vectorized ITER_PREFIX requests if the classifier's buffer is non-null/non-empty.
            IterPrefixBatchBuffer ipBuf = classifier.iterPrefixBuffer();
            if (ipBuf != null && !ipBuf.isEmpty()) {
                dispatchIterPrefix(ipBuf);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
        VectorizedClassifier single =
                new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys);
        single.reset();
        single.offer(request);
        executePuts(single);
        executeDeletes(single);
        executeGets(single);
        executeIters(single);
        AppendMergeBatchBuffer amBuf = single.appendMergeBuffer();
        if (amBuf != null && !amBuf.isEmpty()) {
            dispatchAppendMerge(amBuf);
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

    public void flushDirty() {}

    // -----------------------------------------------------------------
    // Op-type executors
    // -----------------------------------------------------------------

    /**
     * Aggregate stateName for old-style Flink-runtime batches that mix multiple state names.
     * The new-style VectorizedStateRequest path (P5+) will provide per-state attribution.
     */
    private static final String MIXED_STATE = "_mixed";

    private void executePuts(VectorizedClassifier c) {
        int n = c.putCount();
        if (n == 0) {
            return;
        }
        long t0 = System.nanoTime();
        linker.vectorizedBatchPut(
                db,
                cf,
                c.putKeys().offsetsSegment(),
                c.putKeys().dataSegment(),
                c.putValues().offsetsSegment(),
                c.putValues().dataSegment(),
                n);
        long latencyNs = System.nanoTime() - t0;
        if (metrics != null) {
            metrics.recordDispatch(VectorizedStateRequest.Kind.PUT, MIXED_STATE, n, 0L, latencyNs);
        }
        StateRequest<?, ?, ?, ?>[] reqs = c.putRequests();
        for (int i = 0; i < n; i++) {
            completePut(reqs[i]);
        }
    }

    private void executeDeletes(VectorizedClassifier c) {
        int n = c.deleteCount();
        if (n == 0) {
            return;
        }
        linker.vectorizedBatchDelete(
                db,
                cf,
                c.deleteKeys().offsetsSegment(),
                c.deleteKeys().dataSegment(),
                n);
        StateRequest<?, ?, ?, ?>[] reqs = c.deleteRequests();
        for (int i = 0; i < n; i++) {
            completePut(reqs[i]);
        }
    }

    private void executeGets(VectorizedClassifier c) {
        int n = c.getCount();
        if (n == 0) {
            return;
        }
        ensureOutCapacity(n);

        long t0 = System.nanoTime();
        // Retry-with-growth loop: if out_data buffer is too small, grow and retry.
        while (true) {
            int rc =
                    linker.vectorizedBatchGet(
                            db,
                            cf,
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
        StateRequest<?, ?, ?, ?>[] reqs = c.getRequests();
        ForStRsInnerTable<?, ?, ?>[] tables = c.getTables();
        for (int i = 0; i < n; i++) {
            byte vld = outValidity.get(ValueLayout.JAVA_BYTE, i);
            byte[] raw = null;
            if (vld != 0) {
                int start = outOffsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
                int end =
                        outOffsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
                int len = end - start;
                if (len > 0) {
                    raw = new byte[len];
                    MemorySegment.copy(outData, ValueLayout.JAVA_BYTE, start, raw, 0, len);
                }
            }
            completeGet(reqs[i], tables[i], raw);
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
     * <p>Per-request dispatch: for each request in the buffer, allocates a small scratch
     * {@link Arena} to hold the {@code operand_ptrs} and {@code operand_lens} arrays, then calls
     * {@code frs_vec_merge_append} with the key and N operand slices. V1 uses one FFI call per
     * request; true multi-request batching is V1.x.
     *
     * <p>Each request carries N value slices (one per list element). For {@link
     * org.apache.flink.state.forstrs.state.ForStRsListStateV2#addAll(java.util.List)}, N &gt; 1 so
     * the call is effectively batched at the element level even in this per-request form.
     *
     * @param buffer the APPEND_MERGE batch buffer populated by the classifier
     */
    public void dispatchAppendMerge(AppendMergeBatchBuffer buffer) {
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
            int keyStart = keyBuf.offsetsSegment().get(
                    ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int keyEnd = keyBuf.offsetsSegment().get(
                    ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
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
                int rc = linker.frsVecMergeAppend(
                        db.handle(), cf.handle(),
                        keyPtr, keyLen,
                        ptrs, lens, vs.length);
                FrsErrorCode code = FrsErrorCode.fromU32(rc);
                if (code == FrsErrorCode.OK) {
                    future.complete(null);
                    rowsProcessed += vs.length;
                } else if (code.isFailProcess()) {
                    if (metrics != null) {
                        metrics.recordFfiError(
                                VectorizedStateRequest.Kind.APPEND_MERGE,
                                "_mixed", code);
                    }
                    FrsEnginePanicError panicErr = new FrsEnginePanicError(
                            code, "kind=APPEND_MERGE row=" + row);
                    if (fatalHandler != null) {
                        fatalHandler.onFatalError(panicErr);
                    }
                    future.completeExceptionally(panicErr);
                } else {
                    if (metrics != null) {
                        metrics.recordFfiError(
                                VectorizedStateRequest.Kind.APPEND_MERGE,
                                "_mixed", code);
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

            int rc = linker.frsVecIterPrefixOpen(
                    db.handle(), cf.handle(),
                    prefix, (int) prefix.byteSize(),
                    chunkBuf, (int) chunkBuf.byteSize(),
                    outHandle, outRowCount, outBytesUsed);

            FrsErrorCode code = FrsErrorCode.fromU32(rc);
            if (code != FrsErrorCode.OK) {
                perIterArena.close();
                future.completeExceptionally(
                        new FrsException(code, row, new byte[0]));
                if (metrics != null) {
                    metrics.recordFfiError(
                            VectorizedStateRequest.Kind.ITER_PREFIX, "_mixed", code);
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
     * Dispatches an ITER_RANGE batch via {@code frs_vec_iter_range_open} FFI.
     *
     * <p>Real implementation lands in P9.
     *
     * @param buffer the ITER_RANGE batch buffer populated by the classifier
     * @throws UnsupportedOperationException always (P9 pending)
     */
    public void dispatchIterRange(IterRangeBatchBuffer buffer) {
        throw new UnsupportedOperationException(
                "ITER_RANGE dispatch lands in P9");
    }

    // -----------------------------------------------------------------
    // Future completion
    // -----------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void completePut(StateRequest<?, ?, ?, ?> request) {
        ((InternalAsyncFuture<Object>) request.getFuture()).complete(null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void completeGet(
            StateRequest<?, ?, ?, ?> request, ForStRsInnerTable table, byte[] rawValue) {
        Object result;
        StateRequestType type = request.getRequestType();
        if (type == StateRequestType.MAP_CONTAINS) {
            result = rawValue != null;
        } else {
            result = rawValue == null ? null : table.deserializeValue(rawValue);
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).complete(result);
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

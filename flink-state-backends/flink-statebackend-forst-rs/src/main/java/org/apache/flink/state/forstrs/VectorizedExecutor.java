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
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.CompletableFuture;

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

    private void executePuts(VectorizedClassifier c) {
        int n = c.putCount();
        if (n == 0) {
            return;
        }
        linker.vectorizedBatchPut(
                db,
                cf,
                c.putKeys().offsetsSegment(),
                c.putKeys().dataSegment(),
                c.putValues().offsetsSegment(),
                c.putValues().dataSegment(),
                n);
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
                break;
            }
            if (rc == FRS_STATUS_BUFFER_TOO_SMALL) {
                long needed = outDataLenSeg.get(ValueLayout.JAVA_LONG, 0L);
                long newCap = Math.max(outDataCap * 2L, needed);
                outData = arena.allocate(newCap);
                outDataCap = newCap;
                continue;
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
     * Dispatches an APPEND_MERGE batch via {@code frs_vec_merge_append} FFI.
     *
     * <p>Real implementation lands in P6 (umbrella spec §3 Trace B). Until then, any attempt to
     * execute an APPEND_MERGE batch fails cleanly so callers discover the gap at test time rather
     * than silently producing wrong results.
     *
     * @param buffer the APPEND_MERGE batch buffer populated by the classifier
     * @throws UnsupportedOperationException always (P6 pending)
     */
    public void dispatchAppendMerge(AppendMergeBatchBuffer buffer) {
        throw new UnsupportedOperationException(
                "APPEND_MERGE dispatch lands in P6 (umbrella spec §3 Trace B)");
    }

    /**
     * Dispatches an ITER_PREFIX batch via {@code frs_vec_iter_prefix_open} FFI.
     *
     * <p>Real implementation lands in P3 (umbrella spec §3 Trace D).
     *
     * @param buffer the ITER_PREFIX batch buffer populated by the classifier
     * @throws UnsupportedOperationException always (P3 pending)
     */
    public void dispatchIterPrefix(IterPrefixBatchBuffer buffer) {
        throw new UnsupportedOperationException(
                "ITER_PREFIX dispatch lands in P3 (umbrella spec §3 Trace D)");
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

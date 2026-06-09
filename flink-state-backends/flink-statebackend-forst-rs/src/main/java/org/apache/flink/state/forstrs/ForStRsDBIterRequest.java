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
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsEnginePanicError;
import org.apache.flink.state.forstrs.ffm.FrsErrorCode;
import org.apache.flink.state.forstrs.ffm.FrsIterator;

import javax.annotation.Nullable;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Encapsulates a MAP_ITER/MAP_ITER_KEY/MAP_ITER_VALUE request for prefix-scan execution. Uses the
 * vectorized {@link ForStRsLinker#frsVecIterPrefixOpen} + {@link
 * ForStRsLinker#frsVecIterPrefixNext} chunked path (Commit A of violation #1 fix): a single FFM
 * down-call yields up to {@link #CACHE_SIZE_LIMIT} entries packed into a 64 KB native chunk buffer,
 * replacing the prior per-entry {@code iteratorNext} loop.
 *
 * <p>Implements {@link VectorizedStateRequest} as Kind.ITER_PREFIX. The {@link #future()} method
 * returns {@code null} because completion is handled via Flink's {@code InternalAsyncFuture}.
 *
 * <p>Commit B: the per-entry byte[] copy has been replaced by {@link IteratorEntryView}, a (offset,
 * length) slice into the chunk {@link MemorySegment}. The drain loop produces views synchronously
 * decoded into UK/UV instances inside {@link #completeWithEntries}, eliminating {@code 2 × N ×
 * byte[]} allocations per chunk. The inner {@code DataInputDeserializer byte[]} copy remains
 * (deferred to V1.2 / MemorySegment-backed DataInputView).
 */
@Internal
public non-sealed class ForStRsDBIterRequest<K, N, UK, UV> implements VectorizedStateRequest {

    static final int CACHE_SIZE_LIMIT = 128;

    private final byte[] prefix;
    private final StateRequest<K, N, ?, ?> request;
    private final StateRequestType originalRequestType;
    private final ForStRsIterableState<K, N, UK, UV> iterableState;
    @Nullable private FrsIterator existingIterator;

    /** Non-zero if continuation uses the vectorized iter path (frs_vec_iter_prefix_*). */
    private long existingVecHandle = 0L;

    private String stateName = "unknown";

    /** Chunk size for MAP_ITER/MAP_ITER_KEY/MAP_ITER_VALUE drains: 64 KB. */
    private static final int CHUNK_BUF_CAP = 64 * 1024;

    /** Chunk size for MAP_IS_EMPTY existence probe: 8 KB (one row is sufficient). */
    private static final int IS_EMPTY_CHUNK_BUF_CAP = 8 * 1024;

    // --- VectorizedStateRequest implementation ---

    @Override
    public Kind kind() {
        return Kind.ITER_PREFIX;
    }

    /**
     * V1 transition placeholder — returns {@code "unknown"} unless {@link #setStateName} called.
     */
    @Override
    public String stateName() {
        return stateName;
    }

    /** Sets the state name for classifier grouping and per-state metrics. */
    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    /**
     * Returns {@code null} — completion uses Flink's {@code InternalAsyncFuture} via {@link
     * #completeBatch} / {@link #process}.
     */
    @Override
    public CompletableFuture<?> future() {
        return null;
    }

    public ForStRsDBIterRequest(
            byte[] prefix,
            StateRequest<K, N, ?, ?> request,
            StateRequestType originalRequestType,
            ForStRsIterableState<K, N, UK, UV> iterableState,
            @Nullable FrsIterator existingIterator) {
        this.prefix = prefix;
        this.request = request;
        this.originalRequestType = originalRequestType;
        this.iterableState = iterableState;
        this.existingIterator = existingIterator;
    }

    public boolean hasExistingIterator() {
        return existingIterator != null || existingVecHandle != 0L;
    }

    /**
     * R20-M1: package-private accessor used by {@link VectorizedClassifier} to drain pending
     * per-row futures when a sibling row's {@code onClear} hook throws. Exposes the underlying
     * Flink-runtime {@link StateRequest} that owns this iter row's future.
     */
    StateRequest<K, N, ?, ?> getStateRequest() {
        return request;
    }

    public long getExistingVecHandle() {
        return existingVecHandle;
    }

    public void setExistingVecHandle(long handle) {
        this.existingVecHandle = handle;
    }

    public byte[] getPrefix() {
        return prefix;
    }

    /**
     * Legacy entry point that accepts a fully-materialized {@code IteratorEntry[]} array. Retained
     * for backwards compatibility with callers outside the chunked vec-iter path; currently unused
     * (the chunked path goes through {@link #process}). Converts each entry into a heap-backed
     * {@link IteratorEntryView} (wrapping a small heap {@link MemorySegment}) so it can feed the
     * unified view-based {@link #completeWithEntries} decoder.
     */
    /**
     * E8-H3: completes the underlying {@code InternalAsyncFuture} with a failure so the
     * runtime mailbox unblocks when the executor catches a throw before completing the
     * iterator normally.
     */
    @SuppressWarnings("unchecked")
    public void completeExceptionally(Throwable t) {
        ((InternalAsyncFuture<Object>) (InternalAsyncFuture<?>) request.getFuture())
                .completeExceptionally("ITER request failed", t);
    }

    @SuppressWarnings("unchecked")
    public void completeBatch(
            ForStRsLinker.IteratorEntry[] entries,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena) {
        if (originalRequestType == StateRequestType.MAP_IS_EMPTY) {
            boolean isEmpty = (entries.length == 0);
            ((InternalAsyncFuture<Boolean>) (InternalAsyncFuture<?>) request.getFuture())
                    .complete(isEmpty);
            return;
        }
        ArrayList<IteratorEntryView> views = new ArrayList<>(entries.length);
        for (ForStRsLinker.IteratorEntry e : entries) {
            byte[] k = e.key();
            byte[] v = e.value();
            int klen = k == null ? 0 : k.length;
            int vlen = v == null ? 0 : v.length;
            MemorySegment seg = arena.allocate(klen + vlen);
            if (klen > 0) {
                MemorySegment.copy(k, 0, seg, ValueLayout.JAVA_BYTE, 0, klen);
            }
            if (vlen > 0) {
                MemorySegment.copy(v, 0, seg, ValueLayout.JAVA_BYTE, klen, vlen);
            }
            views.add(new IteratorEntryView(seg, 0, klen, klen, vlen));
        }
        boolean encounterEnd = (entries.length < CACHE_SIZE_LIMIT);
        completeWithEntries(views, encounterEnd, null);
    }

    @SuppressWarnings("unchecked")
    public void process(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
        // MAP_IS_EMPTY: single open + close with a small chunk; emptiness == 0 rows returned.
        if (originalRequestType == StateRequestType.MAP_IS_EMPTY) {
            // FRS-ITER-LEAK-FIX: these are all TRANSIENT — consumed before this
            // method returns. Allocate them from a per-call confined Arena that
            // is freed on close, NOT from the long-lived executor `arena`. The
            // pre-fix `arena.allocate` per probe never freed, so the executor
            // arena's segment list grew O(n_probes) → each subsequent allocate
            // got slower (the q7 join open crept 2µs→12µs and the native heap
            // ballooned). MAP_IS_EMPTY returns no views, so nothing here needs
            // the long-lived arena.
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment chunkBuf = scratch.allocate(IS_EMPTY_CHUNK_BUF_CAP);
                MemorySegment outHandle = scratch.allocate(ValueLayout.JAVA_LONG);
                MemorySegment outRowCount = scratch.allocate(ValueLayout.JAVA_INT);
                MemorySegment outBytesUsed = scratch.allocate(ValueLayout.JAVA_INT);

                MemorySegment prefixSeg = allocPrefixSegment(scratch);
                int rc =
                        linker.frsVecIterPrefixOpen(
                                db.handle(),
                                cf.handle(),
                                prefixSeg,
                                prefix == null ? 0 : prefix.length,
                                chunkBuf,
                                IS_EMPTY_CHUNK_BUF_CAP,
                                outHandle,
                                outRowCount,
                                outBytesUsed);
                if (rc != FrsStatus.OK.code()) {
                    throwIfFatal(rc, "frs_vec_iter_prefix_open");
                    throw new FrsBackendException(
                            statusOrPanic(rc), "frs_vec_iter_prefix_open rc=" + rc);
                }
                long handle = outHandle.get(ValueLayout.JAVA_LONG, 0);
                int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                linker.frsVecIterPrefixClose(handle);
                boolean isEmpty = (rowCount == 0);
                ((InternalAsyncFuture<Boolean>) (InternalAsyncFuture<?>) request.getFuture())
                        .complete(isEmpty);
                return;
            }
        }

        // MAP_ITER / MAP_ITER_KEY / MAP_ITER_VALUE drain via chunked vec iter.
        //
        // Drain semantics:
        //   - Open seeds the first chunk into chunkBuf (cap = CHUNK_BUF_CAP). Reading rowCount
        //     from open's out-param is iter-1's row count; calling frs_vec_iter_prefix_next would
        //     fetch iter-2+ (open already advanced the underlying iterator past iter-1's rows).
        //   - We then call frs_vec_iter_prefix_next in a loop until either (a) the iterator is
        //     truly exhausted (out_row_count == 0 per the Rust API contract), or (b) we've
        //     accumulated >= CACHE_SIZE_LIMIT entries (soft cap; the rest goes in continuation).
        //   - The continuation path (existingVecHandle != 0) skips the open and goes straight
        //     into the drain loop.
        // FRS-ITER-LEAK-FIX (2026-06-08): EVERYTHING this drain allocates off-heap
        // — chunkBuf, out-params, the openVecIter prefix/handle segments, AND the
        // per-chunk view snapshots (parseChunkInto) — is TRANSIENT: consumed by
        // completeWithEntries (which deserializes each view into a DETACHED on-heap
        // UK/UV) before this try(scratch) block closes. So all of it goes on the
        // per-call confined `scratch`, NOT the long-lived executor `arena` (the
        // `arena` param is now unused on this path). The pre-fix routed the view
        // snapshots into `arena` and never freed them, so a join's millions of
        // MapState iterations grew the executor arena ~9 GB off-heap (UNcounted by
        // Flink's process.size budget) → q9/q20/q4 cgroup OOM-kill. The continuation
        // handle (existingVecHandle) is a native iterator id, not an arena segment,
        // so it safely spans process() calls.
        try (Arena scratch = Arena.ofConfined()) {
        MemorySegment chunkBuf = scratch.allocate(CHUNK_BUF_CAP);
        MemorySegment outRowCount = scratch.allocate(ValueLayout.JAVA_INT);
        MemorySegment outBytesUsed = scratch.allocate(ValueLayout.JAVA_INT);

        long handle;
        boolean firstChunkFromOpen;
        if (existingVecHandle != 0L) {
            handle = existingVecHandle;
            firstChunkFromOpen = false;
        } else {
            handle = openVecIterIntoBuf(linker, db, cf, scratch, chunkBuf, outRowCount, outBytesUsed);
            firstChunkFromOpen = true;
        }

        ArrayList<IteratorEntryView> drained = new ArrayList<>();
        boolean exhausted = false;
        try {
            // Step 1 (open-path only): consume the first chunk that frs_vec_iter_prefix_open
            // already wrote into chunkBuf. Rust seeded this chunk by popping the inner iterator;
            // we MUST parse it (Issue #1 — passing cap=0 instead of CHUNK_BUF_CAP would silently
            // drop the popped rows). We do NOT short-circuit on rowCount==0 here so that the
            // subsequent next-loop always runs at least once (the canonical "step 2" call that
            // verification mocks observe).
            if (firstChunkFromOpen) {
                int firstRowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                int firstBytesUsed = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
                if (firstRowCount > 0) {
                    parseChunkInto(chunkBuf, firstRowCount, firstBytesUsed, drained, scratch);
                }
            }

            // Step 2: drain via frs_vec_iter_prefix_next until the iterator is truly exhausted
            // (out_row_count == 0 per the Rust API contract — buffer-underfilled-but-nonzero is
            // NOT exhaustion since the chunker is byte-budget bounded, not row-count bounded) or
            // we hit the CACHE_SIZE_LIMIT soft cap (remainder returns via continuation).
            //
            // We always invoke next() at least once after the open-path first chunk; this is
            // safe (an exhausted iterator returns 0/0 with rc=OK) and gives a single, uniform
            // exhaustion signal regardless of whether open's first chunk filled the buffer.
            do {
                int rc =
                        linker.frsVecIterPrefixNext(
                                handle, chunkBuf, CHUNK_BUF_CAP, outRowCount, outBytesUsed);
                if (rc != FrsStatus.OK.code()) {
                    throwIfFatal(rc, "frs_vec_iter_prefix_next");
                    throw new FrsBackendException(
                            statusOrPanic(rc), "frs_vec_iter_prefix_next rc=" + rc);
                }
                int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                int bytesUsed = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
                if (rowCount == 0) {
                    exhausted = true;
                    break;
                }
                parseChunkInto(chunkBuf, rowCount, bytesUsed, drained, scratch);
            } while (drained.size() < CACHE_SIZE_LIMIT);
        } catch (Throwable t) {
            // Any escape (parse failure, FrsBackendException, anything) must release the native
            // handle before propagating — otherwise the iterator leaks for the lifetime of the
            // process.
            try {
                linker.frsVecIterPrefixClose(handle);
            } catch (Throwable ignored) {
                // best-effort close; surface the original failure
            }
            this.existingVecHandle = 0L;
            throw t;
        }

        // encounterEnd is true ONLY when Rust reported out_row_count == 0 (true exhaustion).
        // Soft-cap-reached returns the current batch and stashes the handle for continuation.
        boolean encounterEnd = exhausted;
        long continuationHandle = handle;
        if (encounterEnd) {
            linker.frsVecIterPrefixClose(handle);
            continuationHandle = 0L;
        }
        this.existingVecHandle = continuationHandle;

        // Decode views into UK/UV. Each view references a per-chunk snapshot segment allocated
        // from `scratch` (see parseChunkInto). The reusable chunkBuf is overwritten on each
        // next() call, but the per-chunk snapshots are NOT — they remain immutable for the
        // lifetime of `scratch`, so views from earlier chunks survive subsequent next() calls.
        // FRS-ITER-LEAK-FIX (2026-06-08): snapshots now live on `scratch`, NOT the long-lived
        // executor `arena`. completeWithEntries deserializes every view into a DETACHED on-heap
        // UK/UV (deserializeUserKey/Value copy the bytes out via the serializer) BEFORE this
        // try(scratch) block closes — so freeing scratch here is safe, and the per-iteration
        // off-heap footprint is bounded by ONE drain instead of growing O(n_iterations) forever
        // (the q9/q20/q4 join-OOM root cause: the executor arena ballooned ~9 GB off-heap).
        completeWithEntries(drained, encounterEnd, null);
        } // end try(scratch): frees the transient chunkBuf + out-params per probe
    }

    /** The prefix bytes this request iterates (used by the batched-parallel open path). */
    byte[] prefix() {
        return prefix;
    }

    /** True when this is a MAP_IS_EMPTY probe (handled by the single-open path, never batched). */
    boolean isMapIsEmpty() {
        return originalRequestType == StateRequestType.MAP_IS_EMPTY;
    }

    /** True when this request is a continuation (resumes a prior iterator) — not batchable. */
    boolean hasExistingVecHandle() {
        return existingVecHandle != 0L;
    }

    /** The uniform per-probe chunk buffer capacity the batched-parallel open path must allocate. */
    static int chunkBufCap() {
        return CHUNK_BUF_CAP;
    }

    /**
     * Batched-parallel MAP_ITER drain (the join read-path lever, q7/q9/q20). The caller
     * ({@link VectorizedExecutor#executeIters}, gated on {@code FRS_RS_PARALLEL_ITER}) has already
     * opened this probe's iterator via the ONE batched-parallel FFI crossing
     * ({@code frs_vec_iter_prefix_open_batch_parallel}) — which built+drained the K probes across the
     * engine read pool — and hands back this probe's native {@code handle} plus its already-filled
     * first chunk ({@code firstChunkBuf}/{@code firstRowCount}/{@code firstBytesUsed}). This method
     * does the SAME parse-first-chunk → {@code _next} drain → {@link #completeWithEntries} as
     * {@link #process}'s MAP_ITER path; the only difference is the open is skipped (already done in the
     * batch) and the first chunk is supplied rather than produced here. Decode is byte-identical to the
     * serial path (same {@code parseChunkInto} + zero-copy {@code VIEW_TL} deserialize).
     *
     * <p>Only valid for non-MAP_IS_EMPTY, fresh-open requests ({@code existingVecHandle == 0}); the
     * caller enforces this and routes everything else to {@link #process}.
     */
    public void processFromBatchedOpen(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            long handle,
            MemorySegment firstChunkBuf,
            int firstRowCount,
            int firstBytesUsed) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment chunkBuf = scratch.allocate(CHUNK_BUF_CAP);
            MemorySegment outRowCount = scratch.allocate(ValueLayout.JAVA_INT);
            MemorySegment outBytesUsed = scratch.allocate(ValueLayout.JAVA_INT);

            ArrayList<IteratorEntryView> drained = new ArrayList<>();
            boolean exhausted = false;
            try {
                // Step 1: parse the first chunk the batched open already filled (its rows were
                // popped from this probe's iterator during the parallel open — same as the
                // single-open first-chunk consume in process()).
                if (firstRowCount > 0) {
                    parseChunkInto(firstChunkBuf, firstRowCount, firstBytesUsed, drained, scratch);
                }
                // Step 2: continue via frs_vec_iter_prefix_next (identical to process()'s drain
                // loop). The batched open registered the handle (a light shell once exhausted),
                // so the first next() returns 0/0 with OK when the first chunk held everything.
                do {
                    int rc =
                            linker.frsVecIterPrefixNext(
                                    handle, chunkBuf, CHUNK_BUF_CAP, outRowCount, outBytesUsed);
                    if (rc != FrsStatus.OK.code()) {
                        throwIfFatal(rc, "frs_vec_iter_prefix_next");
                        throw new FrsBackendException(
                                statusOrPanic(rc), "frs_vec_iter_prefix_next rc=" + rc);
                    }
                    int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                    int bytesUsed = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
                    if (rowCount == 0) {
                        exhausted = true;
                        break;
                    }
                    parseChunkInto(chunkBuf, rowCount, bytesUsed, drained, scratch);
                } while (drained.size() < CACHE_SIZE_LIMIT);
            } catch (Throwable t) {
                try {
                    linker.frsVecIterPrefixClose(handle);
                } catch (Throwable ignored) {
                    // best-effort close; surface the original failure
                }
                this.existingVecHandle = 0L;
                throw t;
            }

            boolean encounterEnd = exhausted;
            long continuationHandle = handle;
            if (encounterEnd) {
                linker.frsVecIterPrefixClose(handle);
                continuationHandle = 0L;
            }
            this.existingVecHandle = continuationHandle;
            completeWithEntries(drained, encounterEnd, null);
        }
    }

    /**
     * Allocates a native segment carrying the prefix bytes into the request arena, or returns
     * {@link MemorySegment#NULL} when the prefix is empty.
     */
    private MemorySegment allocPrefixSegment(Arena arena) {
        if (prefix == null || prefix.length == 0) {
            return MemorySegment.NULL;
        }
        MemorySegment seg = arena.allocate(prefix.length);
        MemorySegment.copy(prefix, 0, seg, ValueLayout.JAVA_BYTE, 0, prefix.length);
        return seg;
    }

    /**
     * Opens a new vectorized prefix iterator, seeding the first chunk into the caller's {@code
     * chunkBuf} (capacity {@link #CHUNK_BUF_CAP}). The first chunk's row count and byte count are
     * written into {@code outRowCount} / {@code outBytesUsed}; the handle is returned.
     *
     * <p>Passing a real (non-NULL, cap &gt; 0) buffer is mandatory: the Rust side's {@code
     * frs_vec_iter_prefix_open} unconditionally pops the first chunk from the inner iterator, and
     * with {@code cap == 0} the bounds check in {@code write_chunk_into_buf} drops those rows on
     * the floor (data-loss bug). See {@code crates/forst-rs-ffi/src/lib.rs} around line 3597.
     *
     * <p>The handle's lifetime is the request — callers must close it via {@link
     * ForStRsLinker#frsVecIterPrefixClose}.
     */
    private long openVecIterIntoBuf(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            MemorySegment chunkBuf,
            MemorySegment outRowCount,
            MemorySegment outBytesUsed) {
        MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
        MemorySegment prefixSeg = allocPrefixSegment(arena);
        int rc =
                linker.frsVecIterPrefixOpen(
                        db.handle(),
                        cf.handle(),
                        prefixSeg,
                        prefix == null ? 0 : prefix.length,
                        chunkBuf,
                        CHUNK_BUF_CAP,
                        outHandle,
                        outRowCount,
                        outBytesUsed);
        if (rc != FrsStatus.OK.code()) {
            throwIfFatal(rc, "frs_vec_iter_prefix_open");
            throw new FrsBackendException(
                    statusOrPanic(rc), "frs_vec_iter_prefix_open rc=" + rc);
        }
        return outHandle.get(ValueLayout.JAVA_LONG, 0);
    }

    /**
     * Parses a chunk buffer written by Rust's {@code write_chunk_into_buf} and appends one {@link
     * IteratorEntryView} per row to {@code out}. Layout per row: {@code [u32 klen LE][u32 vlen
     * LE][key bytes][value bytes]}.
     *
     * <p>Correctness note: the views must reference a stable byte range that is NOT overwritten by
     * subsequent {@code frs_vec_iter_prefix_next} calls (which reuse {@code chunkBuf} as the
     * destination). We therefore copy this chunk's {@code bytesUsed} bytes into a freshly-allocated
     * per-chunk arena snapshot and make the views reference the snapshot. The snapshot is immutable
     * for the lifetime of the arena, so views from chunk #1 survive chunk #2's drain. Cost is one
     * {@code arena.allocate(bytesUsed)} plus one bulk copy per chunk — independent of row count and
     * bounded by {@code O(chunks_per_drain × bytesUsed_per_chunk)}; far cheaper than the legacy
     * per-entry byte[] copy this commit replaced.
     */
    /**
     * R89-M1: defensive conversion from raw FFI return code to a typed
     * {@link FrsStatus}. Sister-helper of R88-H1's inline pattern in
     * {@link VectorizedExecutor}. The vectorized iterator FFI surface
     * returns extended {@link FrsErrorCode} values (110, 200, 201, 300,
     * 900, …) not mirrored in the legacy {@link FrsStatus} enum; the
     * bare {@code FrsStatus.fromCode(rc)} throws
     * {@code IllegalArgumentException} for those codes, escaping as an
     * unchecked crash that bypasses downstream {@code catch
     * (FrsBackendException)} handlers. Returning {@link FrsStatus#PANIC}
     * on unknown codes keeps the exception type consistent across
     * iterator + non-iterator FFI paths.
     */
    private static FrsStatus statusOrPanic(int rc) {
        try {
            return FrsStatus.fromCode(rc);
        } catch (IllegalArgumentException ignored) {
            return FrsStatus.PANIC;
        }
    }

    /**
     * R94-H1 / R93-H2: if the FFI rc is fail-process (PANIC_CAUGHT=900
     * etc.), throw a typed {@link FrsEnginePanicError} so the executor's
     * outer catch can route it through `fatalHandler.onFatalError(...)`.
     * Otherwise this method is a no-op and the caller proceeds to wrap
     * the rc via {@link #statusOrPanic} as before.
     *
     * <p>The iter FFI surface is reached by every MapState iterator
     * (MAP_ITER / MAP_IS_EMPTY / MAP_ITER_KEY / MAP_ITER_VALUE) routed
     * through the executor's `executeIters`; pre-fix a PANIC_CAUGHT
     * there became `FrsBackendException(PANIC, ...)` and the engine
     * kept running on poisoned state.
     */
    private static void throwIfFatal(int rc, String fn) {
        FrsErrorCode code = FrsErrorCode.fromU32(rc);
        if (code.isFailProcess()) {
            throw new FrsEnginePanicError(code, fn + " rc=" + rc);
        }
    }

    private static void parseChunkInto(
            MemorySegment chunkBuf,
            int rowCount,
            int bytesUsed,
            ArrayList<IteratorEntryView> out,
            Arena arena) {
        // Snapshot the chunk into a stable per-chunk arena segment. chunkBuf is reused as the
        // destination of every frs_vec_iter_prefix_next call, so views referencing it directly
        // would observe data corruption once the next() overwrites it.
        MemorySegment chunkSnapshot = arena.allocate(bytesUsed);
        MemorySegment.copy(chunkBuf, 0, chunkSnapshot, 0, bytesUsed);

        int off = 0;
        for (int i = 0; i < rowCount; i++) {
            int klen = chunkSnapshot.get(ValueLayout.JAVA_INT_UNALIGNED, off);
            off += 4;
            int vlen = chunkSnapshot.get(ValueLayout.JAVA_INT_UNALIGNED, off);
            off += 4;
            int keyOff = off;
            off += klen;
            int valOff = off;
            off += vlen;
            out.add(new IteratorEntryView(chunkSnapshot, keyOff, klen, valOff, vlen));
        }
        // bytesUsed paranoia check (asserts only under -ea).
        assert off == bytesUsed : "chunk parse off=" + off + " != bytesUsed=" + bytesUsed;
    }

    @SuppressWarnings("unchecked")
    private void completeWithEntries(
            ArrayList<IteratorEntryView> views,
            boolean encounterEnd,
            @Nullable FrsIterator continuationIter) {
        int prefixLen = prefix.length;
        StateRequestHandler handler = iterableState.getStateRequestHandler();
        int n = views.size();

        switch (originalRequestType) {
            case MAP_ITER:
                Collection<Map.Entry<UK, UV>> mapEntries = new ArrayList<>(n);
                for (IteratorEntryView v : views) {
                    UK uk = iterableState.deserializeUserKey(v, prefixLen);
                    UV uv = iterableState.deserializeUserValue(v);
                    if (uv != null) {
                        mapEntries.add(new SimpleEntry<>(uk, uv));
                    }
                }
                ForStRsMapIterator<Map.Entry<UK, UV>> entryIter =
                        new ForStRsMapIterator<>(
                                iterableState.asState(),
                                StateRequestType.MAP_ITER,
                                handler,
                                mapEntries,
                                encounterEnd,
                                continuationIter,
                                encounterEnd ? 0L : existingVecHandle);
                ((InternalAsyncFuture<StateIterator<Map.Entry<UK, UV>>>)
                                (InternalAsyncFuture<?>) request.getFuture())
                        .complete(entryIter);
                break;

            case MAP_ITER_KEY:
                Collection<UK> keys = new ArrayList<>(n);
                for (IteratorEntryView v : views) {
                    UK uk = iterableState.deserializeUserKey(v, prefixLen);
                    keys.add(uk);
                }
                ForStRsMapIterator<UK> keyIter =
                        new ForStRsMapIterator<>(
                                iterableState.asState(),
                                StateRequestType.MAP_ITER_KEY,
                                handler,
                                keys,
                                encounterEnd,
                                continuationIter,
                                encounterEnd ? 0L : existingVecHandle);
                ((InternalAsyncFuture<StateIterator<UK>>)
                                (InternalAsyncFuture<?>) request.getFuture())
                        .complete(keyIter);
                break;

            case MAP_ITER_VALUE:
                Collection<UV> values = new ArrayList<>(n);
                for (IteratorEntryView v : views) {
                    UV uv = iterableState.deserializeUserValue(v);
                    if (uv != null) {
                        values.add(uv);
                    }
                }
                ForStRsMapIterator<UV> valueIter =
                        new ForStRsMapIterator<>(
                                iterableState.asState(),
                                StateRequestType.MAP_ITER_VALUE,
                                handler,
                                values,
                                encounterEnd,
                                continuationIter,
                                encounterEnd ? 0L : existingVecHandle);
                ((InternalAsyncFuture<StateIterator<UV>>)
                                (InternalAsyncFuture<?>) request.getFuture())
                        .complete(valueIter);
                break;

            default:
                throw new IllegalArgumentException(
                        "Unknown iter request type: " + originalRequestType);
        }
    }

    static class SimpleEntry<K, V> implements Map.Entry<K, V> {
        private final K key;
        private V value;

        SimpleEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V v) {
            V old = value;
            value = v;
            return old;
        }
    }
}

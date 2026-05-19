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
 * <p>Per-entry {@code IteratorEntry} byte[] copy semantics are preserved here. Commit B will
 * introduce a slice-based view to isolate the allocation cost.
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

    public long getExistingVecHandle() {
        return existingVecHandle;
    }

    public void setExistingVecHandle(long handle) {
        this.existingVecHandle = handle;
    }

    public byte[] getPrefix() {
        return prefix;
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
        boolean encounterEnd = (entries.length < CACHE_SIZE_LIMIT);
        completeWithEntries(entries, encounterEnd, null);
    }

    @SuppressWarnings("unchecked")
    public void process(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
        // MAP_IS_EMPTY: single open + close with a small chunk; emptiness == 0 rows returned.
        if (originalRequestType == StateRequestType.MAP_IS_EMPTY) {
            MemorySegment chunkBuf = arena.allocate(IS_EMPTY_CHUNK_BUF_CAP);
            MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment outRowCount = arena.allocate(ValueLayout.JAVA_INT);
            MemorySegment outBytesUsed = arena.allocate(ValueLayout.JAVA_INT);

            MemorySegment prefixSeg = allocPrefixSegment(arena);
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
                throw new FrsBackendException(
                        FrsStatus.fromCode(rc), "frs_vec_iter_prefix_open rc=" + rc);
            }
            long handle = outHandle.get(ValueLayout.JAVA_LONG, 0);
            int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
            linker.frsVecIterPrefixClose(handle);
            boolean isEmpty = (rowCount == 0);
            ((InternalAsyncFuture<Boolean>) (InternalAsyncFuture<?>) request.getFuture())
                    .complete(isEmpty);
            return;
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
        MemorySegment chunkBuf = arena.allocate(CHUNK_BUF_CAP);
        MemorySegment outRowCount = arena.allocate(ValueLayout.JAVA_INT);
        MemorySegment outBytesUsed = arena.allocate(ValueLayout.JAVA_INT);

        long handle;
        boolean firstChunkFromOpen;
        if (existingVecHandle != 0L) {
            handle = existingVecHandle;
            firstChunkFromOpen = false;
        } else {
            handle = openVecIterIntoBuf(linker, db, cf, arena, chunkBuf, outRowCount, outBytesUsed);
            firstChunkFromOpen = true;
        }

        ArrayList<ForStRsLinker.IteratorEntry> drained = new ArrayList<>();
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
                    parseChunkInto(chunkBuf, firstRowCount, firstBytesUsed, drained);
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
                    throw new FrsBackendException(
                            FrsStatus.fromCode(rc), "frs_vec_iter_prefix_next rc=" + rc);
                }
                int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                int bytesUsed = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
                if (rowCount == 0) {
                    exhausted = true;
                    break;
                }
                parseChunkInto(chunkBuf, rowCount, bytesUsed, drained);
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

        ForStRsLinker.IteratorEntry[] entries = drained.toArray(new ForStRsLinker.IteratorEntry[0]);

        // encounterEnd is true ONLY when Rust reported out_row_count == 0 (true exhaustion).
        // Soft-cap-reached returns the current batch and stashes the handle for continuation.
        boolean encounterEnd = exhausted;
        long continuationHandle = handle;
        if (encounterEnd) {
            linker.frsVecIterPrefixClose(handle);
            continuationHandle = 0L;
        }
        this.existingVecHandle = continuationHandle;

        completeWithEntries(entries, encounterEnd, null);
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
            throw new FrsBackendException(
                    FrsStatus.fromCode(rc), "frs_vec_iter_prefix_open rc=" + rc);
        }
        return outHandle.get(ValueLayout.JAVA_LONG, 0);
    }

    /**
     * Parses a chunk buffer written by Rust's {@code write_chunk_into_buf} and appends the
     * resulting {@link ForStRsLinker.IteratorEntry} records to {@code out}. Layout per row: {@code
     * [u32 klen LE][u32 vlen LE][key bytes][value bytes]}. Per-entry byte[] copy semantics are
     * preserved (Commit B introduces the slice-based view).
     */
    private static void parseChunkInto(
            MemorySegment chunkBuf,
            int rowCount,
            int bytesUsed,
            ArrayList<ForStRsLinker.IteratorEntry> out) {
        long off = 0;
        for (int i = 0; i < rowCount; i++) {
            int klen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
            off += 4;
            int vlen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
            off += 4;
            byte[] keyCopy = new byte[klen];
            MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, off, keyCopy, 0, klen);
            off += klen;
            byte[] valCopy = new byte[vlen];
            MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, off, valCopy, 0, vlen);
            off += vlen;
            out.add(new ForStRsLinker.IteratorEntry(keyCopy, valCopy));
        }
        // bytesUsed paranoia check (asserts only under -ea; Commit B will harden this).
        assert off == bytesUsed : "chunk parse off=" + off + " != bytesUsed=" + bytesUsed;
    }

    @SuppressWarnings("unchecked")
    private void completeWithEntries(
            ForStRsLinker.IteratorEntry[] entries,
            boolean encounterEnd,
            @Nullable FrsIterator continuationIter) {
        int prefixLen = prefix.length;
        StateRequestHandler handler = iterableState.getStateRequestHandler();

        switch (originalRequestType) {
            case MAP_ITER:
                Collection<Map.Entry<UK, UV>> mapEntries = new ArrayList<>(entries.length);
                for (ForStRsLinker.IteratorEntry e : entries) {
                    UK uk = iterableState.deserializeUserKey(e.key(), prefixLen);
                    UV uv = iterableState.deserializeUserValue(e.value());
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
                Collection<UK> keys = new ArrayList<>(entries.length);
                for (ForStRsLinker.IteratorEntry e : entries) {
                    UK uk = iterableState.deserializeUserKey(e.key(), prefixLen);
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
                Collection<UV> values = new ArrayList<>(entries.length);
                for (ForStRsLinker.IteratorEntry e : entries) {
                    UV uv = iterableState.deserializeUserValue(e.value());
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

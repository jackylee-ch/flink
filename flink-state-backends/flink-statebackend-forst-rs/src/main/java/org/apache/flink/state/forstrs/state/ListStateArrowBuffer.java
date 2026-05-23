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

package org.apache.flink.state.forstrs.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * PR-C2: per-state-instance off-heap accumulator for {@code ForStRsAsyncListStateV2.asyncAdd}.
 *
 * <p>Holds {@code N} rows of {@code (composite_key_bytes, single_element_chunk_bytes)}. Each row is
 * one {@code asyncAdd(v)} call: the chunk payload is {@code [count=1 LE][serialized_elem]} which is
 * exactly the operand that the engine's merge-operator concatenates verbatim on flush (Format B,
 * see {@code 2026-05-22-v20-liststate-format-unification-design.md}).
 *
 * <p><b>Architectural goal:</b> skip the {@code byte[]}-on-heap intermediate that the
 * {@code recordAppendMerge → AppendMergeBatchBuffer → dispatchAppendMergeBatch} path materialises
 * per row. Element bytes are written directly into a single off-heap {@link Arena} via
 * {@link java.io.OutputStream}-style appends; on {@link #flushTo} one FFI call
 * ({@code frs_vec_merge_append_batch}) drains every accumulated row.
 *
 * <p><b>Ordering invariant (Format B / V20 §7.4):</b> rows are kept in append-call order and
 * flushed in that order. The engine merge-operator concatenates operand bytes verbatim, so on the
 * downstream {@code asyncGet} {@link
 * org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2#deserializeValue} decodes the
 * concatenated chunks in submit order. The buffer therefore MUST be a sequential append log — not
 * a hash-indexed structure that would coalesce same-key writes — because two appends under the
 * same {@code (key, namespace)} must produce two distinct chunks at distinct offsets.
 *
 * <p>Single-threaded: per-state-instance, owned by the operator's mailbox thread. No
 * synchronisation.
 */
@Internal
public final class ListStateArrowBuffer implements AutoCloseable {

    /** Auto-flush trigger: row count threshold. */
    public static final int DEFAULT_MAX_ROWS = 1024;

    /** Auto-flush trigger: byte-size threshold for {@code keyData + opsData}. */
    public static final long DEFAULT_MAX_BYTES = 1L << 20; // 1 MiB

    private Arena arena;

    // Off-heap row data — sequential append log, NOT key-indexed.
    private MemorySegment keyOffsets; // (rowCap + 1) × int
    private MemorySegment keyData; // raw bytes — composite key (KEY_PREFIX + key + / + name + / + ns)
    private MemorySegment opsOffsets; // (rowCap + 1) × int
    private MemorySegment opsData; // raw bytes — per-row [count=1 LE][serialized_elem]
    private long keyDataCapacity;
    private long opsDataCapacity;
    private int rowCap;

    private int rowCount;
    private long keyDataUsed;
    private long opsDataUsed;

    private final int maxRows;
    private final long maxBytes;

    // Futures parallel to rows (one per append). Caller completes them after flush.
    private final List<CompletableFuture<Void>> futures = new ArrayList<>();

    public ListStateArrowBuffer() {
        this(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
    }

    public ListStateArrowBuffer(int maxRows, long maxBytes) {
        this.maxRows = Math.max(maxRows, 8);
        this.maxBytes = Math.max(maxBytes, 4096L);
        this.arena = Arena.ofShared();
        this.rowCap = Math.min(64, this.maxRows);
        this.keyDataCapacity = 4096;
        this.opsDataCapacity = 4096;
        this.keyOffsets = arena.allocate((long) (rowCap + 1) * Integer.BYTES);
        this.opsOffsets = arena.allocate((long) (rowCap + 1) * Integer.BYTES);
        this.keyData = arena.allocate(keyDataCapacity);
        this.opsData = arena.allocate(opsDataCapacity);
        // Initialise the head-offset cells (rows 0's start = 0).
        keyOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
        opsOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
    }

    public int rowCount() {
        return rowCount;
    }

    public boolean isEmpty() {
        return rowCount == 0;
    }

    /** Bytes currently consumed by accumulated key + ops data (for tests / metrics). */
    public long bytesUsed() {
        return keyDataUsed + opsDataUsed;
    }

    /**
     * True when the buffer has hit either the row or byte-size auto-flush threshold. The caller
     * should invoke {@link #flushTo} promptly to avoid unbounded growth.
     */
    public boolean shouldAutoFlush() {
        return rowCount >= maxRows || (keyDataUsed + opsDataUsed) >= maxBytes;
    }

    /**
     * Appends one (composite key bytes, pre-encoded chunk bytes) row. The chunk bytes must already
     * carry the {@code [count=1 LE][serialized_elem]} Format-B-compatible payload.
     *
     * <p>Returns a future that will be completed (or completed exceptionally) on the next
     * {@link #flushTo} invocation.
     */
    public CompletableFuture<Void> append(byte[] keyBytes, byte[] chunkBytes) {
        return append(
                MemorySegment.ofArray(keyBytes),
                0L,
                keyBytes.length,
                MemorySegment.ofArray(chunkBytes),
                0L,
                chunkBytes.length);
    }

    /** Memory-segment view of {@link #append(byte[], byte[])} — copies into the off-heap arena. */
    public CompletableFuture<Void> append(
            MemorySegment keySeg,
            long keyOffset,
            int keyLen,
            MemorySegment chunkSeg,
            long chunkOffset,
            int chunkLen) {
        ensureRowCapacity();
        ensureKeyDataCapacity(keyDataUsed + keyLen);
        ensureOpsDataCapacity(opsDataUsed + chunkLen);
        MemorySegment.copy(keySeg, keyOffset, keyData, keyDataUsed, keyLen);
        MemorySegment.copy(chunkSeg, chunkOffset, opsData, opsDataUsed, chunkLen);
        keyDataUsed += keyLen;
        opsDataUsed += chunkLen;
        rowCount++;
        keyOffsets.set(ValueLayout.JAVA_INT, (long) rowCount * Integer.BYTES, (int) keyDataUsed);
        opsOffsets.set(ValueLayout.JAVA_INT, (long) rowCount * Integer.BYTES, (int) opsDataUsed);
        CompletableFuture<Void> f = new CompletableFuture<>();
        futures.add(f);
        return f;
    }

    /**
     * Flushes all accumulated rows via a single {@code frs_vec_merge_append_batch} FFI call, then
     * completes the per-row futures, then resets. Order of submission equals order of {@link
     * #append} — required by Format B (per V20 §7.4: "engine merge operator concatenates operands
     * in submit order").
     *
     * <p>Idempotent on empty buffer.
     */
    public void flushTo(ForStRsLinker linker, FrsDb db, FrsCfHandle cf) {
        if (rowCount == 0) {
            return;
        }
        int n = rowCount;
        // Copy futures BEFORE reset so subsequent appends from a re-entrant continuation don't
        // race with their own completion.
        List<CompletableFuture<Void>> toComplete = new ArrayList<>(futures);
        int rc =
                linker.frsVecMergeAppendBatch(
                        db.handle(),
                        cf.handle(),
                        keyOffsets,
                        keyData,
                        opsOffsets,
                        opsData,
                        n);
        // Reset BEFORE completing the futures — a continuation that re-enters asyncAdd inside
        // .thenApply must see the buffer ready for the next batch.
        reset();
        if (rc == 0) {
            for (CompletableFuture<Void> f : toComplete) {
                f.complete(null);
            }
        } else {
            RuntimeException err =
                    new RuntimeException(
                            "ListStateArrowBuffer.flushTo: frs_vec_merge_append_batch failed with rc="
                                    + rc);
            for (CompletableFuture<Void> f : toComplete) {
                f.completeExceptionally(err);
            }
        }
    }

    /** Resets row count and byte counters (does NOT release the arena). */
    public void reset() {
        rowCount = 0;
        keyDataUsed = 0;
        opsDataUsed = 0;
        keyOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
        opsOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
        futures.clear();
    }

    /**
     * R21-H2: discard every accumulated row WITHOUT dispatching them to the engine. Completes
     * each pending per-row future exceptionally with {@code cause} so observers see the failure,
     * then resets the buffer. Called from
     * {@link ForStRsAsyncListStateV2#discardBufferedRows(Throwable)} when a sibling row in the
     * same batch poisons the dispatch (e.g. an {@code onClear} FFI throw on a different state
     * instance).
     *
     * <p>Design invariant: off-heap rows buffered for a StateRequest whose future was completed
     * exceptionally must NOT be written to the engine on the next batch's flush. Without this
     * method the {@link #flushTo} path would re-dispatch them indistinguishably from clean rows,
     * silently breaking exactly-once semantics for the poisoned StateRequests.
     */
    public void discardWithCause(Throwable cause) {
        if (rowCount == 0) {
            return;
        }
        // Copy futures BEFORE reset so a continuation that re-enters asyncAdd inside an
        // exceptionally-callback can't race the completion.
        List<CompletableFuture<Void>> toFail = new ArrayList<>(futures);
        reset();
        for (CompletableFuture<Void> f : toFail) {
            if (f != null && !f.isDone()) {
                f.completeExceptionally(cause);
            }
        }
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
        }
    }

    // ------------------------------------------------------------
    // Capacity management
    // ------------------------------------------------------------

    private void ensureRowCapacity() {
        if (rowCount < rowCap) {
            return;
        }
        // Grow row capacity (offsets) — copy old offsets across.
        int newCap = Math.max(rowCap * 2, 16);
        MemorySegment newKeyOff = arena.allocate((long) (newCap + 1) * Integer.BYTES);
        MemorySegment newOpsOff = arena.allocate((long) (newCap + 1) * Integer.BYTES);
        MemorySegment.copy(keyOffsets, 0L, newKeyOff, 0L, (long) (rowCount + 1) * Integer.BYTES);
        MemorySegment.copy(opsOffsets, 0L, newOpsOff, 0L, (long) (rowCount + 1) * Integer.BYTES);
        keyOffsets = newKeyOff;
        opsOffsets = newOpsOff;
        rowCap = newCap;
    }

    private void ensureKeyDataCapacity(long needed) {
        if (needed <= keyDataCapacity) {
            return;
        }
        long newCap = Math.max(keyDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(keyData, 0L, newSeg, 0L, keyDataUsed);
        keyData = newSeg;
        keyDataCapacity = newCap;
    }

    private void ensureOpsDataCapacity(long needed) {
        if (needed <= opsDataCapacity) {
            return;
        }
        long newCap = Math.max(opsDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(opsData, 0L, newSeg, 0L, opsDataUsed);
        opsData = newSeg;
        opsDataCapacity = newCap;
    }

    // ------------------------------------------------------------
    // Test-only accessors
    // ------------------------------------------------------------

    /** Test accessor: copy of accumulated key bytes for row {@code i}. */
    public byte[] copyKeyAt(int row) {
        int start = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int end = keyOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        byte[] out = new byte[end - start];
        MemorySegment.copy(keyData, ValueLayout.JAVA_BYTE, start, out, 0, end - start);
        return out;
    }

    /** Test accessor: copy of accumulated chunk bytes for row {@code i}. */
    public byte[] copyChunkAt(int row) {
        int start = opsOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int end = opsOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        byte[] out = new byte[end - start];
        MemorySegment.copy(opsData, ValueLayout.JAVA_BYTE, start, out, 0, end - start);
        return out;
    }
}

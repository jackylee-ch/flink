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

/**
 * Per-state-instance off-heap staging buffer for V2 async MapState (PR-C1).
 *
 * <p>Mirrors the V1-sync {@code statebuf} pattern: every {@code asyncPut} stages the composite
 * (operatorKey + namespace + userKey) → value pair into an underlying {@link ArrowBinaryBuffer}
 * whose key/value bytes live off-heap in a per-instance shared Arena. Subsequent {@code asyncGet}
 * / {@code asyncContains} on the same composite key resolve from the buffer without re-crossing
 * the FFM boundary; on flush the buffer drains its rows via a single {@code linker.batchPut}.
 *
 * <h3>Why not extend ArrowBinaryBuffer</h3>
 *
 * <p>{@link ArrowBinaryBuffer} is {@code final}. This wrapper composes one and adds:
 *
 * <ul>
 *   <li>a heap byte[] → off-heap MemorySegment staging arena, so callers can hand in the byte[]
 *       keys/values produced by the V2 framework's serializeKey/serializeValue without writing
 *       their own MemorySegment encoder;
 *   <li>tombstone short-circuiting on {@code asyncRemove} so reads after a remove return {@code
 *       null} without an engine round-trip — implemented zero-alloc via the underlying buffer's
 *       per-row tombstone bitmap (Cleanup-C1; no HashSet wrapper allocation per call);
 *   <li>a {@code drainOnFlush=true} flag wiring the auto-flush gates to the V2 snapshot hook.
 * </ul>
 *
 * <h3>Cleanup-C1: zero-alloc tombstone path</h3>
 *
 * <p>The previous incarnation tracked tombstones in a {@code HashSet<BytesKey>}, allocating a
 * {@code BytesKey} wrapper object (and its internal {@code Arrays.hashCode} byte[] read) on
 * every {@code asyncRemove}. The new path delegates tombstone tracking to
 * {@link ArrowBinaryBuffer#tombstone} / {@link ArrowBinaryBuffer#findOrTombstone}, which use a
 * per-row off-heap bitmap. {@code asyncRemove} no longer allocates on the heap; {@code asyncGet}
 * after a remove resolves the tombstone via the off-heap bitmap probe and short-circuits to
 * {@code null} without an engine round-trip — identical observable behaviour, zero heap allocation
 * on the hot path.
 *
 * <h3>NOT cross-state-shared</h3>
 *
 * <p>The v3.3 cross-state attempt regressed Q5 to 586s — each MapState instance owns its buffer.
 * Composite keys built by {@link ForStRsMapStateV2#serializeMapEntryKey} already include the
 * state name so a per-instance buffer never sees foreign rows even if a future audit chooses to
 * share buffers.
 *
 * <h3>Threading</h3>
 *
 * <p>Same contract as the underlying {@code statebuf}: single-threaded, dispatched from Flink's
 * async-state RecordContext-serialized path. No internal synchronization.
 */
@Internal
public final class MapStateArrowBuffer implements AutoCloseable {

    /** Default capacity matches the V1-sync MapState cap (legacy MAP_WRITE_BUFFER_THRESHOLD). */
    public static final int DEFAULT_CAPACITY = ArrowBinaryBuffer.MAX_CAPACITY_MAP_STATE;

    private final ArrowBinaryBuffer buf;
    private final ArrowBinaryBufferAutoTuner tuner;

    /**
     * Staging arena for inbound byte[] keys/values. Reused across inserts: each {@link #put} writes
     * key bytes at offset 0 and value bytes immediately after, then memcpys them into the
     * underlying buffer's off-heap key/value data regions via {@link ArrowBinaryBuffer#insert}.
     */
    private final Arena stagingArena;

    private MemorySegment staging;
    private int stagingCap;

    private boolean closed;

    public MapStateArrowBuffer() {
        this(ArrowBinaryBuffer.MIN_CAPACITY, DEFAULT_CAPACITY);
    }

    public MapStateArrowBuffer(int initialCapacity, int maxCapacity) {
        this.tuner = new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
        this.buf = new ArrowBinaryBuffer(initialCapacity, maxCapacity, tuner);
        this.stagingArena = Arena.ofShared();
        this.stagingCap = 4096;
        this.staging = stagingArena.allocate(stagingCap);
    }

    /** Returns the current row count (live + tombstoned-in-index). */
    public int size() {
        return buf.size();
    }

    /** Returns the current capacity of the underlying buffer. */
    public int capacity() {
        return buf.capacity();
    }

    /** Returns the underlying buffer (visible for tests, NOT for hot-path use). */
    public ArrowBinaryBuffer underlying() {
        return buf;
    }

    /**
     * Stages a (compositeKey → value) pair. The composite key is the same bytes produced by
     * {@code ForStRsMapStateV2.serializeMapEntryKey} (operatorKey + namespace + userKey) — the
     * caller is responsible for the V2 key layout including the namespace component (PR-A2).
     *
     * <p>On capacity exhaustion (size == maxCapacity AND tuner refuses growth) this flushes
     * to the engine via {@code flushTo} BEFORE retrying the insert. Callers must therefore pass
     * the {@code linker/db/cf} bound at construction time.
     */
    public void put(byte[] compositeKey, byte[] value, ForStRsLinker linker, FrsDb db, FrsCfHandle cf) {
        ensureClosed();
        int needed = compositeKey.length + (value == null ? 0 : value.length);
        ensureStagingCapacity(needed);
        MemorySegment.copy(compositeKey, 0, staging, ValueLayout.JAVA_BYTE, 0, compositeKey.length);
        int valLen = value == null ? 0 : value.length;
        if (valLen > 0) {
            MemorySegment.copy(
                    value, 0, staging, ValueLayout.JAVA_BYTE, compositeKey.length, valLen);
        }
        // Opportunistic + forced flush gates, same pattern as V1-sync ForStRsMapState.put.
        if (buf.needsFlush() || buf.shouldAutoFlush()) {
            flushTo(linker, db, cf);
        }
        // PUT supersedes any pending tombstone on the row (ArrowBinaryBuffer.insert clears the
        // tombstone bit on overwrite — Cleanup-C1).
        int row =
                buf.insert(staging, 0, compositeKey.length, staging, compositeKey.length, valLen);
        if (row == ArrowBinaryBuffer.INSERT_NEEDS_FLUSH) {
            flushTo(linker, db, cf);
            buf.insert(staging, 0, compositeKey.length, staging, compositeKey.length, valLen);
        }
    }

    /**
     * Probes the buffer for {@code compositeKey}. Returns a {@link Lookup} carrying an explicit
     * {@code cached} flag so callers can distinguish:
     *
     * <ul>
     *   <li>{@code cached=true, tombstone=true} → key is known-removed in this buffer; return null
     *       without consulting the engine.
     *   <li>{@code cached=true, tombstone=false} → key has a buffered value; return
     *       {@link #valueBytesOf} of the row.
     *   <li>{@code cached=false} → miss; fall through to engine.
     * </ul>
     *
     * <p>Cleanup-C1: the tombstone branch is decided by the underlying buffer's off-heap
     * tombstone bitmap (no HashSet probe, no per-call BytesKey allocation).
     */
    public Lookup lookup(byte[] compositeKey) {
        ensureClosed();
        ensureStagingCapacity(compositeKey.length);
        MemorySegment.copy(compositeKey, 0, staging, ValueLayout.JAVA_BYTE, 0, compositeKey.length);
        int row = buf.findOrTombstone(staging, 0, compositeKey.length);
        if (row == ArrowBinaryBuffer.TOMBSTONE_FOUND) {
            // Match the pre-C1 observeRead semantics: tombstones short-circuit before the live-hit
            // path, so they did not feed the AutoTuner's hit-rate signal. Preserve that.
            return Lookup.TOMBSTONE;
        }
        tuner.observeRead(row >= 0, buf.size(), buf.capacity());
        if (row >= 0) {
            return new Lookup(true, false, row);
        }
        return Lookup.MISS;
    }

    /** Returns a fresh byte[] copy of the value bytes for the given row. */
    public byte[] valueBytesOf(int row) {
        return buf.copyValue(row);
    }

    /** Returns the off-heap value-data segment + offset/length for zero-copy decode by callers. */
    public MemorySegment valueDataSegment() {
        return buf.valueDataSegment();
    }

    public int valueOffsetOf(int row) {
        return buf.valueOffsetOf(row);
    }

    public int valueLengthOf(int row) {
        return buf.valueLengthOf(row);
    }

    /**
     * Marks {@code compositeKey} as removed. Subsequent {@link #lookup} returns a tombstone hit
     * without an engine probe. On {@link #flushTo} the tombstoned rows are issued as native
     * deletes alongside the buffered PUTs.
     *
     * <p>Cleanup-C1: tombstone tracking is delegated to the underlying buffer's per-row
     * bitmap. No HashSet, no BytesKey allocation.
     */
    public void remove(byte[] compositeKey, ForStRsLinker linker, FrsDb db, FrsCfHandle cf) {
        ensureClosed();
        ensureStagingCapacity(compositeKey.length);
        MemorySegment.copy(compositeKey, 0, staging, ValueLayout.JAVA_BYTE, 0, compositeKey.length);
        if (buf.needsFlush() || buf.shouldAutoFlush()) {
            flushTo(linker, db, cf);
        }
        buf.tombstone(staging, 0, compositeKey.length);
    }

    /**
     * Drains all buffered rows to the engine via {@code linker.batchPut} and issues a delete
     * for every tombstoned row. Clears the buffer on return.
     *
     * <p>Called by:
     *
     * <ul>
     *   <li>{@link #put} when {@code needsFlush()} or {@code shouldAutoFlush()} fires.
     *   <li>{@code ForStRsMapStateV2.flushOffHeapBuffer()} from the backend snapshot pre-hook
     *       (Trace E barrier drain).
     *   <li>{@code ForStRsMapStateV2.close()} for slot teardown.
     * </ul>
     */
    public void flushTo(ForStRsLinker linker, FrsDb db, FrsCfHandle cf) {
        if (closed) {
            return;
        }
        // Issue per-row deletes for tombstoned rows BEFORE flushTo clears the buffer. The
        // tombstoned rows' key bytes still live in the off-heap keyData region, but flushTo's
        // clear() wipes them, so we have to extract them first.
        int[] tombstones = buf.tombstonedRows();
        for (int row : tombstones) {
            byte[] keyBytes = buf.copyKey(row);
            linker.delete(db, cf, keyBytes);
        }
        if (buf.size() > 0) {
            buf.flushTo(linker, db, cf);
        }
    }

    /**
     * Returns the number of tombstoned rows awaiting flush. Visible for tests.
     *
     * <p>Cleanup-C1: implemented via the underlying buffer's tombstone-bit scan; no HashSet to
     * size.
     */
    public int tombstoneCount() {
        return buf.tombstonedRows().length;
    }

    /**
     * PR-A6 (S1-11 / E2-HIGH-2): invalidate buffered entries owned by the operator-key + namespace
     * prefix that is being cleared via {@code StateRequestType.CLEAR}. Because the underlying
     * {@link ArrowBinaryBuffer} does not expose a public prefix-scan, this drains the entire buffer
     * to the engine via {@link #flushTo} and then drops every buffered row plus all tombstones.
     *
     * <p>Ordering guarantee: the synchronous {@code linker.batchPut} inside {@code flushTo}
     * completes before this method returns, so a CLEAR request that the framework enqueues
     * immediately afterwards will be dispatched AFTER the flushed PUTs hit the engine. The engine
     * then performs the namespace-prefix delete and the next {@code asyncGet} reads the
     * post-clear state.
     *
     * <p>Over-aggressive note: this also flushes pending writes for OTHER (operatorKey, namespace)
     * pairs that happen to share this buffer instance. That is a perf cost on CLEAR, not a
     * correctness cost — the staged data is durably persisted, not lost. CLEAR is end-of-window
     * and rare relative to per-record PUT/GET, so the trade-off is acceptable for V1.
     */
    public void clearForPrefix(byte[] prefix, ForStRsLinker linker, FrsDb db, FrsCfHandle cf) {
        ensureClosed();
        // Drain any staged PUTs + pending tombstones to the engine first so we don't lose them.
        flushTo(linker, db, cf);
        // Then drop every buffered row (clear() also resets all tombstone bits).
        buf.clear();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        buf.close();
        stagingArena.close();
    }

    // -----------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------

    private void ensureClosed() {
        if (closed) {
            throw new IllegalStateException("MapStateArrowBuffer is closed");
        }
    }

    private void ensureStagingCapacity(int needed) {
        if (needed <= stagingCap) {
            return;
        }
        int newCap = Math.max(stagingCap * 2, needed);
        // Old arena keeps existing data alive until close (the staging segment is only used
        // synchronously inside a single put/remove call), so we can simply allocate fresh.
        staging = stagingArena.allocate(newCap);
        stagingCap = newCap;
    }

    /** Result of {@link #lookup}. {@code row} is meaningful only when {@code cached && !tombstone}. */
    public static final class Lookup {
        public static final Lookup MISS = new Lookup(false, false, -1);
        public static final Lookup TOMBSTONE = new Lookup(true, true, -1);

        public final boolean cached;
        public final boolean tombstone;
        public final int row;

        Lookup(boolean cached, boolean tombstone, int row) {
            this.cached = cached;
            this.tombstone = tombstone;
            this.row = row;
        }
    }
}

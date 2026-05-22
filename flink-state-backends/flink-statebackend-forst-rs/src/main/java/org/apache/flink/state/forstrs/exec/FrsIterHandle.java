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

package org.apache.flink.state.forstrs.exec;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsErrorCode;
import org.apache.flink.state.forstrs.ffm.FrsException;
import org.apache.flink.state.forstrs.ffm.FrsIteratorExpiredException;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java-side handle for a native vectorized prefix-scan iterator. Wraps a per-iterator Arena plus
 * the opaque u64 handle ID returned by {@code frs_vec_iter_prefix_open}. Implements {@link
 * AutoCloseable} so try-with-resources or {@link SlotArenaScope#exitTurn()} can release it.
 *
 * <p><b>Watchdog interaction.</b> {@link IterLifetimeWatchdog} sets {@link #closeRequested} on
 * idle/max-lifetime breach; the operator thread observes the flag at the next {@link #next} call
 * and performs the native close. This keeps all FFI calls on the operator thread.
 *
 * <p><b>Spec §1 §b.</b> Handles do NOT outlive a single async-v2 turn. {@link
 * SlotArenaScope#exitTurn()} force-closes any leaked handles.
 */
public final class FrsIterHandle implements AutoCloseable {

    /** Stable Java-side key used in the {@link SlotArenaScope} iter registry. */
    private final long handleId;

    /** Opaque native handle ID returned by {@code frs_vec_iter_prefix_open} (a Rust u64). */
    private final long nativeHandleId;

    private final ForStRsLinker linker;

    /**
     * Per-iterator Arena for scratch out-parameter segments (outRc, outBu). Closed on handle close
     * so the scratch memory is reclaimed promptly rather than waiting for the slot Arena.
     *
     * <p>When {@link #ownsArena} is {@code false} this handle was constructed from a batched-open
     * (PR-E3) that supplies a shared, long-lived executor-level Arena — in that case {@link #close()}
     * MUST NOT close the Arena (it is owned by the executor, not by this handle).
     */
    private final Arena perIterArena;

    /**
     * {@code true} if this handle owns {@link #perIterArena} and is responsible for closing it on
     * {@link #close()}. {@code false} when the Arena is borrowed from a long-lived owner (executor
     * level arena, PR-E3 batched-open path).
     */
    private final boolean ownsArena;

    private final SlotArenaScope slotScope;

    /** Nanosecond timestamp of the last successful {@link #next} call (updated atomically). */
    private final AtomicLong lastNextNs;

    /** Wall-clock millisecond timestamp of when this handle was opened. */
    private final long openedAtMs;

    /**
     * Set by {@link IterLifetimeWatchdog} when the handle is idle or exceeds max lifetime. Observed
     * by {@link #next} on the operator thread.
     */
    private final AtomicBoolean closeRequested = new AtomicBoolean(false);

    /** Set atomically on first close; guards against double-close. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public FrsIterHandle(
            long handleId,
            long nativeHandleId,
            ForStRsLinker linker,
            Arena perIterArena,
            SlotArenaScope slotScope) {
        this(handleId, nativeHandleId, linker, perIterArena, slotScope, /* ownsArena= */ true);
    }

    /**
     * PR-E3 constructor: explicit ownership flag. Used by the batched-open path in
     * {@link org.apache.flink.state.forstrs.VectorizedExecutor#dispatchIterPrefix} so the
     * executor's long-lived Arena can be shared across all handles in the batch without per-row
     * {@code Arena.ofShared()} allocations.
     */
    public FrsIterHandle(
            long handleId,
            long nativeHandleId,
            ForStRsLinker linker,
            Arena perIterArena,
            SlotArenaScope slotScope,
            boolean ownsArena) {
        this.handleId = handleId;
        this.nativeHandleId = nativeHandleId;
        this.linker = linker;
        this.perIterArena = perIterArena;
        this.ownsArena = ownsArena;
        this.slotScope = slotScope;
        this.lastNextNs = new AtomicLong(System.nanoTime());
        this.openedAtMs = System.currentTimeMillis();
    }

    /** Returns the stable Java-side handle ID (used as registry key). */
    public long handleId() {
        return handleId;
    }

    /** Returns the native (Rust u64) handle ID passed to {@code frs_vec_iter_prefix_next}. */
    public long nativeHandleId() {
        return nativeHandleId;
    }

    /** Returns the nanosecond timestamp of the last {@link #next} call. */
    public long lastNextNs() {
        return lastNextNs.get();
    }

    /** Returns the wall-clock millisecond timestamp when this handle was opened. */
    public long openedAtMs() {
        return openedAtMs;
    }

    /** Returns {@code true} if the watchdog has requested a close. */
    public boolean closeRequested() {
        return closeRequested.get();
    }

    /**
     * Pulls the next chunk of rows into a caller-owned buffer.
     *
     * <p>If the watchdog has set {@link #closeRequested}, this method closes the handle and throws
     * {@link FrsIteratorExpiredException} — so the operator thread performs the native close rather
     * than the watchdog thread (keeps all FFI calls single-threaded per slot).
     *
     * @param chunkBuf caller-owned native buffer to receive row data; its {@link
     *     MemorySegment#byteSize()} is used as the capacity passed to the native call
     * @return number of rows written into {@code chunkBuf} (0 means iterator exhausted)
     * @throws FrsIteratorExpiredException if the iterator has expired or was aborted by watchdog
     * @throws FrsException on other native errors
     * @throws IllegalStateException if already closed
     */
    public int next(MemorySegment chunkBuf) {
        if (closed.get()) {
            throw new FrsIteratorExpiredException(0);
        }
        if (closeRequested.get()) {
            // Watchdog asked for close — perform it now on the operator thread.
            close();
            throw new FrsIteratorExpiredException(0);
        }
        // Scratch out-params reuse the per-iterator Arena (cheap bump allocation).
        MemorySegment outRc = perIterArena.allocate(ValueLayout.JAVA_INT);
        MemorySegment outBu = perIterArena.allocate(ValueLayout.JAVA_INT);
        int rc =
                linker.frsVecIterPrefixNext(
                        nativeHandleId, chunkBuf, (int) chunkBuf.byteSize(), outRc, outBu);
        lastNextNs.set(System.nanoTime());
        if (rc != FrsErrorCode.OK.code()) {
            FrsErrorCode code = FrsErrorCode.fromU32(rc);
            if (code == FrsErrorCode.ITER_EXPIRED || code == FrsErrorCode.ITER_CURSOR_INVALID) {
                close();
                throw new FrsIteratorExpiredException(0);
            }
            throw new FrsException(code, 0, new byte[0]);
        }
        return outRc.get(ValueLayout.JAVA_INT, 0);
    }

    /**
     * Requests that this handle be closed. Called by {@link IterLifetimeWatchdog} only — the actual
     * native close is deferred to the next {@link #next} call on the operator thread.
     */
    public void requestClose() {
        closeRequested.set(true);
    }

    /**
     * Closes this handle, releasing the native iterator and the per-iterator Arena. Idempotent —
     * subsequent calls are no-ops.
     *
     * <p>Unregisters from the {@link SlotArenaScope} so the slot does not attempt a double-close at
     * turn boundary.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return; // idempotent
        }
        try {
            linker.frsVecIterPrefixClose(nativeHandleId);
        } finally {
            if (ownsArena) {
                try {
                    perIterArena.close();
                } catch (Throwable ignored) {
                    // Best-effort; perIterArena is a confined Arena so close should
                    // always succeed on the owning thread. Swallow to protect callers.
                }
            }
            // ownsArena == false: borrowed long-lived Arena (PR-E3); do not close —
            // the executor closes it at its own lifecycle boundary.
            slotScope.unregisterIter(handleId);
        }
    }

    /**
     * Force-closes this handle, bypassing the request-then-observe watchdog protocol. Used on slot
     * teardown ({@link SlotArenaScope#closeSlot()}) and turn-boundary leak recovery.
     *
     * <p>Best-effort: calls {@code frs_vec_iter_prefix_abort} first to unblock any in-progress
     * native reads, then delegates to {@link #close()}.
     */
    public void forceClose() {
        try {
            linker.frsVecIterPrefixAbort(nativeHandleId);
        } catch (Throwable ignored) {
            // Best-effort abort; swallow so we always reach close().
        }
        close();
    }
}

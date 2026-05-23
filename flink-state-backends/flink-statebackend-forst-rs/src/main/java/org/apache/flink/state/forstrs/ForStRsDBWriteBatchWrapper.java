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
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;

/**
 * Atomic write-batch wrapper — mirrors the surface of forst's {@code ForStDBWriteBatchWrapper} on
 * top of the vectorized FFI {@code frs_writebatch_*} symbols (see {@code
 * crates/forst-rs-ffi/src/lib.rs}).
 *
 * <p>Two staging buffers (puts and deletes) accumulate keys + values until {@link #flush} or {@link
 * #commit} is called. {@link #flushIfFull} auto-flushes when either staging buffer hits {@link
 * #thresholdEntries} entries or {@link #thresholdBytes} of accumulated bytes.
 *
 * <p>{@code flush} dispatches the staged entries into the engine WriteBatch handle. {@code commit}
 * flushes once more then commits the engine WriteBatch atomically; the handle is invalid after
 * commit. {@code close} drops the handle and any staged entries that were not flushed/committed.
 *
 * <p>Thread-safety: not thread-safe. Mirrors forst's contract.
 */
@Internal
public final class ForStRsDBWriteBatchWrapper implements AutoCloseable {

    private static final int DEFAULT_THRESHOLD_ENTRIES = 1024;
    private static final int DEFAULT_THRESHOLD_BYTES = 4 * 1024 * 1024; // 4 MiB

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final Arena arena;
    private final FrsCfHandle defaultCf;

    private final int thresholdEntries;
    private final int thresholdBytes;

    private final ColumnarBatchBuffer putKeys;
    private final ColumnarBatchBuffer putValues;
    private final ColumnarBatchBuffer deleteKeys;

    /** 0 = no handle (closed / committed). */
    private long handle;

    /** {@code true} once {@link #commit} or {@link #close} has been called. */
    private boolean disposed;

    public ForStRsDBWriteBatchWrapper(
            ForStRsLinker linker, FrsDb db, Arena arena, FrsCfHandle defaultCf) {
        this(linker, db, arena, defaultCf, DEFAULT_THRESHOLD_ENTRIES, DEFAULT_THRESHOLD_BYTES);
    }

    public ForStRsDBWriteBatchWrapper(
            ForStRsLinker linker,
            FrsDb db,
            Arena arena,
            FrsCfHandle defaultCf,
            int thresholdEntries,
            int thresholdBytes) {
        this.linker = linker;
        this.db = db;
        this.arena = arena;
        this.defaultCf = defaultCf;
        this.thresholdEntries = thresholdEntries;
        this.thresholdBytes = thresholdBytes;
        this.putKeys = new ColumnarBatchBuffer(arena);
        this.putValues = new ColumnarBatchBuffer(arena);
        this.deleteKeys = new ColumnarBatchBuffer(arena);
        this.handle = linker.writebatchOpen(arena);
    }

    /** Stages a put against the default CF. */
    public void put(byte[] key, byte[] value) {
        put(defaultCf, key, value);
    }

    public void put(FrsCfHandle cf, byte[] key, byte[] value) {
        ensureOpen();
        // For multi-CF support the staging buffers would need per-CF separation;
        // current implementation flushes the existing CF's batch before switching.
        if (cf != defaultCf && (putKeys.count() > 0 || deleteKeys.count() > 0)) {
            flush(defaultCf);
        }
        putKeys.append(key);
        putValues.append(value == null ? new byte[0] : value);
        flushIfFull(cf);
    }

    /** Stages a delete against the default CF. */
    public void delete(byte[] key) {
        delete(defaultCf, key);
    }

    public void delete(FrsCfHandle cf, byte[] key) {
        ensureOpen();
        if (cf != defaultCf && (putKeys.count() > 0 || deleteKeys.count() > 0)) {
            flush(defaultCf);
        }
        deleteKeys.append(key);
        flushIfFull(cf);
    }

    /**
     * Returns {@code true} if the staging buffers are empty. Useful for callers that want to skip a
     * commit roundtrip when nothing was staged.
     */
    public boolean isEmpty() {
        return putKeys.count() == 0 && deleteKeys.count() == 0;
    }

    private void flushIfFull(FrsCfHandle cf) {
        if (putKeys.count() >= thresholdEntries
                || deleteKeys.count() >= thresholdEntries
                || putKeys.dataPos() + putValues.dataPos() + deleteKeys.dataPos()
                        >= thresholdBytes) {
            flush(cf);
        }
    }

    /** Dispatches all currently-staged entries to the engine WriteBatch handle. */
    public void flush() {
        flush(defaultCf);
    }

    public void flush(FrsCfHandle cf) {
        ensureOpen();
        if (putKeys.count() > 0) {
            linker.writebatchPut(
                    handle,
                    cf,
                    putKeys.offsetsSegment(),
                    putKeys.dataSegment(),
                    putValues.offsetsSegment(),
                    putValues.dataSegment(),
                    putKeys.count());
            putKeys.reset();
            putValues.reset();
        }
        if (deleteKeys.count() > 0) {
            linker.writebatchDelete(
                    handle,
                    cf,
                    deleteKeys.offsetsSegment(),
                    deleteKeys.dataSegment(),
                    deleteKeys.count());
            deleteKeys.reset();
        }
    }

    /**
     * Atomically commits all staged + pending entries. After this call the wrapper is closed; any
     * further put/delete/flush throws.
     *
     * <p>R25-M1: pre-fix the {@code handle} field was cleared and {@code disposed=true} was set
     * BEFORE the {@code writebatchCommit} FFI call. If the commit threw, the subsequent {@code
     * close()} short-circuited on {@code disposed=true} and the native WriteBatch was never
     * released → memory leak on the engine side. The fix below clears Java-side state only
     * AFTER the native call returns (success path) or after an explicit {@code writebatchClose}
     * runs in the failure-cleanup branch. The contract assumed for {@code frs_writebatch_commit}:
     * commit does NOT release the handle (the Rust-side stub at {@code
     * crates/forst-rs-ffi/src/lib.rs:2701} is currently NOT_SUPPORTED with no lifecycle effect;
     * the canonical release entry point is {@code frs_writebatch_close}). So on commit
     * SUCCESS we still need to close; on FAILURE we also need to close. We perform the close
     * unconditionally in the finally block when the handle is still non-zero.
     */
    public void commit() {
        ensureOpen();
        flush();
        long h = handle;
        Throwable thrown = null;
        try {
            linker.writebatchCommit(h, db);
        } catch (Throwable t) {
            thrown = t;
            throw t;
        } finally {
            // Always release the native batch — the engine's commit does NOT free it (see
            // contract note above). Order: native release first, then flip Java-side flags so
            // a re-entrant close() through this same wrapper is a structural no-op.
            try {
                linker.writebatchClose(h);
            } catch (Throwable closeFailure) {
                if (thrown != null) {
                    thrown.addSuppressed(closeFailure);
                }
                // If commit succeeded but close failed, surface the close failure so callers
                // are not silently leaked. We deliberately do not swallow.
                if (thrown == null) {
                    handle = 0L;
                    disposed = true;
                    throw closeFailure instanceof RuntimeException
                            ? (RuntimeException) closeFailure
                            : new RuntimeException(closeFailure);
                }
            }
            handle = 0L;
            disposed = true;
        }
    }

    /** Drops all staged + pending entries without committing. Idempotent. */
    @Override
    public void close() {
        if (disposed) {
            return;
        }
        long h = handle;
        handle = 0L;
        disposed = true;
        // Staged Java-side entries are dropped automatically via the Arena.
        // The native WriteBatch must be explicitly closed.
        if (h != 0L) {
            linker.writebatchClose(h);
        }
    }

    private void ensureOpen() {
        if (disposed) {
            throw new IllegalStateException("ForStRsDBWriteBatchWrapper is closed");
        }
    }
}

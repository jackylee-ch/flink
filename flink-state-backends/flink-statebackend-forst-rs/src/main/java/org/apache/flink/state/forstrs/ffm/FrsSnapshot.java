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

package org.apache.flink.state.forstrs.ffm;

import java.lang.foreign.MemorySegment;

/**
 * Java handle wrapping a native {@code FrsSnapshot} pointer obtained from {@code frs_db_snapshot}.
 * Implements {@link AutoCloseable} so callers can use try-with-resources to release via {@code
 * frs_db_release_snapshot}.
 *
 * <p>Per spec §10.0 ABI lifetime contract:
 *
 * <ul>
 *   <li>The handle is bound to the issuing {@link FrsDb}; passing it to a different DB returns
 *       {@code FRS_STATUS_INVALID_ARGUMENT} on the Rust side and the box is intentionally re-leaked
 *       to prevent double-free.
 *   <li>{@link #close()} is idempotent — a second call is a no-op.
 *   <li>{@link #handle()} throws {@link IllegalStateException} after close so accidental
 *       use-after-close fails fast on the Java side rather than corrupting the native registry.
 * </ul>
 *
 * <p>Holding the snapshot pinned prevents compaction from reclaiming any version with {@code seq
 * &lt;= snapshot.seq}, so callers should release as soon as the snapshot-aware reads (e.g. {@code
 * frs_get_at}, {@code frs_iterator_open_at}, {@code frs_create_incremental_checkpoint_at}) finish.
 */
public final class FrsSnapshot implements AutoCloseable {

    private final ForStRsLinker linker;
    private final FrsDb db;
    private MemorySegment handle;

    FrsSnapshot(ForStRsLinker linker, FrsDb db, MemorySegment handle) {
        this.linker = linker;
        this.db = db;
        this.handle = handle;
    }

    /**
     * Returns the raw native handle. Throws {@link IllegalStateException} if the snapshot has
     * already been closed (use-after-close on the Java side, before any native call).
     */
    public MemorySegment handle() {
        if (handle == null) {
            throw new IllegalStateException("FrsSnapshot already closed");
        }
        return handle;
    }

    /** Returns {@code true} once {@link #close()} has been invoked at least once. */
    public boolean isClosed() {
        return handle == null;
    }

    /**
     * Releases the snapshot back to the engine. Idempotent: calling more than once is a no-op (does
     * not double-release). Per spec §10.0 the underlying release call cannot fail with
     * {@code INVALID_ARGUMENT} as long as the snapshot was originally obtained from {@code db}.
     */
    @Override
    public void close() {
        if (handle != null) {
            linker.dbReleaseSnapshot(db, this);
            handle = null;
        }
    }
}

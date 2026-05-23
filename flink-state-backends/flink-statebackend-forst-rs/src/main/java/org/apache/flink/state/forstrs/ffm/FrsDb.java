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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Opaque ForSt-RS database handle. Wraps a {@code FrsDb} pointer (raw {@code *mut c_void} from the
 * C ABI). Use try-with-resources to ensure {@code frs_db_close} runs.
 */
public final class FrsDb implements AutoCloseable {

    private final ForStRsLinker linker;
    private volatile MemorySegment handle;
    // R22-L1: AtomicBoolean + CAS protects against concurrent close() races. Pre-fix, two threads
    // observing closed=false could both call linker.dbClose(handle) and double-free the native
    // handle. The dispose()/close() lifecycle ordering in the backend normally serializes these,
    // but defense-in-depth: any future caller racing through close() is now guaranteed exactly-one
    // dbClose by the CAS — the loser observes closed=true and skips the native call.
    private final AtomicBoolean closed = new AtomicBoolean(false);

    FrsDb(ForStRsLinker linker, MemorySegment handle) {
        this.linker = linker;
        this.handle = handle;
    }

    public MemorySegment handle() {
        if (closed.get()) {
            throw new IllegalStateException("FrsDb already closed");
        }
        return handle;
    }

    /** R22-H1 regression-test hook: query the closed flag without throwing. */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Returns the {@link ForStRsLinker} that produced this handle. Exposed so higher-level helpers
     * (e.g. {@code ForStRsStateMigration}) can route additional FFM calls through the same linker
     * without requiring a separate constructor parameter. Package callers should prefer the
     * existing convenience methods on {@link ForStRsLinker} where they exist.
     */
    public ForStRsLinker linker() {
        return linker;
    }

    @Override
    public void close() {
        // R22-L1: compareAndSet — only the thread that flips false→true performs the native
        // dbClose call; concurrent callers observe the post-set value and return without
        // touching the native handle. Skips the native call entirely on the loser path.
        if (closed.compareAndSet(false, true)) {
            linker.dbClose(handle);
            handle = MemorySegment.NULL;
        }
    }
}

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

/** Opaque column-family handle. */
public final class FrsCfHandle implements AutoCloseable {

    private final ForStRsLinker linker;
    private volatile MemorySegment handle;
    // R22-L1: AtomicBoolean + CAS — see FrsDb for full rationale. Same double-free guard for the
    // column-family handle.
    private final AtomicBoolean closed = new AtomicBoolean(false);

    FrsCfHandle(ForStRsLinker linker, MemorySegment handle) {
        this.linker = linker;
        this.handle = handle;
    }

    public MemorySegment handle() {
        if (closed.get()) {
            throw new IllegalStateException("FrsCfHandle already closed");
        }
        return handle;
    }

    /** R22-H1 regression-test hook: query the closed flag without throwing. */
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        // R22-L1: compareAndSet ensures exactly-one cfClose under concurrent callers.
        if (closed.compareAndSet(false, true)) {
            linker.cfClose(handle);
            handle = MemorySegment.NULL;
        }
    }
}

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
 * Opaque ForSt-RS database handle. Wraps a {@code FrsDb} pointer
 * (raw {@code *mut c_void} from the C ABI). Use try-with-resources
 * to ensure {@code frs_db_close} runs.
 */
public final class FrsDb implements AutoCloseable {

    private final ForStRsLinker linker;
    private MemorySegment handle;
    private boolean closed = false;

    FrsDb(ForStRsLinker linker, MemorySegment handle) {
        this.linker = linker;
        this.handle = handle;
    }

    public MemorySegment handle() {
        if (closed) {
            throw new IllegalStateException("FrsDb already closed");
        }
        return handle;
    }

    @Override
    public void close() {
        if (!closed) {
            linker.dbClose(handle);
            closed = true;
            handle = MemorySegment.NULL;
        }
    }
}

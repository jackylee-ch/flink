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
 * Opaque ForSt-RS iterator handle. Wraps a {@code FrsIterator} pointer (raw {@code *mut c_void}
 * from the C ABI). Use try-with-resources to ensure the matching {@code frs_iterator_close} or
 * {@code frs_prefix_lookup_close} runs.
 *
 * <p>{@code prefix} flag determines which close symbol is invoked at the close site. Both ABIs
 * alias the same impl in Rust, but exposing both keeps the Java side honest about the semantics
 * requested by the caller.
 */
public final class FrsIterator implements AutoCloseable {

    private final ForStRsLinker linker;
    private MemorySegment handle;
    private final boolean prefix;
    private boolean closed = false;

    FrsIterator(ForStRsLinker linker, MemorySegment handle, boolean prefix) {
        this.linker = linker;
        this.handle = handle;
        this.prefix = prefix;
    }

    public MemorySegment handle() {
        if (closed) {
            throw new IllegalStateException("FrsIterator already closed");
        }
        return handle;
    }

    @Override
    public void close() {
        if (!closed) {
            linker.iteratorClose(handle, prefix);
            closed = true;
            handle = MemorySegment.NULL;
        }
    }
}

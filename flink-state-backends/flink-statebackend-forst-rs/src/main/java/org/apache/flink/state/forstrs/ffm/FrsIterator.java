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

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Opaque ForSt-RS iterator handle. Wraps a {@code FrsIterator} pointer (raw {@code *mut c_void}
 * from the C ABI). Use try-with-resources to ensure the matching {@code frs_iterator_close} or
 * {@code frs_prefix_lookup_close} runs.
 *
 * <p>{@code prefix} flag determines which close symbol is invoked at the close site. Both ABIs
 * alias the same impl in Rust, but exposing both keeps the Java side honest about the semantics
 * requested by the caller.
 *
 * <p>R32-H3: AtomicBoolean+CAS double-close guard — mirrors {@link FrsSnapshot} and {@link
 * FrsCfHandle}. Without the CAS, two threads racing on {@code close()} could both observe {@code
 * closed=false} and both invoke {@code linker.iteratorClose}, which the Rust side treats as a
 * double-free of the native iterator (the freed box is reused, the second {@code Box::from_raw}
 * is UB). The CAS guarantees exactly-one native close regardless of interleaving. The {@code
 * handle} field is {@code volatile} so the post-close transition to {@link MemorySegment#NULL}
 * is safely published to other threads observing {@link #handle()}.
 */
public final class FrsIterator implements AutoCloseable {

    private final ForStRsLinker linker;
    // R32-H3: volatile for safe publication of the handle reference between close()
    // (which CAS-wins and stores NULL) and other threads inspecting handle() before
    // their own close() call observes the closed flag.
    private volatile MemorySegment handle;
    private final boolean prefix;
    // R32-H3: AtomicBoolean + CAS — mirrors FrsSnapshot/FrsCfHandle. The previous
    // `if (!closed) { iteratorClose(handle, prefix); closed = true; }` was a classic
    // check-then-act race that admitted double-free under concurrent close().
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // D-R3-H4: per-iterator scratch arena hosting the 3 small out-slots
    // (outKey FrsBytes, outValue FrsBytes, outValid bool) used by
    // {@link ForStRsLinker#iteratorNext(FrsIterator)}. Pre-fix each
    // iteratorNext call opened a fresh Arena.ofConfined() and allocated
    // these 3 slots — for a 1000-row scan that's 1000 arena lifecycles
    // + 3000 native allocs. With the arena hoisted to the iterator's
    // lifetime, those allocations happen ONCE at construction.
    // Confined-arena lifecycle bounded by FrsIterator close().
    // D-R3-H4-NEW-H1: must mirror ForStRsLinker.FRS_BYTES_LAYOUT exactly —
    // FrsBytes is {data: *mut u8, len: usize, capacity: usize} = 24 bytes
    // on 64-bit. Pre-fix this used a 16-byte struct (ADDRESS + JAVA_LONG)
    // and Rust's `frs_iterator_next` wrote the third capacity field 8
    // bytes past the segment, corrupting the adjacent scratch slot and
    // making `frs_bytes_free` read a bogus capacity → heap corruption.
    private static final MemoryLayout FRS_BYTES_LAYOUT =
            MemoryLayout.structLayout(
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG);
    private final Arena scratchArena;
    final MemorySegment scratchKey;
    final MemorySegment scratchValue;
    final MemorySegment scratchValid;

    FrsIterator(ForStRsLinker linker, MemorySegment handle, boolean prefix) {
        this.linker = linker;
        this.handle = handle;
        this.prefix = prefix;
        // Allocate scratch in a confined arena owned by this iterator.
        // Closed by close() below — bounded lifecycle.
        this.scratchArena = Arena.ofConfined();
        this.scratchKey = scratchArena.allocate(FRS_BYTES_LAYOUT);
        this.scratchValue = scratchArena.allocate(FRS_BYTES_LAYOUT);
        this.scratchValid = scratchArena.allocate(ValueLayout.JAVA_BOOLEAN);
    }

    public MemorySegment handle() {
        if (closed.get()) {
            throw new IllegalStateException("FrsIterator already closed");
        }
        return handle;
    }

    /** R32-H3 regression-test hook: query the closed flag without throwing. */
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        // R32-H3: compareAndSet ensures exactly-one iteratorClose under concurrent
        // callers. The CAS-losing thread observes closed=true and falls through.
        if (closed.compareAndSet(false, true)) {
            MemorySegment h = handle;
            handle = MemorySegment.NULL;
            try {
                linker.iteratorClose(h, prefix);
            } finally {
                // D-R3-H4: release the scratch arena owning the per-iterator
                // out-slots. Best-effort — engine close already happened.
                try {
                    scratchArena.close();
                } catch (Throwable ignore) {
                    // already-closed or thread-affinity issues — non-fatal.
                }
            }
        }
    }
}

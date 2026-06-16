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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide accounting of the FFM (java.lang.foreign) off-heap working set that the forst-rs
 * state backend allocates from its long-lived per-worker {@link java.lang.foreign.Arena}s.
 *
 * <p><b>Why this exists.</b> {@code taskmanager.memory.process.size} budgets the JVM heap + Flink
 * managed memory but NOT the native off-heap that the FFM API allocates (anonymous, non-reclaimable
 * by the page cache). On the big NEXMark join builds (q9/q19/q5) the per-worker reusable staging
 * buffers — {@link ColumnarBatchBuffer} (getKeys/putKeys/putValues/deleteKeys) and the {@code
 * VectorizedExecutor} iter/GET scratch — grow to the largest batch ever observed and, because the
 * worker arena is {@link java.lang.foreign.Arena#ofShared()} (which never frees an individual
 * allocation), every doubling-growth leaks its predecessor segment for the slot's whole life.
 * That uncounted, unbounded FFM off-heap is the term that pushes per-TM RSS over the 16 GiB cgroup.
 *
 * <p>This counter (a) lets us PROFILE how many GiB of FFM off-heap are live at the join-build peak
 * and WHICH category dominates, and (b) gives the bounded-buffer fix a place to record that it now
 * frees-on-grow / shrinks-on-reset so the live total stops climbing. Enable the periodic dump with
 * {@code -Dforst.rs.ffmdiag=1} or env {@code FRS_FFM_DIAG=1}.
 */
@Internal
public final class FfmOffHeapAccounting {

    private FfmOffHeapAccounting() {}

    /** Live off-heap bytes held by {@link ColumnarBatchBuffer} instances (classifier staging). */
    public static final AtomicLong COLUMNAR_BYTES = new AtomicLong();

    /** Live off-heap bytes held by the {@code VectorizedExecutor} GET output buffers. */
    public static final AtomicLong GET_OUT_BYTES = new AtomicLong();

    /** Live off-heap bytes held by the {@code VectorizedExecutor} ITER-prefix/range scratch. */
    public static final AtomicLong ITER_SCRATCH_BYTES = new AtomicLong();

    /** Count of grow events that freed a predecessor segment (proof the bound is working). */
    public static final AtomicLong FREED_ON_GROW = new AtomicLong();

    /** Count of reset-driven shrink events. */
    public static final AtomicLong SHRINK_ON_RESET = new AtomicLong();

    public static final boolean DIAG =
            "1".equals(System.getProperty("forst.rs.ffmdiag"))
                    || "1".equals(System.getenv("FRS_FFM_DIAG"))
                    || "true".equalsIgnoreCase(String.valueOf(System.getenv("FRS_FFM_DIAG")));

    private static final AtomicLong NEXT_DUMP_MS = new AtomicLong(0L);

    /** Sum of all tracked categories (live FFM off-heap working set), in bytes. */
    public static long liveTotal() {
        return COLUMNAR_BYTES.get() + GET_OUT_BYTES.get() + ITER_SCRATCH_BYTES.get();
    }

    /**
     * Periodic stderr dump (throttled to ~once / 5 s) of the live FFM off-heap breakdown. Called
     * from the buffer grow paths; cheap no-op when {@link #DIAG} is off.
     */
    public static void maybeDump() {
        if (!DIAG) {
            return;
        }
        long now = System.currentTimeMillis();
        long next = NEXT_DUMP_MS.get();
        if (now < next || !NEXT_DUMP_MS.compareAndSet(next, now + 5000L)) {
            return;
        }
        long col = COLUMNAR_BYTES.get();
        long get = GET_OUT_BYTES.get();
        long iter = ITER_SCRATCH_BYTES.get();
        System.err.println(
                "[FRS_FFM_DIAG] liveMiB="
                        + ((col + get + iter) >> 20)
                        + " columnarMiB="
                        + (col >> 20)
                        + " getOutMiB="
                        + (get >> 20)
                        + " iterScratchMiB="
                        + (iter >> 20)
                        + " freedOnGrow="
                        + FREED_ON_GROW.get()
                        + " shrinkOnReset="
                        + SHRINK_ON_RESET.get());
    }
}

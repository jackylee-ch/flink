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
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * STAGE-0 diagnostic (FRS_REENTRY_DIAG=3): per-{@link StateRequestType} offered vs completed vs
 * failed tallies across the whole TM, dumped periodically and on JVM shutdown. Purpose: split the
 * routing-async q8 under-emit into TRIGGER STARVATION (offered plateaus, done==offered) vs LOST
 * COMPLETIONS (done+fail &lt; offered, AEC stalls on the gap). Counters are process-global like
 * the sibling STREAM_STATS; the benchmark runs one job per TM so attribution is unambiguous.
 */
@Internal
public final class DiagCompletionCounters {

    public static final boolean ENABLED = "3".equals(System.getenv("FRS_REENTRY_DIAG"));

    private static final int N = StateRequestType.values().length;
    private static final AtomicLongArray OFFERED = new AtomicLongArray(N);
    private static final AtomicLongArray COMPLETED = new AtomicLongArray(N);
    private static final AtomicLongArray FAILED = new AtomicLongArray(N);

    static {
        if (ENABLED) {
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> System.err.println("[DIAG_COMPLETION final] " + report()),
                                    "frs-diag-completion-dump"));
        }
    }

    private DiagCompletionCounters() {}

    public static void offered(StateRequestType t) {
        long n = OFFERED.incrementAndGet(t.ordinal());
        // Periodic dump every 2^20 offers of any single type — cheap, mirrors STREAM_STATS.
        if ((n & ((1L << 20) - 1)) == 0) {
            System.err.println("[DIAG_COMPLETION] " + report());
        }
    }

    public static void completed(StateRequestType t) {
        COMPLETED.incrementAndGet(t.ordinal());
    }

    public static void failed(StateRequestType t) {
        FAILED.incrementAndGet(t.ordinal());
    }

    /** One entry per seen type: {@code [!]TYPE off=N done=N fail=N}; '!' marks done+fail < off. */
    public static String report() {
        StringBuilder sb = new StringBuilder();
        StateRequestType[] vals = StateRequestType.values();
        for (int i = 0; i < vals.length; i++) {
            long off = OFFERED.get(i);
            long done = COMPLETED.get(i);
            long fail = FAILED.get(i);
            if (off == 0 && done == 0 && fail == 0) {
                continue;
            }
            sb.append(done + fail < off ? '!' : ' ')
                    .append(vals[i])
                    .append(" off=").append(off)
                    .append(" done=").append(done)
                    .append(" fail=").append(fail);
        }
        for (java.util.Map.Entry<String, java.util.concurrent.atomic.AtomicLong> e : NAMED.entrySet()) {
            sb.append(' ').append(e.getKey()).append('=').append(e.getValue().get());
        }
        return sb.toString();
    }

    /** Named auxiliary counters (e.g. operator-side asyncAdd call counts). */
    private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicLong>
            NAMED = new java.util.concurrent.ConcurrentHashMap<>();

    /** Counts an auxiliary named event; appears in {@link #report()} as {@code name=N}. */
    public static void named(String name) {
        NAMED.computeIfAbsent(name, k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
    }

    /**
     * Stage-3 Unit-2: adds {@code delta} to an auxiliary named counter (e.g. {@code
     * mixedBatchOps} aggregates per-batch row counts; {@code named} alone would only count
     * batches). Appears in {@link #report()} as {@code name=N}.
     */
    public static void namedAdd(String name, long delta) {
        NAMED.computeIfAbsent(name, k -> new java.util.concurrent.atomic.AtomicLong()).addAndGet(delta);
    }

    /** Reads a named counter (0 when never incremented). Test/diagnostic accessor. */
    public static long namedValue(String name) {
        java.util.concurrent.atomic.AtomicLong v = NAMED.get(name);
        return v == null ? 0L : v.get();
    }

    /** Test hook. */
    static void resetForTests() {
        for (int i = 0; i < N; i++) {
            OFFERED.set(i, 0);
            COMPLETED.set(i, 0);
            FAILED.set(i, 0);
        }
    }
}

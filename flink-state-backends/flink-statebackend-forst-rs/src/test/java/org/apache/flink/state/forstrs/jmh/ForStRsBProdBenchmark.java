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

package org.apache.flink.state.forstrs.jmh;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsSnapshot;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * B-Prod-P5 acceptance bench (Tasks 5.1 + 5.3) for spec §16:
 *
 * <ul>
 *   <li>{@code dbSnapshot()} P99 &lt; 100µs under 100 concurrent in-flight snapshots.
 *   <li>Sync-phase (snapshot + {@code createIncrementalCheckpointAt}) P95 &lt; 1ms under the same
 *       100-snapshot in-flight pressure.
 *   <li>Single-CF vs per-state-CF (CfMode comparison) point-lookup + write throughput at {@code
 *       state.size = 1 GiB} (or whatever {@code -Dbench.preload.entries} caps the test at).
 * </ul>
 *
 * <h2>Why a {@code @Test} and not a JMH {@code @Benchmark}</h2>
 *
 * <p>The flink-statebackend-forst-rs module has JMH on the test classpath (jmh-core 1.37) but does
 * not wire the JMH maven plugin. Existing peer benches ({@link ForStRsFfmBenchmark}, {@link
 * ForStCompareBenchmark}) follow the "plain {@code main()} driven by surefire" pattern. We follow
 * the JUnit variant of that pattern so the bench can be invoked as {@code
 * -Dtest=ForStRsBProdBenchmark} and inherits the surefire JVM args (the {@code
 * --enable-native-access=ALL-UNNAMED} the FFM API requires).
 *
 * <p>Each {@code @Test} method runs {@code OPS} timed iterations (default 50_000), records every
 * sample's nanosecond latency into a {@code long[]}, sorts, and prints P50 / P95 / P99 / max plus
 * the spec acceptance pass/fail line. Tests are tagged {@code @Tag("bench")} so a normal {@code mvn
 * test} run can exclude them via {@code -DexcludedGroups=bench}; the recommended invocation is the
 * explicit {@code -Dtest=} filter.
 *
 * <h2>"100 concurrent in-flight snapshots"</h2>
 *
 * <p>Spec §16 demands measurement under realistic compaction back-pressure: at any instant ~100
 * snapshots are pinned, so the engine cannot reclaim the seq versions any of them cover. We
 * implement this with a {@link Deque} of pre-captured {@link FrsSnapshot} handles, sized to 100
 * (configurable via {@code -Dbench.inflight.snapshots}). Each measured iteration:
 *
 * <ol>
 *   <li>Captures one new snapshot (the one we're measuring).
 *   <li>Releases the oldest snapshot from the deque (so the in-flight count stays at the cap).
 * </ol>
 *
 * <p>This keeps the registry hot — every {@code dbSnapshot} call sees the registry already holding
 * 100 active entries, so the recorded latency reflects the real production pressure pattern.
 *
 * <h2>Bench sizing knobs</h2>
 *
 * <table>
 *   <caption>System-property overrides</caption>
 *   <tr><th>Property</th><th>Default</th><th>Meaning</th></tr>
 *   <tr><td>{@code bench.preload.entries}</td><td>100_000</td>
 *       <td>How many key/value pairs to preload before measurement. {@code 100_000 × 1 KiB ≈ 100
 *       MiB} state. {@code 1_000_000 × 1 KiB ≈ 1 GiB}.</td></tr>
 *   <tr><td>{@code bench.preload.value.bytes}</td><td>1024</td>
 *       <td>Value size. Combined with entries gives the working-set size.</td></tr>
 *   <tr><td>{@code bench.inflight.snapshots}</td><td>100</td>
 *       <td>Number of pre-captured snapshots held pinned during measurement.</td></tr>
 *   <tr><td>{@code bench.measure.ops}</td><td>50_000</td>
 *       <td>Number of measured iterations per workload. Lower for huge state (1 GiB takes ~ten
 *       seconds at 5 µs/op, plus preload setup).</td></tr>
 *   <tr><td>{@code bench.warmup.ops}</td><td>5_000</td>
 *       <td>Iterations used to warm the JIT before measurement.</td></tr>
 *   <tr><td>{@code bench.cf.mode}</td><td>{@code single}</td>
 *       <td>{@code single} or {@code per-state}. Drives the CfMode comparison in {@link
 *       #cfModePointLookup()} / {@link #cfModeSequentialPut()}.</td></tr>
 * </table>
 */
@Tag("bench")
public class ForStRsBProdBenchmark {

    // --- Default sizing knobs (override via -Dbench.* system properties) ---
    private static final int DEFAULT_PRELOAD = 100_000;
    private static final int DEFAULT_VALUE_BYTES = 1024;
    private static final int DEFAULT_INFLIGHT_SNAPSHOTS = 100;
    private static final int DEFAULT_MEASURE_OPS = 50_000;
    private static final int DEFAULT_WARMUP_OPS = 5_000;

    /** Number of CFs in the per-state-CF mode (matches a typical 4-state Flink job). */
    private static final int PER_STATE_CF_COUNT = 4;

    private static int intProp(String name, int def) {
        return Integer.parseInt(System.getProperty(name, Integer.toString(def)));
    }

    /**
     * Encodes an integer key as fixed 16-byte ASCII so all preload + lookup keys carry the same
     * length (matches the production encoding shape: 2-byte BE key-group + serialized user key).
     */
    private static byte[] keyOf(int i) {
        byte[] b = new byte[16];
        for (int p = 15; p >= 0; p--) {
            b[p] = (byte) ('0' + (i % 10));
            i /= 10;
        }
        return b;
    }

    /** Builds a value blob of the requested size, all bytes set to {@code (byte) (i & 0xff)}. */
    private static byte[] valueOf(int i, int sizeBytes) {
        byte[] v = new byte[sizeBytes];
        Arrays.fill(v, (byte) (i & 0xff));
        return v;
    }

    /** Pretty-prints sorted latency samples (ns -> µs) at canonical percentiles. */
    private static void reportPercentiles(String label, long[] samplesNs) {
        long[] sorted = samplesNs.clone();
        Arrays.sort(sorted);
        long p50 = sorted[(int) (sorted.length * 0.50)];
        long p95 = sorted[(int) (sorted.length * 0.95)];
        long p99 = sorted[(int) (sorted.length * 0.99)];
        long p999 = sorted[Math.min(sorted.length - 1, (int) (sorted.length * 0.999))];
        long max = sorted[sorted.length - 1];
        System.out.printf(
                "[%s] n=%,d  p50=%.3fµs  p95=%.3fµs  p99=%.3fµs  p99.9=%.3fµs  max=%.3fµs%n",
                label, sorted.length, p50 / 1e3, p95 / 1e3, p99 / 1e3, p999 / 1e3, max / 1e3);
    }

    /**
     * Loads {@code preload} entries into the given CF, then issues a {@code flush} so the data
     * lives in an SST (puts get_at on the snapshot-pinned compaction-blocked path, which is the
     * realistic hot-path for an active streaming job).
     */
    private static void preload(
            ForStRsLinker linker, FrsDb db, FrsCfHandle cf, int preload, int valueBytes) {
        long start = System.nanoTime();
        for (int i = 0; i < preload; i++) {
            byte[] k = keyOf(i);
            byte[] v = valueOf(i, valueBytes);
            linker.put(db, cf, k, v);
        }
        linker.flush(db);
        long elapsed = System.nanoTime() - start;
        System.out.printf(
                "[setup] preloaded %,d entries (%,d-byte values, ~%.1f MiB) in %.2f s%n",
                preload, valueBytes, preload * (long) valueBytes / 1048576.0, elapsed / 1e9);
    }

    // ------------------------------------------------------------------
    // Task 5.1: dbSnapshot P99 under 100 concurrent in-flight snapshots.
    // ------------------------------------------------------------------

    /**
     * Spec §16 acceptance: {@code dbSnapshot()} P99 &lt; 100µs under 100 concurrent in-flight
     * snapshots. We hold a steady-state ring of {@code inflight} pre-captured snapshots and time
     * each fresh capture; for every new capture we release the oldest, so the registry size is
     * pinned at {@code inflight} for the whole measurement window.
     */
    @Test
    void dbSnapshotP99UnderInflightLoad() throws Exception {
        int preloadEntries = intProp("bench.preload.entries", DEFAULT_PRELOAD);
        int valueBytes = intProp("bench.preload.value.bytes", DEFAULT_VALUE_BYTES);
        int inflight = intProp("bench.inflight.snapshots", DEFAULT_INFLIGHT_SNAPSHOTS);
        int measureOps = intProp("bench.measure.ops", DEFAULT_MEASURE_OPS);
        int warmupOps = intProp("bench.warmup.ops", DEFAULT_WARMUP_OPS);

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                preload(linker, db, cf, preloadEntries, valueBytes);

                // Pre-fill the in-flight ring so the registry already holds `inflight` snapshots
                // when measurement starts.
                Deque<FrsSnapshot> ring = new ArrayDeque<>(inflight + 1);
                for (int i = 0; i < inflight; i++) {
                    ring.add(linker.dbSnapshot(db, arena));
                }
                System.out.printf(
                        "[setup] pre-captured %d in-flight snapshots; entering warmup%n", inflight);

                // ---- Warmup ----
                for (int i = 0; i < warmupOps; i++) {
                    FrsSnapshot s = linker.dbSnapshot(db, arena);
                    ring.add(s);
                    ring.removeFirst().close();
                }

                // ---- Measure ----
                long[] samples = new long[measureOps];
                for (int i = 0; i < measureOps; i++) {
                    long t0 = System.nanoTime();
                    FrsSnapshot s = linker.dbSnapshot(db, arena);
                    long t1 = System.nanoTime();
                    samples[i] = t1 - t0;
                    ring.add(s);
                    ring.removeFirst().close();
                }

                // ---- Drain remaining snapshots ----
                for (FrsSnapshot s : ring) {
                    s.close();
                }
                ring.clear();

                System.out.printf(
                        "%n=== Task 5.1: dbSnapshot() under %d in-flight snapshots, "
                                + "preload=%,d entries (%,d-byte values) ===%n",
                        inflight, preloadEntries, valueBytes);
                reportPercentiles("dbSnapshot", samples);
                long[] sorted = samples.clone();
                Arrays.sort(sorted);
                long p99Ns = sorted[(int) (sorted.length * 0.99)];
                String verdict = p99Ns < 100_000L ? "PASS" : "FAIL";
                System.out.printf(
                        "[acceptance] spec §16 dbSnapshot p99 < 100µs: %s (measured %.3fµs)%n",
                        verdict, p99Ns / 1e3);
            }
        }
    }

    /**
     * Spec §16 acceptance: sync-phase (snapshot + {@code createIncrementalCheckpointAt}) P95 &lt;
     * 1ms under 100 concurrent in-flight snapshots. We measure the full sync-phase critical section
     * of {@link org.apache.flink.state.forstrs.keyed.ForStRsSnapshotStrategy} — the part that runs
     * on the task thread and must not stall barrier propagation.
     *
     * <p>Each iteration:
     *
     * <ol>
     *   <li>Captures a fresh snapshot (the one being measured).
     *   <li>Allocates a 32-byte {@code FrsIncrementalCheckpointResult} struct.
     *   <li>Calls {@code frs_create_incremental_checkpoint_at} with monotonically-increasing ids.
     *   <li>Frees the inner allocations of the result struct ({@code
     *       dbIncrementalCheckpointResultFree}).
     *   <li>Releases the oldest snapshot from the in-flight ring.
     * </ol>
     *
     * <p>Steps 1+2+3+4 are exactly what the production sync phase ({@code syncPrepareResources})
     * runs on the task thread; step 5 maintains the registry-pinning load.
     */
    @Test
    void syncPhaseP95UnderInflightLoad() throws Exception {
        int preloadEntries = intProp("bench.preload.entries", DEFAULT_PRELOAD);
        int valueBytes = intProp("bench.preload.value.bytes", DEFAULT_VALUE_BYTES);
        int inflight = intProp("bench.inflight.snapshots", DEFAULT_INFLIGHT_SNAPSHOTS);
        // Sync-phase measurement is more expensive per iteration than dbSnapshot (it persists a
        // manifest), so default to 1/10 the iteration count. Override via -Dbench.measure.ops.
        int measureOps = intProp("bench.measure.ops", DEFAULT_MEASURE_OPS / 10);
        int warmupOps = intProp("bench.warmup.ops", DEFAULT_WARMUP_OPS / 10);

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                preload(linker, db, cf, preloadEntries, valueBytes);

                Deque<FrsSnapshot> ring = new ArrayDeque<>(inflight + 1);
                for (int i = 0; i < inflight; i++) {
                    ring.add(linker.dbSnapshot(db, arena));
                }
                System.out.printf(
                        "[setup] pre-captured %d in-flight snapshots; entering warmup%n", inflight);

                AtomicLong ckptIdCounter = new AtomicLong(1L);
                MemorySegment resultBuf = arena.allocate(32L);

                // ---- Warmup ----
                for (int i = 0; i < warmupOps; i++) {
                    FrsSnapshot s = linker.dbSnapshot(db, arena);
                    long id = ckptIdCounter.getAndIncrement();
                    linker.createIncrementalCheckpointAt(db, s, id, id - 1, resultBuf);
                    linker.dbIncrementalCheckpointResultFree(resultBuf);
                    ring.add(s);
                    ring.removeFirst().close();
                }

                // ---- Measure ----
                long[] samples = new long[measureOps];
                for (int i = 0; i < measureOps; i++) {
                    long t0 = System.nanoTime();
                    FrsSnapshot s = linker.dbSnapshot(db, arena);
                    long id = ckptIdCounter.getAndIncrement();
                    linker.createIncrementalCheckpointAt(db, s, id, id - 1, resultBuf);
                    linker.dbIncrementalCheckpointResultFree(resultBuf);
                    long t1 = System.nanoTime();
                    samples[i] = t1 - t0;
                    ring.add(s);
                    ring.removeFirst().close();
                }

                for (FrsSnapshot s : ring) {
                    s.close();
                }
                ring.clear();

                System.out.printf(
                        "%n=== Task 5.1: sync-phase (snapshot + create_incremental_checkpoint_at "
                                + "+ free) under %d in-flight snapshots, preload=%,d entries ===%n",
                        inflight, preloadEntries);
                reportPercentiles("syncPhase", samples);
                long[] sorted = samples.clone();
                Arrays.sort(sorted);
                long p95Ns = sorted[(int) (sorted.length * 0.95)];
                String verdict = p95Ns < 1_000_000L ? "PASS" : "FAIL";
                System.out.printf(
                        "[acceptance] spec §16 sync-phase p95 < 1ms: %s (measured %.3fµs)%n",
                        verdict, p95Ns / 1e3);
            }
        }
    }

    /**
     * Stress variant that drives {@code dbSnapshot} from {@link #DEFAULT_INFLIGHT_SNAPSHOTS} JVM
     * threads concurrently — an alternative interpretation of "100 concurrent in-flight" that
     * captures lock-contention overhead as well as registry-size cost. Each thread runs {@code
     * measureOps / threadCount} captures with try-with-resources release; the bench reports
     * aggregate throughput plus per-iteration percentile across the merged sample stream.
     */
    @Test
    void dbSnapshotConcurrentThreads() throws Exception {
        int preloadEntries = intProp("bench.preload.entries", DEFAULT_PRELOAD);
        int valueBytes = intProp("bench.preload.value.bytes", DEFAULT_VALUE_BYTES);
        int threadCount = intProp("bench.threads", DEFAULT_INFLIGHT_SNAPSHOTS);
        int measureOps = intProp("bench.measure.ops", DEFAULT_MEASURE_OPS);
        int perThread = Math.max(1, measureOps / threadCount);

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                preload(linker, db, cf, preloadEntries, valueBytes);

                long[] samples = new long[threadCount * perThread];
                AtomicInteger sampleIdx = new AtomicInteger(0);
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(threadCount);
                ExecutorService pool = Executors.newFixedThreadPool(threadCount);
                for (int t = 0; t < threadCount; t++) {
                    pool.submit(
                            () -> {
                                try {
                                    start.await();
                                    for (int i = 0; i < perThread; i++) {
                                        long t0 = System.nanoTime();
                                        FrsSnapshot s = linker.dbSnapshot(db, arena);
                                        long t1 = System.nanoTime();
                                        s.close();
                                        int idx = sampleIdx.getAndIncrement();
                                        if (idx < samples.length) {
                                            samples[idx] = t1 - t0;
                                        }
                                    }
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                } finally {
                                    done.countDown();
                                }
                            });
                }
                long t0 = System.nanoTime();
                start.countDown();
                done.await(5, TimeUnit.MINUTES);
                long elapsed = System.nanoTime() - t0;
                pool.shutdown();
                pool.awaitTermination(30, TimeUnit.SECONDS);

                int filled = Math.min(samples.length, sampleIdx.get());
                long[] trimmed = new long[filled];
                System.arraycopy(samples, 0, trimmed, 0, filled);

                System.out.printf(
                        "%n=== Task 5.1 (concurrent variant): dbSnapshot from %d threads, "
                                + "%d ops/thread, preload=%,d ===%n",
                        threadCount, perThread, preloadEntries);
                reportPercentiles("dbSnapshot.concurrent", trimmed);
                System.out.printf(
                        "[throughput] %,d total snapshots in %.3f s -> %.0f ops/s%n",
                        filled, elapsed / 1e9, filled * 1e9 / elapsed);
            }
        }
    }

    // ------------------------------------------------------------------
    // Task 5.3: Single-CF vs per-state-CF (CfMode) point-lookup + write throughput.
    // ------------------------------------------------------------------

    /**
     * Single-CF point-lookup throughput: all preloaded keys live in one CF; the lookup workload
     * touches a uniformly-random subset (modulo {@code preloadEntries}). The companion {@link
     * #cfModePerStatePointLookup()} runs the same total number of lookups but spread across {@link
     * #PER_STATE_CF_COUNT} CFs. Aggregated, the two report the throughput delta the spec §16
     * acceptance bar wants: single-CF should win on point-lookup at the per-CF metadata cost,
     * per-state-CF wins when state classes have wildly different working sets.
     */
    @Test
    void cfModeSingleCfPointLookup() throws Exception {
        int preloadEntries = intProp("bench.preload.entries", DEFAULT_PRELOAD);
        int valueBytes = intProp("bench.preload.value.bytes", DEFAULT_VALUE_BYTES);
        int measureOps = intProp("bench.measure.ops", DEFAULT_MEASURE_OPS);
        int warmupOps = intProp("bench.warmup.ops", DEFAULT_WARMUP_OPS);

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                preload(linker, db, cf, preloadEntries, valueBytes);

                // Warmup
                long sink = 0L;
                for (int i = 0; i < warmupOps; i++) {
                    byte[] v = linker.lookupKv(db, cf, keyOf(i % preloadEntries));
                    if (v != null) {
                        sink += v[0];
                    }
                }

                long t0 = System.nanoTime();
                for (int i = 0; i < measureOps; i++) {
                    byte[] v = linker.lookupKv(db, cf, keyOf(i % preloadEntries));
                    if (v != null) {
                        sink += v[0];
                    }
                }
                long elapsed = System.nanoTime() - t0;

                System.out.printf(
                        "%n=== Task 5.3 (single-CF): point-lookup throughput, preload=%,d ===%n",
                        preloadEntries);
                System.out.printf(
                        "[single-cf.lookup] %,d ops in %.3f s -> %.0f ops/s (sink=%d)%n",
                        measureOps, elapsed / 1e9, measureOps * 1e9 / elapsed, sink);
            }
        }
    }

    /**
     * Per-state-CF point-lookup throughput: the same {@code preloadEntries} are spread across
     * {@link #PER_STATE_CF_COUNT} CFs (round-robin by key index), and the lookup workload likewise
     * round-robins across CFs.
     */
    @Test
    void cfModePerStatePointLookup() throws Exception {
        int preloadEntries = intProp("bench.preload.entries", DEFAULT_PRELOAD);
        int valueBytes = intProp("bench.preload.value.bytes", DEFAULT_VALUE_BYTES);
        int measureOps = intProp("bench.measure.ops", DEFAULT_MEASURE_OPS);
        int warmupOps = intProp("bench.warmup.ops", DEFAULT_WARMUP_OPS);

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                List<FrsCfHandle> cfs = new ArrayList<>(PER_STATE_CF_COUNT);
                cfs.add(linker.dbDefaultCf(db, arena));
                for (int i = 1; i < PER_STATE_CF_COUNT; i++) {
                    cfs.add(linker.dbCreateCf(db, arena, "state-cf-" + i));
                }
                try {
                    long start = System.nanoTime();
                    for (int i = 0; i < preloadEntries; i++) {
                        FrsCfHandle target = cfs.get(i % PER_STATE_CF_COUNT);
                        linker.put(db, target, keyOf(i), valueOf(i, valueBytes));
                    }
                    linker.flush(db);
                    long preloadElapsed = System.nanoTime() - start;
                    System.out.printf(
                            "[setup] preloaded %,d entries across %d CFs (%,d-byte values, "
                                    + "~%.1f MiB) in %.2f s%n",
                            preloadEntries,
                            PER_STATE_CF_COUNT,
                            valueBytes,
                            preloadEntries * (long) valueBytes / 1048576.0,
                            preloadElapsed / 1e9);

                    long sink = 0L;
                    for (int i = 0; i < warmupOps; i++) {
                        FrsCfHandle target = cfs.get(i % PER_STATE_CF_COUNT);
                        byte[] v = linker.lookupKv(db, target, keyOf(i % preloadEntries));
                        if (v != null) {
                            sink += v[0];
                        }
                    }

                    long t0 = System.nanoTime();
                    for (int i = 0; i < measureOps; i++) {
                        FrsCfHandle target = cfs.get(i % PER_STATE_CF_COUNT);
                        byte[] v = linker.lookupKv(db, target, keyOf(i % preloadEntries));
                        if (v != null) {
                            sink += v[0];
                        }
                    }
                    long elapsed = System.nanoTime() - t0;

                    System.out.printf(
                            "%n=== Task 5.3 (per-state-CF, %d CFs): point-lookup throughput, "
                                    + "preload=%,d ===%n",
                            PER_STATE_CF_COUNT, preloadEntries);
                    System.out.printf(
                            "[per-state-cf.lookup] %,d ops in %.3f s -> %.0f ops/s (sink=%d)%n",
                            measureOps, elapsed / 1e9, measureOps * 1e9 / elapsed, sink);
                } finally {
                    for (FrsCfHandle h : cfs) {
                        try {
                            h.close();
                        } catch (Throwable ignored) {
                            // proceed with the remaining handles
                        }
                    }
                }
            }
        }
    }

    /** Single-CF write throughput counterpart to {@link #cfModeSingleCfPointLookup()}. */
    @Test
    void cfModeSingleCfSequentialPut() throws Exception {
        int preloadEntries = intProp("bench.preload.entries", DEFAULT_PRELOAD);
        int valueBytes = intProp("bench.preload.value.bytes", DEFAULT_VALUE_BYTES);
        int measureOps = intProp("bench.measure.ops", DEFAULT_MEASURE_OPS);
        int warmupOps = intProp("bench.warmup.ops", DEFAULT_WARMUP_OPS);

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                preload(linker, db, cf, preloadEntries, valueBytes);

                for (int i = 0; i < warmupOps; i++) {
                    linker.put(db, cf, keyOf(preloadEntries + i), valueOf(i, valueBytes));
                }

                long t0 = System.nanoTime();
                for (int i = 0; i < measureOps; i++) {
                    linker.put(
                            db, cf, keyOf(preloadEntries + warmupOps + i), valueOf(i, valueBytes));
                }
                long elapsed = System.nanoTime() - t0;

                System.out.printf(
                        "%n=== Task 5.3 (single-CF): sequentialPut throughput, preload=%,d ===%n",
                        preloadEntries);
                System.out.printf(
                        "[single-cf.put] %,d ops in %.3f s -> %.0f ops/s%n",
                        measureOps, elapsed / 1e9, measureOps * 1e9 / elapsed);
            }
        }
    }

    /** Per-state-CF write throughput counterpart to {@link #cfModePerStatePointLookup()}. */
    @Test
    void cfModePerStateSequentialPut() throws Exception {
        int preloadEntries = intProp("bench.preload.entries", DEFAULT_PRELOAD);
        int valueBytes = intProp("bench.preload.value.bytes", DEFAULT_VALUE_BYTES);
        int measureOps = intProp("bench.measure.ops", DEFAULT_MEASURE_OPS);
        int warmupOps = intProp("bench.warmup.ops", DEFAULT_WARMUP_OPS);

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                List<FrsCfHandle> cfs = new ArrayList<>(PER_STATE_CF_COUNT);
                cfs.add(linker.dbDefaultCf(db, arena));
                for (int i = 1; i < PER_STATE_CF_COUNT; i++) {
                    cfs.add(linker.dbCreateCf(db, arena, "state-cf-" + i));
                }
                try {
                    // Preload (round-robin)
                    for (int i = 0; i < preloadEntries; i++) {
                        FrsCfHandle target = cfs.get(i % PER_STATE_CF_COUNT);
                        linker.put(db, target, keyOf(i), valueOf(i, valueBytes));
                    }
                    linker.flush(db);

                    for (int i = 0; i < warmupOps; i++) {
                        FrsCfHandle target = cfs.get(i % PER_STATE_CF_COUNT);
                        linker.put(db, target, keyOf(preloadEntries + i), valueOf(i, valueBytes));
                    }

                    long t0 = System.nanoTime();
                    for (int i = 0; i < measureOps; i++) {
                        FrsCfHandle target = cfs.get(i % PER_STATE_CF_COUNT);
                        linker.put(
                                db,
                                target,
                                keyOf(preloadEntries + warmupOps + i),
                                valueOf(i, valueBytes));
                    }
                    long elapsed = System.nanoTime() - t0;

                    System.out.printf(
                            "%n=== Task 5.3 (per-state-CF, %d CFs): sequentialPut throughput, "
                                    + "preload=%,d ===%n",
                            PER_STATE_CF_COUNT, preloadEntries);
                    System.out.printf(
                            "[per-state-cf.put] %,d ops in %.3f s -> %.0f ops/s%n",
                            measureOps, elapsed / 1e9, measureOps * 1e9 / elapsed);
                } finally {
                    for (FrsCfHandle h : cfs) {
                        try {
                            h.close();
                        } catch (Throwable ignored) {
                            // proceed with the remaining handles
                        }
                    }
                }
            }
        }
    }

    /**
     * Disabled by default — full 1 GiB preload runs ~30 minutes wall-clock. Enable explicitly via
     * {@code -Dbench.preload.entries=1048576 -Dtest=ForStRsBProdBenchmark#large1GiB}.
     *
     * <p>Acts as a copy of {@link #dbSnapshotP99UnderInflightLoad()} sized at the upper bound the
     * spec asked us to measure. Kept as a separate {@code @Disabled} method so a normal CI run
     * doesn't accidentally hit the long-running variant.
     */
    @Test
    @Disabled("Long-running 1 GiB variant — enable explicitly via -Dbench.preload.entries=1048576")
    void large1GiB() throws Exception {
        // Same body as dbSnapshotP99UnderInflightLoad — exists as a marker so future runs can
        // explicitly target this name with -Dtest=ForStRsBProdBenchmark#large1GiB while the
        // bench.preload.entries override pushes the working set to 1 GiB.
        dbSnapshotP99UnderInflightLoad();
    }
}

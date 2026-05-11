/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forstrs.jmh;

import org.forstdb.RocksDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 3-way comparison harness: this class benchmarks the {@link RocksDB} flat JNI surface against any
 * cdylib that provides matching {@code Java_org_forstdb_RocksDB_*} symbols. By swapping {@code
 * -Dorg.forstdb.libpath=} on the command line, the same byte-code drives either:
 *
 * <ul>
 *   <li>{@code libforst_rs_ffi.dylib} — the ForSt-RS Rust engine via the compat-JNI shim (this
 *       repo).
 *   <li>{@code forstjni-community.dylib} — the upstream community ForSt JNI. NOTE: the community
 *       library does <b>not</b> expose flat {@code open(String)} / {@code put(JJ[BII[BII)V}
 *       symbols, it expects Options + DBOptions + ColumnFamilyDescriptor lifecycle. Running this
 *       bench against the community cdylib will therefore {@code UnsatisfiedLinkError}; see {@code
 *       JMH_BENCHMARK.md} for details.
 * </ul>
 *
 * <p>The class is structured so that each "benchmark" is a static method returning {@code long ops}
 * — the harness in {@link #main(String[])} drives the warmup/measurement loop. JMH annotations are
 * intentionally absent so the file compiles against a stock JDK 25 install with zero extra
 * dependencies; the same logic could be wrapped in {@code @Benchmark}-annotated stubs if a full
 * JMH-maven build is desired.
 */
public final class ForStCompareBenchmark {

    /** Pre-loaded keys for the point-lookup workload. */
    private static final int PRELOAD = 100_000;

    /**
     * Number of rows per WriteBatch in the {@code batchedPut} workload — matches the realistic
     * per-checkpoint-barrier write fan-out in production Flink state backends.
     */
    private static final int BATCH_SIZE = 1000;

    private static final byte[] VALUE = new byte[128];

    static {
        for (int i = 0; i < VALUE.length; i++) {
            VALUE[i] = (byte) (i & 0xff);
        }
    }

    private ForStCompareBenchmark() {}

    /** Encodes an integer key as ASCII {@code k########} (9-byte key). */
    private static byte[] keyOf(int i) {
        byte[] b = new byte[9];
        b[0] = 'k';
        for (int p = 8; p >= 1; p--) {
            b[p] = (byte) ('0' + (i % 10));
            i /= 10;
        }
        return b;
    }

    /** Encodes batch index N as ASCII {@code bk%010d} (12-byte key). */
    private static byte[] batchKeyOf(int n) {
        byte[] b = new byte[12];
        b[0] = 'b';
        b[1] = 'k';
        for (int p = 11; p >= 2; p--) {
            b[p] = (byte) ('0' + (n % 10));
            n /= 10;
        }
        return b;
    }

    /** Encodes batch index N as ASCII {@code bv%010d} (12-byte value). */
    private static byte[] batchValueOf(int n) {
        byte[] b = new byte[12];
        b[0] = 'b';
        b[1] = 'v';
        for (int p = 11; p >= 2; p--) {
            b[p] = (byte) ('0' + (n % 10));
            n /= 10;
        }
        return b;
    }

    // --- benchmark workloads -------------------------------------------------

    /** Returns the byte[] read; caller can use it to defeat dead-code elimination. */
    static byte[] pointLookup(long db, long cf, byte[] probeKey) {
        return RocksDB.get(db, cf, probeKey, 0, probeKey.length);
    }

    /** Inserts a unique key derived from {@code counter}. */
    static void sequentialPut(long db, long cf, AtomicLong counter) {
        long c = counter.getAndIncrement();
        byte[] k = keyOf(PRELOAD + (int) (c & 0x7fff_ffffL));
        RocksDB.put(db, cf, k, 0, k.length, VALUE, 0, VALUE.length);
    }

    // --- harness -------------------------------------------------------------

    /**
     * Manual JMH-style harness. Three phases per workload: warmup, measurement, teardown. Each
     * phase runs for a fixed wall-clock budget; we count invocations and divide by elapsed time to
     * derive throughput in ops/sec.
     */
    public static void main(String[] args) throws Exception {
        final long warmupNanos =
                Long.parseLong(System.getProperty("bench.warmup.s", "6")) * 1_000_000_000L;
        final long measureNanos =
                Long.parseLong(System.getProperty("bench.measure.s", "25")) * 1_000_000_000L;

        Path tmp = Files.createTempDirectory("forst-bench-");
        long db = 0L;
        try {
            db = RocksDB.open(tmp.toString());
            if (db == 0L) {
                throw new IllegalStateException("RocksDB.open returned 0");
            }
            long cf = RocksDB.getDefaultColumnFamily(db);
            if (cf == 0L) {
                throw new IllegalStateException("getDefaultColumnFamily returned 0");
            }

            // Pre-load 100k entries for the point-lookup workload.
            System.out.printf(
                    "[setup] preloading %d entries (each value=%d bytes)%n", PRELOAD, VALUE.length);
            long preloadStart = System.nanoTime();
            for (int i = 0; i < PRELOAD; i++) {
                byte[] k = keyOf(i);
                RocksDB.put(db, cf, k, 0, k.length, VALUE, 0, VALUE.length);
            }
            RocksDB.flush(db);
            long preloadElapsed = System.nanoTime() - preloadStart;
            System.out.printf(
                    "[setup] preload done in %.3f s (%.0f put/s)%n",
                    preloadElapsed / 1e9, PRELOAD / (preloadElapsed / 1e9));

            // ---- pointLookup ---------------------------------------------------
            byte[] probeKey = keyOf(50_000);
            long sink = 0L; // accumulate to defeat JIT dead-code elimination

            System.out.println("[pointLookup] warmup...");
            long deadline = System.nanoTime() + warmupNanos;
            long warmupOps = 0;
            while (System.nanoTime() < deadline) {
                byte[] v = pointLookup(db, cf, probeKey);
                if (v != null) {
                    sink += v[0];
                }
                warmupOps++;
            }

            System.out.println("[pointLookup] measure...");
            deadline = System.nanoTime() + measureNanos;
            long start = System.nanoTime();
            long ops = 0;
            while (System.nanoTime() < deadline) {
                byte[] v = pointLookup(db, cf, probeKey);
                if (v != null) {
                    sink += v[0];
                }
                ops++;
            }
            long elapsed = System.nanoTime() - start;
            double pointThroughput = ops * 1e9 / elapsed;
            System.out.printf(
                    "[pointLookup] %,d ops in %.3f s -> %.0f ops/s (sink=%d)%n",
                    ops, elapsed / 1e9, pointThroughput, sink);

            // ---- sequentialPut -----------------------------------------------
            AtomicLong counter = new AtomicLong();

            System.out.println("[sequentialPut] warmup...");
            deadline = System.nanoTime() + warmupNanos;
            while (System.nanoTime() < deadline) {
                sequentialPut(db, cf, counter);
            }

            System.out.println("[sequentialPut] measure...");
            deadline = System.nanoTime() + measureNanos;
            start = System.nanoTime();
            ops = 0;
            while (System.nanoTime() < deadline) {
                sequentialPut(db, cf, counter);
                ops++;
            }
            elapsed = System.nanoTime() - start;
            double putThroughput = ops * 1e9 / elapsed;
            System.out.printf(
                    "[sequentialPut] %,d ops in %.3f s -> %.0f ops/s%n",
                    ops, elapsed / 1e9, putThroughput);

            // ---- batchedPut --------------------------------------------------
            // Pre-generate BATCH_SIZE keys + values; the workload writes the entire
            // batch via one writeBatch() JNI call per invocation. Reports rows/sec
            // (= batches/sec * BATCH_SIZE) for direct comparison with sequentialPut.
            byte[][] bk = new byte[BATCH_SIZE][];
            byte[][] bv = new byte[BATCH_SIZE][];
            for (int i = 0; i < BATCH_SIZE; i++) {
                bk[i] = batchKeyOf(i);
                bv[i] = batchValueOf(i);
            }

            System.out.println("[batchedPut] warmup...");
            deadline = System.nanoTime() + warmupNanos;
            while (System.nanoTime() < deadline) {
                RocksDB.writeBatch(db, cf, bk, bv);
            }

            System.out.println("[batchedPut] measure...");
            deadline = System.nanoTime() + measureNanos;
            start = System.nanoTime();
            long batches = 0;
            while (System.nanoTime() < deadline) {
                RocksDB.writeBatch(db, cf, bk, bv);
                batches++;
            }
            elapsed = System.nanoTime() - start;
            double batchedRowsPerSec = batches * (double) BATCH_SIZE * 1e9 / elapsed;
            System.out.printf(
                    "[batchedPut] %,d batches (%,d rows) in %.3f s -> %.0f rows/s%n",
                    batches, batches * (long) BATCH_SIZE, elapsed / 1e9, batchedRowsPerSec);

            // ---- summary -----------------------------------------------------
            System.out.println();
            System.out.println("=== summary ===");
            System.out.printf("pointLookup     %.0f ops/s%n", pointThroughput);
            System.out.printf("sequentialPut   %.0f ops/s%n", putThroughput);
            System.out.printf(
                    "batchedPut      %.0f rows/s (batch=%d)%n", batchedRowsPerSec, BATCH_SIZE);
            System.out.printf(
                    "variant.libpath %s%n",
                    System.getProperty("org.forstdb.libpath", "<via java.library.path>"));
        } finally {
            try {
                if (db != 0L) {
                    RocksDB.close(db);
                }
            } catch (Throwable t) {
                System.err.println("[teardown] close threw: " + t);
            }
            // Best-effort recursive delete.
            try {
                Files.walk(tmp)
                        .sorted(Comparator.reverseOrder())
                        .forEach(
                                p -> {
                                    try {
                                        Files.deleteIfExists(p);
                                    } catch (IOException ignored) {
                                    }
                                });
            } catch (IOException ignored) {
            }
        }
    }
}

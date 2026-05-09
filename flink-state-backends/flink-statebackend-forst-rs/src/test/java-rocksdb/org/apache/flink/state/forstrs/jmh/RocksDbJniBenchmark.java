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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Same workload as {@link ForStCompareBenchmark} / {@link ForStRsFfmBenchmark} / {@link
 * ForStCommunityBenchmark}, but bound against the canonical {@code org.rocksdb:rocksdbjni} Maven
 * artifact (Facebook RocksDB via its bundled JNI). Provides the missing Java-layer baseline so the
 * 4-way comparison in {@code JMH_BENCHMARK.md} mirrors the Rust criterion {@code rocksdb_compare}
 * bench at the engine layer.
 *
 * <p>The bench keeps the {@code public static void main} harness pattern of its sister benches: a
 * preload + warmup + measurement loop with throughput in {@code ops/s}. Tunables: {@code
 * -Dbench.warmup.s} and {@code -Dbench.measure.s} (defaults 6 / 25).
 *
 * <p>RocksDB JNI auto-loads its bundled native library via {@link RocksDB#loadLibrary()}; no
 * library path system property is required.
 */
public final class RocksDbJniBenchmark {

    private static final int PRELOAD = 100_000;

    /** Number of rows per WriteBatch in the {@code batchedPut} workload. */
    private static final int BATCH_SIZE = 1000;

    private static final byte[] VALUE = new byte[128];

    static {
        for (int i = 0; i < VALUE.length; i++) {
            VALUE[i] = (byte) (i & 0xff);
        }
    }

    private RocksDbJniBenchmark() {}

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

    public static void main(String[] args) throws Exception {
        final long warmupNanos =
                Long.parseLong(System.getProperty("bench.warmup.s", "6")) * 1_000_000_000L;
        final long measureNanos =
                Long.parseLong(System.getProperty("bench.measure.s", "25")) * 1_000_000_000L;

        // Auto-extracts and dlopen's the bundled librocksdbjni-osx-arm64.jnilib (or platform peer).
        RocksDB.loadLibrary();

        Path tmp = Files.createTempDirectory("rocksdb-jni-bench-");
        Options opts = null;
        RocksDB db = null;
        try {
            opts = new Options().setCreateIfMissing(true);
            db = RocksDB.open(opts, tmp.toString());
            ColumnFamilyHandle cf = db.getDefaultColumnFamily();

            // Pre-load 100k entries.
            System.out.printf(
                    "[setup] preloading %d entries (each value=%d bytes)%n",
                    PRELOAD, VALUE.length);
            long preloadStart = System.nanoTime();
            for (int i = 0; i < PRELOAD; i++) {
                byte[] k = keyOf(i);
                db.put(cf, k, VALUE);
            }
            long preloadElapsed = System.nanoTime() - preloadStart;
            System.out.printf(
                    "[setup] preload done in %.3f s (%.0f put/s)%n",
                    preloadElapsed / 1e9, PRELOAD / (preloadElapsed / 1e9));

            // ---- pointLookup -------------------------------------------------
            byte[] probeKey = keyOf(50_000);
            long sink = 0L;

            System.out.println("[pointLookup] warmup...");
            long deadline = System.nanoTime() + warmupNanos;
            while (System.nanoTime() < deadline) {
                byte[] v = db.get(cf, probeKey);
                if (v != null) {
                    sink += v[0];
                }
            }

            System.out.println("[pointLookup] measure...");
            deadline = System.nanoTime() + measureNanos;
            long start = System.nanoTime();
            long ops = 0;
            while (System.nanoTime() < deadline) {
                byte[] v = db.get(cf, probeKey);
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

            // ---- sequentialPut ------------------------------------------------
            AtomicLong counter = new AtomicLong();

            System.out.println("[sequentialPut] warmup...");
            deadline = System.nanoTime() + warmupNanos;
            while (System.nanoTime() < deadline) {
                long c = counter.getAndIncrement();
                byte[] k = keyOf(PRELOAD + (int) (c & 0x7fff_ffffL));
                db.put(cf, k, VALUE);
            }

            System.out.println("[sequentialPut] measure...");
            deadline = System.nanoTime() + measureNanos;
            start = System.nanoTime();
            ops = 0;
            while (System.nanoTime() < deadline) {
                long c = counter.getAndIncrement();
                byte[] k = keyOf(PRELOAD + (int) (c & 0x7fff_ffffL));
                db.put(cf, k, VALUE);
                ops++;
            }
            elapsed = System.nanoTime() - start;
            double putThroughput = ops * 1e9 / elapsed;
            System.out.printf(
                    "[sequentialPut] %,d ops in %.3f s -> %.0f ops/s%n",
                    ops, elapsed / 1e9, putThroughput);

            // ---- batchedPut --------------------------------------------------
            // Per-invocation: clear-and-refill the WriteBatch with BATCH_SIZE rows,
            // then RocksDB.write(WriteOptions, WriteBatch). Reports rows/sec for
            // direct comparison with the FFM/JNI-shim/community variants.
            byte[][] bk = new byte[BATCH_SIZE][];
            byte[][] bv = new byte[BATCH_SIZE][];
            for (int i = 0; i < BATCH_SIZE; i++) {
                bk[i] = batchKeyOf(i);
                bv[i] = batchValueOf(i);
            }
            try (WriteOptions writeOpt = new WriteOptions();
                    WriteBatch wb = new WriteBatch(BATCH_SIZE * 24)) {

                System.out.println("[batchedPut] warmup...");
                deadline = System.nanoTime() + warmupNanos;
                while (System.nanoTime() < deadline) {
                    wb.clear();
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        wb.put(bk[i], bv[i]);
                    }
                    db.write(writeOpt, wb);
                }

                System.out.println("[batchedPut] measure...");
                deadline = System.nanoTime() + measureNanos;
                start = System.nanoTime();
                long batches = 0;
                while (System.nanoTime() < deadline) {
                    wb.clear();
                    for (int i = 0; i < BATCH_SIZE; i++) {
                        wb.put(bk[i], bv[i]);
                    }
                    db.write(writeOpt, wb);
                    batches++;
                }
                elapsed = System.nanoTime() - start;
                double batchedRowsPerSec = batches * (double) BATCH_SIZE * 1e9 / elapsed;
                System.out.printf(
                        "[batchedPut] %,d batches (%,d rows) in %.3f s -> %.0f rows/s%n",
                        batches, batches * (long) BATCH_SIZE, elapsed / 1e9, batchedRowsPerSec);

                System.out.println();
                System.out.println("=== summary ===");
                System.out.printf("pointLookup     %.0f ops/s%n", pointThroughput);
                System.out.printf("sequentialPut   %.0f ops/s%n", putThroughput);
                System.out.printf("batchedPut      %.0f rows/s (batch=%d)%n",
                        batchedRowsPerSec, BATCH_SIZE);
                System.out.printf("rocksdb.version %s%n", RocksDB.rocksdbVersion());
            }
        } finally {
            if (db != null) {
                db.close();
            }
            if (opts != null) {
                opts.close();
            }
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

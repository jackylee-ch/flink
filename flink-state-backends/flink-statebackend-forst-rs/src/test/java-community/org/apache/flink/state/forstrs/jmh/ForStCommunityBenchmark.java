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

import org.forstdb.FlushOptions;
import org.forstdb.Options;
import org.forstdb.RocksDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Same workload as {@link ForStCompareBenchmark} but bound against the
 * upstream COMMUNITY {@code com.ververica:forstjni} cdylib symbol layout.
 * Uses Options + FlushOptions handles to satisfy the heavier community ABI.
 */
public final class ForStCommunityBenchmark {

    private static final int PRELOAD = 100_000;
    private static final byte[] VALUE = new byte[128];

    static {
        for (int i = 0; i < VALUE.length; i++) {
            VALUE[i] = (byte) (i & 0xff);
        }
    }

    private ForStCommunityBenchmark() {}

    private static byte[] keyOf(int i) {
        byte[] b = new byte[9];
        b[0] = 'k';
        for (int p = 8; p >= 1; p--) {
            b[p] = (byte) ('0' + (i % 10));
            i /= 10;
        }
        return b;
    }

    public static void main(String[] args) throws Exception {
        final long warmupNanos =
                Long.parseLong(System.getProperty("bench.warmup.s", "6")) * 1_000_000_000L;
        final long measureNanos =
                Long.parseLong(System.getProperty("bench.measure.s", "25")) * 1_000_000_000L;

        Path tmp = Files.createTempDirectory("forst-community-bench-");
        long opt = 0L, db = 0L, flushOpt = 0L;
        try {
            opt = Options.newOptions();
            Options.setCreateIfMissing(opt, true);
            db = RocksDB.open(opt, tmp.toString());
            if (db == 0L) {
                throw new IllegalStateException("community RocksDB.open returned 0");
            }
            long cf = RocksDB.getDefaultColumnFamily(db);
            if (cf == 0L) {
                throw new IllegalStateException("getDefaultColumnFamily returned 0");
            }
            flushOpt = FlushOptions.newFlushOptions();

            // Pre-load 100k entries.
            System.out.printf(
                    "[setup] preloading %d entries (each value=%d bytes)%n",
                    PRELOAD, VALUE.length);
            long preloadStart = System.nanoTime();
            for (int i = 0; i < PRELOAD; i++) {
                byte[] k = keyOf(i);
                RocksDB.put(db, k, 0, k.length, VALUE, 0, VALUE.length);
            }
            // NOTE: skipping RocksDB.flush(db, flushOpt) — the community
            // _Java_org_forstdb_RocksDB_flush symbol crashes JDK 25 with a
            // SIGSEGV in oop_access_barrier when called with the (J,J)
            // overload (it appears to walk the heap expecting a CF-list
            // jlongArray rather than a single CFH long). Letting the
            // memtable absorb the preload is acceptable for a point-lookup
            // throughput bench — the warm cache is what we want to measure.
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
                byte[] v = RocksDB.get(db, probeKey, 0, probeKey.length);
                if (v != null) {
                    sink += v[0];
                }
            }

            System.out.println("[pointLookup] measure...");
            deadline = System.nanoTime() + measureNanos;
            long start = System.nanoTime();
            long ops = 0;
            while (System.nanoTime() < deadline) {
                byte[] v = RocksDB.get(db, probeKey, 0, probeKey.length);
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
                RocksDB.put(db, k, 0, k.length, VALUE, 0, VALUE.length);
            }

            System.out.println("[sequentialPut] measure...");
            deadline = System.nanoTime() + measureNanos;
            start = System.nanoTime();
            ops = 0;
            while (System.nanoTime() < deadline) {
                long c = counter.getAndIncrement();
                byte[] k = keyOf(PRELOAD + (int) (c & 0x7fff_ffffL));
                RocksDB.put(db, k, 0, k.length, VALUE, 0, VALUE.length);
                ops++;
            }
            elapsed = System.nanoTime() - start;
            double putThroughput = ops * 1e9 / elapsed;
            System.out.printf(
                    "[sequentialPut] %,d ops in %.3f s -> %.0f ops/s%n",
                    ops, elapsed / 1e9, putThroughput);

            System.out.println();
            System.out.println("=== summary ===");
            System.out.printf("pointLookup     %.0f ops/s%n", pointThroughput);
            System.out.printf("sequentialPut   %.0f ops/s%n", putThroughput);
            System.out.printf("variant.libpath %s%n",
                    System.getProperty("org.forstdb.libpath", "<via java.library.path>"));
        } finally {
            try {
                if (flushOpt != 0L) {
                    FlushOptions.disposeInternal(flushOpt);
                }
            } catch (Throwable t) {
                System.err.println("[teardown] flushOpt dispose threw: " + t);
            }
            try {
                if (db != 0L) {
                    RocksDB.closeDatabase(db);
                    RocksDB.disposeInternal(db);
                }
            } catch (Throwable t) {
                System.err.println("[teardown] db close threw: " + t);
            }
            try {
                if (opt != 0L) {
                    Options.disposeInternal(opt);
                }
            } catch (Throwable t) {
                System.err.println("[teardown] opt dispose threw: " + t);
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

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

import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.forstdb.FlushOptions;
import org.forstdb.Options;
import org.forstdb.RocksDB;
import org.forstdb.WriteBatch;
import org.forstdb.WriteOptions;

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

    /** Number of rows per WriteBatch in the {@code batchedPut} workload. */
    private static final int BATCH_SIZE = 1000;

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

        // Match the FFM bench's bench.preset semantics so that ForSt-RS-tuned vs
        // community-tuned numbers can be compared head-to-head. Each preset
        // pushes the same memtable / bg-thread budget into the community
        // Options handle via the legacy RocksDB JNI setters.
        final String preset = System.getProperty("bench.preset", "default");
        long writeBufferSize = 0L;
        int maxWriteBufferNumber = 0;
        int maxBackgroundCompactions = 0;
        int maxBackgroundFlushes = 0;
        switch (preset) {
            case "A":
                writeBufferSize = 256L * 1024L * 1024L;
                maxWriteBufferNumber = 8;
                maxBackgroundCompactions = 4;
                maxBackgroundFlushes = 4;
                break;
            case "B":
                writeBufferSize = 512L * 1024L * 1024L;
                maxWriteBufferNumber = 4;
                maxBackgroundCompactions = 8;
                maxBackgroundFlushes = 8;
                break;
            case "default":
            default:
                break;
        }
        System.out.printf(
                "[setup] preset=%s write_buffer_size=%d max_write_buffer_number=%d "
                        + "max_background_compactions=%d max_background_flushes=%d%n",
                preset,
                writeBufferSize,
                maxWriteBufferNumber,
                maxBackgroundCompactions,
                maxBackgroundFlushes);

        Path tmp = Files.createTempDirectory("forst-community-bench-");
        long opt = 0L, dbOpt = 0L, cfOpt = 0L, db = 0L, flushOpt = 0L, writeOpt = 0L, writeBatch = 0L;
        try {
            if (writeBufferSize > 0 || maxWriteBufferNumber > 0 || maxBackgroundCompactions > 0 || maxBackgroundFlushes > 0) {
                dbOpt = DBOptions.newDBOptions();
                cfOpt = ColumnFamilyOptions.newColumnFamilyOptions();
                if (writeBufferSize > 0) {
                    ColumnFamilyOptions.setWriteBufferSize(cfOpt, writeBufferSize);
                }
                if (maxWriteBufferNumber > 0) {
                    ColumnFamilyOptions.setMaxWriteBufferNumber(cfOpt, maxWriteBufferNumber);
                }
                int maxBackgroundJobs = maxBackgroundCompactions + maxBackgroundFlushes;
                if (maxBackgroundJobs > 0) {
                    DBOptions.setMaxBackgroundJobs(dbOpt, maxBackgroundJobs);
                }
                opt = Options.newOptions(dbOpt, cfOpt);
            } else {
                opt = Options.newOptions();
            }
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

            // ---- batchedPut --------------------------------------------------
            // Pre-generate BATCH_SIZE keys + values; per invocation, build a fresh
            // WriteBatch (clear-and-refill for the same handle) and apply it via
            // RocksDB.write0(WriteOptions, WriteBatch). Reports rows/sec to be
            // directly comparable with sequentialPut and the FFM/JNI-shim variants.
            byte[][] bk = new byte[BATCH_SIZE][];
            byte[][] bv = new byte[BATCH_SIZE][];
            for (int i = 0; i < BATCH_SIZE; i++) {
                bk[i] = batchKeyOf(i);
                bv[i] = batchValueOf(i);
            }
            writeOpt = WriteOptions.newWriteOptions();
            writeBatch = WriteBatch.newWriteBatch(BATCH_SIZE * 24);

            System.out.println("[batchedPut] warmup...");
            deadline = System.nanoTime() + warmupNanos;
            while (System.nanoTime() < deadline) {
                WriteBatch.clear0(writeBatch);
                for (int i = 0; i < BATCH_SIZE; i++) {
                    WriteBatch.put(writeBatch, bk[i], bk[i].length, bv[i], bv[i].length);
                }
                RocksDB.write0(db, writeOpt, writeBatch);
            }

            System.out.println("[batchedPut] measure...");
            deadline = System.nanoTime() + measureNanos;
            start = System.nanoTime();
            long batches = 0;
            while (System.nanoTime() < deadline) {
                WriteBatch.clear0(writeBatch);
                for (int i = 0; i < BATCH_SIZE; i++) {
                    WriteBatch.put(writeBatch, bk[i], bk[i].length, bv[i], bv[i].length);
                }
                RocksDB.write0(db, writeOpt, writeBatch);
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
            System.out.printf("variant.libpath %s%n",
                    System.getProperty("org.forstdb.libpath", "<via java.library.path>"));
            System.out.printf("bench.preset    %s%n", preset);
        } finally {
            try {
                if (writeBatch != 0L) {
                    WriteBatch.disposeInternal(writeBatch);
                }
            } catch (Throwable t) {
                System.err.println("[teardown] writeBatch dispose threw: " + t);
            }
            try {
                if (writeOpt != 0L) {
                    WriteOptions.disposeInternal(writeOpt);
                }
            } catch (Throwable t) {
                System.err.println("[teardown] writeOpt dispose threw: " + t);
            }
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
                if (cfOpt != 0L) {
                    ColumnFamilyOptions.disposeInternal(cfOpt);
                }
            } catch (Throwable t) {
                System.err.println("[teardown] cfOpt dispose threw: " + t);
            }
            try {
                if (dbOpt != 0L) {
                    DBOptions.disposeInternal(dbOpt);
                }
            } catch (Throwable t) {
                System.err.println("[teardown] dbOpt dispose threw: " + t);
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

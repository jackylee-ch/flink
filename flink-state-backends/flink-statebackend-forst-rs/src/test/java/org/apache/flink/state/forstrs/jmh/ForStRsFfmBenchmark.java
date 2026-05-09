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

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manual JMH-style harness for the JDK 25 FFM (Foreign Function and Memory API) path through
 * {@link ForStRsLinker}. Mirrors {@link ForStCompareBenchmark} so the numbers are directly
 * comparable, but routes every call through {@code Linker.nativeLinker()} +
 * {@code MethodHandle.invokeExact(...)} instead of the JNI {@code Java_org_forstdb_RocksDB_*}
 * shim. This is the path the production state backend uses; it eliminates the JNI argument
 * marshaling overhead that the {@link ForStCompareBenchmark} measures.
 *
 * <p>The harness is intentionally annotation-free (no {@code @Benchmark}, no
 * {@code @State}) so it compiles against a stock JDK 25 install with no JMH dependency
 * resolved. The same logic could be wrapped in JMH stubs trivially — see {@code
 * JMH_BENCHMARK.md}.
 *
 * <p>Library lookup honours {@code -Dforstrs.native.libpath=&lt;path&gt;}; the runner sets this
 * to the same {@code libforst_rs_ffi.dylib} the JNI-shim variant uses, so any throughput
 * delta is purely the cost of the JNI bridge vs FFM downcall handles.
 */
public final class ForStRsFfmBenchmark {

    /** Pre-loaded keys for the point-lookup workload. */
    private static final int PRELOAD = 100_000;

    /**
     * Number of rows per WriteBatch in the {@code batchedPut} workload — matches the
     * realistic per-checkpoint-barrier write fan-out in production Flink state backends.
     */
    private static final int BATCH_SIZE = 1000;

    private static final byte[] VALUE = new byte[128];

    static {
        for (int i = 0; i < VALUE.length; i++) {
            VALUE[i] = (byte) (i & 0xff);
        }
    }

    private ForStRsFfmBenchmark() {}

    /** Encodes an integer key as ASCII {@code k########} (9-byte key) — matches sister bench. */
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

        // A shared Arena owns the cdylib symbol lookup for the lifetime of the process — its
        // close() unloads the library and invalidates every MemorySegment we obtained.
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = null;
            FrsCfHandle cf = null;
            try {
                db = linker.dbOpenMemory(arena);
                cf = linker.dbDefaultCf(db, arena);

                // Pre-load 100k entries for the point-lookup workload (matches sister bench).
                System.out.printf(
                        "[setup] preloading %d entries (each value=%d bytes)%n",
                        PRELOAD, VALUE.length);
                long preloadStart = System.nanoTime();
                for (int i = 0; i < PRELOAD; i++) {
                    byte[] k = keyOf(i);
                    linker.put(db, cf, k, VALUE);
                }
                linker.flush(db);
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
                    byte[] v = linker.lookupKv(db, cf, probeKey);
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
                    byte[] v = linker.lookupKv(db, cf, probeKey);
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
                    long c = counter.getAndIncrement();
                    byte[] k = keyOf(PRELOAD + (int) (c & 0x7fff_ffffL));
                    linker.put(db, cf, k, VALUE);
                }

                System.out.println("[sequentialPut] measure...");
                deadline = System.nanoTime() + measureNanos;
                start = System.nanoTime();
                ops = 0;
                while (System.nanoTime() < deadline) {
                    long c = counter.getAndIncrement();
                    byte[] k = keyOf(PRELOAD + (int) (c & 0x7fff_ffffL));
                    linker.put(db, cf, k, VALUE);
                    ops++;
                }
                elapsed = System.nanoTime() - start;
                double putThroughput = ops * 1e9 / elapsed;
                System.out.printf(
                        "[sequentialPut] %,d ops in %.3f s -> %.0f ops/s%n",
                        ops, elapsed / 1e9, putThroughput);

                // ---- batchedPut -------------------------------------------------
                // Pre-stage BATCH_SIZE keys + values into native memory (owned by
                // `arena`) so that each batchPut() call is a single FFM downcall
                // through frs_batch_put with no per-iteration staging cost. This
                // is the realistic write hot path for production state backends.
                long ptrSz = ValueLayout.ADDRESS.byteSize();
                long lenSz = ValueLayout.JAVA_LONG.byteSize();
                MemorySegment keyPtrs = arena.allocate(BATCH_SIZE * ptrSz);
                MemorySegment keyLens = arena.allocate(BATCH_SIZE * lenSz);
                MemorySegment valPtrs = arena.allocate(BATCH_SIZE * ptrSz);
                MemorySegment valLens = arena.allocate(BATCH_SIZE * lenSz);
                for (int i = 0; i < BATCH_SIZE; i++) {
                    byte[] kBytes = batchKeyOf(i);
                    byte[] vBytes = batchValueOf(i);
                    MemorySegment ks = arena.allocate(kBytes.length);
                    MemorySegment vs = arena.allocate(vBytes.length);
                    MemorySegment.copy(kBytes, 0, ks, ValueLayout.JAVA_BYTE, 0, kBytes.length);
                    MemorySegment.copy(vBytes, 0, vs, ValueLayout.JAVA_BYTE, 0, vBytes.length);
                    keyPtrs.set(ValueLayout.ADDRESS, i * ptrSz, ks);
                    valPtrs.set(ValueLayout.ADDRESS, i * ptrSz, vs);
                    keyLens.set(ValueLayout.JAVA_LONG, i * lenSz, (long) kBytes.length);
                    valLens.set(ValueLayout.JAVA_LONG, i * lenSz, (long) vBytes.length);
                }

                System.out.println("[batchedPut] warmup...");
                deadline = System.nanoTime() + warmupNanos;
                while (System.nanoTime() < deadline) {
                    linker.batchPut(db, cf, keyPtrs, keyLens, valPtrs, valLens, BATCH_SIZE);
                }

                System.out.println("[batchedPut] measure...");
                deadline = System.nanoTime() + measureNanos;
                start = System.nanoTime();
                long batches = 0;
                while (System.nanoTime() < deadline) {
                    linker.batchPut(db, cf, keyPtrs, keyLens, valPtrs, valLens, BATCH_SIZE);
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
                System.out.printf("batchedPut      %.0f rows/s (batch=%d)%n",
                        batchedRowsPerSec, BATCH_SIZE);
                System.out.printf("variant.libpath %s%n",
                        System.getProperty("forstrs.native.libpath", "<via java.library.path>"));
            } finally {
                // CF must close before DB, DB must close before Arena (which owns the symbol lookup).
                try {
                    if (cf != null) {
                        cf.close();
                    }
                } catch (Throwable t) {
                    System.err.println("[teardown] cf.close threw: " + t);
                }
                try {
                    if (db != null) {
                        db.close();
                    }
                } catch (Throwable t) {
                    System.err.println("[teardown] db.close threw: " + t);
                }
            }
        }
    }
}

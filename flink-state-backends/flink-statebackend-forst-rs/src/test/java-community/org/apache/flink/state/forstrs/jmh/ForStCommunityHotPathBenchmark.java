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

import org.forstdb.Options;
import org.forstdb.RocksDB;
import org.forstdb.WriteBatch;
import org.forstdb.WriteOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/** ForStBackend-shaped JNI hot-path matrix for the community ForSt JNI library. */
public final class ForStCommunityHotPathBenchmark {

    private static final int STATE_ROWS = 8_192;
    private static final int BATCH_ROWS = 512;
    private static final int PROBE_ROWS = 1_024;
    private static final byte[] VALUE = valueOf(96, 7);
    private static volatile long blackhole;

    private ForStCommunityHotPathBenchmark() {}

    @FunctionalInterface
    private interface Workload {
        long run() throws Exception;
    }

    private static final class Result {
        final String name;
        final long ops;
        final long elapsedNanos;
        final boolean supported;
        final String note;

        Result(String name, long ops, long elapsedNanos, boolean supported, String note) {
            this.name = name;
            this.ops = ops;
            this.elapsedNanos = elapsedNanos;
            this.supported = supported;
            this.note = note;
        }

        double opsPerSec() {
            return ops * 1_000_000_000.0 / elapsedNanos;
        }
    }

    public static void main(String[] args) throws Exception {
        long warmupNanos =
                Long.parseLong(System.getProperty("bench.warmup.s", "6")) * 1_000_000_000L;
        long measureNanos =
                Long.parseLong(System.getProperty("bench.measure.s", "25")) * 1_000_000_000L;

        System.out.printf(
                "[setup] variant=forst-community-jni-hotpaths stateRows=%d batchRows=%d probeRows=%d%n",
                STATE_ROWS, BATCH_ROWS, PROBE_ROWS);

        Result openClose = measure("openClose", warmupNanos, measureNanos, ForStCommunityHotPathBenchmark::openClose);
        Result writeBatchResult = measureWriteBatch(warmupNanos, measureNanos);

        Path tmp = Files.createTempDirectory("forst-community-hotpaths-");
        long opt = 0L;
        long db = 0L;
        long writeOpt = 0L;
        long writeBatch = 0L;
        try {
            opt = Options.newOptions();
            Options.setCreateIfMissing(opt, true);
            db = RocksDB.open(opt, tmp.toString());
            if (db == 0L) {
                throw new IllegalStateException("failed to open community ForSt DB");
            }
            writeOpt = WriteOptions.newWriteOptions();
            writeBatch = WriteBatch.newWriteBatch(BATCH_ROWS * 128);

            byte[][] keys = keys("value/q4", STATE_ROWS);
            byte[][] values = values(STATE_ROWS, 64);
            for (int i = 0; i < STATE_ROWS; i++) {
                RocksDB.put(db, keys[i], 0, keys[i].length, values[i], 0, values[i].length);
            }
            validateGet(db, keys[STATE_ROWS / 2], 64);

            byte[][] batchKeys = keys("batch/q4", BATCH_ROWS);
            byte[][] batchValues = values(BATCH_ROWS, 96);
            byte[][] probes = first(keys, PROBE_ROWS);
            int[] offsets = new int[PROBE_ROWS];
            int[] lengths = lengths(probes);
            byte[] deleteKey = key("delete/q5", 42);
            AtomicLong putCounter = new AtomicLong();

            final long dbHandle = db;
            final long writeOptionsHandle = writeOpt;
            final long writeBatchHandle = writeBatch;
            Result pointGet =
                    measure(
                            "pointGet",
                            warmupNanos,
                            measureNanos,
                            () -> {
                                consume(
                                        checksum(
                                                RocksDB.get(
                                                        dbHandle,
                                                        keys[STATE_ROWS / 2],
                                                        0,
                                                        keys[STATE_ROWS / 2].length)));
                                return 1L;
                            });
            Result pointPut =
                    measure(
                            "pointPut",
                            warmupNanos,
                            measureNanos,
                            () -> {
                                long c = putCounter.getAndIncrement();
                                byte[] k = key("put/q4", (int) c);
                                RocksDB.put(dbHandle, k, 0, k.length, VALUE, 0, VALUE.length);
                                return 1L;
                            });
            Result delete =
                    measure(
                            "deleteThenGet",
                            warmupNanos,
                            measureNanos,
                            () -> {
                                RocksDB.put(
                                        dbHandle,
                                        deleteKey,
                                        0,
                                        deleteKey.length,
                                        VALUE,
                                        0,
                                        VALUE.length);
                                RocksDB.delete(dbHandle, deleteKey, 0, deleteKey.length);
                                byte[] got = RocksDB.get(dbHandle, deleteKey, 0, deleteKey.length);
                                if (got != null) {
                                    throw new IllegalStateException("deleted key still readable");
                                }
                                return 1L;
                            });
            Result multiGet =
                    measureOptional(
                            "multiGet",
                            warmupNanos,
                            measureNanos,
                            () -> {
                                consume(checksum(RocksDB.multiGet(dbHandle, probes, offsets, lengths)));
                                return PROBE_ROWS;
                            });

            System.out.println();
            System.out.println("=== hotpath summary ===");
            print(openClose);
            print(pointGet);
            print(pointPut);
            print(delete);
            print(writeBatchResult);
            print(multiGet);
            System.out.printf(
                    "variant.libpath %s%n",
                    System.getProperty("org.forstdb.libpath", "<via java.library.path>"));
        } finally {
            if (writeBatch != 0L) {
                WriteBatch.disposeInternal(writeBatch);
            }
            if (writeOpt != 0L) {
                WriteOptions.disposeInternal(writeOpt);
            }
            if (db != 0L) {
                RocksDB.closeDatabase(db);
            }
            if (opt != 0L) {
                Options.disposeInternal(opt);
            }
            deleteRecursively(tmp);
        }
    }

    private static Result measureWriteBatch(long warmupNanos, long measureNanos) throws Exception {
        Path tmp = Files.createTempDirectory("forst-community-batch-put-");
        long opt = 0L;
        long db = 0L;
        long writeOpt = 0L;
        long writeBatch = 0L;
        try {
            opt = Options.newOptions();
            Options.setCreateIfMissing(opt, true);
            db = RocksDB.open(opt, tmp.toString());
            if (db == 0L) {
                throw new IllegalStateException("failed to open community ForSt DB for writeBatch");
            }
            writeOpt = WriteOptions.newWriteOptions();
            writeBatch = WriteBatch.newWriteBatch(BATCH_ROWS * 128);
            final long dbHandle = db;
            final long writeOptionsHandle = writeOpt;
            final long writeBatchHandle = writeBatch;
            byte[][] batchKeys = keys("batch/q4", BATCH_ROWS);
            byte[][] batchValues = values(BATCH_ROWS, 96);
            return measure(
                    "writeBatch",
                    warmupNanos,
                    measureNanos,
                    () -> {
                        WriteBatch.clear0(writeBatchHandle);
                        for (int i = 0; i < BATCH_ROWS; i++) {
                            WriteBatch.put(
                                    writeBatchHandle,
                                    batchKeys[i],
                                    batchKeys[i].length,
                                    batchValues[i],
                                    batchValues[i].length);
                        }
                        RocksDB.write0(dbHandle, writeOptionsHandle, writeBatchHandle);
                        return BATCH_ROWS;
                    });
        } finally {
            if (writeBatch != 0L) {
                WriteBatch.disposeInternal(writeBatch);
            }
            if (writeOpt != 0L) {
                WriteOptions.disposeInternal(writeOpt);
            }
            if (db != 0L) {
                RocksDB.closeDatabase(db);
            }
            if (opt != 0L) {
                Options.disposeInternal(opt);
            }
            deleteRecursively(tmp);
        }
    }

    private static Result measure(String name, long warmupNanos, long measureNanos, Workload workload)
            throws Exception {
        long deadline = System.nanoTime() + warmupNanos;
        long sink = 0L;
        while (System.nanoTime() < deadline) {
            sink += workload.run();
        }
        deadline = System.nanoTime() + measureNanos;
        long start = System.nanoTime();
        long ops = 0L;
        while (System.nanoTime() < deadline) {
            ops += workload.run();
        }
        long elapsed = System.nanoTime() - start;
        if (sink == Long.MIN_VALUE) {
            throw new AssertionError("unreachable sink");
        }
        consume(sink);
        return new Result(name, ops, elapsed, true, "");
    }

    private static Result measureOptional(
            String name, long warmupNanos, long measureNanos, Workload workload) throws Exception {
        try {
            return measure(name, warmupNanos, measureNanos, workload);
        } catch (UnsatisfiedLinkError e) {
            return new Result(name, 0L, 1L, false, e.getMessage());
        }
    }

    private static void print(Result result) {
        if (!result.supported) {
            System.out.printf("%-18s unsupported (%s)%n", result.name, result.note);
            return;
        }
        System.out.printf(
                "%-18s %,12d units in %.3f s -> %.0f units/s%n",
                result.name, result.ops, result.elapsedNanos / 1e9, result.opsPerSec());
    }

    private static long openClose() throws Exception {
        Path tmp = Files.createTempDirectory("forst-community-open-close-");
        long opt = 0L;
        long db = 0L;
        try {
            opt = Options.newOptions();
            Options.setCreateIfMissing(opt, true);
            db = RocksDB.open(opt, tmp.toString());
            if (db == 0L) {
                throw new IllegalStateException("openClose failed");
            }
            return 1L;
        } finally {
            if (db != 0L) {
                RocksDB.closeDatabase(db);
            }
            if (opt != 0L) {
                Options.disposeInternal(opt);
            }
            deleteRecursively(tmp);
        }
    }

    private static void validateGet(long db, byte[] key, int expectedLen) throws Exception {
        byte[] got = RocksDB.get(db, key, 0, key.length);
        if (got == null || got.length != expectedLen) {
            throw new IllegalStateException("get validation failed");
        }
    }

    private static byte[][] keys(String prefix, int count) {
        byte[][] out = new byte[count][];
        for (int i = 0; i < count; i++) {
            out[i] = key(prefix, i);
        }
        return out;
    }

    private static byte[] key(String prefix, int i) {
        return String.format("%s/key-group=%04d/key=%08d", prefix, i & 127, i).getBytes();
    }

    private static byte[][] values(int count, int len) {
        byte[][] out = new byte[count][];
        for (int i = 0; i < count; i++) {
            out[i] = valueOf(len, i);
        }
        return out;
    }

    private static byte[] valueOf(int len, int seed) {
        byte[] value = new byte[len];
        for (int i = 0; i < len; i++) {
            value[i] = (byte) ((seed + i) & 0xff);
        }
        return value;
    }

    private static byte[][] first(byte[][] values, int count) {
        byte[][] out = new byte[count][];
        System.arraycopy(values, 0, out, 0, count);
        return out;
    }

    private static int[] lengths(byte[][] values) {
        int[] out = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = values[i].length;
        }
        return out;
    }

    private static long checksum(byte[] value) {
        if (value == null) {
            return 0L;
        }
        long sum = value.length;
        for (byte b : value) {
            sum += b & 0xff;
        }
        return sum;
    }

    private static long checksum(byte[][] values) {
        if (values == null) {
            throw new IllegalStateException("null result array");
        }
        long sum = 0L;
        for (byte[] value : values) {
            sum += checksum(value);
        }
        if (sum == 0L) {
            throw new IllegalStateException("empty checksum");
        }
        return sum;
    }

    private static void consume(long value) {
        blackhole ^= value;
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(ForStCommunityHotPathBenchmark::deletePath);
        }
    }

    private static void deletePath(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

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

/** ForStBackend-shaped JNI hot-path matrix for the ForSt-RS compat-JNI shim. */
public final class ForStRsJniHotPathBenchmark {

    private static final int STATE_ROWS = 8_192;
    private static final int BATCH_ROWS = 512;
    private static final int PROBE_ROWS = 1_024;
    private static final int PREFIX_ROWS = 512;
    private static final byte[] VALUE = valueOf(96, 7);
    private static volatile long blackhole;

    private ForStRsJniHotPathBenchmark() {}

    @FunctionalInterface
    private interface Workload {
        long run() throws Exception;
    }

    private static final class Result {
        final String name;
        final long ops;
        final long elapsedNanos;

        Result(String name, long ops, long elapsedNanos) {
            this.name = name;
            this.ops = ops;
            this.elapsedNanos = elapsedNanos;
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
                "[setup] variant=forst-rs-jni-hotpaths stateRows=%d batchRows=%d probeRows=%d prefixRows=%d%n",
                STATE_ROWS, BATCH_ROWS, PROBE_ROWS, PREFIX_ROWS);

        Result openClose = measure("openClose", warmupNanos, measureNanos, ForStRsJniHotPathBenchmark::openClose);
        Result batchPut = measureBatchPut(warmupNanos, measureNanos);

        Path tmp = Files.createTempDirectory("forst-rs-jni-hotpaths-");
        long db = 0L;
        try {
            db = RocksDB.open(tmp.toString());
            long cf = RocksDB.getDefaultColumnFamily(db);
            if (db == 0L || cf == 0L) {
                throw new IllegalStateException("failed to open ForSt-RS JNI DB");
            }
            final long dbHandle = db;
            final long cfHandle = cf;

            byte[][] keys = keys("value/q4", STATE_ROWS);
            byte[][] values = values(STATE_ROWS, 64);
            RocksDB.batchPut(dbHandle, cfHandle, keys, values);
            validateGet(dbHandle, cfHandle, keys[STATE_ROWS / 2], 64);

            byte[][] batchKeys = keys("batch/q4", BATCH_ROWS);
            byte[][] batchValues = values(BATCH_ROWS, 96);
            byte[][] probes = first(keys, PROBE_ROWS);
            int[] offsets = new int[PROBE_ROWS];
            int[] lengths = lengths(probes);
            byte[] deleteKey = key("delete/q5", 42);
            byte[] prefix = "map/q11/key-group=0042/namespace=active/".getBytes();
            byte[][] prefixKeys = prefixedKeys(prefix, PREFIX_ROWS);
            byte[][] prefixValues = values(PREFIX_ROWS, 48);
            RocksDB.batchPut(dbHandle, cfHandle, prefixKeys, prefixValues);
            validatePrefixScan(dbHandle, cfHandle, prefix, PREFIX_ROWS);

            AtomicLong putCounter = new AtomicLong();
            Result pointGet =
                    measure(
                            "pointGet",
                            warmupNanos,
                            measureNanos,
                            () ->
                                    {
                                        consume(
                                                checksum(
                                                        RocksDB.get(
                                                                dbHandle,
                                                                cfHandle,
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
                                RocksDB.put(
                                        dbHandle, cfHandle, k, 0, k.length, VALUE, 0, VALUE.length);
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
                                        cfHandle,
                                        deleteKey,
                                        0,
                                        deleteKey.length,
                                        VALUE,
                                        0,
                                        VALUE.length);
                                RocksDB.delete(dbHandle, cfHandle, deleteKey, 0, deleteKey.length);
                                byte[] got =
                                        RocksDB.get(
                                                dbHandle, cfHandle, deleteKey, 0, deleteKey.length);
                                if (got != null) {
                                    throw new IllegalStateException("deleted key still readable");
                                }
                                return 1L;
                            });
            Result batchGet =
                    measure(
                            "batchGet",
                            warmupNanos,
                            measureNanos,
                            () -> {
                                consume(checksum(RocksDB.batchGet(dbHandle, cfHandle, probes)));
                                return PROBE_ROWS;
                            });
            Result multiGet =
                    measure(
                            "multiGet",
                            warmupNanos,
                            measureNanos,
                            () -> {
                                consume(checksum(RocksDB.multiGet(dbHandle, probes, offsets, lengths)));
                                return PROBE_ROWS;
                            });
            Result prefixScan =
                    measure(
                            "prefixScan",
                            warmupNanos,
                            measureNanos,
                            () -> prefixScanRows(dbHandle, cfHandle, prefix));
            Result flushCompactRead =
                    measure(
                            "flushCompactRead",
                            warmupNanos,
                            measureNanos,
                            () -> {
                                RocksDB.flushCf(dbHandle, cfHandle);
                                RocksDB.compactRange(dbHandle, cfHandle);
                                validateGet(dbHandle, cfHandle, keys[STATE_ROWS / 3], 64);
                                return 1L;
                            });

            System.out.println();
            System.out.println("=== hotpath summary ===");
            print(openClose);
            print(pointGet);
            print(pointPut);
            print(delete);
            print(batchPut);
            print(batchGet);
            print(multiGet);
            print(prefixScan);
            print(flushCompactRead);
            System.out.printf(
                    "variant.libpath %s%n",
                    System.getProperty("org.forstdb.libpath", "<via java.library.path>"));
        } finally {
            if (db != 0L) {
                RocksDB.close(db);
            }
            deleteRecursively(tmp);
        }
    }

    private static Result measureBatchPut(long warmupNanos, long measureNanos) throws Exception {
        Path tmp = Files.createTempDirectory("forst-rs-jni-batch-put-");
        long db = 0L;
        try {
            db = RocksDB.open(tmp.toString());
            long cf = RocksDB.getDefaultColumnFamily(db);
            if (db == 0L || cf == 0L) {
                throw new IllegalStateException("failed to open ForSt-RS JNI DB for batchPut");
            }
            final long dbHandle = db;
            final long cfHandle = cf;
            byte[][] batchKeys = keys("batch/q4", BATCH_ROWS);
            byte[][] batchValues = values(BATCH_ROWS, 96);
            return measure(
                    "batchPut",
                    warmupNanos,
                    measureNanos,
                    () -> {
                        RocksDB.writeBatch(dbHandle, cfHandle, batchKeys, batchValues);
                        return BATCH_ROWS;
                    });
        } finally {
            if (db != 0L) {
                RocksDB.close(db);
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
        return new Result(name, ops, elapsed);
    }

    private static void print(Result result) {
        System.out.printf(
                "%-18s %,12d units in %.3f s -> %.0f units/s%n",
                result.name, result.ops, result.elapsedNanos / 1e9, result.opsPerSec());
    }

    private static long openClose() throws Exception {
        Path tmp = Files.createTempDirectory("forst-rs-jni-open-close-");
        long db = 0L;
        try {
            db = RocksDB.open(tmp.toString());
            long cf = RocksDB.getDefaultColumnFamily(db);
            if (db == 0L || cf == 0L) {
                throw new IllegalStateException("openClose failed");
            }
            return 1L;
        } finally {
            if (db != 0L) {
                RocksDB.close(db);
            }
            deleteRecursively(tmp);
        }
    }

    private static void validateGet(long db, long cf, byte[] key, int expectedLen) {
        byte[] got = RocksDB.get(db, cf, key, 0, key.length);
        if (got == null || got.length != expectedLen) {
            throw new IllegalStateException("get validation failed");
        }
    }

    private static void validatePrefixScan(long db, long cf, byte[] prefix, int expectedRows) {
        long rows = prefixScanRows(db, cf, prefix);
        if (rows != expectedRows) {
            throw new IllegalStateException(
                    "prefix validation failed: expected " + expectedRows + " got " + rows);
        }
    }

    private static long prefixScanRows(long db, long cf, byte[] prefix) {
        long iter = RocksDB.prefixLookupOpen(db, cf, prefix, 0, prefix.length);
        long rows = 0L;
        try {
            while (true) {
                byte[][] entry = RocksDB.prefixLookupNext(iter);
                if (entry == null) {
                    return rows;
                }
                if (entry.length != 2 || entry[0] == null || !startsWith(entry[0], prefix)) {
                    throw new IllegalStateException("prefix iterator returned invalid row");
                }
                rows++;
            }
        } finally {
            RocksDB.prefixLookupClose(iter);
        }
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (value[i] != prefix[i]) {
                return false;
            }
        }
        return true;
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

    private static byte[][] prefixedKeys(byte[] prefix, int count) {
        byte[][] out = new byte[count][];
        for (int i = 0; i < count; i++) {
            byte[] suffix = String.format("user=%08d", i).getBytes();
            byte[] key = new byte[prefix.length + suffix.length];
            System.arraycopy(prefix, 0, key, 0, prefix.length);
            System.arraycopy(suffix, 0, key, prefix.length, suffix.length);
            out[i] = key;
        }
        return out;
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
            stream.sorted(Comparator.reverseOrder()).forEach(ForStRsJniHotPathBenchmark::deletePath);
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

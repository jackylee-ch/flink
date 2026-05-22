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

import org.apache.flink.state.forstrs.cache.MapStateCache;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH bench (PR-F4) for the PR-F3 off-heap {@link MapStateCache}. Closes B3-JMH: the legacy
 * bench harnesses never exercised this cache despite it being on the Q11/Q12/Q16 hot path.
 *
 * <p>The cache exposes three measurable cost dimensions:
 *
 * <ul>
 *   <li><b>HIT path</b> — {@link MapStateCache#lookup(byte[])} on a present key returns the same
 *       reusable {@link MapStateCache.Lookup} instance and updates a clock stamp. Spec asserts
 *       zero allocation; the bench reports ns/hit.
 *   <li><b>MISS path</b> — {@link MapStateCache#lookup(byte[])} on an unknown key probes the
 *       open-addressed hash index and returns {@code null}. Reports ns/miss.
 *   <li><b>PUT path</b> — {@link MapStateCache#put(byte[], Object)} appends to the off-heap key
 *       region and the hash index. Reports ns/put.
 * </ul>
 *
 * <p>The bench builds a cache loaded to 50% occupancy (matches steady-state Q16 workload) and a
 * pool of pre-built keys: half of them are present (HIT lookups), the other half are absent (MISS
 * lookups). A {@link SplittableRandom} drives the access pattern.
 *
 * <p>Invoke via the {@code jmh} maven profile. Not part of {@code mvn test}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class MapStateCacheJmhBenchmark {

    /**
     * Cache capacity. 64K matches a typical Q16-style working set; 1M tests the upper bound at
     * which the open-addressed hash mask + key-data buffer remain hot in L2.
     */
    @Param({"65536"})
    public int capacity;

    /** Per-key byte length — matches the (operatorKey + userKey) composite key shape. */
    @Param({"32"})
    public int keyBytes;

    private MapStateCache<byte[]> cache;
    private byte[][] presentKeys; // capacity/2 entries — guaranteed HIT
    private byte[][] absentKeys; // capacity/2 entries — guaranteed MISS
    private byte[][] putKeys; // capacity/2 fresh entries used by the PUT bench
    private byte[][] putValues;
    private SplittableRandom rng;

    @Setup(Level.Trial)
    public void setUp() {
        this.cache = new MapStateCache<>(capacity);

        int half = Math.max(1, capacity / 2);
        this.presentKeys = new byte[half][];
        this.absentKeys = new byte[half][];
        this.putKeys = new byte[half][];
        this.putValues = new byte[half][];

        // Pre-populate the cache to 50% occupancy with `presentKeys`.
        for (int i = 0; i < half; i++) {
            presentKeys[i] = keyOf("hit-" + i);
            cache.put(presentKeys[i], ("v-" + i).getBytes());
        }
        // Build the MISS-key pool (never inserted).
        for (int i = 0; i < half; i++) {
            absentKeys[i] = keyOf("miss-" + i);
        }
        // Build the PUT-key pool — fresh keys the put bench can insert without colliding with
        // either present or absent pools. (PUT iterations are bounded by JMH; the cache may grow
        // up to its capacity at which point clock-sweep eviction kicks in.)
        for (int i = 0; i < half; i++) {
            putKeys[i] = keyOf("put-" + i);
            putValues[i] = ("nv-" + i).getBytes();
        }
        this.rng = new SplittableRandom(0xC0FFEE);
    }

    private byte[] keyOf(String prefix) {
        byte[] b = new byte[keyBytes];
        byte[] src = prefix.getBytes();
        System.arraycopy(src, 0, b, 0, Math.min(src.length, b.length));
        // Pad with a deterministic suffix so the hash distribution is even.
        for (int i = src.length; i < b.length; i++) {
            b[i] = (byte) (i & 0xff);
        }
        return b;
    }

    // -----------------------------------------------------------------
    // @Benchmark methods
    // -----------------------------------------------------------------

    /** Pure HIT path — every lookup returns a cached value. Reports ns/hit. */
    @Benchmark
    public void lookupHit(Blackhole bh) {
        int idx = rng.nextInt(presentKeys.length);
        MapStateCache.Lookup<byte[]> r = cache.lookup(presentKeys[idx]);
        bh.consume(r);
    }

    /** Pure MISS path — every lookup probes the open-addressed index and returns {@code null}. */
    @Benchmark
    public void lookupMiss(Blackhole bh) {
        int idx = rng.nextInt(absentKeys.length);
        MapStateCache.Lookup<byte[]> r = cache.lookup(absentKeys[idx]);
        bh.consume(r);
    }

    /** Mixed (50/50 HIT/MISS) — drives the steady-state workload the cache exists to accelerate. */
    @Benchmark
    public void lookupMixed5050(Blackhole bh) {
        boolean hit = rng.nextBoolean();
        byte[][] pool = hit ? presentKeys : absentKeys;
        int idx = rng.nextInt(pool.length);
        bh.consume(cache.lookup(pool[idx]));
    }

    /**
     * PUT path on a key that already exists (overwrite). Reports ns/put on the hot path where the
     * key is already in the index — no resize, no eviction.
     */
    @Benchmark
    public void putOverwrite(Blackhole bh) {
        int idx = rng.nextInt(presentKeys.length);
        cache.put(presentKeys[idx], putValues[idx % putValues.length]);
        bh.consume(idx);
    }
}

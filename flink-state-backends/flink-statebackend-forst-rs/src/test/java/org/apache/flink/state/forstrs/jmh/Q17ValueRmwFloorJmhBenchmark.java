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

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.cache.LongReducingAggregatingCache;

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

import java.io.IOException;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * q17 async-coordination FLOOR micro-bench (PMC-1 2026-06-15; NOT NexMark).
 *
 * <p>q17 is an UNBOUNDED keyed group-agg ({@code GROUP BY auction, day} with
 * count/min/max/avg/sum). Under the async-state backend its operator
 * ({@code AsyncStateGroupAggFunction#processElement}) runs, PER RECORD:
 *
 * <pre>
 *   accState.asyncValue()                                  // VALUE_GET  → AEC round-trip #1
 *       .thenAccept(acc -> aggHelper.processElement(...));  // fold in operator
 *           // → updateAccumulatorsState → accState.asyncUpdate(acc)     // VALUE_UPDATE → AEC round-trip #2
 * </pre>
 *
 * RocksDB's SYNCHRONOUS backend does a direct JNI {@code get} + {@code put} with NONE of the
 * async-state framework coordination (in-flight accounting, key-occupy/epoch ordering, request
 * buffering, batch-trigger, future allocation + {@code thenAccept} continuation). That coordination
 * is the documented q17 "async floor" (root-cause doc §q17). This micro isolates and SIZES that
 * floor, and measures whether a ValueState RMW cache (the lever this cycle proposes — the exact
 * mechanism {@code ForStRsAsyncReducingStateV2#asyncAdd} already uses for ReducingState, which on a
 * cache HIT returns {@code completedVoidFuture()} WITHOUT calling {@code handleRequest}) closes it.
 *
 * <h3>Arms (each = ONE q17 record's RMW)</h3>
 *
 * <ul>
 *   <li><b>{@code aecPathFloor}</b> (Arm A, today's no-value-cache path): the per-record work the
 *       AEC path forces and the lever removes — serialize the composite key + allocate the two
 *       per-record continuation objects a get→fold→update dependent chain needs (the {@code get}
 *       future, the {@code thenAccept} continuation lambda, the {@code update} future). Engine I/O
 *       and the AEC's internal buffer/epoch bookkeeping are NOT modelled here, so Arm A is a LOWER
 *       BOUND on the real per-record floor (the real path adds buffer enqueue, key-occupy, batch
 *       trigger, and two FFM crossings on top).
 *   <li><b>{@code valueRmwCacheHit}</b> (Arm B, the lever): serialize the composite key into the
 *       reusable buffer (same cost both arms pay) + a cache {@code tryFold} HIT — the value folds in
 *       place on the operator thread and the call returns a completed future. ZERO StateRequest,
 *       ZERO future-chain, ZERO engine round-trip. This is the {@code asyncAdd} HIT path generalized
 *       to ValueState.
 * </ul>
 *
 * <p>q17's accumulators (count, sum, min, max) are all reducible {@code long}s, so the
 * {@link LongReducingAggregatingCache} is the right model for the fold (a real q17 lever would pack
 * the accumulator row, but the dominant per-record cost — what Arm A pays and Arm B does not — is
 * the StateRequest/future coordination, not the fold arithmetic, so the long model is faithful to
 * the gap being measured).
 *
 * <p><b>Interpretation:</b> {@code aecPathFloor − valueRmwCacheHit} is the per-record coordination
 * the lever removes on a cache HIT. q17's hot pattern is heavy key recurrence (the same (auction,
 * day) folds many bids), so the cache hit-rate is high and most records take Arm B. Whether the
 * REMAINING floor (the parts not modelled here — the AEC buffer/epoch + the unavoidable barrier
 * flush) still loses RocksDB is the structural question the doc answers; this micro sizes the
 * CLOSEABLE portion.
 *
 * <p>Invoke via the {@code jmh} maven profile. Not part of {@code mvn test}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(2)
@State(Scope.Benchmark)
public class Q17ValueRmwFloorJmhBenchmark {

    /** Number of distinct (auction, day) hot keys recurring in the stream (q17 working set). */
    @Param({"65536"})
    public int hotKeys;

    /** Backing key length — (operatorKey + stateName + namespace) composite, q17-shape. */
    @Param({"24"})
    public int keyBytes;

    private LongReducingAggregatingCache cache;
    private long[] auctionIds; // the BIGINT auction component of each hot key
    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private SplittableRandom rng;

    // Arm-A sink: a real consumer so the JIT cannot elide the continuation allocation.
    private Consumer<Long> armASink;
    private long armAState;

    // Arm-A escape ring: the AEC buffers in-flight request futures (they do NOT die at the call
    // site — they live in the active/blocking buffers until the batch drains). Storing each
    // record's futures into a ring that OUTLIVES the call defeats escape-analysis scalar
    // replacement, so Arm A measures REAL per-record allocation the way the AEC pays it.
    private static final int RING = 256;
    private Object[] escapeRing;
    private int ringPos;

    @Setup(Level.Trial)
    public void setUp() {
        // Sum combiner stands in for the q17 count/sum fold; min/max are the same shape.
        this.cache = new LongReducingAggregatingCache(Long::sum, (k, v) -> {}, hotKeys * 2);
        this.auctionIds = new long[hotKeys];
        this.rng = new SplittableRandom(0x17C0FFEEL);
        // Pre-populate every hot key so EVERY measured fold is a cache HIT (the q17 steady state —
        // a key is missed once on first sight, then hits for the rest of the day).
        for (int i = 0; i < hotKeys; i++) {
            auctionIds[i] = 1_000_000L + i;
            int len = serializeKey(auctionIds[i]);
            cache.put(keyOut.getSharedBuffer(), 0, len, 0L);
        }
        this.armASink = v -> armAState += v;
        this.escapeRing = new Object[RING];
        this.ringPos = 0;
    }

    /** q17 composite-key serialization: {@code "k/" + auctionBE + "/state/" + day}. */
    private int serializeKey(long auction) {
        try {
            keyOut.clear();
            keyOut.write('k');
            keyOut.write('/');
            keyOut.writeLong(auction);
            keyOut.write('/');
            keyOut.write('a');
            keyOut.write('/');
            keyOut.writeLong(0x323032362d3036L); // a fixed "day" component, BE-ish filler
            return keyOut.length();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // -----------------------------------------------------------------
    // Arm A — the per-record AEC coordination the lever removes (LOWER bound)
    // -----------------------------------------------------------------

    /**
     * Models the per-record work a get→fold→update dependent chain forces through the async-state
     * framework that the value-RMW cache bypasses: key serialization (paid by BOTH arms) + the get
     * future + the {@code thenAccept} fold continuation + the update future. Each {@code
     * CompletableFuture} + lambda allocation is a real per-record object the cache-hit path never
     * creates.
     */
    @Benchmark
    public void aecPathFloor(Blackhole bh) {
        int idx = rng.nextInt(hotKeys);
        long auction = auctionIds[idx];
        int len = serializeKey(auction);
        bh.consume(len);
        // get future (VALUE_GET request resolution surrogate)
        CompletableFuture<Long> getFut = new CompletableFuture<>();
        // the operator's fold continuation — a real captured lambda per record
        long input = idx;
        CompletableFuture<Void> chain =
                getFut.thenAccept(
                        old -> {
                            long folded = (old == null ? 0L : old) + input;
                            armASink.accept(folded);
                        });
        // resolve the get (engine returns the prior accumulator) then the update future
        getFut.complete((long) idx);
        CompletableFuture<Void> updFut = new CompletableFuture<>(); // VALUE_UPDATE request surrogate
        updFut.complete(null);
        // Force the per-record futures to ESCAPE the call site (the AEC holds them in its in-flight
        // buffers), defeating scalar replacement so the allocation cost is actually measured.
        escapeRing[ringPos] = getFut;
        ringPos = (ringPos + 1) & (RING - 1);
        escapeRing[ringPos] = updFut;
        ringPos = (ringPos + 1) & (RING - 1);
        escapeRing[ringPos] = chain;
        ringPos = (ringPos + 1) & (RING - 1);
        bh.consume(chain);
        bh.consume(updFut);
    }

    // -----------------------------------------------------------------
    // Arm B — the lever: value-RMW cache HIT (zero StateRequest, zero future, zero engine)
    // -----------------------------------------------------------------

    /**
     * The proposed q17 lever's hot path: serialize the composite key (same cost as Arm A) + fold the
     * input into the cache in place. On a HIT this returns a completed future immediately — no
     * StateRequest, no future-chain, no engine round-trip. This is exactly {@code
     * ForStRsAsyncReducingStateV2#asyncAdd}'s {@code tryFold → completedVoidFuture()} path, applied
     * to the ValueState accumulator q17 uses.
     */
    @Benchmark
    public void valueRmwCacheHit(Blackhole bh) {
        int idx = rng.nextInt(hotKeys);
        long auction = auctionIds[idx];
        int len = serializeKey(auction);
        boolean hit = cache.tryFold(keyOut.getSharedBuffer(), 0, len, idx);
        bh.consume(hit);
    }

    /**
     * Control: the shared key-serialization cost alone, so {@code aecPathFloor − keySerializeOnly}
     * and {@code valueRmwCacheHit − keySerializeOnly} isolate the coordination delta from the
     * per-record key work both arms pay.
     */
    @Benchmark
    public void keySerializeOnly(Blackhole bh) {
        int idx = rng.nextInt(hotKeys);
        int len = serializeKey(auctionIds[idx]);
        bh.consume(len);
        bh.consume(keyOut.getSharedBuffer());
    }
}

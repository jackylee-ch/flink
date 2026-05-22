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

import org.apache.flink.state.forstrs.state.ArrowBinaryBuffer;

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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH bench (PR-F4) for the V1-sync {@link ArrowBinaryBuffer}. Closes B3-JMH: the legacy bench
 * files never exercised the off-heap insert / find / remove path even though it is the hot path
 * for V1-sync ValueState / MapState / ListState write buffering.
 *
 * <p>Three @Benchmark methods cover the three primary public ops:
 *
 * <ul>
 *   <li>{@link #insertHot} — append a new (key, value) into the buffer; measures the per-row
 *       overhead of the hash-index probe + offsets/data writes.
 *   <li>{@link #findHit} — locate an existing row; measures the open-addressed probe + zero-alloc
 *       {@link MemorySegment#mismatch} compare.
 *   <li>{@link #removeHit} — mark an existing row as tombstoned; measures the same probe path as
 *       find plus a single sentinel write.
 * </ul>
 *
 * <p>Capacity is parameterised across the practical operating range:
 *
 * <ul>
 *   <li>{@code 1024} — the {@link ArrowBinaryBuffer#MIN_CAPACITY} corner;
 *   <li>{@code 65536} — typical mid-range tuning;
 *   <li>{@code 1048576} — {@link ArrowBinaryBuffer#MAX_CAPACITY}, the auto-tuner saturation
 *       point. Reveals any L1-/L2-miss tax at maximum live-row count.
 * </ul>
 *
 * <p>Invoke via the {@code jmh} maven profile. Not part of {@code mvn test}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class ArrowBufferJmhBenchmark {

    /**
     * Live rows pre-populated for the find / remove benches. The insert bench creates its own
     * fresh buffer per trial so it can grow naturally.
     */
    @Param({"1024", "65536", "1048576"})
    public int capacity;

    @Param({"32"})
    public int keyBytes;

    @Param({"128"})
    public int valueBytes;

    private Arena arena;

    // Buffer pre-loaded to ~50% occupancy for find / remove benches.
    private ArrowBinaryBuffer findBuffer;
    private MemorySegment keyPoolSeg;
    private int[] keyOffsetsHot; // offset into keyPoolSeg for the i-th probe
    private int populated;

    // Per-iteration insert buffer (recreated each invocation level for the insert bench so we
    // don't measure cumulative buffer-growth cost; see {@link #setUpInsertIteration}).
    private ArrowBinaryBuffer insertBuffer;
    private MemorySegment insertKeyPool;
    private MemorySegment insertValuePool;
    private int insertCursor;

    private SplittableRandom rng;

    @Setup(Level.Trial)
    public void setUpTrial() {
        this.arena = Arena.ofShared();
        this.rng = new SplittableRandom(0xBADCAFE);

        // ---- Build the find / remove buffer ----
        this.findBuffer = new ArrowBinaryBuffer(capacity, capacity);
        this.populated = Math.max(1, capacity / 2);

        long keyPoolBytes = (long) populated * keyBytes;
        this.keyPoolSeg = arena.allocate(keyPoolBytes);
        this.keyOffsetsHot = new int[populated];
        // Compose deterministic keys into the pool segment so we have stable MemorySegment-view
        // arguments to pass to find() — matches production where keys live in an Arrow column,
        // not in heap byte[]s.
        MemorySegment valueSeg = arena.allocate(valueBytes);
        for (int i = 0; i < valueBytes; i++) {
            valueSeg.set(ValueLayout.JAVA_BYTE, i, (byte) (i & 0xff));
        }
        for (int i = 0; i < populated; i++) {
            long off = (long) i * keyBytes;
            keyOffsetsHot[i] = (int) off;
            for (int p = 0; p < keyBytes; p++) {
                keyPoolSeg.set(
                        ValueLayout.JAVA_BYTE,
                        off + p,
                        (byte) ((p * 31 + i) & 0xff));
            }
            findBuffer.insert(keyPoolSeg, off, keyBytes, valueSeg, 0L, valueBytes);
        }

        // ---- Pool for the insert bench ----
        long insertKeyPoolBytes = (long) capacity * keyBytes;
        long insertValPoolBytes = (long) capacity * valueBytes;
        this.insertKeyPool = arena.allocate(insertKeyPoolBytes);
        this.insertValuePool = arena.allocate(insertValPoolBytes);
        // Fill with deterministic content so each insert sees a fresh key.
        for (long i = 0; i < insertKeyPoolBytes; i++) {
            insertKeyPool.set(ValueLayout.JAVA_BYTE, i, (byte) (i & 0xff));
        }
        for (long i = 0; i < insertValPoolBytes; i++) {
            insertValuePool.set(ValueLayout.JAVA_BYTE, i, (byte) ((i * 7) & 0xff));
        }
        this.insertCursor = 0;
        this.insertBuffer = new ArrowBinaryBuffer(capacity, capacity);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        try {
            findBuffer.close();
        } catch (Throwable ignored) {
            // closed buffers may have been resized; ignore double-close
        }
        try {
            insertBuffer.close();
        } catch (Throwable ignored) {
            // ditto
        }
        arena.close();
    }

    /**
     * Re-create the insert buffer between iterations so we always measure the same operating
     * regime (newly-created buffer, climbing toward capacity). Without this hook, JMH's
     * within-iteration loop would push the buffer to {@code size==capacity} after a few thousand
     * inserts and then every subsequent insert would either overwrite (much cheaper) or throw on
     * the maxCapacity bound.
     */
    @Setup(Level.Iteration)
    public void setUpInsertIteration() {
        if (insertBuffer != null) {
            insertBuffer.clear();
        }
        insertCursor = 0;
    }

    // -----------------------------------------------------------------
    // @Benchmark methods
    // -----------------------------------------------------------------

    /** Insert a fresh (key, value) row. Measures ns/insert on the hot path. */
    @Benchmark
    public void insertHot(Blackhole bh) {
        int slot = insertCursor;
        if (slot >= capacity) {
            // Wrap around — buffer at capacity, the next insert would auto-grow (above max) or
            // overwrite. To keep the bench measuring a single operating regime we reset.
            insertBuffer.clear();
            insertCursor = 0;
            slot = 0;
        }
        long kOff = (long) slot * keyBytes;
        long vOff = (long) slot * valueBytes;
        int row = insertBuffer.insert(
                insertKeyPool, kOff, keyBytes, insertValuePool, vOff, valueBytes);
        insertCursor++;
        bh.consume(row);
    }

    /** Hit-path find: every lookup matches an existing row. Measures ns/probe-and-equal. */
    @Benchmark
    public void findHit(Blackhole bh) {
        int idx = rng.nextInt(populated);
        long off = keyOffsetsHot[idx];
        int row = findBuffer.find(keyPoolSeg, off, keyBytes);
        bh.consume(row);
    }

    /**
     * Miss-path find: probes an unknown key. Measures the worst-case open-addressed scan-to-empty
     * (drives the hash collision tax visible in production at high occupancy).
     */
    @Benchmark
    public void findMiss(Blackhole bh) {
        // Use a fresh key constructed from the insert-key-pool which was NOT inserted into
        // findBuffer — guaranteed miss.
        int slot = rng.nextInt(capacity);
        long off = (long) slot * keyBytes;
        int row = findBuffer.find(insertKeyPool, off, keyBytes);
        bh.consume(row);
    }

    /** Hit-path remove: mark an existing row tombstoned. */
    @Benchmark
    public void removeHit(Blackhole bh) {
        int idx = rng.nextInt(populated);
        long off = keyOffsetsHot[idx];
        findBuffer.remove(keyPoolSeg, off, keyBytes);
        // Re-insert immediately so subsequent iterations have something to remove (the JMH loop
        // would otherwise drain the buffer). Cost of insert is amortised into the bench but
        // declared explicitly here so the measurement remains stable across iteration counts.
        MemorySegment valueSeg = insertValuePool;
        findBuffer.insert(keyPoolSeg, off, keyBytes, valueSeg, 0L, valueBytes);
        bh.consume(idx);
    }
}

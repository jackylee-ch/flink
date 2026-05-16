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

package org.apache.flink.state.forstrs.bench;

import org.openjdk.jmh.annotations.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

/**
 * Component microbenches gating V1 forst-rs vectorized parity implementation.
 *
 * <p>Run via the JMH profile:
 * <pre>
 *   mvn -B -pl flink-state-backends/flink-statebackend-forst-rs -Pjmh test-compile exec:exec \
 *     -Dexec.executable=java \
 *     -Dexec.args='-cp %classpath \
 *       --enable-native-access=ALL-UNNAMED \
 *       --add-modules jdk.incubator.vector \
 *       org.openjdk.jmh.Main \
 *       org.apache.flink.state.forstrs.bench.ComponentMicrobench'
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = {
    "--enable-native-access=ALL-UNNAMED",
    "--add-modules", "jdk.incubator.vector",
    "-XX:+UseZGC",
    "-XX:+UseCompactObjectHeaders"
})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class ComponentMicrobench {

    private static final long SLOT_TURN_BYTES = 8L * 1024 * 1024;

    private Arena slotArena;
    private MemorySegment turnRegion;
    private long bumpOffset;
    private java.util.ArrayDeque<Object> stubBatch;
    private java.util.concurrent.ConcurrentHashMap<Long, Object> stubMisses;

    @Setup(Level.Trial)
    public void setup() {
        slotArena = Arena.ofShared();
        turnRegion = slotArena.allocate(SLOT_TURN_BYTES, 64);
        bumpOffset = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        slotArena.close();
    }

    @Setup(Level.Iteration)
    public void perIterSetup() {
        stubBatch = new java.util.ArrayDeque<>(64);
        stubMisses = new java.util.concurrent.ConcurrentHashMap<>();
    }

    /**
     * Empty turn round-trip: save bump offset, restore bump offset.
     * Models SlotArenaScope.enter()/exit() for a turn with no allocations.
     * Target: ≤ 200 ns (per Spec Appendix — Pre-implementation validation gate).
     */
    @Benchmark
    public long emptyTurnRoundTrip() {
        long mark = bumpOffset;
        // SlotArenaScope.enter() — nothing to do for empty turn
        // SlotArenaScope.exit() — restore mark
        bumpOffset = mark;
        return mark;
    }

    /**
     * Bump-allocate a 256B aligned slice on the turn region.
     * Models the hot path for state ops that allocate transient buffers.
     * Target: ≤ 50 ns (per Spec Appendix).
     */
    @Benchmark
    public MemorySegment turnRegionAllocate256B() {
        long off = (bumpOffset + 63) & ~63L;
        MemorySegment seg = turnRegion.asSlice(off, 256);
        bumpOffset = off + 256;
        // Reset before overflow to keep the bench measuring steady-state alloc cost
        if (bumpOffset > SLOT_TURN_BYTES - 4096) {
            bumpOffset = 0;
        }
        return seg;
    }

    /**
     * Encode a composite state-key: kg(2B) | sk(8B) | '/' | stateName(8B) | '/' | uk(8B).
     * Total ~28 B per key — representative of MapState user-key encoding.
     * Target: ≤ 100 ns (per Spec Appendix).
     */
    @Benchmark
    public long encodeKeyInto() {
        long off = (bumpOffset + 63) & ~63L;
        turnRegion.set(java.lang.foreign.ValueLayout.JAVA_SHORT, off, (short) 42);
        // Inline state-name + separators
        turnRegion.set(java.lang.foreign.ValueLayout.JAVA_LONG, off + 2, 0x2F6D795374617465L); // "/myState"
        turnRegion.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off + 10, (byte) '/');
        turnRegion.set(java.lang.foreign.ValueLayout.JAVA_LONG, off + 12, 0xDEADBEEFCAFEBABEL);
        bumpOffset = off + 28;
        if (bumpOffset > SLOT_TURN_BYTES - 4096) {
            bumpOffset = 0;
        }
        return bumpOffset;
    }

    /**
     * Encode a 256 B value into the turn region by byte-fill.
     * Models small-value PUT path serialization cost.
     * Target: ≤ 150 ns (per Spec Appendix).
     */
    @Benchmark
    public long encodeValueInto256B() {
        long off = (bumpOffset + 63) & ~63L;
        for (int i = 0; i < 256; i++) {
            turnRegion.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off + i, (byte) (i & 0xFF));
        }
        bumpOffset = off + 256;
        if (bumpOffset > SLOT_TURN_BYTES - 4096) {
            bumpOffset = 0;
        }
        return bumpOffset;
    }

    /**
     * Stub for classifier.submit(): append to an in-flight batch, flush at 64.
     * Real VectorizedClassifier work measured post-P5 once state ops are migrated.
     * Target: ≤ 100 ns (per Spec Appendix).
     */
    @Benchmark
    public Object classifierSubmitStub() {
        Object request = new Object();
        stubBatch.add(request);
        if (stubBatch.size() >= 64) {
            stubBatch.clear();
        }
        return request;
    }

    /**
     * Stub for executor.dispatch(): write 64 ints into the batch buffer, read them back.
     * Models 64-row batch round-trip without FFI. Target: ≤ 5 µs total ≈ 80 ns/row.
     */
    @Benchmark
    public int executorDispatchStub() {
        long off = (bumpOffset + 63) & ~63L;
        for (int i = 0; i < 64; i++) {
            turnRegion.set(java.lang.foreign.ValueLayout.JAVA_INT, off + i * 8L, i);
        }
        int sum = 0;
        for (int i = 0; i < 64; i++) {
            sum += turnRegion.get(java.lang.foreign.ValueLayout.JAVA_INT, off + i * 8L);
        }
        bumpOffset = off + 64L * 8;
        if (bumpOffset > SLOT_TURN_BYTES - 4096) {
            bumpOffset = 0;
        }
        return sum;
    }

    /**
     * Stub for pendingMisses.computeIfAbsent (hit case — same key every call).
     * Target: ≤ 50 ns (per Spec Appendix).
     */
    @Benchmark
    public Object pendingMissComputeIfAbsentHit() {
        return stubMisses.computeIfAbsent(42L, k -> new Object());
    }

    /**
     * Stub for pendingMisses.computeIfAbsent (miss case — new key every call).
     * Target: ≤ 200 ns (per Spec Appendix).
     */
    @Benchmark
    public Object pendingMissComputeIfAbsentMiss() {
        return stubMisses.computeIfAbsent(System.nanoTime(), k -> new Object());
    }
}

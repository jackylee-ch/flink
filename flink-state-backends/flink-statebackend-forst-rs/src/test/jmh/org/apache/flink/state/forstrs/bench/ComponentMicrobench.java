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
}

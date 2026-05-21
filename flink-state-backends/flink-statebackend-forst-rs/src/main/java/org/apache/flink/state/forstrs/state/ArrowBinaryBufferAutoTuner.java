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

package org.apache.flink.state.forstrs.state;

import org.apache.flink.annotation.Internal;

/**
 * Dual-gate (hit-rate + occupancy) grow/shrink policy for {@link ArrowBinaryBuffer}.
 *
 * <p>Samples every SAMPLE_WINDOW reads; on each window:
 *
 * <ul>
 *   <li>hit-rate &ge; {@link #GROW_RATE} (0.80) AND occupancy &ge; {@link #GROW_OCCUPANCY}
 *       (0.70) AND capacity &lt; {@link ArrowBinaryBuffer#MAX_CAPACITY} &rarr; 2&times; grow.
 *   <li>hit-rate &le; {@link #SHRINK_RATE} (0.30) AND occupancy &le;
 *       {@link #SHRINK_OCCUPANCY} (0.20) AND capacity &gt;
 *       {@link ArrowBinaryBuffer#MIN_CAPACITY} &rarr; 0.5&times; shrink.
 *   <li>otherwise: no change (hysteresis zone).
 * </ul>
 *
 * <p>The occupancy gate prevents the small-working-set / high-hit-rate workloads (e.g. Q11
 * session-window accumulators) from triggering wasteful resize events: hit rate at any cap is
 * ~100% because the same accumulator key is repeatedly touched, but the buffer's effective
 * occupancy stays low — without the occupancy gate the tuner would grow the buffer all the way
 * to MAX_CAPACITY paying the per-resize Arena allocation + hashIndex rebuild cost at each step.
 */
@Internal
public final class ArrowBinaryBufferAutoTuner {

    static final int SAMPLE_WINDOW = 1024;
    static final double GROW_RATE = 0.80;
    static final double SHRINK_RATE = 0.30;
    static final double GROW_OCCUPANCY = 0.70;
    static final double SHRINK_OCCUPANCY = 0.20;

    private int hits;
    private int samples;

    /** Last observed buffer fill from {@link #observeRead}, used by the occupancy gate. */
    private int lastSize;

    /** Last observed buffer capacity from {@link #observeRead}, used by the occupancy gate. */
    private int lastCapacity;

    @SuppressWarnings("unused")
    private final int initialCapacity;

    public ArrowBinaryBufferAutoTuner(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    /**
     * Records a single read observation: whether it hit the off-heap buffer, plus the current
     * size + capacity (used by {@link #shouldResizeTo} to compute occupancy).
     */
    public void observeRead(boolean wasHit, int currentSize, int currentCapacity) {
        if (wasHit) {
            hits++;
        }
        samples++;
        this.lastSize = currentSize;
        this.lastCapacity = currentCapacity;
    }

    /**
     * Decides the next capacity for the buffer, based on the current accumulated hit-rate AND
     * the last observed occupancy if the SAMPLE_WINDOW is full. If the window isn't full yet,
     * returns currentCapacity unchanged. After this call, the sample counters reset.
     */
    public int shouldResizeTo(int currentCapacity) {
        if (samples < SAMPLE_WINDOW) {
            return currentCapacity;
        }
        double rate = (double) hits / samples;
        double occupancy = lastCapacity > 0 ? (double) lastSize / lastCapacity : 0.0;
        hits = 0;
        samples = 0;
        if (rate >= GROW_RATE
                && occupancy >= GROW_OCCUPANCY
                && currentCapacity < ArrowBinaryBuffer.MAX_CAPACITY) {
            return Math.min(currentCapacity * 2, ArrowBinaryBuffer.MAX_CAPACITY);
        }
        if (rate <= SHRINK_RATE
                && occupancy <= SHRINK_OCCUPANCY
                && currentCapacity > ArrowBinaryBuffer.MIN_CAPACITY) {
            return Math.max(currentCapacity / 2, ArrowBinaryBuffer.MIN_CAPACITY);
        }
        return currentCapacity;
    }
}

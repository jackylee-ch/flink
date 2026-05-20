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
 * Hit-rate-driven grow/shrink policy for {@link ArrowBinaryBuffer}. Samples every
 * SAMPLE_WINDOW reads; on each window:
 *
 * <ul>
 *   <li>hit-rate &ge; GROW_RATE (0.80) AND capacity &lt; MAX_CAPACITY &rarr; 2&times; grow
 *   <li>hit-rate &le; SHRINK_RATE (0.30) AND capacity &gt; MIN_CAPACITY &rarr; 0.5&times; shrink
 *   <li>otherwise: no change (hysteresis zone)
 * </ul>
 */
@Internal
public final class ArrowBinaryBufferAutoTuner {

    static final int SAMPLE_WINDOW = 1024;
    static final double GROW_RATE = 0.80;
    static final double SHRINK_RATE = 0.30;

    private int hits;
    private int samples;

    @SuppressWarnings("unused")
    private final int initialCapacity;

    public ArrowBinaryBufferAutoTuner(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    public void observeRead(boolean wasHit) {
        if (wasHit) {
            hits++;
        }
        samples++;
    }

    /**
     * Decides the next capacity for the buffer, based on the current accumulated hit-rate if the
     * SAMPLE_WINDOW is full. If the window isn't full yet, returns currentCapacity unchanged.
     * After this call, the sample counters reset.
     */
    public int shouldResizeTo(int currentCapacity) {
        if (samples < SAMPLE_WINDOW) {
            return currentCapacity;
        }
        double rate = (double) hits / samples;
        hits = 0;
        samples = 0;
        if (rate >= GROW_RATE && currentCapacity < ArrowBinaryBuffer.MAX_CAPACITY) {
            return Math.min(currentCapacity * 2, ArrowBinaryBuffer.MAX_CAPACITY);
        }
        if (rate <= SHRINK_RATE && currentCapacity > ArrowBinaryBuffer.MIN_CAPACITY) {
            return Math.max(currentCapacity / 2, ArrowBinaryBuffer.MIN_CAPACITY);
        }
        return currentCapacity;
    }
}

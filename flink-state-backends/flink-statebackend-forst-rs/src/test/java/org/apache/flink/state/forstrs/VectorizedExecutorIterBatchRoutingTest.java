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

package org.apache.flink.state.forstrs;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.assertj.core.api.Assertions.assertThat;

class VectorizedExecutorIterBatchRoutingTest {

    @Test
    void parallelIterRequiresExplicitGateLargeBatchAndRowsPerPrefix() {
        assertThat(VectorizedExecutor.shouldUseParallelIter(false, 256, 32.0d)).isFalse();
        assertThat(VectorizedExecutor.shouldUseParallelIter(true, 63, 32.0d)).isFalse();
        assertThat(VectorizedExecutor.shouldUseParallelIter(true, 64, 15.99d)).isFalse();
        assertThat(VectorizedExecutor.shouldUseParallelIter(true, 64, 16.0d)).isTrue();
    }

    @Test
    void initFrsChunkForOpenClearsStaleOutputFields() {
        try (Arena arena = Arena.ofConfined()) {
            long stride = ForStRsLinker.frsChunkLayoutByteSize();
            MemorySegment chunks = arena.allocate(stride);
            MemorySegment buf = arena.allocate(128);
            chunks.set(ValueLayout.JAVA_INT, 12L, 123);
            chunks.set(ValueLayout.JAVA_INT, 16L, 456);
            chunks.set(ValueLayout.JAVA_INT, 20L, ForStRsLinker.FRS_CHUNK_EOF);

            ForStRsLinker.initFrsChunkForOpen(chunks, 0, buf, 128);

            assertThat(chunks.get(ValueLayout.JAVA_INT, 8L)).isEqualTo(128);
            assertThat(ForStRsLinker.getFrsChunkRowCount(chunks, 0)).isZero();
            assertThat(ForStRsLinker.getFrsChunkBytesUsed(chunks, 0)).isZero();
            assertThat(ForStRsLinker.getFrsChunkReserved(chunks, 0)).isZero();
        }
    }
}

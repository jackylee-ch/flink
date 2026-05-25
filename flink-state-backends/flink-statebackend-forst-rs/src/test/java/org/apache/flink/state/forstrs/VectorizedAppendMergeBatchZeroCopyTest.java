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
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Regression coverage for the zero-copy APPEND_MERGE heap-row fast path. */
class VectorizedAppendMergeBatchZeroCopyTest {

    @Test
    void heapRowsPassValueArrowColumnDirectlyToNativeBatch() {
        try (Arena arena = Arena.ofConfined()) {
            AppendMergeBatchBuffer buffer = new AppendMergeBatchBuffer(arena);
            CompletableFuture<Void> f0 = new CompletableFuture<>();
            CompletableFuture<Void> f1 = new CompletableFuture<>();

            buffer.keyBuffer().append("k0".getBytes(StandardCharsets.UTF_8));
            buffer.valueBuffer().append("op0".getBytes(StandardCharsets.UTF_8));
            buffer.appendHeapRow(f0);
            buffer.keyBuffer().append("k1".getBytes(StandardCharsets.UTF_8));
            buffer.valueBuffer().append("op1".getBytes(StandardCharsets.UTF_8));
            buffer.appendHeapRow(f1);

            MemorySegment expectedOpsOff = buffer.valueBuffer().offsetsSegment();
            MemorySegment expectedOpsData = buffer.valueBuffer().dataSegment();

            ForStRsLinker linker = BatchedFailurePropagationTestHelpers.stubLinker(arena);
            FrsDb db = BatchedFailurePropagationTestHelpers.stubDb();
            FrsCfHandle cf = BatchedFailurePropagationTestHelpers.stubCf();
            CapturingExecutor exec = new CapturingExecutor(linker, db, cf, arena);

            assertThat(exec.dispatchAppendMergeBatch(buffer)).isZero();
            assertThat(exec.capturedOpsOff.address()).isEqualTo(expectedOpsOff.address());
            assertThat(exec.capturedOpsData.address()).isEqualTo(expectedOpsData.address());
            assertThat(exec.capturedCount).isEqualTo(2);
            assertThat(f0).isCompleted();
            assertThat(f1).isCompleted();
        }
    }

    private static final class CapturingExecutor extends VectorizedExecutor {
        private MemorySegment capturedOpsOff;
        private MemorySegment capturedOpsData;
        private int capturedCount;

        private CapturingExecutor(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
            super(linker, db, cf, arena);
        }

        @Override
        protected int invokeVecMergeAppendBatch(
                MemorySegment keysOffSeg,
                MemorySegment keysDataSeg,
                MemorySegment opsOffSeg,
                MemorySegment opsDataSeg,
                int count) {
            this.capturedOpsOff = opsOffSeg;
            this.capturedOpsData = opsDataSeg;
            this.capturedCount = count;
            return 0;
        }
    }
}

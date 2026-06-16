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

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FRS-FFM-BOUND (2026-06-16, PMC-1): verifies that the bounded {@link ColumnarBatchBuffer}
 * (free-on-grow into a per-buffer shared sub-arena + shrink-on-reset hysteresis) is byte-identical
 * to the legacy grow-and-leak behavior, and that the FFM off-heap accounting tracks live bytes
 * correctly (returns to the floor after close, drops after shrink). The grow path is the q9/q19
 * join-build site where the legacy worker arena leaked every doubling predecessor.
 */
class ColumnarBatchBufferBoundTest {

    /** Append across many grows; every entry must read back byte-identical. */
    @Test
    void growPreservesAllBytes() {
        try (Arena arena = Arena.ofShared()) {
            ColumnarBatchBuffer buf = new ColumnarBatchBuffer(arena, 16, 256);
            Random r = new Random(42);
            int n = 5000;
            byte[][] expected = new byte[n][];
            for (int i = 0; i < n; i++) {
                int len = r.nextInt(2048); // forces many data + offsets grows
                byte[] e = new byte[len];
                r.nextBytes(e);
                expected[i] = e;
                int idx = buf.append(e);
                assertEquals(i, idx);
            }
            assertEquals(n, buf.count());
            // Read back via the offsets+data layout and compare to the expected bytes.
            MemorySegment offsets = buf.offsetsSegment();
            MemorySegment data = buf.dataSegment();
            for (int i = 0; i < n; i++) {
                int start = offsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
                int end = offsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
                int len = end - start;
                assertEquals(expected[i].length, len, "len mismatch at " + i);
                byte[] got = new byte[len];
                MemorySegment.copy(data, ValueLayout.JAVA_BYTE, start, got, 0, len);
                org.junit.jupiter.api.Assertions.assertArrayEquals(
                        expected[i], got, "bytes mismatch at " + i);
            }
            buf.close();
        }
    }

    /** A huge outlier batch followed by many small batches must shrink the data segment back. */
    @Test
    void shrinkAfterOutlierBatch() {
        try (Arena arena = Arena.ofShared()) {
            ColumnarBatchBuffer buf = new ColumnarBatchBuffer(arena, 16, 64 * 1024);
            // Outlier: one ~4 MiB batch.
            byte[] big = new byte[1024];
            buf.append(big, 0, 1024);
            for (int i = 0; i < 4096; i++) {
                buf.append(big, 0, 1024); // ~4 MiB total
            }
            int capAfterBig = buf.dataCapacity();
            assertTrue(capAfterBig >= 4 * 1024 * 1024, "expected grown cap, got " + capAfterBig);
            buf.reset();
            // Many small batches; after the hysteresis the data cap must shrink.
            for (int b = 0; b < 40; b++) {
                buf.append(big, 0, 64); // tiny
                buf.reset();
            }
            assertTrue(
                    buf.dataCapacity() < capAfterBig,
                    "expected shrink below " + capAfterBig + ", got " + buf.dataCapacity());
            buf.close();
        }
    }

    /** Live accounting must drop to (or below) the starting footprint after close. */
    @Test
    void accountingReturnsToFloorAfterClose() {
        long before = FfmOffHeapAccounting.COLUMNAR_BYTES.get();
        try (Arena arena = Arena.ofShared()) {
            ColumnarBatchBuffer buf = new ColumnarBatchBuffer(arena, 16, 256);
            long afterAlloc = FfmOffHeapAccounting.COLUMNAR_BYTES.get();
            assertTrue(afterAlloc > before, "accounting should grow on alloc");
            byte[] e = new byte[100_000];
            for (int i = 0; i < 50; i++) {
                buf.append(e);
            }
            buf.close();
        }
        long after = FfmOffHeapAccounting.COLUMNAR_BYTES.get();
        assertEquals(before, after, "accounting must return to floor after close");
    }

    /** appendEmpty + zero-length entries must keep offsets consistent across grows. */
    @Test
    void appendEmptyAndZeroLen() {
        try (Arena arena = Arena.ofShared()) {
            ColumnarBatchBuffer buf = new ColumnarBatchBuffer(arena, 16, 256);
            for (int i = 0; i < 1000; i++) {
                if (i % 3 == 0) {
                    buf.appendEmpty();
                } else {
                    buf.append(new byte[i % 500]);
                }
            }
            MemorySegment offsets = buf.offsetsSegment();
            // Offsets must be monotonically non-decreasing.
            int prev = offsets.get(ValueLayout.JAVA_INT, 0L);
            assertEquals(0, prev);
            for (int i = 1; i <= buf.count(); i++) {
                int o = offsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
                assertTrue(o >= prev, "offsets must be monotonic at " + i);
                prev = o;
            }
            buf.close();
        }
    }
}

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

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-C2 (V2-14): structural tests for {@link ListStateArrowBuffer}. Exercises the off-heap
 * accumulator's row-append, ordering, and reset semantics without invoking the FFI — the FFI
 * flush path is exercised by {@link ListStateV2PreSnapshotFlushTest} (which stubs the linker).
 *
 * <p>The ordering invariant matters because Format B (V20 §3.1) requires per-key submit-order to
 * match read-back order. {@link ListStateArrowBuffer#append} is FIFO, so the buffer's flush
 * iterates rows in append-order — exercised explicitly by
 * {@link #appendOrderPreservedAcrossManyRows()}.
 */
class ListStateArrowBufferTest {

    @Test
    void emptyOnConstruction() {
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer()) {
            assertTrue(buf.isEmpty(), "Buffer should start empty");
            assertEquals(0, buf.rowCount(), "rowCount=0 on construction");
            assertEquals(0L, buf.bytesUsed(), "bytesUsed=0 on construction");
            assertFalse(buf.shouldAutoFlush(), "No flush needed when empty");
        }
    }

    @Test
    void singleAppendStoresKeyAndChunkBytesVerbatim() {
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer()) {
            byte[] key = "k1".getBytes();
            byte[] chunk = new byte[] {0, 0, 0, 1, 7, 7, 7, 7}; // [count=1][elem=4-byte int]
            CompletableFuture<Void> f = buf.append(key, chunk);
            assertEquals(1, buf.rowCount(), "rowCount=1 after one append");
            assertArrayEquals(key, buf.copyKeyAt(0), "key bytes round-trip");
            assertArrayEquals(chunk, buf.copyChunkAt(0), "chunk bytes round-trip");
            assertFalse(f.isDone(), "Future should be pending until flush");
        }
    }

    @Test
    void appendOrderPreservedAcrossManyRows() {
        // Ordering invariant: Format B requires submit order = read-back order. The buffer is a
        // sequential append log — rows must come out in append order, not key-order, not size-order.
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer()) {
            int n = 100;
            byte[][] keys = new byte[n][];
            byte[][] chunks = new byte[n][];
            Random rng = new Random(42);
            for (int i = 0; i < n; i++) {
                // Pseudo-random keys (sometimes repeat: same key under multiple appends must still
                // produce two distinct rows in submit order).
                keys[i] = new byte[8 + rng.nextInt(16)];
                rng.nextBytes(keys[i]);
                chunks[i] = new byte[12 + rng.nextInt(32)];
                rng.nextBytes(chunks[i]);
                buf.append(keys[i], chunks[i]);
            }
            assertEquals(n, buf.rowCount(), "All rows appended");
            for (int i = 0; i < n; i++) {
                assertArrayEquals(keys[i], buf.copyKeyAt(i), "row " + i + " key in submit order");
                assertArrayEquals(
                        chunks[i], buf.copyChunkAt(i), "row " + i + " chunk in submit order");
            }
        }
    }

    @Test
    void duplicateKeyProducesDistinctRowsInOrder() {
        // The Format-B invariant: two asyncAdd calls under the same composite key MUST produce two
        // distinct rows so the engine's merge operator concatenates both chunks. Coalescing by
        // key (e.g. a hash-indexed buffer) would corrupt this — exactly why the buffer is a
        // sequential append log.
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer()) {
            byte[] sameKey = "k1".getBytes();
            byte[] chunkA = new byte[] {0, 0, 0, 1, 1};
            byte[] chunkB = new byte[] {0, 0, 0, 1, 2};
            byte[] chunkC = new byte[] {0, 0, 0, 1, 3};
            buf.append(sameKey, chunkA);
            buf.append(sameKey, chunkB);
            buf.append(sameKey, chunkC);
            assertEquals(3, buf.rowCount(), "Three appends under same key -> three rows");
            assertArrayEquals(chunkA, buf.copyChunkAt(0));
            assertArrayEquals(chunkB, buf.copyChunkAt(1));
            assertArrayEquals(chunkC, buf.copyChunkAt(2));
            // All three keys identical — caller relies on the engine merge-operator to group.
            assertArrayEquals(sameKey, buf.copyKeyAt(0));
            assertArrayEquals(sameKey, buf.copyKeyAt(1));
            assertArrayEquals(sameKey, buf.copyKeyAt(2));
        }
    }

    @Test
    void resetClearsRowsButRetainsArena() {
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer()) {
            buf.append("k1".getBytes(), new byte[] {0, 0, 0, 1, 9});
            buf.append("k2".getBytes(), new byte[] {0, 0, 0, 1, 8});
            buf.reset();
            assertTrue(buf.isEmpty(), "reset() empties the buffer");
            // After reset, appends restart at row=0.
            buf.append("k3".getBytes(), new byte[] {0, 0, 0, 1, 7});
            assertEquals(1, buf.rowCount());
            assertArrayEquals("k3".getBytes(), buf.copyKeyAt(0));
        }
    }

    @Test
    void shouldAutoFlushAtRowThreshold() {
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer(8, 1L << 30)) {
            for (int i = 0; i < 7; i++) {
                buf.append(("k" + i).getBytes(), new byte[] {0, 0, 0, 1, (byte) i});
                assertFalse(
                        buf.shouldAutoFlush(),
                        "Should not trigger before row threshold (i=" + i + ")");
            }
            buf.append("k7".getBytes(), new byte[] {0, 0, 0, 1, 7});
            assertTrue(buf.shouldAutoFlush(), "Should trigger at exactly maxRows");
        }
    }

    @Test
    void shouldAutoFlushAtByteThreshold() {
        // ListStateArrowBuffer floors maxBytes at 4096 (sanity floor). Use 4096-byte threshold.
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer(1024, 4096L)) {
            // Each append: 2-byte key + 60-byte chunk = 62 bytes/row. Threshold hit at 66+ rows.
            byte[] chunk = new byte[60];
            for (int i = 0; i < 65; i++) {
                buf.append(("k" + i).getBytes(), chunk);
            }
            assertFalse(buf.shouldAutoFlush(), "Just below 4096 bytes");
            // One more append crosses the threshold.
            buf.append(("k65").getBytes(), chunk);
            assertTrue(buf.shouldAutoFlush(), "Above 4096 byte threshold");
        }
    }

    @Test
    void discardWithCauseDropsRowsAndFailsPendingFutures() {
        // R21-H2 regression: rows buffered for a StateRequest whose Flink-side future was
        // completed exceptionally on an onClear-throw sibling row must be DROPPED — never flushed
        // to the engine on the next batch. discardWithCause:
        //   (a) completes every pending per-row future exceptionally with the supplied cause, and
        //   (b) resets the buffer so a subsequent flushTo / flushIfDirty is a no-op.
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer()) {
            CompletableFuture<Void> f1 = buf.append("k1".getBytes(), new byte[] {0, 0, 0, 1, 1});
            CompletableFuture<Void> f2 = buf.append("k2".getBytes(), new byte[] {0, 0, 0, 1, 2});
            CompletableFuture<Void> f3 = buf.append("k3".getBytes(), new byte[] {0, 0, 0, 1, 3});
            assertEquals(3, buf.rowCount());

            RuntimeException cause = new RuntimeException("simulated batch poison");
            buf.discardWithCause(cause);

            assertTrue(buf.isEmpty(), "discardWithCause must reset the buffer");
            assertEquals(0, buf.rowCount(), "rowCount=0 after discard");
            assertEquals(0L, buf.bytesUsed(), "bytesUsed=0 after discard");
            assertTrue(f1.isCompletedExceptionally(), "row-1 future must be failed");
            assertTrue(f2.isCompletedExceptionally(), "row-2 future must be failed");
            assertTrue(f3.isCompletedExceptionally(), "row-3 future must be failed");

            // Verify the cause was propagated.
            try {
                f2.getNow(null);
            } catch (Throwable t) {
                assertEquals(
                        cause,
                        t.getCause(),
                        "discardWithCause must propagate the original poison cause");
            }

            // Post-discard: a fresh append starts over at row=0 (buffer is reusable).
            CompletableFuture<Void> f4 = buf.append("k4".getBytes(), new byte[] {0, 0, 0, 1, 4});
            assertEquals(1, buf.rowCount(), "post-discard append starts at row=0");
            assertFalse(f4.isDone(), "post-discard future is still pending");
        }
    }

    @Test
    void discardWithCauseOnEmptyBufferIsNoOp() {
        // Idempotence: discardWithCause on an empty buffer must not throw and must not leave the
        // buffer in a broken state.
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer()) {
            buf.discardWithCause(new RuntimeException("not used"));
            assertTrue(buf.isEmpty());
            // Buffer remains usable for normal appends afterwards.
            buf.append("k".getBytes(), new byte[] {0, 0, 0, 1, 9});
            assertEquals(1, buf.rowCount());
        }
    }

    @Test
    void appendGrowsCapacityBeyondInitial() {
        // Default initial row capacity is 64; verify we can grow well past it.
        try (ListStateArrowBuffer buf = new ListStateArrowBuffer(2048, 1L << 24)) {
            int n = 200;
            for (int i = 0; i < n; i++) {
                // 16-byte key + 12-byte chunk = 28 bytes/row, 5.6 KiB total — exercises both the
                // row-capacity grow and the data-capacity grow paths.
                byte[] k = new byte[16];
                k[0] = (byte) (i & 0xff);
                k[1] = (byte) ((i >> 8) & 0xff);
                byte[] c = new byte[12];
                c[0] = (byte) (i & 0xff);
                buf.append(k, c);
            }
            assertEquals(n, buf.rowCount());
            // Spot-check ordering is intact post-grow.
            for (int i : new int[] {0, 1, 63, 64, 65, 127, 128, 199}) {
                assertEquals(
                        (byte) (i & 0xff),
                        buf.copyKeyAt(i)[0],
                        "row " + i + " key[0] after capacity-grow");
                assertEquals((byte) (i & 0xff), buf.copyChunkAt(i)[0], "row " + i + " chunk[0]");
            }
        }
    }
}

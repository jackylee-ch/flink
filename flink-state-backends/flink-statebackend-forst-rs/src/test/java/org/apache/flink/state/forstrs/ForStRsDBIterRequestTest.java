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

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link ForStRsDBIterRequest#process} uses the chunked vectorized iterator API
 * ({@code frs_vec_iter_prefix_next}) instead of the per-entry {@code iteratorNext} loop, and that
 * the drain path does not allocate a {@code byte[]} per entry (Commit B — {@link
 * IteratorEntryView} slice-based decode).
 */
class ForStRsDBIterRequestTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void process_uses_one_chunked_call_not_per_entry_loop() {
        ForStRsLinker linker = mock(ForStRsLinker.class);
        FrsDb db = mock(FrsDb.class);
        FrsCfHandle cf = mock(FrsCfHandle.class);

        // frs_vec_iter_prefix_open: write 0 rows / 0 bytes into the out-params and return OK.
        when(linker.frsVecIterPrefixOpen(
                        any(), any(), any(), anyInt(), any(), anyInt(), any(), any(), any()))
                .thenReturn(0); // FrsErrorCode.OK
        when(linker.frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any())).thenReturn(0);
        when(linker.frsVecIterPrefixClose(anyLong())).thenReturn(0);

        byte[] prefix = "k/test/".getBytes();
        ForStRsIterableState mockState = mock(ForStRsIterableState.class);
        StateRequest sr = mock(StateRequest.class);
        InternalAsyncFuture future = mock(InternalAsyncFuture.class);
        when(sr.getFuture()).thenReturn(future);

        ForStRsDBIterRequest<?, ?, ?, ?> req =
                new ForStRsDBIterRequest<>(prefix, sr, StateRequestType.MAP_ITER, mockState, null);

        try (Arena arena = Arena.ofConfined()) {
            req.process(linker, db, cf, arena);
        }

        verify(linker, times(1)).frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any());
        verify(linker, never()).iteratorNext(any());
        verify(linker, never()).prefixLookupOpen(any(), any(), any(), any());
    }

    /**
     * Commit B: the drain path must not allocate a {@code byte[]} per (key, value) pair. We prove
     * this by running {@code process()} twice with the same row count but different value sizes
     * (16 B vs 256 B). With slice-based views, the allocation delta is independent of value size
     * (views store only offset+length). With the legacy per-entry byte[] copy, value-bytes are
     * materialized into heap arrays — so increasing the value size by 240 B per row over 64 rows
     * would increase the alloc delta by ~15 KB.
     *
     * <p>We assert that the alloc delta from the small-value run and the large-value run agree to
     * within 4 KB. The legacy path would diverge by ~15 KB. This is a tighter, value-size-
     * independent invariant than a single absolute byte threshold.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void process_does_not_allocate_per_entry_byte_arrays() {
        long deltaSmall = runProcessAndMeasureAlloc(64, 8, 16);
        long deltaLarge = runProcessAndMeasureAlloc(64, 8, 256);
        long diff = Math.abs(deltaLarge - deltaSmall);
        // Slice path: diff is dominated by JVM bookkeeping noise (TLAB resizing, async sampler
        // intrusion). 4 KB is generous; the legacy byte[] path would diverge by ~15 KB.
        assertThat(diff)
                .as(
                        "alloc delta must be independent of value size (slice-based decode)."
                                + " small=%d large=%d diff=%d",
                        deltaSmall, deltaLarge, diff)
                .isLessThan(4_000);
    }

    /**
     * Multi-chunk drain must not corrupt earlier chunks' view data. Regression test for the
     * chunkBuf-reuse hazard: {@code frs_vec_iter_prefix_next} writes each chunk into the SAME
     * reusable {@code chunkBuf} segment, so if views from chunk #1 reference {@code chunkBuf}
     * directly, the second {@code next()} call overwrites their backing bytes before
     * {@code completeWithEntries} decodes them — entries 0..59 would decode to chunk #2's payload.
     *
     * <p>This test mocks the drain to return two distinct chunks (60 × "aaa"/"111", then 60 ×
     * "bbb"/"222", then 0/0 exhaustion). The recording state stub captures the bytes each view
     * yields at decode time. The FRS-ZERO-SNAPSHOT fix decodes each chunk's entries to detached
     * on-heap UK/UV IN PLACE over {@code chunkBuf} IMMEDIATELY after parsing it — BEFORE the next
     * {@code frs_vec_iter_prefix_next} overwrites the buffer with chunk #2 — so entries 0..59 must
     * yield ("aaa", "111") and entries 60..119 must yield ("bbb", "222"). The prior implementation
     * snapshotted each chunk to a fresh arena segment; this version removes that per-chunk native
     * alloc and instead relies on decode-before-overwrite ordering. Without correct ordering, all
     * 120 entries would yield the same (corrupted) bytes.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void process_multi_chunk_drain_no_corruption() {
        ForStRsLinker linker = mock(ForStRsLinker.class);
        FrsDb db = mock(FrsDb.class);
        FrsCfHandle cf = mock(FrsCfHandle.class);

        final byte[] keyA = "aaa".getBytes();
        final byte[] valA = "111".getBytes();
        final byte[] keyB = "bbb".getBytes();
        final byte[] valB = "222".getBytes();
        final int rowsPerChunk = 60;

        // Open seeds chunk #1 into chunkBuf and writes its row/byte counts to the out params.
        when(linker.frsVecIterPrefixOpen(
                        any(), any(), any(), anyInt(), any(), anyInt(), any(), any(), any()))
                .thenAnswer(
                        inv -> {
                            MemorySegment chunkBuf = inv.getArgument(4);
                            MemorySegment outHandle = inv.getArgument(6);
                            MemorySegment outRc = inv.getArgument(7);
                            MemorySegment outBu = inv.getArgument(8);
                            int written = writeRowsInto(chunkBuf, rowsPerChunk, keyA, valA);
                            outHandle.set(ValueLayout.JAVA_LONG, 0, 0xCAFEL);
                            outRc.set(ValueLayout.JAVA_INT, 0, rowsPerChunk);
                            outBu.set(ValueLayout.JAVA_INT, 0, written);
                            return 0;
                        });

        // next() returns chunk #2 on the first call, then exhaustion on the second.
        final int[] nextCallCount = new int[1];
        when(linker.frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any()))
                .thenAnswer(
                        inv -> {
                            MemorySegment chunkBuf = inv.getArgument(1);
                            MemorySegment outRc = inv.getArgument(3);
                            MemorySegment outBu = inv.getArgument(4);
                            int call = nextCallCount[0]++;
                            if (call == 0) {
                                int written = writeRowsInto(chunkBuf, rowsPerChunk, keyB, valB);
                                outRc.set(ValueLayout.JAVA_INT, 0, rowsPerChunk);
                                outBu.set(ValueLayout.JAVA_INT, 0, written);
                                return 0;
                            }
                            outRc.set(ValueLayout.JAVA_INT, 0, 0);
                            outBu.set(ValueLayout.JAVA_INT, 0, 0);
                            return 0;
                        });
        when(linker.frsVecIterPrefixClose(anyLong())).thenReturn(0);

        RecordingIterableState recState = new RecordingIterableState();
        StateRequest sr = mock(StateRequest.class);
        InternalAsyncFuture future = mock(InternalAsyncFuture.class);
        when(sr.getFuture()).thenReturn(future);
        byte[] prefix = "k/test/".getBytes();

        ForStRsDBIterRequest<?, ?, ?, ?> req =
                new ForStRsDBIterRequest<>(prefix, sr, StateRequestType.MAP_ITER, recState, null);

        try (Arena arena = Arena.ofConfined()) {
            req.process(linker, db, cf, arena);
        }

        // Two chunks * 60 rows each.
        assertThat(recState.observedKeys).hasSize(120);
        assertThat(recState.observedValues).hasSize(120);

        // Chunk #1 (0..59) must still decode to ("aaa", "111") — would be corrupted by chunk #2's
        // overwrite if views referenced chunkBuf directly.
        for (int i = 0; i < rowsPerChunk; i++) {
            assertThat(recState.observedKeys.get(i))
                    .as("key at index %d (chunk #1)", i)
                    .containsExactly(keyA);
            assertThat(recState.observedValues.get(i))
                    .as("value at index %d (chunk #1)", i)
                    .containsExactly(valA);
        }
        // Chunk #2 (60..119) must decode to ("bbb", "222").
        for (int i = rowsPerChunk; i < 2 * rowsPerChunk; i++) {
            assertThat(recState.observedKeys.get(i))
                    .as("key at index %d (chunk #2)", i)
                    .containsExactly(keyB);
            assertThat(recState.observedValues.get(i))
                    .as("value at index %d (chunk #2)", i)
                    .containsExactly(valB);
        }
    }

    /**
     * Lever-2 batched-open drain (single chunk): {@link ForStRsDBIterRequest#processFromBatchedOpen}
     * is handed a handle + an already-filled first chunk by the coalesced
     * {@code frs_vec_iter_prefix_open_batch_parallel} crossing, then drains via {@code _next}. The
     * supplied first chunk (60 × "aaa"/"111") must decode correctly; the single {@code next()} call
     * returns 0/0 exhaustion. Verifies the batched-open path uses the SAME zero-snapshot in-place
     * decode as the serial {@code process()} path.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void processFromBatchedOpen_single_chunk_decodes_correctly() {
        ForStRsLinker linker = mock(ForStRsLinker.class);
        FrsDb db = mock(FrsDb.class);
        FrsCfHandle cf = mock(FrsCfHandle.class);

        final byte[] keyA = "aaa".getBytes();
        final byte[] valA = "111".getBytes();
        final int rows = 60;

        // The batched open already filled this probe's first chunk; next() signals exhaustion.
        when(linker.frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any()))
                .thenAnswer(
                        inv -> {
                            MemorySegment outRc = inv.getArgument(3);
                            MemorySegment outBu = inv.getArgument(4);
                            outRc.set(ValueLayout.JAVA_INT, 0, 0);
                            outBu.set(ValueLayout.JAVA_INT, 0, 0);
                            return 0;
                        });
        when(linker.frsVecIterPrefixClose(anyLong())).thenReturn(0);

        RecordingIterableState recState = new RecordingIterableState();
        StateRequest sr = mock(StateRequest.class);
        InternalAsyncFuture future = mock(InternalAsyncFuture.class);
        when(sr.getFuture()).thenReturn(future);
        byte[] prefix = "k/test/".getBytes();

        ForStRsDBIterRequest<?, ?, ?, ?> req =
                new ForStRsDBIterRequest<>(prefix, sr, StateRequestType.MAP_ITER, recState, null);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment firstChunk = arena.allocate(ForStRsDBIterRequest.chunkBufCap());
            int bytesUsed = writeRowsInto(firstChunk, rows, keyA, valA);
            req.processFromBatchedOpen(linker, db, cf, 0xBEEFL, firstChunk, rows, bytesUsed);
        }

        // Exactly one next() drain call, and the supplied first chunk decoded to (aaa, 111).
        verify(linker, times(1)).frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any());
        verify(linker, times(1)).frsVecIterPrefixClose(anyLong());
        assertThat(recState.observedKeys).hasSize(rows);
        assertThat(recState.observedValues).hasSize(rows);
        for (int i = 0; i < rows; i++) {
            assertThat(recState.observedKeys.get(i)).as("key %d", i).containsExactly(keyA);
            assertThat(recState.observedValues.get(i)).as("value %d", i).containsExactly(valA);
        }
    }

    /**
     * Lever-2 batched-open drain (multi chunk): the supplied first chunk lives in its OWN slice (the
     * batched open packs K probes into K distinct chunk slices) and is never overwritten by the
     * {@code _next} buffer (a separate internal scratch segment). The first chunk (60 × "aaa"/"111")
     * plus one {@code next()} chunk (60 × "bbb"/"222") must BOTH decode correctly with the
     * zero-snapshot in-place decode — chunk #1 not corrupted by chunk #2.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void processFromBatchedOpen_multi_chunk_no_corruption() {
        ForStRsLinker linker = mock(ForStRsLinker.class);
        FrsDb db = mock(FrsDb.class);
        FrsCfHandle cf = mock(FrsCfHandle.class);

        final byte[] keyA = "aaa".getBytes();
        final byte[] valA = "111".getBytes();
        final byte[] keyB = "bbb".getBytes();
        final byte[] valB = "222".getBytes();
        final int rows = 60;

        final int[] nextCallCount = new int[1];
        when(linker.frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any()))
                .thenAnswer(
                        inv -> {
                            MemorySegment chunkBuf = inv.getArgument(1);
                            MemorySegment outRc = inv.getArgument(3);
                            MemorySegment outBu = inv.getArgument(4);
                            if (nextCallCount[0]++ == 0) {
                                int written = writeRowsInto(chunkBuf, rows, keyB, valB);
                                outRc.set(ValueLayout.JAVA_INT, 0, rows);
                                outBu.set(ValueLayout.JAVA_INT, 0, written);
                                return 0;
                            }
                            outRc.set(ValueLayout.JAVA_INT, 0, 0);
                            outBu.set(ValueLayout.JAVA_INT, 0, 0);
                            return 0;
                        });
        when(linker.frsVecIterPrefixClose(anyLong())).thenReturn(0);

        RecordingIterableState recState = new RecordingIterableState();
        StateRequest sr = mock(StateRequest.class);
        InternalAsyncFuture future = mock(InternalAsyncFuture.class);
        when(sr.getFuture()).thenReturn(future);
        byte[] prefix = "k/test/".getBytes();

        ForStRsDBIterRequest<?, ?, ?, ?> req =
                new ForStRsDBIterRequest<>(prefix, sr, StateRequestType.MAP_ITER, recState, null);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment firstChunk = arena.allocate(ForStRsDBIterRequest.chunkBufCap());
            int bytesUsed = writeRowsInto(firstChunk, rows, keyA, valA);
            req.processFromBatchedOpen(linker, db, cf, 0xBEEFL, firstChunk, rows, bytesUsed);
        }

        assertThat(recState.observedKeys).hasSize(2 * rows);
        assertThat(recState.observedValues).hasSize(2 * rows);
        for (int i = 0; i < rows; i++) {
            assertThat(recState.observedKeys.get(i)).as("chunk#1 key %d", i).containsExactly(keyA);
            assertThat(recState.observedValues.get(i)).as("chunk#1 val %d", i).containsExactly(valA);
        }
        for (int i = rows; i < 2 * rows; i++) {
            assertThat(recState.observedKeys.get(i)).as("chunk#2 key %d", i).containsExactly(keyB);
            assertThat(recState.observedValues.get(i)).as("chunk#2 val %d", i).containsExactly(valB);
        }
    }

    /**
     * Writes {@code rowCount} rows of ({@code key}, {@code value}) into {@code chunkBuf} using the
     * same wire layout as Rust's {@code write_chunk_into_buf}: {@code [u32 klen LE][u32 vlen LE][key
     * bytes][value bytes]} per row. Returns the total bytes written ({@code bytesUsed}).
     */
    private static int writeRowsInto(
            MemorySegment chunkBuf, int rowCount, byte[] key, byte[] value) {
        int off = 0;
        for (int i = 0; i < rowCount; i++) {
            chunkBuf.set(ValueLayout.JAVA_INT_UNALIGNED, off, key.length);
            off += 4;
            chunkBuf.set(ValueLayout.JAVA_INT_UNALIGNED, off, value.length);
            off += 4;
            MemorySegment.copy(key, 0, chunkBuf, ValueLayout.JAVA_BYTE, off, key.length);
            off += key.length;
            MemorySegment.copy(value, 0, chunkBuf, ValueLayout.JAVA_BYTE, off, value.length);
            off += value.length;
        }
        return off;
    }

    /**
     * Runs {@link ForStRsDBIterRequest#process} against a mocked linker that synthesizes one chunk
     * with {@code rowCount} rows of ({@code keyLen} B key, {@code valueLen} B value), then signals
     * exhaustion on the next call. Returns the thread allocation delta of the measured run.
     * Performs a warm-up run first to fault-in classes and JIT-compile hot paths.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static long runProcessAndMeasureAlloc(int rowCount, int keyLen, int valueLen) {
        ForStRsLinker linker = mock(ForStRsLinker.class);
        FrsDb db = mock(FrsDb.class);
        FrsCfHandle cf = mock(FrsCfHandle.class);
        when(linker.frsVecIterPrefixOpen(
                        any(), any(), any(), anyInt(), any(), anyInt(), any(), any(), any()))
                .thenReturn(0);
        when(linker.frsVecIterPrefixClose(anyLong())).thenReturn(0);

        final int[] callCount = new int[1];
        when(linker.frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any()))
                .thenAnswer(
                        inv -> {
                            MemorySegment chunkBuf = inv.getArgument(1);
                            MemorySegment outRc = inv.getArgument(3);
                            MemorySegment outBu = inv.getArgument(4);
                            if (callCount[0]++ > 0) {
                                outRc.set(ValueLayout.JAVA_INT, 0, 0);
                                outBu.set(ValueLayout.JAVA_INT, 0, 0);
                                return 0;
                            }
                            int off = 0;
                            for (int i = 0; i < rowCount; i++) {
                                chunkBuf.set(ValueLayout.JAVA_INT_UNALIGNED, off, keyLen);
                                off += 4;
                                chunkBuf.set(ValueLayout.JAVA_INT_UNALIGNED, off, valueLen);
                                off += 4;
                                for (int b = 0; b < keyLen; b++) {
                                    chunkBuf.set(ValueLayout.JAVA_BYTE, off + b, (byte) i);
                                }
                                off += keyLen;
                                for (int b = 0; b < valueLen; b++) {
                                    chunkBuf.set(ValueLayout.JAVA_BYTE, off + b, (byte) -1);
                                }
                                off += valueLen;
                            }
                            outRc.set(ValueLayout.JAVA_INT, 0, rowCount);
                            outBu.set(ValueLayout.JAVA_INT, 0, off);
                            return 0;
                        });

        ForStRsIterableState mockState = new NoopIterableState();
        StateRequest sr = mock(StateRequest.class);
        InternalAsyncFuture future = mock(InternalAsyncFuture.class);
        when(sr.getFuture()).thenReturn(future);
        byte[] prefix = "k/test/".getBytes();

        com.sun.management.ThreadMXBean tmb =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long tid = Thread.currentThread().getId();

        // Warm-up.
        try (Arena arena = Arena.ofConfined()) {
            new ForStRsDBIterRequest<>(prefix, sr, StateRequestType.MAP_ITER, mockState, null)
                    .process(linker, db, cf, arena);
        }

        callCount[0] = 0;
        try (Arena arena = Arena.ofConfined()) {
            long before = tmb.getThreadAllocatedBytes(tid);
            new ForStRsDBIterRequest<>(prefix, sr, StateRequestType.MAP_ITER, mockState, null)
                    .process(linker, db, cf, arena);
            long after = tmb.getThreadAllocatedBytes(tid);
            return after - before;
        }
    }

    /**
     * Records the bytes each {@code deserializeUserKey/Value(IteratorEntryView)} call observes
     * (materialized via {@link IteratorEntryView#keyBytes()} / {@link IteratorEntryView#valueBytes()}),
     * in iteration order. Used by {@code process_multi_chunk_drain_no_corruption} to assert that
     * earlier-chunk views have not been overwritten by later-chunk drain calls.
     */
    private static final class RecordingIterableState
            implements ForStRsIterableState<Object, Object, byte[], byte[]> {
        final List<byte[]> observedKeys = new ArrayList<>();
        final List<byte[]> observedValues = new ArrayList<>();

        @Override
        public byte[] getIterPrefix(StateRequest<Object, Object, ?, ?> request) {
            return null;
        }

        @Override
        public byte[] deserializeUserKey(byte[] rawKey, int userKeyOffset) {
            return rawKey;
        }

        @Override
        public byte[] deserializeUserValue(byte[] rawValue) {
            return rawValue;
        }

        @Override
        public byte[] deserializeUserKey(IteratorEntryView view, int userKeyPrefixOffset) {
            byte[] k = view.keyBytes();
            observedKeys.add(k);
            return k;
        }

        @Override
        public byte[] deserializeUserValue(IteratorEntryView view) {
            byte[] v = view.valueBytes();
            observedValues.add(v);
            return v;
        }

        @Override
        public StateRequestHandler getStateRequestHandler() {
            return null;
        }

        @Override
        public State asState() {
            return null;
        }
    }

    /**
     * Minimal {@link ForStRsIterableState} stub for the allocation test. Returns null from both
     * decoders to short-circuit the SimpleEntry creation path inside
     * {@code completeWithEntries}, which keeps the measurement focused on the drain loop.
     */
    private static final class NoopIterableState
            implements ForStRsIterableState<Object, Object, Object, Object> {
        @Override
        public byte[] getIterPrefix(StateRequest<Object, Object, ?, ?> request) {
            return null;
        }

        @Override
        public Object deserializeUserKey(byte[] rawKey, int userKeyOffset) {
            return null;
        }

        @Override
        public Object deserializeUserValue(byte[] rawValue) {
            return null;
        }

        @Override
        public Object deserializeUserKey(IteratorEntryView view, int userKeyPrefixOffset) {
            return null;
        }

        @Override
        public Object deserializeUserValue(IteratorEntryView view) {
            return null;
        }

        @Override
        public StateRequestHandler getStateRequestHandler() {
            return null;
        }

        @Override
        public State asState() {
            return null;
        }
    }
}

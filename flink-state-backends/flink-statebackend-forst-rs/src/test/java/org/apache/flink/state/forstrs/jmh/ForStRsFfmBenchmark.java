/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forstrs.jmh;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manual JMH-style harness for the JDK 25 FFM (Foreign Function and Memory API) path through {@link
 * ForStRsLinker}. Mirrors {@link ForStCompareBenchmark} so the numbers are directly comparable, but
 * routes every call through {@code Linker.nativeLinker()} + {@code MethodHandle.invokeExact(...)}
 * instead of the JNI {@code Java_org_forstdb_RocksDB_*} shim. This is the path the production state
 * backend uses; it eliminates the JNI argument marshaling overhead that the {@link
 * ForStCompareBenchmark} measures.
 *
 * <p>The harness is intentionally annotation-free (no {@code @Benchmark}, no {@code @State}) so it
 * compiles against a stock JDK 25 install with no JMH dependency resolved. The same logic could be
 * wrapped in JMH stubs trivially — see {@code JMH_BENCHMARK.md}.
 *
 * <p>Library lookup honours {@code -Dforstrs.native.libpath=&lt;path&gt;}; the runner sets this to
 * the same {@code libforst_rs_ffi.dylib} the JNI-shim variant uses, so any throughput delta is
 * purely the cost of the JNI bridge vs FFM downcall handles.
 */
public final class ForStRsFfmBenchmark {

    /** Pre-loaded keys for the point-lookup workload. */
    private static final int PRELOAD = 100_000;

    /**
     * Number of rows per WriteBatch in the {@code batchedPut} workload — matches the realistic
     * per-checkpoint-barrier write fan-out in production Flink state backends.
     */
    private static final int BATCH_SIZE = 1000;

    private static final byte[] VALUE = new byte[128];

    static {
        for (int i = 0; i < VALUE.length; i++) {
            VALUE[i] = (byte) (i & 0xff);
        }
    }

    private ForStRsFfmBenchmark() {}

    // ---- Arrow C Data Interface ABI offsets (hand-rolled, no arrow-java dep) -------
    //
    // The C structs we mirror live in arrow-rs and mirror the canonical Arrow C
    // Data Interface spec. Both structs are #[repr(C)] with no padding on
    // 64-bit ABIs (every field is i64 / pointer / fn pointer = 8 bytes).
    //
    // FFI_ArrowSchema: 9 fields × 8 = 72 bytes.
    //   0  : format        const char*
    //   8  : name          const char*
    //   16 : metadata      const char*
    //   24 : flags         i64
    //   32 : n_children    i64
    //   40 : children      ArrowSchema**
    //   48 : dictionary    ArrowSchema*
    //   56 : release       void (*)(ArrowSchema*)
    //   64 : private_data  void*
    //
    // FFI_ArrowArray: 5×i64 + 4 ptrs + 1 fn ptr + 1 ptr = 80 bytes.
    //   0  : length        i64
    //   8  : null_count    i64
    //   16 : offset        i64
    //   24 : n_buffers     i64
    //   32 : n_children    i64
    //   40 : buffers       const void**
    //   48 : children      ArrowArray**
    //   56 : dictionary    ArrowArray*
    //   64 : release       void (*)(ArrowArray*)
    //   72 : private_data  void*
    //
    // Flag bit 0x2 == NULLABLE. We set it on the value column field schema so
    // arrow_array::ffi::from_ffi correctly reconstructs the BinaryArray with
    // a null mask buffer.
    private static final long ARROW_SCHEMA_BYTES = 72L;
    private static final long ARROW_ARRAY_BYTES = 80L;
    private static final long FLAG_NULLABLE = 0x2L;

    /**
     * Builds a no-op release callback (C function pointer) for FFI_Arrow{Array,Schema}.
     * Required because arrow-rs's {@code from_ffi} import wraps the array in an Arc
     * whose drop calls release on the embedded fn pointer. We own all memory in the
     * caller arena (closed at bench end), so the callback must NOT free anything —
     * it just sets {@code release = NULL} on the passed-in struct, which is the
     * canonical "consumed" marker per the Arrow C Data Interface contract.
     */
    private static MemorySegment buildNoopReleaseStub(Arena arena, long releaseOffset)
            throws NoSuchMethodException, IllegalAccessException {
        MethodHandle target =
                MethodHandles.lookup()
                        .findStatic(
                                ForStRsFfmBenchmark.class,
                                "noopRelease",
                                MethodType.methodType(
                                        void.class, MemorySegment.class, long.class));
        // Bind the release-offset constant into the target so the upcall can
        // zero out the release slot of any passed-in FFI struct (matches arrow-rs's
        // own release_array, which sets self.release = None).
        MethodHandle bound = MethodHandles.insertArguments(target, 1, releaseOffset);
        return Linker.nativeLinker()
                .upcallStub(
                        bound,
                        FunctionDescriptor.ofVoid(ValueLayout.ADDRESS),
                        arena);
    }

    @SuppressWarnings("unused")
    private static void noopRelease(MemorySegment self, long releaseOffset) {
        // Arrow C Data Interface: setting release to NULL on the passed-in struct
        // marks it as "consumed". We do this even though arrow-rs already
        // overwrites our struct via std::ptr::write(empty()) on entry — this
        // path defends against the (rare) case where arrow-rs's Drop is invoked
        // on a non-overwritten struct due to some future refactor.
        if (self.address() == 0L) {
            return;
        }
        MemorySegment view = self.reinterpret(ARROW_ARRAY_BYTES);
        view.set(ValueLayout.ADDRESS, releaseOffset, MemorySegment.NULL);
    }

    /**
     * Stages the canonical {@code key: Binary, value: Binary nullable, op_type: UInt8}
     * RecordBatch (BATCH_SIZE rows) as a hand-rolled FFI_ArrowArray + FFI_ArrowSchema
     * pair. Returns a record holding the live (per-call) and template (constants)
     * segments so the bench can {@code MemorySegment.copy(template -> live)} between
     * iterations, since {@code frs_batch_put_arrow} overwrites the live structs with
     * the empty() marker on consumption.
     */
    private static ArrowBatchSegments stageArrowBatch(Arena arena, MemorySegment releaseStub) {
        long ptrSz = ValueLayout.ADDRESS.byteSize();

        // Format strings — arrow-rs CStr::from_ptr expects NUL-terminated UTF-8.
        MemorySegment fmtStruct = cstring(arena, "+s");
        MemorySegment fmtBinary = cstring(arena, "z");
        MemorySegment fmtU8 = cstring(arena, "C");
        MemorySegment fieldKey = cstring(arena, "key");
        MemorySegment fieldValue = cstring(arena, "value");
        MemorySegment fieldOpType = cstring(arena, "op_type");

        // ---- Build the per-column data buffers ----
        // BinaryArray (key) needs: validity (we set NULL since no nulls),
        // offsets[N+1] as i32, data[total_len].
        MemorySegment keyOffsets = arena.allocate((BATCH_SIZE + 1) * 4L, 4L);
        MemorySegment valOffsets = arena.allocate((BATCH_SIZE + 1) * 4L, 4L);
        // Each batchKeyOf / batchValueOf returns 12 bytes — total 12 * BATCH_SIZE.
        MemorySegment keyData = arena.allocate(12L * BATCH_SIZE);
        MemorySegment valData = arena.allocate(12L * BATCH_SIZE);
        for (int i = 0; i < BATCH_SIZE; i++) {
            byte[] k = batchKeyOf(i);
            byte[] v = batchValueOf(i);
            MemorySegment.copy(k, 0, keyData, ValueLayout.JAVA_BYTE, 12L * i, 12);
            MemorySegment.copy(v, 0, valData, ValueLayout.JAVA_BYTE, 12L * i, 12);
            keyOffsets.set(ValueLayout.JAVA_INT, 4L * i, i * 12);
            valOffsets.set(ValueLayout.JAVA_INT, 4L * i, i * 12);
        }
        keyOffsets.set(ValueLayout.JAVA_INT, 4L * BATCH_SIZE, BATCH_SIZE * 12);
        valOffsets.set(ValueLayout.JAVA_INT, 4L * BATCH_SIZE, BATCH_SIZE * 12);
        // op_type column data — every row is Put (0).
        MemorySegment opData = arena.allocate(BATCH_SIZE);
        // already zeroed by Arena.allocate.

        // ---- Build the buffers pointer arrays ----
        // BinaryArray: 3 buffers — validity (NULL), offsets, data.
        MemorySegment keyBufs = arena.allocate(3 * ptrSz);
        keyBufs.set(ValueLayout.ADDRESS, 0 * ptrSz, MemorySegment.NULL);
        keyBufs.set(ValueLayout.ADDRESS, 1 * ptrSz, keyOffsets);
        keyBufs.set(ValueLayout.ADDRESS, 2 * ptrSz, keyData);
        MemorySegment valBufs = arena.allocate(3 * ptrSz);
        valBufs.set(ValueLayout.ADDRESS, 0 * ptrSz, MemorySegment.NULL);
        valBufs.set(ValueLayout.ADDRESS, 1 * ptrSz, valOffsets);
        valBufs.set(ValueLayout.ADDRESS, 2 * ptrSz, valData);
        // UInt8: 2 buffers — validity (NULL), data.
        MemorySegment opBufs = arena.allocate(2 * ptrSz);
        opBufs.set(ValueLayout.ADDRESS, 0 * ptrSz, MemorySegment.NULL);
        opBufs.set(ValueLayout.ADDRESS, 1 * ptrSz, opData);
        // Struct: 1 buffer (validity bitmap, NULL since no nulls at struct level).
        MemorySegment structBufs = arena.allocate(ptrSz);
        structBufs.set(ValueLayout.ADDRESS, 0L, MemorySegment.NULL);

        // ---- Per-column FFI_ArrowArray children (live, never overwritten) ----
        MemorySegment keyArray = arena.allocate(ARROW_ARRAY_BYTES);
        writeArrowArray(keyArray, BATCH_SIZE, 0, 0, 3, 0, keyBufs, MemorySegment.NULL, releaseStub);
        MemorySegment valArray = arena.allocate(ARROW_ARRAY_BYTES);
        writeArrowArray(valArray, BATCH_SIZE, 0, 0, 3, 0, valBufs, MemorySegment.NULL, releaseStub);
        MemorySegment opArray = arena.allocate(ARROW_ARRAY_BYTES);
        writeArrowArray(opArray, BATCH_SIZE, 0, 0, 2, 0, opBufs, MemorySegment.NULL, releaseStub);

        // ---- Per-column FFI_ArrowSchema children ----
        MemorySegment keySchema = arena.allocate(ARROW_SCHEMA_BYTES);
        writeArrowSchema(keySchema, fmtBinary, fieldKey, 0L, 0, MemorySegment.NULL, releaseStub);
        MemorySegment valSchema = arena.allocate(ARROW_SCHEMA_BYTES);
        writeArrowSchema(
                valSchema, fmtBinary, fieldValue, FLAG_NULLABLE, 0, MemorySegment.NULL, releaseStub);
        MemorySegment opSchema = arena.allocate(ARROW_SCHEMA_BYTES);
        writeArrowSchema(opSchema, fmtU8, fieldOpType, 0L, 0, MemorySegment.NULL, releaseStub);

        // children pointer arrays — 3 children per top-level struct.
        MemorySegment arrayChildren = arena.allocate(3 * ptrSz);
        arrayChildren.set(ValueLayout.ADDRESS, 0 * ptrSz, keyArray);
        arrayChildren.set(ValueLayout.ADDRESS, 1 * ptrSz, valArray);
        arrayChildren.set(ValueLayout.ADDRESS, 2 * ptrSz, opArray);
        MemorySegment schemaChildren = arena.allocate(3 * ptrSz);
        schemaChildren.set(ValueLayout.ADDRESS, 0 * ptrSz, keySchema);
        schemaChildren.set(ValueLayout.ADDRESS, 1 * ptrSz, valSchema);
        schemaChildren.set(ValueLayout.ADDRESS, 2 * ptrSz, opSchema);

        // ---- Top-level struct FFI_ArrowArray + FFI_ArrowSchema ----
        // Templates hold the canonical bytes; the live segments are the ones
        // we hand to frs_batch_put_arrow. Rust overwrites *array / *schema
        // with empty() on entry, so we restore from template before each call.
        MemorySegment arrayTemplate = arena.allocate(ARROW_ARRAY_BYTES);
        writeArrowArray(
                arrayTemplate, BATCH_SIZE, 0, 0, 1, 3, structBufs, arrayChildren, releaseStub);
        MemorySegment arrayLive = arena.allocate(ARROW_ARRAY_BYTES);
        MemorySegment.copy(arrayTemplate, 0L, arrayLive, 0L, ARROW_ARRAY_BYTES);

        MemorySegment schemaTemplate = arena.allocate(ARROW_SCHEMA_BYTES);
        writeArrowSchema(
                schemaTemplate,
                fmtStruct,
                MemorySegment.NULL,
                0L,
                3,
                schemaChildren,
                releaseStub);
        MemorySegment schemaLive = arena.allocate(ARROW_SCHEMA_BYTES);
        MemorySegment.copy(schemaTemplate, 0L, schemaLive, 0L, ARROW_SCHEMA_BYTES);

        return new ArrowBatchSegments(arrayLive, arrayTemplate, schemaLive, schemaTemplate);
    }

    /** Writes an FFI_ArrowArray at {@code seg} (offset 0). */
    private static void writeArrowArray(
            MemorySegment seg,
            long length,
            long nullCount,
            long offset,
            long nBuffers,
            long nChildren,
            MemorySegment buffers,
            MemorySegment children,
            MemorySegment release) {
        seg.set(ValueLayout.JAVA_LONG, 0, length);
        seg.set(ValueLayout.JAVA_LONG, 8, nullCount);
        seg.set(ValueLayout.JAVA_LONG, 16, offset);
        seg.set(ValueLayout.JAVA_LONG, 24, nBuffers);
        seg.set(ValueLayout.JAVA_LONG, 32, nChildren);
        seg.set(ValueLayout.ADDRESS, 40, buffers);
        seg.set(ValueLayout.ADDRESS, 48, children);
        seg.set(ValueLayout.ADDRESS, 56, MemorySegment.NULL); // dictionary
        seg.set(ValueLayout.ADDRESS, 64, release);
        seg.set(ValueLayout.ADDRESS, 72, MemorySegment.NULL); // private_data
    }

    /** Writes an FFI_ArrowSchema at {@code seg} (offset 0). */
    private static void writeArrowSchema(
            MemorySegment seg,
            MemorySegment format,
            MemorySegment name,
            long flags,
            long nChildren,
            MemorySegment children,
            MemorySegment release) {
        seg.set(ValueLayout.ADDRESS, 0, format);
        seg.set(ValueLayout.ADDRESS, 8, name);
        seg.set(ValueLayout.ADDRESS, 16, MemorySegment.NULL); // metadata
        seg.set(ValueLayout.JAVA_LONG, 24, flags);
        seg.set(ValueLayout.JAVA_LONG, 32, nChildren);
        seg.set(ValueLayout.ADDRESS, 40, children);
        seg.set(ValueLayout.ADDRESS, 48, MemorySegment.NULL); // dictionary
        seg.set(ValueLayout.ADDRESS, 56, release);
        seg.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL); // private_data
    }

    private static MemorySegment cstring(Arena arena, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        MemorySegment seg = arena.allocate(bytes.length + 1L);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        seg.set(ValueLayout.JAVA_BYTE, bytes.length, (byte) 0);
        return seg;
    }

    private record ArrowBatchSegments(
            MemorySegment arrayLive,
            MemorySegment arrayTemplate,
            MemorySegment schemaLive,
            MemorySegment schemaTemplate) {}

    /** Encodes an integer key as ASCII {@code k########} (9-byte key) — matches sister bench. */
    private static byte[] keyOf(int i) {
        byte[] b = new byte[9];
        b[0] = 'k';
        for (int p = 8; p >= 1; p--) {
            b[p] = (byte) ('0' + (i % 10));
            i /= 10;
        }
        return b;
    }

    /** Encodes batch index N as ASCII {@code bk%010d} (12-byte key). */
    private static byte[] batchKeyOf(int n) {
        byte[] b = new byte[12];
        b[0] = 'b';
        b[1] = 'k';
        for (int p = 11; p >= 2; p--) {
            b[p] = (byte) ('0' + (n % 10));
            n /= 10;
        }
        return b;
    }

    /** Encodes batch index N as ASCII {@code bv%010d} (12-byte value). */
    private static byte[] batchValueOf(int n) {
        byte[] b = new byte[12];
        b[0] = 'b';
        b[1] = 'v';
        for (int p = 11; p >= 2; p--) {
            b[p] = (byte) ('0' + (n % 10));
            n /= 10;
        }
        return b;
    }

    public static void main(String[] args) throws Exception {
        final long warmupNanos =
                Long.parseLong(System.getProperty("bench.warmup.s", "6")) * 1_000_000_000L;
        final long measureNanos =
                Long.parseLong(System.getProperty("bench.measure.s", "25")) * 1_000_000_000L;

        // Write-path tuning preset (R-loop write-path tuning, 2026-05-10):
        //   - "default" (or unset): legacy frs_db_open_memory with 64 MiB memtable, 3 buffers
        //   - "A": 256 MiB memtable × 8 buffers, default 4/2 bg threads (2 GiB budget)
        //   - "B": 512 MiB memtable × 4 buffers, 8/8 bg threads (more compaction parallelism)
        // The four longs feed straight into frs_db_open_memory_tuned; 0 = engine default.
        final String preset = System.getProperty("bench.preset", "default");
        long writeBufferSize = 0L;
        long maxWriteBufferNumber = 0L;
        long maxBackgroundCompactions = 0L;
        long maxBackgroundFlushes = 0L;
        switch (preset) {
            case "A":
                writeBufferSize = 256L * 1024L * 1024L;
                maxWriteBufferNumber = 8L;
                maxBackgroundCompactions = 4L;
                maxBackgroundFlushes = 4L;
                break;
            case "B":
                writeBufferSize = 512L * 1024L * 1024L;
                maxWriteBufferNumber = 4L;
                maxBackgroundCompactions = 8L;
                maxBackgroundFlushes = 8L;
                break;
            case "default":
            default:
                // leave all zero — uses frs_db_open_memory legacy defaults below.
                break;
        }
        System.out.printf(
                "[setup] preset=%s write_buffer_size=%d max_write_buffer_number=%d "
                        + "max_background_compactions=%d max_background_flushes=%d%n",
                preset,
                writeBufferSize,
                maxWriteBufferNumber,
                maxBackgroundCompactions,
                maxBackgroundFlushes);

        // A shared Arena owns the cdylib symbol lookup for the lifetime of the process — its
        // close() unloads the library and invalidates every MemorySegment we obtained.
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = null;
            FrsCfHandle cf = null;
            try {
                if ("default".equals(preset)) {
                    db = linker.dbOpenMemory(arena);
                } else {
                    db =
                            linker.dbOpenMemoryTuned(
                                    arena,
                                    writeBufferSize,
                                    maxWriteBufferNumber,
                                    maxBackgroundCompactions,
                                    maxBackgroundFlushes);
                }
                cf = linker.dbDefaultCf(db, arena);

                // Pre-load 100k entries for the point-lookup workload (matches sister bench).
                System.out.printf(
                        "[setup] preloading %d entries (each value=%d bytes)%n",
                        PRELOAD, VALUE.length);
                long preloadStart = System.nanoTime();
                for (int i = 0; i < PRELOAD; i++) {
                    byte[] k = keyOf(i);
                    linker.put(db, cf, k, VALUE);
                }
                linker.flush(db);
                long preloadElapsed = System.nanoTime() - preloadStart;
                System.out.printf(
                        "[setup] preload done in %.3f s (%.0f put/s)%n",
                        preloadElapsed / 1e9, PRELOAD / (preloadElapsed / 1e9));

                // ---- pointLookup ---------------------------------------------------
                byte[] probeKey = keyOf(50_000);
                long sink = 0L; // accumulate to defeat JIT dead-code elimination

                System.out.println("[pointLookup] warmup...");
                long deadline = System.nanoTime() + warmupNanos;
                long warmupOps = 0;
                while (System.nanoTime() < deadline) {
                    byte[] v = linker.lookupKv(db, cf, probeKey);
                    if (v != null) {
                        sink += v[0];
                    }
                    warmupOps++;
                }

                System.out.println("[pointLookup] measure...");
                deadline = System.nanoTime() + measureNanos;
                long start = System.nanoTime();
                long ops = 0;
                while (System.nanoTime() < deadline) {
                    byte[] v = linker.lookupKv(db, cf, probeKey);
                    if (v != null) {
                        sink += v[0];
                    }
                    ops++;
                }
                long elapsed = System.nanoTime() - start;
                double pointThroughput = ops * 1e9 / elapsed;
                System.out.printf(
                        "[pointLookup] %,d ops in %.3f s -> %.0f ops/s (sink=%d)%n",
                        ops, elapsed / 1e9, pointThroughput, sink);

                // ---- sequentialPut -----------------------------------------------
                AtomicLong counter = new AtomicLong();

                System.out.println("[sequentialPut] warmup...");
                deadline = System.nanoTime() + warmupNanos;
                while (System.nanoTime() < deadline) {
                    long c = counter.getAndIncrement();
                    byte[] k = keyOf(PRELOAD + (int) (c & 0x7fff_ffffL));
                    linker.put(db, cf, k, VALUE);
                }

                System.out.println("[sequentialPut] measure...");
                deadline = System.nanoTime() + measureNanos;
                start = System.nanoTime();
                ops = 0;
                while (System.nanoTime() < deadline) {
                    long c = counter.getAndIncrement();
                    byte[] k = keyOf(PRELOAD + (int) (c & 0x7fff_ffffL));
                    linker.put(db, cf, k, VALUE);
                    ops++;
                }
                elapsed = System.nanoTime() - start;
                double putThroughput = ops * 1e9 / elapsed;
                System.out.printf(
                        "[sequentialPut] %,d ops in %.3f s -> %.0f ops/s%n",
                        ops, elapsed / 1e9, putThroughput);

                // ---- batchedPut -------------------------------------------------
                // Pre-stage BATCH_SIZE keys + values into native memory (owned by
                // `arena`) so that each batchPut() call is a single FFM downcall
                // through frs_batch_put with no per-iteration staging cost. This
                // is the realistic write hot path for production state backends.
                long ptrSz = ValueLayout.ADDRESS.byteSize();
                long lenSz = ValueLayout.JAVA_LONG.byteSize();
                MemorySegment keyPtrs = arena.allocate(BATCH_SIZE * ptrSz);
                MemorySegment keyLens = arena.allocate(BATCH_SIZE * lenSz);
                MemorySegment valPtrs = arena.allocate(BATCH_SIZE * ptrSz);
                MemorySegment valLens = arena.allocate(BATCH_SIZE * lenSz);
                for (int i = 0; i < BATCH_SIZE; i++) {
                    byte[] kBytes = batchKeyOf(i);
                    byte[] vBytes = batchValueOf(i);
                    MemorySegment ks = arena.allocate(kBytes.length);
                    MemorySegment vs = arena.allocate(vBytes.length);
                    MemorySegment.copy(kBytes, 0, ks, ValueLayout.JAVA_BYTE, 0, kBytes.length);
                    MemorySegment.copy(vBytes, 0, vs, ValueLayout.JAVA_BYTE, 0, vBytes.length);
                    keyPtrs.set(ValueLayout.ADDRESS, i * ptrSz, ks);
                    valPtrs.set(ValueLayout.ADDRESS, i * ptrSz, vs);
                    keyLens.set(ValueLayout.JAVA_LONG, i * lenSz, (long) kBytes.length);
                    valLens.set(ValueLayout.JAVA_LONG, i * lenSz, (long) vBytes.length);
                }

                System.out.println("[batchedPut] warmup...");
                deadline = System.nanoTime() + warmupNanos;
                try {
                    while (System.nanoTime() < deadline) {
                        linker.batchPut(db, cf, keyPtrs, keyLens, valPtrs, valLens, BATCH_SIZE);
                    }
                } catch (Throwable t) {
                    // TIMED_OUT can fire if the in-memory engine fills its
                    // memtable budget faster than background compaction can
                    // drain it (typical at preset=B with the long
                    // BENCH_MEASURE_S window). Drop into measurement anyway so
                    // we still report whatever steady-state throughput we
                    // observe, otherwise the rest of the bench (incl.
                    // batchedPutArrow) never runs.
                    System.err.println("[batchedPut] warmup interrupted: " + t);
                }

                System.out.println("[batchedPut] measure...");
                deadline = System.nanoTime() + measureNanos;
                start = System.nanoTime();
                long batches = 0;
                try {
                    while (System.nanoTime() < deadline) {
                        linker.batchPut(db, cf, keyPtrs, keyLens, valPtrs, valLens, BATCH_SIZE);
                        batches++;
                    }
                } catch (Throwable t) {
                    System.err.println("[batchedPut] measure interrupted: " + t);
                }
                elapsed = System.nanoTime() - start;
                double batchedRowsPerSec =
                        batches == 0 ? 0.0 : batches * (double) BATCH_SIZE * 1e9 / elapsed;
                System.out.printf(
                        "[batchedPut] %,d batches (%,d rows) in %.3f s -> %.0f rows/s%n",
                        batches, batches * (long) BATCH_SIZE, elapsed / 1e9, batchedRowsPerSec);

                // ---- batchedPutArrow --------------------------------------------
                // C1 zero-copy hot path: dispatch a pre-built Arrow RecordBatch
                // directly into the memtable via the Arrow C Data Interface.
                // The RecordBatch + FFI_ArrowArray + FFI_ArrowSchema are staged
                // ONCE in the shared arena (same allocation pattern as the
                // legacy batchedPut workload above so the comparison is fair).
                // Per-call cost is just two MemorySegment.copy calls (152 bytes
                // total) to restore the top-level structs that frs_batch_put_arrow
                // overwrites with empty() on entry, plus the FFM downcall itself.
                // Engine-level criterion shows ~1.84× speedup vs the WriteBatch
                // path on this workload — this bench exposes that win at the
                // Java FFM layer.
                //
                // We flush before the Arrow workload so the two write workloads
                // start from comparable memtable states (without this, the Arrow
                // workload would inherit the back-pressure from batchedPut's
                // accumulated rows, biasing the throughput downward).
                try {
                    linker.flush(db);
                } catch (Throwable t) {
                    System.err.println("[setup] inter-workload flush failed: " + t);
                }
                MemorySegment releaseStub = buildNoopReleaseStub(arena, 64L);
                ArrowBatchSegments arrowBatch = stageArrowBatch(arena, releaseStub);

                System.out.println("[batchedPutArrow] warmup...");
                deadline = System.nanoTime() + warmupNanos;
                try {
                    while (System.nanoTime() < deadline) {
                        MemorySegment.copy(
                                arrowBatch.arrayTemplate(),
                                0L,
                                arrowBatch.arrayLive(),
                                0L,
                                ARROW_ARRAY_BYTES);
                        MemorySegment.copy(
                                arrowBatch.schemaTemplate(),
                                0L,
                                arrowBatch.schemaLive(),
                                0L,
                                ARROW_SCHEMA_BYTES);
                        linker.batchPutArrow(
                                db, cf, arrowBatch.arrayLive(), arrowBatch.schemaLive());
                    }
                } catch (Throwable t) {
                    System.err.println("[batchedPutArrow] warmup interrupted: " + t);
                }

                System.out.println("[batchedPutArrow] measure...");
                deadline = System.nanoTime() + measureNanos;
                start = System.nanoTime();
                long arrowBatches = 0;
                try {
                    while (System.nanoTime() < deadline) {
                        MemorySegment.copy(
                                arrowBatch.arrayTemplate(),
                                0L,
                                arrowBatch.arrayLive(),
                                0L,
                                ARROW_ARRAY_BYTES);
                        MemorySegment.copy(
                                arrowBatch.schemaTemplate(),
                                0L,
                                arrowBatch.schemaLive(),
                                0L,
                                ARROW_SCHEMA_BYTES);
                        linker.batchPutArrow(
                                db, cf, arrowBatch.arrayLive(), arrowBatch.schemaLive());
                        arrowBatches++;
                    }
                } catch (Throwable t) {
                    System.err.println("[batchedPutArrow] measure interrupted: " + t);
                }
                elapsed = System.nanoTime() - start;
                double batchedArrowRowsPerSec =
                        arrowBatches == 0
                                ? 0.0
                                : arrowBatches * (double) BATCH_SIZE * 1e9 / elapsed;
                System.out.printf(
                        "[batchedPutArrow] %,d batches (%,d rows) in %.3f s -> %.0f rows/s%n",
                        arrowBatches,
                        arrowBatches * (long) BATCH_SIZE,
                        elapsed / 1e9,
                        batchedArrowRowsPerSec);

                // ---- summary -----------------------------------------------------
                System.out.println();
                System.out.println("=== summary ===");
                System.out.printf("pointLookup       %.0f ops/s%n", pointThroughput);
                System.out.printf("sequentialPut     %.0f ops/s%n", putThroughput);
                System.out.printf(
                        "batchedPut        %.0f rows/s (batch=%d)%n",
                        batchedRowsPerSec, BATCH_SIZE);
                System.out.printf(
                        "batchedPutArrow   %.0f rows/s (batch=%d)%n",
                        batchedArrowRowsPerSec, BATCH_SIZE);
                System.out.printf(
                        "variant.libpath %s%n",
                        System.getProperty("forstrs.native.libpath", "<via java.library.path>"));
                System.out.printf("bench.preset    %s%n", preset);
            } finally {
                // CF must close before DB, DB must close before Arena (which owns the symbol
                // lookup).
                try {
                    if (cf != null) {
                        cf.close();
                    }
                } catch (Throwable t) {
                    System.err.println("[teardown] cf.close threw: " + t);
                }
                try {
                    if (db != null) {
                        db.close();
                    }
                } catch (Throwable t) {
                    System.err.println("[teardown] db.close threw: " + t);
                }
            }
        }
    }
}

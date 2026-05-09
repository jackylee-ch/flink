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

package org.apache.flink.state.forstrs.ffm;

import org.apache.flink.state.forstrs.FrsBackendException;
import org.apache.flink.state.forstrs.FrsStatus;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * JDK 25 FFM bridge to libforst_rs_ffi.{dylib,so,dll}.
 *
 * <p>Loads the cdylib and binds downcall MethodHandles for the lifecycle, point ops, flush /
 * checkpoint / metadata, and Delta-Join lookup-iterator surface needed by the embedded local-lookup
 * function.
 *
 * <p>Library lookup order:
 *
 * <ol>
 *   <li>System property {@code forstrs.native.libpath} (absolute path)
 *   <li>{@code System.loadLibrary("forst_rs_ffi")} fallback
 * </ol>
 *
 * <p>Lifetime: the {@link Arena} given to the constructor owns the library handle's symbol lookup;
 * closing the Arena unloads the lib (and invalidates all returned MemorySegments).
 *
 * <p>Reference: docs/design/2.5_ffm_bridge_design.md §3 and
 * docs/design/2.13_deltajoin_localization.md.
 */
public final class ForStRsLinker {

    /** {@code FrsBytes} struct layout: data ptr, len, capacity (24 bytes on 64-bit). */
    public static final StructLayout FRS_BYTES_LAYOUT =
            MemoryLayout.structLayout(
                    ValueLayout.ADDRESS.withName("data"),
                    ValueLayout.JAVA_LONG.withName("len"),
                    ValueLayout.JAVA_LONG.withName("capacity"));

    // ValueLayout.ADDRESS_UNALIGNED and ValueLayout.JAVA_LONG_UNALIGNED are
    // used to read the FrsBytes out struct when it lives in a heap byte[24]
    // segment (which only guarantees 1-byte alignment). Strict-alignment reads
    // through ValueLayout.ADDRESS would throw on a heap-byte-array view.

    private final Linker linker;
    private final SymbolLookup lookup;

    // --- 1. Lifecycle ---
    private final MethodHandle frsDbOpen;
    private final MethodHandle frsDbOpenMemory;
    private final MethodHandle frsDbOpenMemoryTuned;
    private final MethodHandle frsDbOpenFromCheckpoint;
    private final MethodHandle frsDbClose;

    // --- 2. CF management ---
    private final MethodHandle frsDbDefaultCf;
    private final MethodHandle frsDbCreateCf;
    private final MethodHandle frsDbOpenCf;
    private final MethodHandle frsCfClose;

    // --- 3. Point ops ---
    private final MethodHandle frsPut;
    private final MethodHandle frsGet;
    private final MethodHandle frsDelete;

    // --- 3b. Batch ops ---
    private final MethodHandle frsBatchPut;

    // --- 4. Memory management ---
    private final MethodHandle frsBytesFree;

    // --- 5. Flush / checkpoint / metadata ---
    private final MethodHandle frsFlush;
    private final MethodHandle frsCreateCheckpoint;
    private final MethodHandle frsSequenceNumber;

    // --- 6. Delta-Join lookup + iterator ---
    private final MethodHandle frsLookupKv;
    private final MethodHandle frsIteratorOpen;
    private final MethodHandle frsIteratorSeek;
    private final MethodHandle frsIteratorNext;
    private final MethodHandle frsIteratorClose;
    private final MethodHandle frsPrefixLookupOpen;
    private final MethodHandle frsPrefixLookupClose;

    public ForStRsLinker(Arena arena) {
        this.linker = Linker.nativeLinker();

        String explicit = System.getProperty("forstrs.native.libpath");
        if (explicit != null && !explicit.isBlank()) {
            this.lookup = SymbolLookup.libraryLookup(Path.of(explicit), arena);
        } else {
            // Falls back to OS lib loader path; libname forst_rs_ffi.
            System.loadLibrary("forst_rs_ffi");
            this.lookup = SymbolLookup.loaderLookup();
        }

        // 1. Lifecycle
        this.frsDbOpen =
                bind(
                        "frs_db_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db_path (c_char*)
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbOpenMemory =
                bind(
                        "frs_db_open_memory",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // Performance-tuning sibling of frs_db_open_memory: 4 size_t knobs +
        // out_handle. Each knob may be 0, in which case the engine keeps its
        // built-in default for that field (see frs_db_open_memory_tuned doc
        // in lib.rs); out-of-range values flow through EngineOptionsBuilder
        // .try_build() and surface as FRS_STATUS_INVALID_ARGUMENT rather
        // than panicking.
        this.frsDbOpenMemoryTuned =
                bind(
                        "frs_db_open_memory_tuned",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.JAVA_LONG, // write_buffer_size
                                ValueLayout.JAVA_LONG, // max_write_buffer_number
                                ValueLayout.JAVA_LONG, // max_background_compactions
                                ValueLayout.JAVA_LONG, // max_background_flushes
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbOpenFromCheckpoint =
                bind(
                        "frs_db_open_from_checkpoint",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // target_dir (c_char*)
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbClose =
                bind(
                        "frs_db_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // 2. CF management
        this.frsDbDefaultCf =
                bind(
                        "frs_db_default_cf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db handle
                                ValueLayout.ADDRESS)); // out_cf

        this.frsDbCreateCf =
                bind(
                        "frs_db_create_cf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // name (c_char*)
                                ValueLayout.ADDRESS)); // out_cf

        this.frsDbOpenCf =
                bind(
                        "frs_db_open_cf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // name (c_char*)
                                ValueLayout.ADDRESS)); // out_cf

        this.frsCfClose =
                bind(
                        "frs_cf_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // 3. Point ops — bound with Linker.Option.critical(true) so that we can
        // pass MemorySegment.ofArray(byte[]) heap segments directly. The Linker
        // pins the underlying byte[] for the duration of the call (no copy)
        // instead of allocating a per-call native staging buffer; this matches
        // the JNI GetByteArrayElements pin semantics and eliminates the
        // dominant per-op overhead the original confined-Arena path imposed.
        this.frsPut =
                bindCritical(
                        "frs_put",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // value ptr
                                ValueLayout.JAVA_LONG)); // value_len

        this.frsGet =
                bindCritical(
                        "frs_get",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS)); // out FrsBytes*

        this.frsDelete =
                bindCritical(
                        "frs_delete",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG)); // key_len

        // 3b. Batch ops — frs_batch_put takes parallel arrays of native
        // pointers (uint8_t* const*) and sizes (size_t*). Critical mode is NOT
        // applicable here because we pass arrays-of-pointers into-native, which
        // must live in native memory anyway (the byte[] addresses inside a
        // Java [B[] array can't be pinned simultaneously). Caller stages the
        // four arrays via the {@link #batchPut(FrsDb, FrsCfHandle, MemorySegment,
        // MemorySegment, MemorySegment, MemorySegment, long)} overload.
        this.frsBatchPut =
                bind(
                        "frs_batch_put",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // keys (uint8_t* const*)
                                ValueLayout.ADDRESS, // key_lens (size_t*)
                                ValueLayout.ADDRESS, // values (uint8_t* const*)
                                ValueLayout.ADDRESS, // value_lens (size_t*)
                                ValueLayout.JAVA_LONG)); // count (size_t)

        // 4. Memory management — bound critical because every get/lookup_kv path
        // calls frs_bytes_free on its 24-byte FrsBytes out struct, which is now
        // a heap segment passed via MemorySegment.ofArray(byte[24]).
        this.frsBytesFree =
                bindCritical(
                        "frs_bytes_free",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // 5. Flush / checkpoint / metadata
        this.frsFlush =
                bind("frs_flush", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsCreateCheckpoint =
                bind(
                        "frs_create_checkpoint",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // target_dir (c_char*)

        this.frsSequenceNumber =
                bind(
                        "frs_sequence_number",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // out_seq (u64*)

        // 6. Delta-Join lookup + iterator
        // frsLookupKv is hot — same critical-mode binding as frsGet.
        this.frsLookupKv =
                bindCritical(
                        "frs_lookup_kv",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS)); // out FrsBytes*

        this.frsIteratorOpen =
                bind(
                        "frs_iterator_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS)); // out_iter

        this.frsIteratorSeek =
                bind(
                        "frs_iterator_seek",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // iter
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG)); // key_len

        this.frsIteratorNext =
                bind(
                        "frs_iterator_next",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // iter
                                ValueLayout.ADDRESS, // out_key (FrsBytes*)
                                ValueLayout.ADDRESS, // out_value (FrsBytes*)
                                ValueLayout.ADDRESS)); // out_valid (bool*)

        this.frsIteratorClose =
                bind(
                        "frs_iterator_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsPrefixLookupOpen =
                bind(
                        "frs_prefix_lookup_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // prefix ptr
                                ValueLayout.JAVA_LONG, // prefix_len
                                ValueLayout.ADDRESS)); // out_iter

        this.frsPrefixLookupClose =
                bind(
                        "frs_prefix_lookup_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    }

    private MethodHandle bind(String name, FunctionDescriptor descriptor) {
        MemorySegment sym =
                lookup.find(name)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "symbol not found in cdylib: " + name));
        return linker.downcallHandle(sym, descriptor);
    }

    /**
     * Binds {@code name} as a downcall handle in <em>critical</em> mode (JDK 22+: {@link
     * Linker.Option#critical(boolean) critical(true)}). Critical-mode handles can accept heap
     * {@link MemorySegment}s such as {@link MemorySegment#ofArray(byte[])}; the linker pins the
     * underlying primitive array for the duration of the native call instead of forcing the caller
     * to stage bytes through a per-call native arena. This eliminates two {@code Arena.allocate}
     * + {@code MemorySegment.copy} pairs per put/get, making the FFM bridge competitive with the
     * JNI {@code GetByteArrayElements} pin path.
     *
     * <p>Use only on hot point-op symbols ({@code frs_put}, {@code frs_get}, {@code frs_delete},
     * {@code frs_lookup_kv}) — critical mode disables the JVM's safepoint mechanism for the
     * duration of the call, so the native function MUST return promptly and MUST NOT block.
     */
    private MethodHandle bindCritical(String name, FunctionDescriptor descriptor) {
        MemorySegment sym =
                lookup.find(name)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "symbol not found in cdylib: " + name));
        // allowHeapAccess=true so MemorySegment.ofArray(byte[]) is acceptable.
        return linker.downcallHandle(sym, descriptor, Linker.Option.critical(true));
    }

    // ------------------------------------------------------------------
    // 1. Lifecycle
    // ------------------------------------------------------------------

    /** Opens an in-memory ForSt-RS engine. Caller closes via {@link FrsDb#close()}. */
    public FrsDb dbOpenMemory(Arena arena) {
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenMemory.invokeExact(outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_memory threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_memory");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens an in-memory ForSt-RS engine with caller-supplied write-path tuning knobs.
     *
     * <p>This is the JMH-bench / write-pressure-test counterpart to {@link #dbOpenMemory(Arena)}:
     * the four parameters map 1:1 onto {@code EngineOptions.write_buffer_size},
     * {@code max_write_buffer_number}, {@code max_background_compactions} and
     * {@code max_background_flushes}. Any parameter passed as {@code 0} keeps the engine's
     * built-in default for that field; out-of-range values flow through
     * {@code EngineOptionsBuilder::try_build} on the Rust side and surface as
     * {@link FrsStatus#INVALID_ARGUMENT}, not a JVM panic.
     *
     * <p>Recommended sustained-write presets (R-loop write-path tuning, 2026-05-10):
     *
     * <ul>
     *   <li>Preset A: {@code (256 MiB, 8, 4, 4)} — 2 GiB total memtable budget, default-ish bg
     *   <li>Preset B: {@code (512 MiB, 4, 8, 8)} — same total memtable, more bg parallelism
     * </ul>
     */
    public FrsDb dbOpenMemoryTuned(
            Arena arena,
            long writeBufferSize,
            long maxWriteBufferNumber,
            long maxBackgroundCompactions,
            long maxBackgroundFlushes) {
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc =
                    (int)
                            frsDbOpenMemoryTuned.invokeExact(
                                    writeBufferSize,
                                    maxWriteBufferNumber,
                                    maxBackgroundCompactions,
                                    maxBackgroundFlushes,
                                    outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_memory_tuned threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_memory_tuned");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens a filesystem-backed ForSt-RS engine at {@code path}. Caller closes via {@link
     * FrsDb#close()}.
     */
    public FrsDb dbOpen(Arena arena, String path) {
        MemorySegment pathSeg = allocateCString(arena, path);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpen.invokeExact(pathSeg, outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_open threw: " + t.getMessage());
        }
        check(rc, "frs_db_open");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens an engine by restoring its state from a checkpoint directory previously produced by
     * {@link #createCheckpoint(FrsDb, String)}. The checkpoint directory must contain
     * {@code CHECKPOINT.blob} and every SST file the manifest references. The engine is opened with
     * {@code db_path = targetDir}: subsequent reads/writes operate directly on the checkpoint
     * files. Caller closes via {@link FrsDb#close()}.
     */
    public FrsDb dbOpenFromCheckpoint(Arena arena, String targetDir) {
        MemorySegment dirSeg = allocateCString(arena, targetDir);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenFromCheckpoint.invokeExact(dirSeg, outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_from_checkpoint threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_from_checkpoint");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    // ------------------------------------------------------------------
    // 2. Column family management
    // ------------------------------------------------------------------

    /** Returns the default column family. Caller closes via {@link FrsCfHandle#close()}. */
    public FrsCfHandle dbDefaultCf(FrsDb db, Arena arena) {
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbDefaultCf.invokeExact(db.handle(), outCf);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_default_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_default_cf");
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    /** Creates a new named column family. Caller closes via {@link FrsCfHandle#close()}. */
    public FrsCfHandle dbCreateCf(FrsDb db, Arena arena, String name) {
        MemorySegment nameSeg = allocateCString(arena, name);
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbCreateCf.invokeExact(db.handle(), nameSeg, outCf);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_create_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_create_cf");
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    /** Opens an existing named column family. Caller closes via {@link FrsCfHandle#close()}. */
    public FrsCfHandle dbOpenCf(FrsDb db, Arena arena, String name) {
        MemorySegment nameSeg = allocateCString(arena, name);
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenCf.invokeExact(db.handle(), nameSeg, outCf);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_cf");
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    // ------------------------------------------------------------------
    // 3. Point ops
    // ------------------------------------------------------------------

    /**
     * Writes a key/value pair.
     *
     * <p>Hot path: the key/value arrays are passed directly to the critical-mode {@code frs_put}
     * downcall handle via {@link MemorySegment#ofArray(byte[])}. The Linker pins both arrays for
     * the duration of the native call instead of staging them through a per-call native arena.
     */
    public void put(FrsDb db, FrsCfHandle cf, byte[] key, byte[] value) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        MemorySegment valSeg = MemorySegment.ofArray(value);
        int rc;
        try {
            rc =
                    (int)
                            frsPut.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keySeg,
                                    (long) key.length,
                                    valSeg,
                                    (long) value.length);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_put threw: " + t.getMessage());
        }
        check(rc, "frs_put");
    }

    /** Returns the value for {@code key} or {@code null} if absent. */
    public byte[] get(FrsDb db, FrsCfHandle cf, byte[] key) {
        return getInternal(frsGet, "frs_get", db, cf, key);
    }

    /**
     * Batched put — writes {@code count} key/value pairs in one engine call.
     *
     * <p>The four "parallel array" segments must be laid out in native memory:
     *
     * <ul>
     *   <li>{@code keyPtrs}: {@code count} {@code uint8_t*} entries — each pointer
     *       targets a key buffer whose size is the matching {@code keyLens[i]}.
     *   <li>{@code keyLens}: {@code count} {@code size_t} entries (8 bytes each).
     *   <li>{@code valuePtrs}: {@code count} {@code uint8_t*} entries.
     *   <li>{@code valueLens}: {@code count} {@code size_t} entries.
     * </ul>
     *
     * <p>The key/value buffers themselves can live anywhere (native or pinned heap)
     * as long as the pointers inside {@code keyPtrs} / {@code valuePtrs} remain
     * valid for the duration of this call. Hot path callers (e.g. JMH bench) stage
     * everything once into a long-lived native arena and never copy.
     */
    public void batchPut(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment keyPtrs,
            MemorySegment keyLens,
            MemorySegment valuePtrs,
            MemorySegment valueLens,
            long count) {
        int rc;
        try {
            rc =
                    (int)
                            frsBatchPut.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keyPtrs,
                                    keyLens,
                                    valuePtrs,
                                    valueLens,
                                    count);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_batch_put threw: " + t.getMessage());
        }
        check(rc, "frs_batch_put");
    }

    /**
     * Convenience overload — stages {@code keys} / {@code values} into a fresh
     * confined arena and forwards to {@link #batchPut(FrsDb, FrsCfHandle,
     * MemorySegment, MemorySegment, MemorySegment, MemorySegment, long)}. The
     * staging cost (one alloc + N+N copies) makes this UNSUITABLE for benchmarking;
     * use the segment overload with pre-staged buffers for the hot path.
     */
    public void batchPut(FrsDb db, FrsCfHandle cf, byte[][] keys, byte[][] values) {
        if (keys.length != values.length) {
            throw new IllegalArgumentException(
                    "keys.length (" + keys.length + ") != values.length (" + values.length + ")");
        }
        int count = keys.length;
        if (count == 0) {
            return;
        }
        try (Arena local = Arena.ofConfined()) {
            // Stage keys + values payloads into native memory so that their pointers
            // remain stable for the duration of the downcall.
            MemorySegment keyPtrs = local.allocate((long) count * ValueLayout.ADDRESS.byteSize());
            MemorySegment keyLens = local.allocate((long) count * ValueLayout.JAVA_LONG.byteSize());
            MemorySegment valPtrs = local.allocate((long) count * ValueLayout.ADDRESS.byteSize());
            MemorySegment valLens = local.allocate((long) count * ValueLayout.JAVA_LONG.byteSize());
            for (int i = 0; i < count; i++) {
                byte[] k = keys[i];
                byte[] v = values[i];
                MemorySegment ks = local.allocate(k.length == 0 ? 1 : k.length);
                MemorySegment vs = local.allocate(v.length == 0 ? 1 : v.length);
                if (k.length > 0) {
                    MemorySegment.copy(k, 0, ks, ValueLayout.JAVA_BYTE, 0, k.length);
                }
                if (v.length > 0) {
                    MemorySegment.copy(v, 0, vs, ValueLayout.JAVA_BYTE, 0, v.length);
                }
                keyPtrs.set(ValueLayout.ADDRESS, (long) i * ValueLayout.ADDRESS.byteSize(), ks);
                valPtrs.set(ValueLayout.ADDRESS, (long) i * ValueLayout.ADDRESS.byteSize(), vs);
                keyLens.set(
                        ValueLayout.JAVA_LONG,
                        (long) i * ValueLayout.JAVA_LONG.byteSize(),
                        (long) k.length);
                valLens.set(
                        ValueLayout.JAVA_LONG,
                        (long) i * ValueLayout.JAVA_LONG.byteSize(),
                        (long) v.length);
            }
            batchPut(db, cf, keyPtrs, keyLens, valPtrs, valLens, count);
        }
    }

    /** Deletes {@code key} from the column family. No-op if absent. */
    public void delete(FrsDb db, FrsCfHandle cf, byte[] key) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        int rc;
        try {
            rc =
                    (int)
                            frsDelete.invokeExact(
                                    db.handle(), cf.handle(), keySeg, (long) key.length);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_delete threw: " + t.getMessage());
        }
        check(rc, "frs_delete");
    }

    // ------------------------------------------------------------------
    // 4. Flush / checkpoint / metadata
    // ------------------------------------------------------------------

    /** Flushes all pending memtables to L0. */
    public void flush(FrsDb db) {
        int rc;
        try {
            rc = (int) frsFlush.invokeExact(db.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_flush threw: " + t.getMessage());
        }
        check(rc, "frs_flush");
    }

    /** Creates a checkpoint at {@code targetDir} (must not yet exist). */
    public void createCheckpoint(FrsDb db, String targetDir) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment dirSeg = allocateCString(local, targetDir);
            int rc;
            try {
                rc = (int) frsCreateCheckpoint.invokeExact(db.handle(), dirSeg);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_create_checkpoint threw: " + t.getMessage());
            }
            check(rc, "frs_create_checkpoint");
        }
    }

    /** Returns the current global sequence number. */
    public long sequenceNumber(FrsDb db) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment outSeq = local.allocate(ValueLayout.JAVA_LONG);
            int rc;
            try {
                rc = (int) frsSequenceNumber.invokeExact(db.handle(), outSeq);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_sequence_number threw: " + t.getMessage());
            }
            check(rc, "frs_sequence_number");
            return outSeq.get(ValueLayout.JAVA_LONG, 0);
        }
    }

    // ------------------------------------------------------------------
    // 5. Delta-Join lookup + iterator
    // ------------------------------------------------------------------

    /**
     * Single-key exact-match lookup, semantically equivalent to {@link #get} but routed through the
     * dedicated Delta-Join path.
     */
    public byte[] lookupKv(FrsDb db, FrsCfHandle cf, byte[] key) {
        return getInternal(frsLookupKv, "frs_lookup_kv", db, cf, key);
    }

    /** Opens a forward iterator over the entire column family. */
    public FrsIterator iteratorOpen(FrsDb db, FrsCfHandle cf, Arena arena) {
        MemorySegment outIter = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsIteratorOpen.invokeExact(db.handle(), cf.handle(), outIter);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_iterator_open threw: " + t.getMessage());
        }
        check(rc, "frs_iterator_open");
        MemorySegment handle = outIter.get(ValueLayout.ADDRESS, 0);
        return new FrsIterator(this, handle, false);
    }

    /**
     * Opens a forward iterator bounded to keys with {@code prefix}. Returns an iterator backed by
     * {@code frs_prefix_lookup_close} on {@link FrsIterator#close()}.
     */
    public FrsIterator prefixLookupOpen(FrsDb db, FrsCfHandle cf, byte[] prefix, Arena arena) {
        MemorySegment outIter = arena.allocate(ValueLayout.ADDRESS);
        try (Arena local = Arena.ofConfined()) {
            MemorySegment prefixSeg;
            long prefixLen;
            if (prefix == null || prefix.length == 0) {
                prefixSeg = MemorySegment.NULL;
                prefixLen = 0L;
            } else {
                prefixSeg = local.allocate(prefix.length);
                MemorySegment.copy(prefix, 0, prefixSeg, ValueLayout.JAVA_BYTE, 0, prefix.length);
                prefixLen = prefix.length;
            }
            int rc;
            try {
                rc =
                        (int)
                                frsPrefixLookupOpen.invokeExact(
                                        db.handle(), cf.handle(), prefixSeg, prefixLen, outIter);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_prefix_lookup_open threw: " + t.getMessage());
            }
            check(rc, "frs_prefix_lookup_open");
        }
        MemorySegment handle = outIter.get(ValueLayout.ADDRESS, 0);
        return new FrsIterator(this, handle, true);
    }

    /** Repositions the iterator at the first key {@code >=} {@code key}. */
    public void iteratorSeek(FrsIterator iter, byte[] key) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg;
            long keyLen;
            if (key == null || key.length == 0) {
                keySeg = MemorySegment.NULL;
                keyLen = 0L;
            } else {
                keySeg = local.allocate(key.length);
                MemorySegment.copy(key, 0, keySeg, ValueLayout.JAVA_BYTE, 0, key.length);
                keyLen = key.length;
            }
            int rc;
            try {
                rc = (int) frsIteratorSeek.invokeExact(iter.handle(), keySeg, keyLen);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_iterator_seek threw: " + t.getMessage());
            }
            check(rc, "frs_iterator_seek");
        }
    }

    /**
     * Advances {@code iter} and returns the next entry, or {@code null} when the iterator is
     * exhausted. Each returned key/value is heap-owned by Rust; this method copies the bytes into
     * Java heap and frees the native buffers via {@code frs_bytes_free}.
     */
    public IteratorEntry iteratorNext(FrsIterator iter) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment outKey = local.allocate(FRS_BYTES_LAYOUT);
            MemorySegment outValue = local.allocate(FRS_BYTES_LAYOUT);
            MemorySegment outValid = local.allocate(ValueLayout.JAVA_BOOLEAN);
            int rc;
            try {
                rc = (int) frsIteratorNext.invokeExact(iter.handle(), outKey, outValue, outValid);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_iterator_next threw: " + t.getMessage());
            }
            check(rc, "frs_iterator_next");

            boolean valid = outValid.get(ValueLayout.JAVA_BOOLEAN, 0);
            if (!valid) {
                // Rust set both FrsBytes to NULL in this case — nothing to free.
                return null;
            }

            byte[] keyCopy = copyAndFree(outKey, "frs_iterator_next/key");
            byte[] valueCopy = copyAndFree(outValue, "frs_iterator_next/value");
            return new IteratorEntry(keyCopy, valueCopy);
        }
    }

    // ------------------------------------------------------------------
    // Internal close hooks
    // ------------------------------------------------------------------

    /** Internal: invoked by {@link FrsDb#close()}. */
    void dbClose(MemorySegment handle) {
        try {
            int rc = (int) frsDbClose.invokeExact(handle);
            check(rc, "frs_db_close");
        } catch (Throwable t) {
            if (t instanceof FrsBackendException fbe) {
                throw fbe;
            }
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_close threw: " + t.getMessage());
        }
    }

    /** Internal: invoked by {@link FrsCfHandle#close()}. */
    void cfClose(MemorySegment handle) {
        try {
            int rc = (int) frsCfClose.invokeExact(handle);
            check(rc, "frs_cf_close");
        } catch (Throwable t) {
            if (t instanceof FrsBackendException fbe) {
                throw fbe;
            }
            throw new FrsBackendException(FrsStatus.PANIC, "frs_cf_close threw: " + t.getMessage());
        }
    }

    /** Internal: invoked by {@link FrsIterator#close()}. */
    void iteratorClose(MemorySegment handle, boolean prefix) {
        MethodHandle mh = prefix ? frsPrefixLookupClose : frsIteratorClose;
        String fn = prefix ? "frs_prefix_lookup_close" : "frs_iterator_close";
        try {
            int rc = (int) mh.invokeExact(handle);
            check(rc, fn);
        } catch (Throwable t) {
            if (t instanceof FrsBackendException fbe) {
                throw fbe;
            }
            throw new FrsBackendException(FrsStatus.PANIC, fn + " threw: " + t.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Shared Get/LookupKv helper: marshals the key, reads the FrsBytes, frees the buffer.
     *
     * <p>Hot path optimization: both the key buffer and the small (24-byte) {@code FrsBytes} out
     * struct are heap-allocated and passed via {@link MemorySegment#ofArray(byte[])} to the
     * critical-mode handle, eliminating any per-call native allocation. The pointer + length we
     * read back from the heap-byte-array view use unaligned address/long layouts (alignment 1)
     * because a {@code byte[]} only guarantees 1-byte alignment.
     */
    private byte[] getInternal(MethodHandle mh, String fn, FrsDb db, FrsCfHandle cf, byte[] key) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        // FrsBytes layout: data ptr (8) + len (8) + capacity (8) = 24 bytes.
        byte[] outBytesArr = new byte[24];
        MemorySegment outBytes = MemorySegment.ofArray(outBytesArr);
        int rc;
        try {
            rc =
                    (int)
                            mh.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keySeg,
                                    (long) key.length,
                                    outBytes);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, fn + " threw: " + t.getMessage());
        }
        check(rc, fn);

        long dataAddr = outBytes.get(ValueLayout.ADDRESS_UNALIGNED, 0L).address();
        long len =
                outBytes.get(
                        ValueLayout.JAVA_LONG_UNALIGNED, ValueLayout.ADDRESS_UNALIGNED.byteSize());
        if (dataAddr == 0L) {
            return null; // not found
        }
        return copyAndFreeRaw(outBytes, dataAddr, len, fn + "/free");
    }

    /** Reads a non-null FrsBytes payload, copies to byte[], and frees the native buffer. */
    private byte[] copyAndFree(MemorySegment frsBytes, String fn) {
        long dataAddr = frsBytes.get(ValueLayout.ADDRESS, 0).address();
        long len = frsBytes.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize());
        if (dataAddr == 0L) {
            // No data — nothing to free, return empty byte[].
            return new byte[0];
        }
        return copyAndFreeRaw(frsBytes, dataAddr, len, fn);
    }

    private byte[] copyAndFreeRaw(MemorySegment frsBytes, long dataAddr, long len, String fn) {
        try {
            MemorySegment dataSeg = MemorySegment.ofAddress(dataAddr).reinterpret(len);
            byte[] copy = new byte[(int) len];
            MemorySegment.copy(dataSeg, ValueLayout.JAVA_BYTE, 0, copy, 0, (int) len);
            return copy;
        } finally {
            int freeRc;
            try {
                freeRc = (int) frsBytesFree.invokeExact(frsBytes);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_bytes_free threw: " + t.getMessage());
            }
            check(freeRc, fn);
        }
    }

    /** Allocates a NUL-terminated UTF-8 C string in {@code arena}. */
    private static MemorySegment allocateCString(Arena arena, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        MemorySegment seg = arena.allocate(bytes.length + 1L);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        seg.set(ValueLayout.JAVA_BYTE, bytes.length, (byte) 0);
        return seg;
    }

    private static void check(int rc, String fn) {
        if (rc != FrsStatus.OK.code()) {
            throw new FrsBackendException(FrsStatus.fromCode(rc), fn);
        }
    }

    /** Iterator entry returned by {@link #iteratorNext(FrsIterator)}. */
    public record IteratorEntry(byte[] key, byte[] value) {}
}

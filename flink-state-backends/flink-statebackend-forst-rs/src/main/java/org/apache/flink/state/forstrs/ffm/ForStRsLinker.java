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

    /**
     * {@code FrsEngineOptions} struct layout (B-Prod-P7, spec §6d). Mirrors the {@code #[repr(C)]}
     * struct in {@code crates/forst-rs-ffi/src/lib.rs}.
     *
     * <p>Layout (48 bytes on 64-bit):
     *
     * <pre>
     * +0   ADDRESS (8B)   db_path
     * +8   JAVA_LONG (8B) write_buffer_size
     * +16  JAVA_INT  (4B) max_write_buffer_number
     * +20  JAVA_INT  (4B) max_background_compactions
     * +24  JAVA_INT  (4B) max_background_flushes
     * +28  4B padding (alignment for following u64)
     * +32  JAVA_LONG (8B) block_cache_capacity_bytes
     * +40  JAVA_LONG (8B) write_buffer_manager_capacity_bytes
     * </pre>
     *
     * <p>The padding is added explicitly via {@link MemoryLayout#paddingLayout(long)} so that the
     * Java mirror produces the exact 48-byte struct that Rust's {@code #[repr(C)]} layout generates
     * on the same target. Future fields land at +48 onwards (append-only).
     */
    public static final StructLayout FRS_ENGINE_OPTIONS_LAYOUT =
            MemoryLayout.structLayout(
                    ValueLayout.ADDRESS.withName("db_path"),
                    ValueLayout.JAVA_LONG.withName("write_buffer_size"),
                    ValueLayout.JAVA_INT.withName("max_write_buffer_number"),
                    ValueLayout.JAVA_INT.withName("max_background_compactions"),
                    ValueLayout.JAVA_INT.withName("max_background_flushes"),
                    MemoryLayout.paddingLayout(4),
                    ValueLayout.JAVA_LONG.withName("block_cache_capacity_bytes"),
                    ValueLayout.JAVA_LONG.withName("write_buffer_manager_capacity_bytes"));

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
    private final MethodHandle frsDbOpenRemote;
    private final MethodHandle frsDbClose;
    // B-Prod-P7 §6d: structured open + WriteBufferManager diagnostics.
    private final MethodHandle frsDbOpenWithOptions;
    private final MethodHandle frsDbWbmCapacity;
    private final MethodHandle frsDbWbmCurrentBytes;

    // --- 2. CF management ---
    private final MethodHandle frsDbDefaultCf;
    private final MethodHandle frsDbCreateCf;
    private final MethodHandle frsDbOpenCf;
    private final MethodHandle frsCfClose;

    // --- 3. Point ops ---
    private final MethodHandle frsPut;
    private final MethodHandle frsGet;
    private final MethodHandle frsGetPinned;
    private final MethodHandle frsGetAndPut;
    private final MethodHandle frsDelete;

    // --- 3b. Batch ops ---
    private final MethodHandle frsBatchPut;
    private final MethodHandle frsBatchGet;

    // C1: zero-copy batch_put via Arrow C Data Interface. The native side
    // takes an FFI_ArrowArray + FFI_ArrowSchema pair (key | value | op_type)
    // and dispatches DIRECTLY into the memtable's columnar storage with no
    // intermediate WriteBatch allocation. See `frs_batch_put_arrow` in
    // `crates/forst-rs-ffi/src/lib.rs` and `DbImpl::batch_put_arrow` in
    // `crates/forst-rs-engine/src/db.rs`.
    private final MethodHandle frsBatchPutArrow;

    // --- 4. Memory management ---
    private final MethodHandle frsBytesFree;

    // --- 5. Flush / checkpoint / metadata ---
    private final MethodHandle frsFlush;
    private final MethodHandle frsCreateCheckpoint;
    private final MethodHandle frsSequenceNumber;

    // --- 6. Delta-Join lookup + iterator ---
    private final MethodHandle frsLookupKv;
    private final MethodHandle frsGetIntoBuf;
    private final MethodHandle frsIteratorOpen;
    private final MethodHandle frsIteratorSeek;
    private final MethodHandle frsIteratorNext;
    private final MethodHandle frsIteratorClose;
    private final MethodHandle frsPrefixLookupOpen;
    private final MethodHandle frsPrefixLookupClose;

    // --- 7. TTL compaction filter ---
    private final MethodHandle frsCfSetCompactionFilterTtl;

    // --- 8. MVCC snapshot + versioned reads + incremental checkpoint (B-Prod-P2) ---
    private final MethodHandle frsDbSnapshot;
    private final MethodHandle frsDbReleaseSnapshot;
    private final MethodHandle frsGetAt;
    private final MethodHandle frsIteratorOpenAt;
    private final MethodHandle frsCreateIncrementalCheckpointAt;
    private final MethodHandle frsDbOpenFromIncremental;
    private final MethodHandle frsDbIncrementalCheckpointResultFree;

    // --- 9. State import / export migration (B-Prod-P10, spec §6g) ---
    private final MethodHandle frsCfExport;
    private final MethodHandle frsDbCreateCfFromImport;

    // --- 9b. drop_cf + ingest_external_sst (B-Prod-followup-5, spec §6g) ---
    // Native fast-path import: hardlink pre-built SSTs into L0 in
    // O(file-count) rather than scan + replay-put in O(key-count). The
    // drop_cf companion lets callers reclaim a CF name before re-importing
    // under it (the legacy migration path required a fresh name because
    // `drop_cf` did not exist).
    private final MethodHandle frsDbDropCf;
    private final MethodHandle frsDbIngestExternalSst;

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

        // B-Prod-P6: open the engine on top of an OpenDAL URI with a local
        // LRU cache. JSON config is a flat string-to-string object; pass
        // "{}" or NULL when the URI scheme needs no extra config.
        this.frsDbOpenRemote =
                bind(
                        "frs_db_open_remote",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // uri (c_char*)
                                ValueLayout.ADDRESS, // opendal_config_json (c_char*)
                                ValueLayout.ADDRESS, // cache_dir (c_char*)
                                ValueLayout.JAVA_LONG, // cache_capacity_bytes (u64)
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbClose =
                bind(
                        "frs_db_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // B-Prod-P7 §6d: structured open + WBM diagnostics. The opts arg
        // is a pointer to FrsEngineOptions, allocated in the caller's Arena
        // and populated field-by-field (see #dbOpenWithOptions).
        this.frsDbOpenWithOptions =
                bind(
                        "frs_db_open_with_options",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // const FrsEngineOptions*
                                ValueLayout.ADDRESS)); // out_handle (FrsDb*)
        this.frsDbWbmCapacity =
                bind(
                        "frs_db_write_buffer_manager_capacity",
                        FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
        this.frsDbWbmCurrentBytes =
                bind(
                        "frs_db_write_buffer_manager_current_bytes",
                        FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

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

        // Zero-copy pinned get from memtable inline storage. Returns a direct
        // pointer into the memtable's HashEntry.inline_value without any Rust-side
        // Vec allocation. Critical mode: the key byte[] is pinned for the call.
        this.frsGetPinned =
                bindCritical(
                        "frs_get_pinned",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // out_ptr (*const u8*)
                                ValueLayout.ADDRESS)); // out_len (usize*)

        this.frsGetAndPut =
                bindCritical(
                        "frs_get_and_put",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // new_value ptr
                                ValueLayout.JAVA_LONG, // new_value_len
                                ValueLayout.ADDRESS)); // out FrsBytes* (old value)

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

        // frs_batch_get: reads N keys in one call, returning N FrsBytes slots.
        // Native signature:
        //   int frs_batch_get(FrsDb, FrsCfHandle, *const *const u8 keys,
        //                     *const usize key_lens, usize count, *mut FrsBytes out_values)
        this.frsBatchGet =
                bind(
                        "frs_batch_get",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // keys (uint8_t* const*)
                                ValueLayout.ADDRESS, // key_lens (size_t*)
                                ValueLayout.JAVA_LONG, // count (size_t)
                                ValueLayout.ADDRESS)); // out_values (FrsBytes*)

        // C1: zero-copy batch_put via Arrow C Data Interface.
        // Native signature:
        //   int frs_batch_put_arrow(
        //       FrsDb handle, FrsCfHandle cf,
        //       FFI_ArrowArray*  array,
        //       FFI_ArrowSchema* schema);
        // The native side takes ownership of *array / *schema (and zeroes the
        // originals per the Arrow C Data Interface contract), so the caller
        // does not need to free them after the call returns.
        this.frsBatchPutArrow =
                bind(
                        "frs_batch_put_arrow",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // array (FFI_ArrowArray*)
                                ValueLayout.ADDRESS)); // schema (FFI_ArrowSchema*)

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

        this.frsGetIntoBuf =
                bindCritical(
                        "frs_get_into_buf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // out_buf ptr
                                ValueLayout.JAVA_LONG, // out_buf_cap
                                ValueLayout.ADDRESS)); // out_val_len ptr

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

        // 7. TTL compaction filter — Flink-shaped per-CF filter that drops entries
        // older than ttl_ms. Engine-side enforcement runs at flush + L0→L1 compaction;
        // see crates/forst-rs-engine/src/compaction_filter.rs.
        this.frsCfSetCompactionFilterTtl =
                bind(
                        "frs_cf_set_compaction_filter_ttl",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.JAVA_LONG, // ttl_ms (u64)
                                ValueLayout.JAVA_INT, // state_type (i32)
                                ValueLayout.JAVA_LONG)); // timestamp_offset (usize)

        // 8. MVCC snapshot + versioned reads + incremental checkpoint (B-Prod-P2)
        this.frsDbSnapshot =
                bind(
                        "frs_db_snapshot",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // out_snapshot (FrsSnapshot*)

        this.frsDbReleaseSnapshot =
                bind(
                        "frs_db_release_snapshot",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // snapshot (FrsSnapshot)

        // get_at is hot like get/lookup_kv — bound critical so heap-byte-array
        // segments work without per-call native staging.
        this.frsGetAt =
                bindCritical(
                        "frs_get_at",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // snapshot
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS)); // out FrsBytes*

        this.frsIteratorOpenAt =
                bind(
                        "frs_iterator_open_at",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // snapshot
                                ValueLayout.ADDRESS)); // out_iter

        this.frsCreateIncrementalCheckpointAt =
                bind(
                        "frs_create_incremental_checkpoint_at",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // snapshot
                                ValueLayout.JAVA_LONG, // checkpoint_id (u64)
                                ValueLayout.JAVA_LONG, // base_checkpoint_id (u64)
                                ValueLayout.ADDRESS)); // out (FrsIncrementalCheckpointResult*)

        this.frsDbOpenFromIncremental =
                bind(
                        "frs_db_open_from_incremental",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // target_dir (c_char*)
                                ValueLayout.ADDRESS, // base_manifest (c_char*)
                                ValueLayout.ADDRESS, // sst_files (c_char* const*)
                                ValueLayout.JAVA_LONG, // sst_file_count (size_t)
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbIncrementalCheckpointResultFree =
                bind(
                        "frs_db_incremental_checkpoint_result_free",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // 9. State import / export migration (B-Prod-P10, spec §6g).
        //
        // The native side writes a single self-describing blob
        // (`EXPORT.frsblob`) under `export_dir` — see
        // crates/forst-rs-ffi/src/lib.rs::frs_cf_export and
        // crates/forst-rs-engine/src/db.rs::cf_export for the on-disk
        // format. The import side creates a fresh CF and replays the blob.
        this.frsCfExport =
                bind(
                        "frs_cf_export",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS)); // export_dir (c_char*)

        this.frsDbCreateCfFromImport =
                bind(
                        "frs_db_create_cf_from_import",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // name (c_char*)
                                ValueLayout.ADDRESS, // import_dir (c_char*)
                                ValueLayout.ADDRESS)); // out_cf

        // 9b. drop_cf + ingest_external_sst (B-Prod-followup-5).
        // See crates/forst-rs-ffi/src/lib.rs::frs_db_drop_cf and
        // crates/forst-rs-ffi/src/lib.rs::frs_db_ingest_external_sst.
        this.frsDbDropCf =
                bind(
                        "frs_db_drop_cf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // cf

        this.frsDbIngestExternalSst =
                bind(
                        "frs_db_ingest_external_sst",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // sst_paths (const char**)
                                ValueLayout.JAVA_LONG)); // count (usize → JAVA_LONG)
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
     * to stage bytes through a per-call native arena. This eliminates two {@code Arena.allocate} +
     * {@code MemorySegment.copy} pairs per put/get, making the FFM bridge competitive with the JNI
     * {@code GetByteArrayElements} pin path.
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
     * the four parameters map 1:1 onto {@code EngineOptions.write_buffer_size}, {@code
     * max_write_buffer_number}, {@code max_background_compactions} and {@code
     * max_background_flushes}. Any parameter passed as {@code 0} keeps the engine's built-in
     * default for that field; out-of-range values flow through {@code
     * EngineOptionsBuilder::try_build} on the Rust side and surface as {@link
     * FrsStatus#INVALID_ARGUMENT}, not a JVM panic.
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
     * Opens an in-memory or filesystem-backed engine using a structured {@link
     * org.apache.flink.state.forstrs.config.ForStRsOptions} (B-Prod-P7, spec §6d).
     *
     * <p>Builds a native {@code FrsEngineOptions} struct in {@code arena}, sets the cache / WBM /
     * write-buffer / background-thread fields, then dispatches to {@code frs_db_open_with_options}.
     * {@code dbPath} of {@code null} or empty opens an in-memory engine at {@code /db}; non-null
     * opens an on-disk engine at the given path. Each numeric field of {@code 0} keeps the engine's
     * built-in default for that field.
     *
     * <p>The struct is allocated in {@code arena}; the caller may free it as soon as this method
     * returns. Failure modes:
     *
     * <ul>
     *   <li>{@link FrsStatus#INVALID_ARGUMENT} when any field violates the engine's validation caps
     *       (e.g. cache > 1 PiB).
     *   <li>{@link FrsStatus#PANIC} when the JVM cannot reach the cdylib (lookup failure).
     * </ul>
     */
    public FrsDb dbOpenWithOptions(
            Arena arena,
            String dbPath,
            long writeBufferSize,
            int maxWriteBufferNumber,
            int maxBackgroundCompactions,
            int maxBackgroundFlushes,
            long blockCacheCapacityBytes,
            long writeBufferManagerCapacityBytes) {
        MemorySegment optsSeg = arena.allocate(FRS_ENGINE_OPTIONS_LAYOUT);

        MemorySegment pathSeg =
                (dbPath == null || dbPath.isEmpty())
                        ? MemorySegment.NULL
                        : allocateCString(arena, dbPath);
        // Field offsets: 0, 8, 16, 20, 24, 28(pad), 32, 40 — see
        // FRS_ENGINE_OPTIONS_LAYOUT docstring.
        optsSeg.set(ValueLayout.ADDRESS, 0, pathSeg);
        optsSeg.set(ValueLayout.JAVA_LONG, 8, writeBufferSize);
        optsSeg.set(ValueLayout.JAVA_INT, 16, maxWriteBufferNumber);
        optsSeg.set(ValueLayout.JAVA_INT, 20, maxBackgroundCompactions);
        optsSeg.set(ValueLayout.JAVA_INT, 24, maxBackgroundFlushes);
        // 4 bytes padding at offset 28
        optsSeg.set(ValueLayout.JAVA_LONG, 32, blockCacheCapacityBytes);
        optsSeg.set(ValueLayout.JAVA_LONG, 40, writeBufferManagerCapacityBytes);

        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenWithOptions.invokeExact(optsSeg, outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_with_options threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_with_options");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Returns the configured WriteBufferManager capacity in bytes (B-Prod-P7, spec §6d). {@code 0}
     * means unbounded (the engine has no cross-CF cap). Useful for tuning ITs that want to verify
     * their requested cap round-tripped through FFI.
     */
    public long dbWriteBufferManagerCapacity(FrsDb db) {
        try {
            return (long) frsDbWbmCapacity.invokeExact(db.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_db_write_buffer_manager_capacity threw: " + t.getMessage());
        }
    }

    /**
     * Returns the running cross-CF memtable bytes tracked by the WriteBufferManager (B-Prod-P7,
     * spec §6d). Used by ITs to assert the cap actually fires under load.
     */
    public long dbWriteBufferManagerCurrentBytes(FrsDb db) {
        try {
            return (long) frsDbWbmCurrentBytes.invokeExact(db.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_db_write_buffer_manager_current_bytes threw: " + t.getMessage());
        }
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
     * Opens a remote-storage-backed engine with a local LRU SST cache (B-Prod-P6).
     *
     * <p>{@code uri} is an OpenDAL URI such as {@code memory://}, {@code file:///abs/path}, or
     * {@code s3://bucket/}. {@code opendalConfigJson} carries any scheme-specific knobs (e.g.
     * {@code {"region":"us-east-1","endpoint":"https://minio.example.com"}}); pass {@code "{}"} or
     * {@code null} when none are needed. {@code cacheDir} is the local directory used for the LRU
     * SST cache; {@code cacheCapacityBytes} caps its on-disk footprint.
     *
     * <p>Caller closes via {@link FrsDb#close()}.
     */
    public FrsDb dbOpenRemote(
            Arena arena,
            String uri,
            String opendalConfigJson,
            String cacheDir,
            long cacheCapacityBytes) {
        MemorySegment uriSeg = allocateCString(arena, uri);
        MemorySegment cfgSeg =
                opendalConfigJson == null
                        ? MemorySegment.NULL
                        : allocateCString(arena, opendalConfigJson);
        MemorySegment cacheDirSeg = allocateCString(arena, cacheDir);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc =
                    (int)
                            frsDbOpenRemote.invokeExact(
                                    uriSeg, cfgSeg, cacheDirSeg, cacheCapacityBytes, outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_remote threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_remote");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens an engine by restoring its state from a checkpoint directory previously produced by
     * {@link #createCheckpoint(FrsDb, String)}. The checkpoint directory must contain {@code
     * CHECKPOINT.blob} and every SST file the manifest references. The engine is opened with {@code
     * db_path = targetDir}: subsequent reads/writes operate directly on the checkpoint files.
     * Caller closes via {@link FrsDb#close()}.
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

    /** Native status code returned when the value is not inline (too large or in SST). */
    private static final int FRS_STATUS_FALLBACK = 16;

    /**
     * ThreadLocal buffer for the 16-byte out struct used by {@link #getPinned}: 8 bytes for the
     * pointer + 8 bytes for the length. Avoids per-call allocation on the hot path.
     */
    private static final ThreadLocal<byte[]> PINNED_OUT_BUF =
            ThreadLocal.withInitial(() -> new byte[16]);

    /**
     * Zero-copy get from memtable inline storage. Returns the value bytes directly (copied from
     * native pointer) without Rust-side Vec allocation. Returns {@code null} if the value is not
     * inline (caller should fall back to {@link #get}).
     *
     * <p>The returned pointer points directly into the memtable's {@code HashEntry.inline_value:
     * Box<[u8]>}. It is valid until the memtable is flushed (which cannot happen mid-record in
     * Flink's single-threaded-per-slot model).
     */
    public byte[] getPinned(FrsDb db, FrsCfHandle cf, byte[] key) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        byte[] outBuf = PINNED_OUT_BUF.get();
        MemorySegment outSeg = MemorySegment.ofArray(outBuf);
        int rc;
        try {
            rc =
                    (int)
                            frsGetPinned.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keySeg,
                                    (long) key.length,
                                    outSeg.asSlice(0, 8), // out_ptr
                                    outSeg.asSlice(8, 8)); // out_len
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_get_pinned threw: " + t.getMessage());
        }
        if (rc == FRS_STATUS_FALLBACK) {
            return null; // not inline — caller should fall back to get()
        }
        check(rc, "frs_get_pinned");
        // Read ptr + len from the out buffer (heap byte[] — use unaligned reads)
        long ptr = MemorySegment.ofArray(outBuf).get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        long len = MemorySegment.ofArray(outBuf).get(ValueLayout.JAVA_LONG_UNALIGNED, 8);
        if (ptr == 0 || len == 0) {
            return null;
        }
        // Copy from native pointer to byte[] (one copy, no Rust allocation)
        return MemorySegment.ofAddress(ptr).reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
    }

    /**
     * Combined get + put in one FFM call. Returns the old value ({@code null} if the key did not
     * exist before the put). The put always succeeds regardless of whether the key existed.
     *
     * <p>Saves one FFM boundary crossing vs separate {@link #get} + {@link #put} for the dominant
     * ValueState read-modify-write pattern.
     */
    public byte[] getAndPut(FrsDb db, FrsCfHandle cf, byte[] key, byte[] newValue) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        MemorySegment valSeg = MemorySegment.ofArray(newValue);
        byte[] outBytesArr = FRS_BYTES_BUF.get();
        MemorySegment outBytes = MemorySegment.ofArray(outBytesArr);
        int rc;
        try {
            rc =
                    (int)
                            frsGetAndPut.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keySeg,
                                    (long) key.length,
                                    valSeg,
                                    (long) newValue.length,
                                    outBytes);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_get_and_put threw: " + t.getMessage());
        }
        if (rc == FrsStatus.NOT_FOUND.code()) {
            return null; // key didn't exist before the put (put still succeeded)
        }
        check(rc, "frs_get_and_put");

        long dataAddr = outBytes.get(ValueLayout.ADDRESS_UNALIGNED, 0L).address();
        long len =
                outBytes.get(
                        ValueLayout.JAVA_LONG_UNALIGNED, ValueLayout.ADDRESS_UNALIGNED.byteSize());
        if (dataAddr == 0L) {
            return null;
        }
        return copyAndFreeRaw(outBytes, dataAddr, len, "frs_get_and_put/free");
    }

    /**
     * Batched put — writes {@code count} key/value pairs in one engine call.
     *
     * <p>The four "parallel array" segments must be laid out in native memory:
     *
     * <ul>
     *   <li>{@code keyPtrs}: {@code count} {@code uint8_t*} entries — each pointer targets a key
     *       buffer whose size is the matching {@code keyLens[i]}.
     *   <li>{@code keyLens}: {@code count} {@code size_t} entries (8 bytes each).
     *   <li>{@code valuePtrs}: {@code count} {@code uint8_t*} entries.
     *   <li>{@code valueLens}: {@code count} {@code size_t} entries.
     * </ul>
     *
     * <p>The key/value buffers themselves can live anywhere (native or pinned heap) as long as the
     * pointers inside {@code keyPtrs} / {@code valuePtrs} remain valid for the duration of this
     * call. Hot path callers (e.g. JMH bench) stage everything once into a long-lived native arena
     * and never copy.
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
     * Convenience overload — stages {@code keys} / {@code values} into a fresh confined arena and
     * forwards to {@link #batchPut(FrsDb, FrsCfHandle, MemorySegment, MemorySegment, MemorySegment,
     * MemorySegment, long)}. The staging cost (one alloc + N+N copies) makes this UNSUITABLE for
     * benchmarking; use the segment overload with pre-staged buffers for the hot path.
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

    /**
     * Batch point-lookup: reads {@code count} keys in one FFM call, amortizing the boundary
     * crossing cost across N lookups.
     *
     * <p>The caller supplies pre-staged native segments for key pointers and key lengths. The
     * method allocates the output FrsBytes array internally and reads/frees each result.
     *
     * @return array of length {@code count}; null entries mean "not found".
     */
    public byte[][] batchGet(
            FrsDb db, FrsCfHandle cf, MemorySegment keyPtrs, MemorySegment keyLens, long count) {
        if (count == 0) {
            return new byte[0][];
        }
        try (Arena local = Arena.ofConfined()) {
            long bytesSize = FRS_BYTES_LAYOUT.byteSize();
            MemorySegment outValues = local.allocate(bytesSize * count);
            int rc;
            try {
                rc =
                        (int)
                                frsBatchGet.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        keyPtrs,
                                        keyLens,
                                        count,
                                        outValues);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_batch_get threw: " + t.getMessage());
            }
            check(rc, "frs_batch_get");

            byte[][] results = new byte[(int) count][];
            for (int i = 0; i < (int) count; i++) {
                MemorySegment entry = outValues.asSlice(bytesSize * i, bytesSize);
                long dataAddr = entry.get(ValueLayout.ADDRESS, 0).address();
                long len = entry.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize());
                if (dataAddr != 0L && len > 0) {
                    MemorySegment dataSeg = MemorySegment.ofAddress(dataAddr).reinterpret(len);
                    results[i] = new byte[(int) len];
                    MemorySegment.copy(dataSeg, ValueLayout.JAVA_BYTE, 0, results[i], 0, (int) len);
                    // Free the native allocation
                    int freeRc;
                    try {
                        freeRc = (int) frsBytesFree.invokeExact(entry);
                    } catch (Throwable t) {
                        throw new FrsBackendException(
                                FrsStatus.PANIC, "frs_bytes_free threw: " + t.getMessage());
                    }
                    check(freeRc, "frs_batch_get/free");
                }
                // else: results[i] stays null (not found)
            }
            return results;
        }
    }

    /**
     * Convenience overload — stages {@code keys} into a fresh confined arena and forwards to {@link
     * #batchGet(FrsDb, FrsCfHandle, MemorySegment, MemorySegment, long)}.
     *
     * @return array of length {@code keys.length}; null entries mean "not found".
     */
    public byte[][] batchGet(FrsDb db, FrsCfHandle cf, byte[][] keys) {
        int count = keys.length;
        if (count == 0) {
            return new byte[0][];
        }
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keyPtrs = local.allocate((long) count * ValueLayout.ADDRESS.byteSize());
            MemorySegment keyLens = local.allocate((long) count * ValueLayout.JAVA_LONG.byteSize());
            for (int i = 0; i < count; i++) {
                byte[] k = keys[i];
                MemorySegment ks = local.allocate(k.length == 0 ? 1 : k.length);
                if (k.length > 0) {
                    MemorySegment.copy(k, 0, ks, ValueLayout.JAVA_BYTE, 0, k.length);
                }
                keyPtrs.set(ValueLayout.ADDRESS, (long) i * ValueLayout.ADDRESS.byteSize(), ks);
                keyLens.set(
                        ValueLayout.JAVA_LONG,
                        (long) i * ValueLayout.JAVA_LONG.byteSize(),
                        (long) k.length);
            }
            // Allocate out_values in this arena (will be freed with the arena, but we
            // free individual FrsBytes payloads inside batchGetRaw).
            long bytesSize = FRS_BYTES_LAYOUT.byteSize();
            MemorySegment outValues = local.allocate(bytesSize * count);
            int rc;
            try {
                rc =
                        (int)
                                frsBatchGet.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        keyPtrs,
                                        keyLens,
                                        (long) count,
                                        outValues);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_batch_get threw: " + t.getMessage());
            }
            check(rc, "frs_batch_get");

            byte[][] results = new byte[count][];
            for (int i = 0; i < count; i++) {
                MemorySegment entry = outValues.asSlice(bytesSize * i, bytesSize);
                long dataAddr = entry.get(ValueLayout.ADDRESS, 0).address();
                long len = entry.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize());
                if (dataAddr != 0L && len > 0) {
                    MemorySegment dataSeg = MemorySegment.ofAddress(dataAddr).reinterpret(len);
                    results[i] = new byte[(int) len];
                    MemorySegment.copy(dataSeg, ValueLayout.JAVA_BYTE, 0, results[i], 0, (int) len);
                    int freeRc;
                    try {
                        freeRc = (int) frsBytesFree.invokeExact(entry);
                    } catch (Throwable t) {
                        throw new FrsBackendException(
                                FrsStatus.PANIC, "frs_bytes_free threw: " + t.getMessage());
                    }
                    check(freeRc, "frs_batch_get/free");
                }
            }
            return results;
        }
    }

    /**
     * C1 zero-copy batch_put via the Arrow C Data Interface.
     *
     * <p>The caller pre-builds an Arrow {@code RecordBatch} with the canonical schema {@code key:
     * Binary, value: Binary nullable, op_type: UInt8} and exports it to two native segments via
     * Arrow-Java's {@code Data.exportArray} (or any equivalent Arrow C Data Interface producer).
     * Both pointers MUST point to the standard {@code FFI_ArrowArray} / {@code FFI_ArrowSchema}
     * layouts.
     *
     * <p>The native side takes ownership of {@code *array} and {@code *schema} per the Arrow C Data
     * Interface contract: it reads them, then zeroes the originals so the caller does NOT have to
     * free them — release callbacks are invalidated in-place.
     *
     * <p>This bypasses the legacy {@link #batchPut} path's per-row {@code Vec<u8>} allocation in
     * WriteBatch + the subsequent {@code Vec<&[u8]>} re-borrow in {@code db.batch_write}.
     * Engine-level micro-bench shows ~1.8× speedup vs the WriteBatch path on the 1000-row Put-only
     * workload (see {@code crates/forst-rs-bench/benches/batch_put_arrow_vs_write_batch.rs}).
     *
     * @param db database handle
     * @param cf column family handle
     * @param arrayPtr pointer to a populated {@code FFI_ArrowArray} (caller-owned alloc; contents
     *     are consumed by this call)
     * @param schemaPtr pointer to a populated {@code FFI_ArrowSchema} (caller-owned alloc; contents
     *     are consumed by this call)
     */
    public void batchPutArrow(
            FrsDb db, FrsCfHandle cf, MemorySegment arrayPtr, MemorySegment schemaPtr) {
        int rc;
        try {
            rc = (int) frsBatchPutArrow.invokeExact(db.handle(), cf.handle(), arrayPtr, schemaPtr);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_batch_put_arrow threw: " + t.getMessage());
        }
        check(rc, "frs_batch_put_arrow");
    }

    /** Deletes {@code key} from the column family. No-op if absent. */
    public void delete(FrsDb db, FrsCfHandle cf, byte[] key) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        int rc;
        try {
            rc = (int) frsDelete.invokeExact(db.handle(), cf.handle(), keySeg, (long) key.length);
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

    private static final int GET_INTO_BUF_CAP = 4096;
    private static final ThreadLocal<byte[]> GET_INTO_BUF =
            ThreadLocal.withInitial(() -> new byte[GET_INTO_BUF_CAP]);
    private static final ThreadLocal<byte[]> GET_INTO_LEN_BUF =
            ThreadLocal.withInitial(() -> new byte[8]);

    public byte[] getIntoBuf(FrsDb db, FrsCfHandle cf, byte[] key) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        byte[] outBuf = GET_INTO_BUF.get();
        MemorySegment outBufSeg = MemorySegment.ofArray(outBuf);
        byte[] lenBuf = GET_INTO_LEN_BUF.get();
        MemorySegment lenSeg = MemorySegment.ofArray(lenBuf);
        int rc;
        try {
            rc = (int) frsGetIntoBuf.invokeExact(
                    db.handle(), cf.handle(),
                    keySeg, (long) key.length,
                    outBufSeg, (long) GET_INTO_BUF_CAP,
                    lenSeg);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_get_into_buf threw: " + t.getMessage());
        }
        if (rc == 17) { // FRS_STATUS_BUFFER_TOO_SMALL
            return lookupKv(db, cf, key);
        }
        check(rc, "frs_get_into_buf");
        long valLen = MemorySegment.ofArray(lenBuf).get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        if (valLen == 0) {
            return null;
        }
        byte[] result = new byte[(int) valLen];
        System.arraycopy(outBuf, 0, result, 0, (int) valLen);
        return result;
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
     * ThreadLocal pool for the 24-byte FrsBytes out-struct used by {@link #getInternal}. Avoids
     * allocating a fresh {@code byte[24]} on every point-lookup call. Thread-safe because Flink
     * task slots are single-threaded per operator chain.
     */
    private static final ThreadLocal<byte[]> FRS_BYTES_BUF =
            ThreadLocal.withInitial(() -> new byte[24]);

    /**
     * Shared Get/LookupKv helper: marshals the key, reads the FrsBytes, frees the buffer.
     *
     * <p>Hot path optimization: both the key buffer and the small (24-byte) {@code FrsBytes} out
     * struct are heap-allocated and passed via {@link MemorySegment#ofArray(byte[])} to the
     * critical-mode handle, eliminating any per-call native allocation. The pointer + length we
     * read back from the heap-byte-array view use unaligned address/long layouts (alignment 1)
     * because a {@code byte[]} only guarantees 1-byte alignment.
     *
     * <p>The 24-byte out buffer is pooled via ThreadLocal to avoid per-call allocation (Phase B1+B3
     * optimization).
     */
    private byte[] getInternal(MethodHandle mh, String fn, FrsDb db, FrsCfHandle cf, byte[] key) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        // FrsBytes layout: data ptr (8) + len (8) + capacity (8) = 24 bytes.
        byte[] outBytesArr = FRS_BYTES_BUF.get();
        MemorySegment outBytes = MemorySegment.ofArray(outBytesArr);
        int rc;
        try {
            rc =
                    (int)
                            mh.invokeExact(
                                    db.handle(), cf.handle(), keySeg, (long) key.length, outBytes);
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

    // ------------------------------------------------------------------
    // 7. TTL compaction filter — Flink-shaped per-CF filter
    //
    // The engine binds a per-CF FlinkTtlCompactionFilter (Disabled/Value/List)
    // that drops expired entries at flush + L0→L1 compaction time. Apply it
    // immediately after CF creation by calling
    // {@link #setCompactionFilterTtl(FrsDb, FrsCfHandle, long, int, long)}.
    // Once set, the filter cannot be removed without dropping/recreating the CF.
    //
    // The JNI compat shim (libforstjni surface) carries
    // {@code Java_org_forstdb_FlinkCompactionFilter_*} symbols that record
    // configure() calls but cannot reliably wire factory→holder→CF without
    // Java-side cooperation. Production paths must use this FFM API instead.
    // ------------------------------------------------------------------

    /** State-type ordinals; mirror {@code FlinkCompactionFilter.StateType} (and Rust). */
    public static final int STATE_TYPE_DISABLED = 0;

    public static final int STATE_TYPE_VALUE = 1;
    public static final int STATE_TYPE_LIST = 2;

    /**
     * Installs an engine-side TTL compaction filter on {@code cf}. Subsequent flushes and
     * compactions on this CF will drop entries whose recorded timestamp + {@code ttlMs} is older
     * than the current wall clock. The filter is engine-managed; releasing the CF (via {@link
     * FrsCfHandle#close()}) tears it down.
     *
     * <p>Value semantics ({@code stateType == STATE_TYPE_VALUE}): a single timestamp prefix on the
     * value bytes; if expired the entry is dropped. List semantics ({@code STATE_TYPE_LIST}): the
     * filter inspects each list element. {@code STATE_TYPE_DISABLED} is a no-op (kept for API
     * symmetry so callers can pass through whatever ordinal Flink configured without branching).
     *
     * <p>The {@code timestampOffset} parameter is the byte offset within the encoded value where
     * the 8-byte big-endian millisecond timestamp lives; pass {@code 0} unless your serializer
     * pads.
     *
     * @throws FrsBackendException if the native call returns a non-OK status (e.g. closed db or
     *     unknown {@code stateType})
     */
    public void setCompactionFilterTtl(
            FrsDb db, FrsCfHandle cf, long ttlMs, int stateType, long timestampOffset) {
        if (stateType < STATE_TYPE_DISABLED || stateType > STATE_TYPE_LIST) {
            throw new IllegalArgumentException(
                    "stateType must be 0/1/2 (Disabled/Value/List); got " + stateType);
        }
        if (ttlMs < 0L) {
            throw new IllegalArgumentException("ttlMs must be non-negative; got " + ttlMs);
        }
        if (timestampOffset < 0L) {
            throw new IllegalArgumentException(
                    "timestampOffset must be non-negative; got " + timestampOffset);
        }
        int rc;
        try {
            rc =
                    (int)
                            frsCfSetCompactionFilterTtl.invokeExact(
                                    db.handle(), cf.handle(), ttlMs, stateType, timestampOffset);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_cf_set_compaction_filter_ttl threw: " + t.getMessage());
        }
        check(rc, "frs_cf_set_compaction_filter_ttl");
    }

    // ------------------------------------------------------------------
    // 8. MVCC snapshot + versioned reads + incremental checkpoint (B-Prod-P2)
    //
    // These map onto the engine surface added in B-Prod-P2 Tasks 2.1-2.4 and
    // give Flink's snapshot strategy the FFM API it needs:
    //
    //   * dbSnapshot / dbReleaseSnapshot — capture / release a snapshot
    //     pinned at the current sequence number (per spec §10.0 ABI lifetime
    //     contract).
    //   * getAt — versioned point-lookup at the snapshot.
    //   * iteratorOpenAt — versioned forward iterator at the snapshot.
    //   * createIncrementalCheckpointAt + dbOpenFromIncremental — capture +
    //     restore an incremental checkpoint pinned at the snapshot.
    // ------------------------------------------------------------------

    /**
     * Captures a snapshot at the engine's current sequence number. Caller MUST close the returned
     * {@link FrsSnapshot} (try-with-resources is the canonical pattern) so the engine's compaction
     * can advance past the snapshot's seq.
     */
    public FrsSnapshot dbSnapshot(FrsDb db, Arena arena) {
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbSnapshot.invokeExact(db.handle(), outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_snapshot threw: " + t.getMessage());
        }
        check(rc, "frs_db_snapshot");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsSnapshot(this, db, handle);
    }

    /**
     * Internal: invoked by {@link FrsSnapshot#close()}. Public callers should close the snapshot
     * directly; this entry point exists so {@code FrsSnapshot} can route the close through the
     * linker without exposing the method handle.
     *
     * <p>Idempotency is enforced by {@code FrsSnapshot} on the Java side; this method assumes the
     * snapshot is non-null and not yet closed (the wrapper checks before invoking).
     */
    void dbReleaseSnapshot(FrsDb db, FrsSnapshot snapshot) {
        if (snapshot == null || snapshot.isClosed()) {
            return;
        }
        int rc;
        try {
            rc = (int) frsDbReleaseSnapshot.invokeExact(db.handle(), snapshot.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_release_snapshot threw: " + t.getMessage());
        }
        check(rc, "frs_db_release_snapshot");
    }

    /**
     * Versioned point-lookup: returns the value visible at {@code snapshot}'s sequence number, or
     * {@code null} if no version is visible (or the latest visible version is a tombstone).
     */
    public byte[] getAt(FrsDb db, FrsCfHandle cf, FrsSnapshot snapshot, byte[] key) {
        MemorySegment keySeg = MemorySegment.ofArray(key);
        // FrsBytes layout: data ptr (8) + len (8) + capacity (8) = 24 bytes.
        byte[] outBytesArr = new byte[24];
        MemorySegment outBytes = MemorySegment.ofArray(outBytesArr);
        int rc;
        try {
            rc =
                    (int)
                            frsGetAt.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    snapshot.handle(),
                                    keySeg,
                                    (long) key.length,
                                    outBytes);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_get_at threw: " + t.getMessage());
        }
        if (rc == FrsStatus.NOT_FOUND.code()) {
            return null;
        }
        check(rc, "frs_get_at");
        // Heap byte[24] only guarantees 1-byte alignment; use unaligned reads
        // (mirrors `getInternal` for the non-snapshot Get path).
        long dataAddr = outBytes.get(ValueLayout.ADDRESS_UNALIGNED, 0L).address();
        long len =
                outBytes.get(
                        ValueLayout.JAVA_LONG_UNALIGNED, ValueLayout.ADDRESS_UNALIGNED.byteSize());
        if (dataAddr == 0L) {
            // Defensive: native should have returned NOT_FOUND already, but
            // honor the same hit/miss convention as Get.
            return null;
        }
        return copyAndFreeRaw(outBytes, dataAddr, len, "frs_get_at/free");
    }

    /**
     * Opens a forward iterator that yields the latest version of each user-key with {@code seq
     * &lt;= snapshot.seq}. Drive it with {@link #iteratorNext(FrsIterator)} and release with {@link
     * FrsIterator#close()} (uses the standard non-prefix close path).
     */
    public FrsIterator iteratorOpenAt(FrsDb db, FrsCfHandle cf, FrsSnapshot snapshot, Arena arena) {
        MemorySegment outIter = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc =
                    (int)
                            frsIteratorOpenAt.invokeExact(
                                    db.handle(), cf.handle(), snapshot.handle(), outIter);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_iterator_open_at threw: " + t.getMessage());
        }
        check(rc, "frs_iterator_open_at");
        MemorySegment handle = outIter.get(ValueLayout.ADDRESS, 0);
        return new FrsIterator(this, handle, false);
    }

    /**
     * Captures an incremental checkpoint at {@code snapshot}, returning the manifest path + new vs
     * shared SST file lists. Caller MUST eventually call {@link
     * #dbIncrementalCheckpointResultFree(MemorySegment)} on {@code resultPtr} to reclaim the inner
     * allocations (manifest_path C string + the two FrsLiveFileList boxes).
     *
     * <p>{@code resultPtr} must point to a 32-byte caller-allocated buffer matching the C layout:
     *
     * <pre>
     * struct FrsIncrementalCheckpointResult {
     *     char*               manifest_path;        // 8
     *     FrsLiveFileList*    new_ssts;             // 8
     *     FrsLiveFileList*    shared_ssts;          // 8
     *     int                 flush_done_eventfd;   // 4 + 4 padding
     * };
     * </pre>
     */
    public void createIncrementalCheckpointAt(
            FrsDb db,
            FrsSnapshot snapshot,
            long checkpointId,
            long baseCheckpointId,
            MemorySegment resultPtr) {
        int rc;
        try {
            rc =
                    (int)
                            frsCreateIncrementalCheckpointAt.invokeExact(
                                    db.handle(),
                                    snapshot.handle(),
                                    checkpointId,
                                    baseCheckpointId,
                                    resultPtr);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_create_incremental_checkpoint_at threw: " + t.getMessage());
        }
        check(rc, "frs_create_incremental_checkpoint_at");
    }

    /**
     * Releases the inner allocations of an {@link FrsIncrementalCheckpointResult}-shaped struct
     * previously populated by {@link #createIncrementalCheckpointAt}. Idempotent on the native
     * side; no-op for a NULL pointer.
     */
    public void dbIncrementalCheckpointResultFree(MemorySegment resultPtr) {
        int rc;
        try {
            rc = (int) frsDbIncrementalCheckpointResultFree.invokeExact(resultPtr);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_db_incremental_checkpoint_result_free threw: " + t.getMessage());
        }
        check(rc, "frs_db_incremental_checkpoint_result_free");
    }

    /**
     * Opens a fresh DB whose state is reconstructed from a manifest blob and a list of SST file
     * paths. The native side hardlinks (or copies) each {@code sstFiles} entry into {@code
     * targetDir}, then opens the DB from the persisted manifest. Caller closes via {@link
     * FrsDb#close()}.
     */
    public FrsDb dbOpenFromIncremental(
            Arena arena, String targetDir, String baseManifest, java.util.List<String> sstFiles) {
        MemorySegment targetSeg = allocateCString(arena, targetDir);
        MemorySegment manifestSeg = allocateCString(arena, baseManifest);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        long count = sstFiles.size();
        try (Arena local = Arena.ofConfined()) {
            // Allocate `count` pointer slots and a per-path C string in `local`.
            MemorySegment paths = local.allocate(count * ValueLayout.ADDRESS.byteSize());
            for (int i = 0; i < count; i++) {
                MemorySegment p = allocateCString(local, sstFiles.get(i));
                paths.set(ValueLayout.ADDRESS, (long) i * ValueLayout.ADDRESS.byteSize(), p);
            }
            int rc;
            try {
                rc =
                        (int)
                                frsDbOpenFromIncremental.invokeExact(
                                        targetSeg, manifestSeg, paths, count, outHandle);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_db_open_from_incremental threw: " + t.getMessage());
            }
            check(rc, "frs_db_open_from_incremental");
        }
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    // ------------------------------------------------------------------
    // 9. State import / export migration (B-Prod-P10, spec §6g)
    //
    // Cross-job state transfer: dump every (key, value) row in a CF to
    // a self-describing blob under `exportDir`, ship the directory to
    // another job, and reconstitute it as a brand-new CF on the consumer
    // side. The native blob format and atomicity contract live in
    // crates/forst-rs-engine/src/db.rs (search for FRSEXP01 / cf_export
    // / create_cf_from_import).
    //
    // The Java surface is tiny on purpose: two methods that thread
    // strings through the standard `allocateCString` helper and box the
    // returned CF handle. Higher-level orchestration (e.g. shipping the
    // export directory across nodes) lives in
    // {@code org.apache.flink.state.forstrs.migration.ForStRsStateMigration}.
    // ------------------------------------------------------------------

    /**
     * Exports every live (key, value) row from {@code cf} to a single self-describing blob ({@code
     * EXPORT.frsblob}) under {@code exportDir}. The directory is created if it does not exist.
     * Concurrent writers to {@code cf} are not blocked, but only writes committed before this call
     * took the underlying scan are guaranteed to appear in the export (snapshot-at-call-time
     * semantics, mirroring RocksDB's {@code ExportColumnFamily}).
     *
     * @throws FrsBackendException if the native call returns a non-OK status
     */
    public void cfExport(FrsDb db, FrsCfHandle cf, String exportDir) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment dirSeg = allocateCString(local, exportDir);
            int rc;
            try {
                rc = (int) frsCfExport.invokeExact(db.handle(), cf.handle(), dirSeg);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_cf_export threw: " + t.getMessage());
            }
            check(rc, "frs_cf_export");
        }
    }

    /**
     * Creates a brand-new column family with name {@code name} and seeds it with every row from
     * {@code importDir/EXPORT.frsblob}. Returns a {@link FrsCfHandle} the caller MUST close.
     *
     * <p>The original CF name embedded in the blob is intentionally ignored — Flink callers
     * commonly re-namespace state when migrating jobs, and the explicit-name signature mirrors
     * RocksDB's {@code ImportColumnFamily(new_name, metadata)}. Importing under a name that already
     * exists in {@code db} returns {@link FrsStatus#INVALID_ARGUMENT}.
     *
     * @throws FrsBackendException if the blob is missing, the magic header doesn't match, the CF
     *     name is already in use, or the underlying replay puts fail
     */
    /**
     * Drops a column family (B-Prod-followup-5, spec §6g).
     *
     * <p>Removes the CF from the engine's CF maps and flips its shared {@code dropped} flag.
     * Subsequent native operations on the {@link FrsCfHandle} return {@code INVALID_ARGUMENT}.
     * Idempotent on an already-dropped CF (returns OK); rejects the default CF.
     *
     * <p>Note: this method does NOT close the Java {@link FrsCfHandle}. The caller is still
     * responsible for {@code cf.close()} to free the FFM allocation.
     *
     * @throws FrsBackendException if the native call returns a non-OK status (default CF dropped,
     *     unknown handle, …)
     */
    public void dbDropCf(FrsDb db, FrsCfHandle cf) {
        int rc;
        try {
            rc = (int) frsDbDropCf.invokeExact(db.handle(), cf.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_drop_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_drop_cf");
    }

    /**
     * Ingests pre-built SST files into the engine's L0 (B-Prod-followup-5, spec §6g).
     *
     * <p>Each path is hardlinked (or copied cross-FS) into the engine's SST directory, then
     * registered at L0 via a single atomic version edit. See {@code
     * crates/forst-rs-engine/src/db.rs::ingest_external_sst} for the caller contract (source-SST
     * compatibility, key-range overlap rules, CF visibility caveats).
     *
     * <p>An empty {@code sstPaths} array is a no-op (returns OK).
     *
     * @throws FrsBackendException if any hardlink+copy fails, the SST cannot be parsed, or the
     *     engine rejects the CF handle.
     */
    public void dbIngestExternalSst(FrsDb db, FrsCfHandle cf, java.util.List<String> sstPaths) {
        if (sstPaths == null || sstPaths.isEmpty()) {
            // Engine treats empty input as a no-op; mirror that here so
            // callers don't have to special-case.
            return;
        }
        try (Arena local = Arena.ofConfined()) {
            int n = sstPaths.size();
            // Allocate n C-string slots + an n-slot pointer array.
            MemorySegment pathsArray = local.allocate(ValueLayout.ADDRESS, n);
            for (int i = 0; i < n; i++) {
                MemorySegment cstr = allocateCString(local, sstPaths.get(i));
                pathsArray.setAtIndex(ValueLayout.ADDRESS, i, cstr);
            }
            int rc;
            try {
                rc =
                        (int)
                                frsDbIngestExternalSst.invokeExact(
                                        db.handle(), cf.handle(), pathsArray, (long) n);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_db_ingest_external_sst threw: " + t.getMessage());
            }
            check(rc, "frs_db_ingest_external_sst");
        }
    }

    public FrsCfHandle dbCreateCfFromImport(FrsDb db, Arena arena, String name, String importDir) {
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        try (Arena local = Arena.ofConfined()) {
            MemorySegment nameSeg = allocateCString(local, name);
            MemorySegment dirSeg = allocateCString(local, importDir);
            int rc;
            try {
                rc = (int) frsDbCreateCfFromImport.invokeExact(db.handle(), nameSeg, dirSeg, outCf);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_db_create_cf_from_import threw: " + t.getMessage());
            }
            check(rc, "frs_db_create_cf_from_import");
        }
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }
}

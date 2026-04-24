/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.state.forstrs.bridge;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * Foreign Function &amp; Memory (Java 21+) bindings for the ForSt-RS C ABI.
 *
 * <p>This class loads the native library {@code libforst_rs_ffi.so} (Linux),
 * {@code libforst_rs_ffi.dylib} (macOS), or {@code forst_rs_ffi.dll} (Windows)
 * and resolves every {@code frs_*} symbol declared in
 * {@code forst_rs.h}. Bindings are process-wide singletons to amortize
 * {@link Linker#downcallHandle(MemorySegment, FunctionDescriptor,
 * Linker.Option...)} costs.
 *
 * <p><b>Thread safety:</b> every method handle is immutable after
 * initialisation. Consumers can call any binding concurrently.
 *
 * <p><b>Memory management:</b> the caller supplies an {@link Arena} for each
 * call so on-heap temporary buffers (keys, values) can be pinned to native
 * memory and automatically freed when the arena is closed. The engine copies
 * inputs internally.
 *
 * <p><b>Error model:</b> every binding returns a {@code int32_t} status code;
 * non-OK codes are translated by {@link #check(int, String)} into a typed
 * {@link ForStRsException}.
 *
 * @see org.apache.flink.state.forstrs.bridge.ForStRsStatus
 */
public final class ForStRsBridge {

    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBRARY = loadLibrary();

    // ------------------------------------------------------------------
    // Method handles (one per C ABI function)
    // ------------------------------------------------------------------

    /** int frs_db_open(const char* db_path, FrsDb* out_handle); */
    public static final MethodHandle FRS_DB_OPEN =
            downcall(
                    "frs_db_open",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    /** int frs_db_open_memory(FrsDb* out_handle); */
    public static final MethodHandle FRS_DB_OPEN_MEMORY =
            downcall(
                    "frs_db_open_memory",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

    /** int frs_db_close(FrsDb handle); */
    public static final MethodHandle FRS_DB_CLOSE =
            downcall("frs_db_close", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_DB_DEFAULT_CF =
            downcall(
                    "frs_db_default_cf",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_DB_CREATE_CF =
            downcall(
                    "frs_db_create_cf",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS));

    public static final MethodHandle FRS_DB_CREATE_CF_WITH_MERGE =
            downcall(
                    "frs_db_create_cf_with_merge",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS));

    public static final MethodHandle FRS_DB_OPEN_CF =
            downcall(
                    "frs_db_open_cf",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS));

    public static final MethodHandle FRS_CF_CLOSE =
            downcall("frs_cf_close", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

    /** int frs_put(db, cf, key, key_len, value, value_len); */
    public static final MethodHandle FRS_PUT =
            downcall(
                    "frs_put",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG));

    public static final MethodHandle FRS_DELETE =
            downcall(
                    "frs_delete",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG));

    public static final MethodHandle FRS_MERGE =
            downcall(
                    "frs_merge",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG));

    public static final MethodHandle FRS_GET =
            downcall(
                    "frs_get",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS));

    public static final MethodHandle FRS_BATCH_PUT =
            downcall(
                    "frs_batch_put",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG));

    public static final MethodHandle FRS_BATCH_GET =
            downcall(
                    "frs_batch_get",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS));

    public static final MethodHandle FRS_BYTES_FREE =
            downcall(
                    "frs_bytes_free",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_FLUSH =
            downcall("frs_flush", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_FLUSH_CF =
            downcall(
                    "frs_flush_cf",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_COMPACT_CF =
            downcall(
                    "frs_compact_cf",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_COMPACT_ALL =
            downcall(
                    "frs_compact_all",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_CREATE_CHECKPOINT =
            downcall(
                    "frs_create_checkpoint",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_SEQUENCE_NUMBER =
            downcall(
                    "frs_sequence_number",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_DB_OPEN_FROM_CHECKPOINT =
            downcall(
                    "frs_db_open_from_checkpoint",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

    public static final MethodHandle FRS_BATCH_PUT_ARROW =
            downcall(
                    "frs_batch_put_arrow",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS));

    public static final MethodHandle FRS_BATCH_GET_ARROW =
            downcall(
                    "frs_batch_get_arrow",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS));

    public static final MethodHandle FRS_PREFIX_SCAN_ARROW =
            downcall(
                    "frs_prefix_scan_arrow",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS));

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private ForStRsBridge() {}

    /** Throws {@link ForStRsException} if the status indicates failure. */
    public static void check(int status, String operation) {
        if (status != ForStRsStatus.OK) {
            throw new ForStRsException(status, operation);
        }
    }

    private static MethodHandle downcall(String name, FunctionDescriptor desc) {
        MemorySegment sym =
                LIBRARY
                        .find(name)
                        .orElseThrow(
                                () ->
                                        new UnsatisfiedLinkError(
                                                "ForSt-RS symbol '"
                                                        + name
                                                        + "' not found in libforst_rs_ffi"));
        return LINKER.downcallHandle(sym, desc);
    }

    private static SymbolLookup loadLibrary() {
        String explicit = System.getProperty("forst.rs.library.path");
        if (explicit != null && !explicit.isEmpty()) {
            return SymbolLookup.libraryLookup(Path.of(explicit), Arena.global());
        }
        // Falls back to java.library.path lookup — the library name follows
        // platform conventions (libforst_rs_ffi.so on Linux, etc.).
        System.loadLibrary("forst_rs_ffi");
        return SymbolLookup.loaderLookup();
    }
}

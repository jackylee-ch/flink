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
import java.nio.file.Path;

/**
 * JDK 25 FFM bridge to libforst_rs_ffi.{dylib,so,dll}.
 *
 * <p>Loads the cdylib and binds 7 downcall MethodHandles for the MVP
 * surface: db_open_memory / db_close / db_default_cf / cf_close /
 * put / get / bytes_free.
 *
 * <p>Library lookup order:
 * <ol>
 *   <li>System property {@code forstrs.native.libpath} (absolute path)</li>
 *   <li>{@code System.loadLibrary("forst_rs_ffi")} fallback</li>
 * </ol>
 *
 * <p>Lifetime: the {@link Arena} given to the constructor owns the
 * library handle's symbol lookup; closing the Arena unloads the lib
 * (and invalidates all returned MemorySegments).
 *
 * <p>Reference: docs/design/2.5_ffm_bridge_design.md §3.
 */
public final class ForStRsLinker {

    /** {@code FrsBytes} struct layout: data ptr, len, capacity (24 bytes on 64-bit). */
    public static final StructLayout FRS_BYTES_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("data"),
            ValueLayout.JAVA_LONG.withName("len"),
            ValueLayout.JAVA_LONG.withName("capacity"));

    private final Linker linker;
    private final SymbolLookup lookup;

    private final MethodHandle frsDbOpenMemory;
    private final MethodHandle frsDbClose;
    private final MethodHandle frsDbDefaultCf;
    private final MethodHandle frsCfClose;
    private final MethodHandle frsPut;
    private final MethodHandle frsGet;
    private final MethodHandle frsBytesFree;

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

        this.frsDbOpenMemory = bind("frs_db_open_memory",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsDbClose = bind("frs_db_close",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsDbDefaultCf = bind("frs_db_default_cf",
                FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,    // db handle
                        ValueLayout.ADDRESS));  // out_cf

        this.frsCfClose = bind("frs_cf_close",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsPut = bind("frs_put",
                FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,     // db
                        ValueLayout.ADDRESS,     // cf
                        ValueLayout.ADDRESS,     // key ptr
                        ValueLayout.JAVA_LONG,   // key_len
                        ValueLayout.ADDRESS,     // value ptr
                        ValueLayout.JAVA_LONG)); // value_len

        this.frsGet = bind("frs_get",
                FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,     // db
                        ValueLayout.ADDRESS,     // cf
                        ValueLayout.ADDRESS,     // key ptr
                        ValueLayout.JAVA_LONG,   // key_len
                        ValueLayout.ADDRESS));   // out FrsBytes*

        this.frsBytesFree = bind("frs_bytes_free",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    }

    private MethodHandle bind(String name, FunctionDescriptor descriptor) {
        MemorySegment sym = lookup.find(name).orElseThrow(() ->
                new IllegalStateException("symbol not found in cdylib: " + name));
        return linker.downcallHandle(sym, descriptor);
    }

    /** Opens an in-memory ForSt-RS engine. Caller closes via {@link FrsDb#close()}. */
    public FrsDb dbOpenMemory(Arena arena) {
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenMemory.invokeExact(outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_open_memory threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_memory");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /** Returns the default column family. Caller closes via {@link FrsCfHandle#close()}. */
    public FrsCfHandle dbDefaultCf(FrsDb db, Arena arena) {
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbDefaultCf.invokeExact(db.handle(), outCf);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_default_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_default_cf");
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    /** Writes a key/value pair. */
    public void put(FrsDb db, FrsCfHandle cf, byte[] key, byte[] value) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = local.allocate(key.length);
            MemorySegment.copy(key, 0, keySeg, ValueLayout.JAVA_BYTE, 0, key.length);
            MemorySegment valSeg = local.allocate(value.length);
            MemorySegment.copy(value, 0, valSeg, ValueLayout.JAVA_BYTE, 0, value.length);
            int rc;
            try {
                rc = (int) frsPut.invokeExact(
                        db.handle(), cf.handle(),
                        keySeg, (long) key.length,
                        valSeg, (long) value.length);
            } catch (Throwable t) {
                throw new FrsBackendException(FrsStatus.PANIC, "frs_put threw: " + t.getMessage());
            }
            check(rc, "frs_put");
        }
    }

    /** Returns the value for {@code key} or {@code null} if absent. */
    public byte[] get(FrsDb db, FrsCfHandle cf, byte[] key) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = local.allocate(key.length);
            MemorySegment.copy(key, 0, keySeg, ValueLayout.JAVA_BYTE, 0, key.length);
            MemorySegment outBytes = local.allocate(FRS_BYTES_LAYOUT);
            int rc;
            try {
                rc = (int) frsGet.invokeExact(
                        db.handle(), cf.handle(),
                        keySeg, (long) key.length,
                        outBytes);
            } catch (Throwable t) {
                throw new FrsBackendException(FrsStatus.PANIC, "frs_get threw: " + t.getMessage());
            }
            check(rc, "frs_get");

            // Read FrsBytes struct: { *mut u8 data, usize len, usize capacity }
            long dataAddr = outBytes.get(ValueLayout.ADDRESS, 0).address();
            long len = outBytes.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize());
            if (dataAddr == 0L) {
                return null; // not found
            }
            try {
                MemorySegment dataSeg = MemorySegment.ofAddress(dataAddr).reinterpret(len);
                byte[] copy = new byte[(int) len];
                MemorySegment.copy(dataSeg, ValueLayout.JAVA_BYTE, 0, copy, 0, (int) len);
                return copy;
            } finally {
                int freeRc;
                try {
                    freeRc = (int) frsBytesFree.invokeExact(outBytes);
                } catch (Throwable t) {
                    throw new FrsBackendException(FrsStatus.PANIC, "frs_bytes_free threw: " + t.getMessage());
                }
                check(freeRc, "frs_bytes_free");
            }
        }
    }

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

    private static void check(int rc, String fn) {
        if (rc != FrsStatus.OK.code()) {
            throw new FrsBackendException(FrsStatus.fromCode(rc), fn);
        }
    }
}

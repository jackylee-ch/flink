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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * High-level RAII wrapper around the ForSt-RS native database handle.
 *
 * <p>Use {@link #openLocal(String)} for a disk-backed engine, or
 * {@link #openMemory()} for an in-process engine suited to tests. The handle
 * is closed automatically via {@link AutoCloseable#close()}.
 */
public final class ForStRsDb implements AutoCloseable {
    private final MemorySegment handle;
    private final Arena handleArena;

    private ForStRsDb(MemorySegment handle, Arena handleArena) {
        this.handle = handle;
        this.handleArena = handleArena;
    }

    public MemorySegment handle() {
        return handle;
    }

    /** Opens a local filesystem engine at {@code dbPath}. */
    public static ForStRsDb openLocal(String dbPath) {
        Arena arena = Arena.ofShared();
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        MemorySegment cPath = arena.allocateFrom(dbPath, StandardCharsets.UTF_8);
        try {
            int status = (int) ForStRsBridge.FRS_DB_OPEN.invokeExact(cPath, outHandle);
            ForStRsBridge.check(status, "frs_db_open");
            MemorySegment h = outHandle.get(ValueLayout.ADDRESS, 0);
            return new ForStRsDb(h, arena);
        } catch (Throwable t) {
            arena.close();
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_db_open", t);
        }
    }

    /** Opens an in-memory engine (tests only). */
    public static ForStRsDb openMemory() {
        Arena arena = Arena.ofShared();
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        try {
            int status = (int) ForStRsBridge.FRS_DB_OPEN_MEMORY.invokeExact(outHandle);
            ForStRsBridge.check(status, "frs_db_open_memory");
            MemorySegment h = outHandle.get(ValueLayout.ADDRESS, 0);
            return new ForStRsDb(h, arena);
        } catch (Throwable t) {
            arena.close();
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_db_open_memory", t);
        }
    }

    /** Returns a handle to the default CF. */
    public ForStRsColumnFamily defaultCf() {
        Arena arena = Arena.ofShared();
        MemorySegment out = arena.allocate(ValueLayout.ADDRESS);
        try {
            int status = (int) ForStRsBridge.FRS_DB_DEFAULT_CF.invokeExact(handle, out);
            ForStRsBridge.check(status, "frs_db_default_cf");
            return new ForStRsColumnFamily(out.get(ValueLayout.ADDRESS, 0), arena);
        } catch (Throwable t) {
            arena.close();
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_db_default_cf", t);
        }
    }

    /** Creates a new CF. */
    public ForStRsColumnFamily createCf(String name) {
        Arena arena = Arena.ofShared();
        MemorySegment out = arena.allocate(ValueLayout.ADDRESS);
        MemorySegment cName = arena.allocateFrom(name, StandardCharsets.UTF_8);
        try {
            int status = (int) ForStRsBridge.FRS_DB_CREATE_CF.invokeExact(handle, cName, out);
            ForStRsBridge.check(status, "frs_db_create_cf");
            return new ForStRsColumnFamily(out.get(ValueLayout.ADDRESS, 0), arena);
        } catch (Throwable t) {
            arena.close();
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_db_create_cf", t);
        }
    }

    /** put(key, value). */
    public void put(ForStRsColumnFamily cf, byte[] key, byte[] value) {
        try (Arena call = Arena.ofConfined()) {
            MemorySegment k = call.allocate(key.length);
            MemorySegment.copy(key, 0, k, ValueLayout.JAVA_BYTE, 0, key.length);
            MemorySegment v = call.allocate(value.length);
            MemorySegment.copy(value, 0, v, ValueLayout.JAVA_BYTE, 0, value.length);
            int status =
                    (int)
                            ForStRsBridge.FRS_PUT.invokeExact(
                                    handle, cf.handle(), k, (long) key.length, v, (long) value.length);
            ForStRsBridge.check(status, "frs_put");
        } catch (Throwable t) {
            if (t instanceof ForStRsException e) throw e;
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_put", t);
        }
    }

    /** delete(key). */
    public void delete(ForStRsColumnFamily cf, byte[] key) {
        try (Arena call = Arena.ofConfined()) {
            MemorySegment k = call.allocate(key.length);
            MemorySegment.copy(key, 0, k, ValueLayout.JAVA_BYTE, 0, key.length);
            int status =
                    (int) ForStRsBridge.FRS_DELETE.invokeExact(handle, cf.handle(), k, (long) key.length);
            ForStRsBridge.check(status, "frs_delete");
        } catch (Throwable t) {
            if (t instanceof ForStRsException e) throw e;
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_delete", t);
        }
    }

    /** get(key); returns null if absent. */
    public byte[] get(ForStRsColumnFamily cf, byte[] key) {
        try (Arena call = Arena.ofConfined()) {
            MemorySegment k = call.allocate(key.length);
            MemorySegment.copy(key, 0, k, ValueLayout.JAVA_BYTE, 0, key.length);
            // FrsBytes layout: data: *mut u8 | len: size_t | capacity: size_t
            MemorySegment out = call.allocate(24);
            int status =
                    (int) ForStRsBridge.FRS_GET.invokeExact(handle, cf.handle(), k, (long) key.length, out);
            ForStRsBridge.check(status, "frs_get");
            MemorySegment dataPtr = out.get(ValueLayout.ADDRESS, 0);
            if (dataPtr.address() == 0L) {
                return null;
            }
            long len = out.get(ValueLayout.JAVA_LONG, 8);
            byte[] result = new byte[(int) len];
            MemorySegment data = dataPtr.reinterpret(len);
            MemorySegment.copy(data, ValueLayout.JAVA_BYTE, 0, result, 0, (int) len);
            // Free the Rust-owned buffer.
            int freeStatus = (int) ForStRsBridge.FRS_BYTES_FREE.invokeExact(out);
            ForStRsBridge.check(freeStatus, "frs_bytes_free");
            return result;
        } catch (Throwable t) {
            if (t instanceof ForStRsException e) throw e;
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_get", t);
        }
    }

    /** Flushes every CF. */
    public void flush() {
        try {
            int status = (int) ForStRsBridge.FRS_FLUSH.invokeExact(handle);
            ForStRsBridge.check(status, "frs_flush");
        } catch (Throwable t) {
            if (t instanceof ForStRsException e) throw e;
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_flush", t);
        }
    }

    /** Creates a checkpoint at {@code targetDir}. */
    public void createCheckpoint(String targetDir) {
        try (Arena call = Arena.ofConfined()) {
            MemorySegment cPath = call.allocateFrom(targetDir, StandardCharsets.UTF_8);
            int status = (int) ForStRsBridge.FRS_CREATE_CHECKPOINT.invokeExact(handle, cPath);
            ForStRsBridge.check(status, "frs_create_checkpoint");
        } catch (Throwable t) {
            if (t instanceof ForStRsException e) throw e;
            throw new ForStRsException(ForStRsStatus.ERROR, "frs_create_checkpoint", t);
        }
    }

    @Override
    public void close() {
        try {
            int status = (int) ForStRsBridge.FRS_DB_CLOSE.invokeExact(handle);
            ForStRsBridge.check(status, "frs_db_close");
        } catch (Throwable t) {
            // Swallow during close to match AutoCloseable best practice.
        } finally {
            handleArena.close();
        }
    }
}

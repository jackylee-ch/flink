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

package org.forstdb;

/**
 * Mirror class for the COMMUNITY {@code com.ververica:forstjni} cdylib.
 *
 * <p>The upstream RocksDB Java API has 4 overloads per CRUD method:
 *
 * <pre>
 *   put(byte[]...)                              -> _Java_..._put__J_3BII_3BII
 *   put(WriteOptions, byte[]...)                -> _Java_..._put__JJ_3BII_3BII
 *   put(ColumnFamilyHandle, byte[]...)          -> _Java_..._put__J_3BII_3BIIJ
 *   put(WriteOptions, ColumnFamilyHandle, ...)  -> _Java_..._put__JJ_3BII_3BIIJ
 * </pre>
 *
 * The "this handle" is always the first long arg. We expose the simplest
 * default-CF, default-options overload ({@code _Java_..._put__J_3BII_3BII})
 * and declare ALL the others as no-call stubs so JNI uses long-form mangling.
 */
public class RocksDB {

    static {
        String p = System.getProperty("org.forstdb.libpath");
        if (p == null || p.isEmpty()) {
            System.loadLibrary("forstjni");
        } else {
            System.load(p);
        }
    }

    private RocksDB() {}

    // -- open overloads -- (J,String) is what we use; (J,String,[][B,[J) declared to force long form
    public static native long open(long optionsHandle, String path) throws Exception;

    public static native long open(
            long optionsHandle, String path, byte[][] cfNames, long[] cfOptions);

    // -- close (short-form, single Java method) ---------------------------
    public static native void closeDatabase(long handle);

    public static native void disposeInternal(long handle);

    public static native long getDefaultColumnFamily(long handle);

    public static native void flush(long handle, long flushOptionsHandle) throws Exception;

    // -- put: 4 overloads to coerce long-form. We CALL the (J,[B,...) variant.
    public static native void put(
            long handle, byte[] key, int keyOff, int keyLen, byte[] val, int valOff, int valLen)
            throws Exception;

    public static native void put(
            long handle,
            long writeOptionsHandle,
            byte[] key,
            int keyOff,
            int keyLen,
            byte[] val,
            int valOff,
            int valLen);

    public static native void put(
            long handle,
            byte[] key,
            int keyOff,
            int keyLen,
            byte[] val,
            int valOff,
            int valLen,
            long cfHandle);

    public static native void put(
            long handle,
            long writeOptionsHandle,
            byte[] key,
            int keyOff,
            int keyLen,
            byte[] val,
            int valOff,
            int valLen,
            long cfHandle);

    // -- get: 4 overloads. We CALL the (J,[B,...) variant.
    public static native byte[] get(long handle, byte[] key, int keyOff, int keyLen)
            throws Exception;

    public static native byte[] get(
            long handle, long readOptionsHandle, byte[] key, int keyOff, int keyLen);

    public static native byte[] get(
            long handle, byte[] key, int keyOff, int keyLen, long cfHandle);

    public static native byte[] get(
            long handle, long readOptionsHandle, byte[] key, int keyOff, int keyLen, long cfHandle);

    // -- delete: 4 overloads. We don't actually call delete in the bench.
    public static native void delete(long handle, byte[] key, int keyOff, int keyLen)
            throws Exception;

    public static native void delete(
            long handle, long writeOptionsHandle, byte[] key, int keyOff, int keyLen);

    public static native void delete(
            long handle, byte[] key, int keyOff, int keyLen, long cfHandle);

    public static native void delete(
            long handle, long writeOptionsHandle, byte[] key, int keyOff, int keyLen, long cfHandle);
}

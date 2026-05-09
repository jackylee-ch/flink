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
 * Minimal Java surface that matches the JNI symbols exported by the ForSt-RS
 * {@code compat-jni} feature. This is NOT the upstream community {@code
 * org.forstdb.RocksDB} class — it only declares the flat helper methods that
 * the ForSt-RS shim implements. The class name + package are chosen so that
 * the JNI symbol mangling lines up with the {@code Java_org_forstdb_RocksDB_*}
 * symbols in {@code libforst_rs_ffi.dylib}.
 *
 * <p>The cdylib path can be overridden via the {@code org.forstdb.libpath}
 * system property; otherwise we fall back to {@code System.loadLibrary("forstjni")}.
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

    // Lifecycle
    public static native long open(String path);

    public static native void close(long handle);

    public static native boolean isClosed(long handle);

    // Default CF + flush + checkpoint
    public static native long getDefaultColumnFamily(long handle);

    public static native void flush(long handle);

    public static native void flushCf(long handle, long cfHandle);

    public static native void compactRange(long handle, long cfHandle);

    public static native void compactRangeAll(long handle);

    public static native void createCheckpoint(long handle, String targetDir);

    public static native long getLatestSequenceNumber(long handle);

    public static native long l0FileCount(long handle, long cfHandle);

    // CF management
    public static native long createColumnFamily(long handle, String name);

    public static native long openColumnFamily(long handle, String name);

    public static native void dropColumnFamily(long handle, long cfHandle);

    // Point ops — slice form (key/val + offset + length)
    public static native void put(
            long handle,
            long cfHandle,
            byte[] key,
            int keyOff,
            int keyLen,
            byte[] val,
            int valOff,
            int valLen);

    public static native byte[] get(
            long handle, long cfHandle, byte[] key, int keyOff, int keyLen);

    public static native void delete(
            long handle, long cfHandle, byte[] key, int keyOff, int keyLen);

    public static native void merge(
            long handle,
            long cfHandle,
            byte[] key,
            int keyOff,
            int keyLen,
            byte[] val,
            int valOff,
            int valLen);
}

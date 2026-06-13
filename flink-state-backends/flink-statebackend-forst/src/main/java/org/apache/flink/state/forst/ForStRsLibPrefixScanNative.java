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

package org.apache.flink.state.forst;

import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/** JNI bridge for optional forst-rs-lib prefix-scan chunk APIs. */
final class ForStRsLibPrefixScanNative {

    private static final String ENABLED_PROPERTY = "flink.forst.forst-rs-lib.prefix-scan.enabled";

    private static volatile Boolean available;

    private static final AtomicLong nextChunkCalls = new AtomicLong();

    private ForStRsLibPrefixScanNative() {}

    static boolean isAvailable() {
        if (!Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "true"))) {
            return false;
        }
        Boolean cached = available;
        if (cached != null) {
            return cached;
        }
        synchronized (ForStRsLibPrefixScanNative.class) {
            cached = available;
            if (cached != null) {
                return cached;
            }
            boolean detected = detectAvailable();
            available = detected;
            return detected;
        }
    }

    static long prefixLookupOpen(
            long dbHandle, long cfHandle, byte[] prefix, int prefixOffset, int prefixLength)
            throws RocksDBException {
        return prefixLookupOpen0(dbHandle, cfHandle, prefix, prefixOffset, prefixLength);
    }

    static long prefixLookupNextChunk(
            long iterHandle,
            int maxRows,
            ByteBuffer keyOffsets,
            ByteBuffer keyData,
            ByteBuffer valueOffsets,
            ByteBuffer valueData,
            ByteBuffer valueValidity)
            throws RocksDBException {
        long packed =
                prefixLookupNextChunk0(
                        iterHandle,
                        maxRows,
                        keyOffsets,
                        keyData,
                        valueOffsets,
                        valueData,
                        valueValidity);
        nextChunkCalls.incrementAndGet();
        return packed;
    }

    static void resetNextChunkCallsForTesting() {
        nextChunkCalls.set(0);
    }

    static long getNextChunkCallsForTesting() {
        return nextChunkCalls.get();
    }

    static void prefixLookupClose(long iterHandle) throws RocksDBException {
        prefixLookupClose0(iterHandle);
    }

    static int chunkCount(long packed) {
        return (int) packed;
    }

    static boolean chunkEof(long packed) {
        return (packed & (1L << 32)) != 0;
    }

    private static boolean detectAvailable() {
        try {
            RocksDB.loadLibrary();
            return isAvailable0();
        } catch (LinkageError | RuntimeException e) {
            return false;
        }
    }

    private static native boolean isAvailable0();

    private static native long prefixLookupOpen0(
            long dbHandle, long cfHandle, byte[] prefix, int prefixOffset, int prefixLength)
            throws RocksDBException;

    private static native long prefixLookupNextChunk0(
            long iterHandle,
            int maxRows,
            ByteBuffer keyOffsets,
            ByteBuffer keyData,
            ByteBuffer valueOffsets,
            ByteBuffer valueData,
            ByteBuffer valueValidity)
            throws RocksDBException;

    private static native void prefixLookupClose0(long iterHandle) throws RocksDBException;
}

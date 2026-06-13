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

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Prefix-scan cursor backed by the forst-rs-lib compat JNI chunk API. */
class ForStPrefixScanCursor implements Closeable {

    private static final int INITIAL_DATA_BUFFER_CAPACITY = 1024 * 1024;

    private static final int MAX_DATA_BUFFER_CAPACITY = 256 * 1024 * 1024;

    private static final int MAX_RETAINED_DATA_BUFFER_CAPACITY = INITIAL_DATA_BUFFER_CAPACITY;

    private static final String BUFFER_TOO_SMALL_STATUS = "frs_status=110";

    private static final String BUFFER_TOO_SMALL_CAPACITY = "capacity too small";

    private static final Cleaner CLEANER = Cleaner.create();

    private static final ThreadLocal<BufferSet> BUFFER_POOL = new ThreadLocal<>();

    private static final AtomicLong BUFFER_SETS_ALLOCATED = new AtomicLong();

    private static final AtomicLong BUFFER_SETS_REUSED = new AtomicLong();

    private static final AtomicLong BUFFER_SETS_RETURNED = new AtomicLong();

    private static final AtomicLong BUFFER_SETS_DISCARDED = new AtomicLong();

    private static final AtomicLong BUFFER_SETS_GROWN = new AtomicLong();

    private static final AtomicLong BUFFER_SETS_OUTSTANDING = new AtomicLong();

    private static final AtomicLong BUFFER_SETS_RETAINED = new AtomicLong();

    private final PrefixScanCleanup cleanup;

    private final Cleaner.Cleanable cleanable;

    private BufferSet buffers;

    private Chunk prefetchedChunk;

    private ForStPrefixScanCursor(long handle, int maxRows) {
        this.cleanup = new PrefixScanCleanup(handle);
        this.cleanable = CLEANER.register(this, cleanup);
        this.buffers = borrowBuffers(maxRows);
    }

    static ForStPrefixScanCursor open(
            RocksDB db, ColumnFamilyHandle columnFamilyHandle, byte[] prefix, int maxRows)
            throws RocksDBException {
        ForStPrefixScanCursor cursor = new ForStPrefixScanCursor(0, maxRows);
        try {
            cursor.prefetchedChunk = cursor.openFirstChunk(db, columnFamilyHandle, prefix, maxRows);
            return cursor;
        } catch (RocksDBException | RuntimeException | Error e) {
            cursor.closeQuietly();
            throw e;
        }
    }

    Chunk next(int maxRows) throws RocksDBException {
        if (prefetchedChunk != null) {
            Chunk chunk = prefetchedChunk;
            prefetchedChunk = null;
            return chunk;
        }
        ensureOpen();
        return nextChunk(maxRows);
    }

    private Chunk openFirstChunk(
            RocksDB db, ColumnFamilyHandle columnFamilyHandle, byte[] prefix, int maxRows)
            throws RocksDBException {
        long handle;
        while (true) {
            clearBuffers();
            buffers.openFirstMeta.clear();
            try {
                handle =
                        ForStRsLibPrefixScanNative.prefixLookupOpenFirstChunk(
                                db.getNativeHandle(),
                                columnFamilyHandle.getNativeHandle(),
                                prefix,
                                0,
                                prefix.length,
                                maxRows,
                                buffers.openFirstMeta,
                                buffers.keyOffsets,
                                buffers.keyData,
                                buffers.valueOffsets,
                                buffers.valueData,
                                buffers.valueValidity);
                break;
            } catch (RocksDBException e) {
                if (!isBufferTooSmall(e) || !growDataBuffers()) {
                    throw e;
                }
            }
        }
        cleanup.handle = handle;
        long metaHandle = ForStRsLibPrefixScanNative.openFirstHandle(buffers.openFirstMeta);
        if (metaHandle != handle) {
            throw new RocksDBException("ForSt-RS prefix scan open-first handle mismatch");
        }
        return decodeChunk(
                ForStRsLibPrefixScanNative.openFirstCount(buffers.openFirstMeta),
                ForStRsLibPrefixScanNative.openFirstEof(buffers.openFirstMeta));
    }

    private Chunk nextChunk(int maxRows) throws RocksDBException {
        long packed;
        while (true) {
            clearBuffers();
            try {
                packed =
                        ForStRsLibPrefixScanNative.prefixLookupNextChunk(
                                cleanup.handle,
                                maxRows,
                                buffers.keyOffsets,
                                buffers.keyData,
                                buffers.valueOffsets,
                                buffers.valueData,
                                buffers.valueValidity);
                break;
            } catch (RocksDBException e) {
                if (!isBufferTooSmall(e) || !growDataBuffers()) {
                    throw e;
                }
            }
        }
        int count = ForStRsLibPrefixScanNative.chunkCount(packed);
        boolean eof = ForStRsLibPrefixScanNative.chunkEof(packed);
        return decodeChunk(count, eof);
    }

    private Chunk decodeChunk(int count, boolean eof) {
        List<ForStDBIterRequest.RawEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int keyOffset = buffers.keyOffsets.getInt(i * Integer.BYTES);
            int nextKeyOffset = buffers.keyOffsets.getInt((i + 1) * Integer.BYTES);
            int valueOffset = buffers.valueOffsets.getInt(i * Integer.BYTES);
            int nextValueOffset = buffers.valueOffsets.getInt((i + 1) * Integer.BYTES);
            if (buffers.valueValidity.get(i) == 0) {
                continue;
            }
            entries.add(
                    new ForStDBIterRequest.RawEntry(
                            copyBytes(buffers.keyData, keyOffset, nextKeyOffset - keyOffset),
                            copyBytes(
                                    buffers.valueData,
                                    valueOffset,
                                    nextValueOffset - valueOffset)));
        }
        return new Chunk(entries, eof);
    }

    private void clearBuffers() {
        buffers.clear();
    }

    private void ensureOpen() {
        if (cleanup.handle == 0) {
            throw new IllegalStateException("ForSt prefix scan cursor is closed");
        }
    }

    private boolean growDataBuffers() {
        if (buffers.keyData.capacity() >= MAX_DATA_BUFFER_CAPACITY
                && buffers.valueData.capacity() >= MAX_DATA_BUFFER_CAPACITY) {
            return false;
        }
        buffers.keyData =
                ByteBuffer.allocateDirect(nextDataBufferCapacity(buffers.keyData.capacity()));
        buffers.valueData =
                ByteBuffer.allocateDirect(nextDataBufferCapacity(buffers.valueData.capacity()));
        BUFFER_SETS_GROWN.incrementAndGet();
        return true;
    }

    private static int nextDataBufferCapacity(int currentCapacity) {
        return currentCapacity >= MAX_DATA_BUFFER_CAPACITY
                ? MAX_DATA_BUFFER_CAPACITY
                : Math.min(currentCapacity * 2, MAX_DATA_BUFFER_CAPACITY);
    }

    private static boolean isBufferTooSmall(RocksDBException e) {
        String message = e.getMessage();
        return message != null
                && (message.contains(BUFFER_TOO_SMALL_STATUS)
                        || message.contains(BUFFER_TOO_SMALL_CAPACITY));
    }

    private void closeQuietly() {
        try {
            close();
        } catch (IOException ignored) {
            // Best-effort cleanup while propagating the original open failure.
        }
    }

    private static byte[] copyBytes(ByteBuffer source, int offset, int length) {
        byte[] bytes = new byte[length];
        ByteBuffer duplicate = source.duplicate();
        duplicate.position(offset);
        duplicate.limit(offset + length);
        duplicate.get(bytes);
        return bytes;
    }

    @Override
    public void close() throws IOException {
        try {
            cleanup.close();
        } finally {
            returnBuffers();
            cleanable.clean();
        }
    }

    private void returnBuffers() {
        BufferSet current = buffers;
        buffers = null;
        if (current != null) {
            returnBuffers(current);
        }
    }

    static void resetBufferPoolCountersForTesting() {
        BUFFER_POOL.remove();
        BUFFER_SETS_ALLOCATED.set(0);
        BUFFER_SETS_REUSED.set(0);
        BUFFER_SETS_RETURNED.set(0);
        BUFFER_SETS_DISCARDED.set(0);
        BUFFER_SETS_GROWN.set(0);
        BUFFER_SETS_OUTSTANDING.set(0);
        BUFFER_SETS_RETAINED.set(0);
    }

    static long allocatedBufferSetsForTesting() {
        return BUFFER_SETS_ALLOCATED.get();
    }

    static long reusedBufferSetsForTesting() {
        return BUFFER_SETS_REUSED.get();
    }

    static long returnedBufferSetsForTesting() {
        return BUFFER_SETS_RETURNED.get();
    }

    static long discardedBufferSetsForTesting() {
        return BUFFER_SETS_DISCARDED.get();
    }

    static long grownBufferSetsForTesting() {
        return BUFFER_SETS_GROWN.get();
    }

    static long outstandingBufferSetsForTesting() {
        return BUFFER_SETS_OUTSTANDING.get();
    }

    static long retainedBufferSetsForTesting() {
        return BUFFER_SETS_RETAINED.get();
    }

    private static BufferSet borrowBuffers(int maxRows) {
        BufferSet pooled = BUFFER_POOL.get();
        if (pooled != null && pooled.maxRows == maxRows) {
            BUFFER_POOL.remove();
            BUFFER_SETS_RETAINED.decrementAndGet();
            BUFFER_SETS_REUSED.incrementAndGet();
            BUFFER_SETS_OUTSTANDING.incrementAndGet();
            return pooled;
        }
        if (pooled != null) {
            BUFFER_POOL.remove();
            BUFFER_SETS_RETAINED.decrementAndGet();
            BUFFER_SETS_DISCARDED.incrementAndGet();
        }
        BUFFER_SETS_ALLOCATED.incrementAndGet();
        BUFFER_SETS_OUTSTANDING.incrementAndGet();
        return new BufferSet(maxRows);
    }

    private static void returnBuffers(BufferSet bufferSet) {
        BUFFER_SETS_OUTSTANDING.decrementAndGet();
        if (!bufferSet.retainable()) {
            BUFFER_SETS_DISCARDED.incrementAndGet();
            return;
        }
        BufferSet previous = BUFFER_POOL.get();
        if (previous != null) {
            BUFFER_SETS_DISCARDED.incrementAndGet();
        } else {
            BUFFER_SETS_RETAINED.incrementAndGet();
        }
        bufferSet.clear();
        BUFFER_POOL.set(bufferSet);
        BUFFER_SETS_RETURNED.incrementAndGet();
    }

    private static class BufferSet {
        private final int maxRows;
        private final ByteBuffer keyOffsets;
        private final ByteBuffer valueOffsets;
        private final ByteBuffer openFirstMeta;
        private final ByteBuffer valueValidity;
        private ByteBuffer keyData;
        private ByteBuffer valueData;

        private BufferSet(int maxRows) {
            this.maxRows = maxRows;
            this.keyOffsets =
                    ByteBuffer.allocateDirect((maxRows + 1) * Integer.BYTES)
                            .order(ByteOrder.nativeOrder());
            this.valueOffsets =
                    ByteBuffer.allocateDirect((maxRows + 1) * Integer.BYTES)
                            .order(ByteOrder.nativeOrder());
            this.openFirstMeta =
                    ByteBuffer.allocateDirect(ForStRsLibPrefixScanNative.OPEN_FIRST_META_BYTES)
                            .order(ByteOrder.nativeOrder());
            this.keyData = ByteBuffer.allocateDirect(INITIAL_DATA_BUFFER_CAPACITY);
            this.valueData = ByteBuffer.allocateDirect(INITIAL_DATA_BUFFER_CAPACITY);
            this.valueValidity = ByteBuffer.allocateDirect(maxRows);
        }

        private void clear() {
            keyOffsets.clear();
            valueOffsets.clear();
            openFirstMeta.clear();
            keyData.clear();
            valueData.clear();
            valueValidity.clear();
        }

        private boolean retainable() {
            return keyData.capacity() <= MAX_RETAINED_DATA_BUFFER_CAPACITY
                    && valueData.capacity() <= MAX_RETAINED_DATA_BUFFER_CAPACITY;
        }
    }

    private static class PrefixScanCleanup implements Runnable {
        private volatile long handle;

        private PrefixScanCleanup(long handle) {
            this.handle = handle;
        }

        private void close() throws IOException {
            long current = handle;
            handle = 0;
            if (current != 0) {
                try {
                    ForStRsLibPrefixScanNative.prefixLookupClose(current);
                } catch (RocksDBException e) {
                    throw new IOException("Failed to close ForSt-RS prefix scan cursor", e);
                }
            }
        }

        @Override
        public void run() {
            long current = handle;
            handle = 0;
            if (current != 0) {
                try {
                    ForStRsLibPrefixScanNative.prefixLookupClose(current);
                } catch (Throwable ignored) {
                    // Cleaner cannot surface exceptions; explicit close() reports them.
                }
            }
        }
    }

    static class Chunk {
        private final List<ForStDBIterRequest.RawEntry> entries;

        private final boolean eof;

        Chunk(List<ForStDBIterRequest.RawEntry> entries, boolean eof) {
            this.entries = entries;
            this.eof = eof;
        }

        List<ForStDBIterRequest.RawEntry> getEntries() {
            return entries;
        }

        boolean isEof() {
            return eof;
        }
    }
}

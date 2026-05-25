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

package org.apache.flink.state.forstrs.keyed.sst;

import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link ForStRsSstUploader} (B-Prod-P3 Task 3.2). */
class ForStRsSstUploaderTest {

    @Test
    void uploadStreamsFileBytesViaVirtualThread(@TempDir Path tmp) throws Exception {
        Path sst = tmp.resolve("000123.sst");
        byte[] payload = new byte[16 * 1024 + 7]; // crosses the 8KiB buffer boundary
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        Files.write(sst, payload);

        ForStRsSstUploader uploader = new ForStRsSstUploader();
        // 1MiB cap is plenty for our 16KiB-ish payload.
        MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(1024 * 1024);

        CompletableFuture<StreamStateHandle> fut =
                uploader.upload(sst, factory, CheckpointedStateScope.SHARED);
        StreamStateHandle handle = fut.get(30, TimeUnit.SECONDS);
        assertNotNull(handle);

        // Read the bytes back through the handle and verify equality.
        Optional<byte[]> inMemory = handle.asBytesIfInMemory();
        assertTrue(inMemory.isPresent(), "MemCheckpointStreamFactory produces in-memory handles");
        assertArrayEquals(payload, inMemory.get(), "uploaded bytes match source SST");
    }

    @Test
    void uploadOnMissingFileCompletesExceptionally(@TempDir Path tmp) {
        Path missing = tmp.resolve("does-not-exist.sst");
        ForStRsSstUploader uploader = new ForStRsSstUploader();
        MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(1024);

        CompletableFuture<StreamStateHandle> fut =
                uploader.upload(missing, factory, CheckpointedStateScope.EXCLUSIVE);
        ExecutionException ee =
                assertThrows(ExecutionException.class, () -> fut.get(30, TimeUnit.SECONDS));
        // The wrapped cause is some IOException (NoSuchFileException is the most likely).
        assertNotNull(ee.getCause());
    }

    @Test
    void cancelInterruptsOrClosesRunningUpload(@TempDir Path tmp) throws Exception {
        Path sst = tmp.resolve("000125.sst");
        Files.write(sst, new byte[16 * 1024]);

        ForStRsSstUploader uploader = new ForStRsSstUploader();
        BlockingCheckpointStreamFactory factory = new BlockingCheckpointStreamFactory();
        CompletableFuture<StreamStateHandle> fut =
                uploader.upload(sst, factory, CheckpointedStateScope.SHARED);

        assertTrue(factory.writeEntered.await(30, TimeUnit.SECONDS));
        assertTrue(fut.cancel(true));
        assertTrue(factory.unblocked.await(30, TimeUnit.SECONDS));
        assertTrue(fut.isCancelled());
    }

    @Test
    void uploadBlockingReturnsHandle(@TempDir Path tmp) throws Exception {
        Path sst = tmp.resolve("000124.sst");
        byte[] payload = "hello forst-rs sst upload".getBytes();
        Files.write(sst, payload);

        ForStRsSstUploader uploader = new ForStRsSstUploader();
        MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(1024);
        StreamStateHandle handle =
                uploader.uploadBlocking(sst, factory, CheckpointedStateScope.EXCLUSIVE);
        assertNotNull(handle);
        Optional<byte[]> bytes = handle.asBytesIfInMemory();
        assertTrue(bytes.isPresent());
        assertTrue(Arrays.equals(payload, bytes.get()));
    }

    private static final class BlockingCheckpointStreamFactory implements CheckpointStreamFactory {
        final CountDownLatch writeEntered = new CountDownLatch(1);
        final CountDownLatch unblocked = new CountDownLatch(1);

        @Override
        public CheckpointStateOutputStream createCheckpointStateOutputStream(
                CheckpointedStateScope scope) {
            return new BlockingCheckpointStateOutputStream(writeEntered, unblocked);
        }

        @Override
        public boolean canFastDuplicate(StreamStateHandle stateHandle, CheckpointedStateScope scope) {
            return false;
        }

        @Override
        public java.util.List<StreamStateHandle> duplicate(
                java.util.List<StreamStateHandle> stateHandles, CheckpointedStateScope scope) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class BlockingCheckpointStateOutputStream
            extends CheckpointStateOutputStream {
        private final CountDownLatch writeEntered;
        private final CountDownLatch unblocked;
        private final AtomicBoolean closed = new AtomicBoolean();

        BlockingCheckpointStateOutputStream(
                CountDownLatch writeEntered, CountDownLatch unblocked) {
            this.writeEntered = writeEntered;
            this.unblocked = unblocked;
        }

        @Override
        public long getPos() {
            return 0L;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            writeEntered.countDown();
            while (!closed.get()) {
                if (Thread.currentThread().isInterrupted()) {
                    unblocked.countDown();
                    throw new InterruptedIOException("upload cancelled");
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
            unblocked.countDown();
        }

        @Override
        public void flush() {}

        @Override
        public void sync() {}

        @Override
        public StreamStateHandle closeAndGetHandle() {
            closed.set(true);
            return null;
        }

        @Override
        public void close() {
            closed.set(true);
            unblocked.countDown();
        }
    }
}

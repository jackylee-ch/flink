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

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link ForStRsSnapshotStrategy} (B-Prod-P3 Task 3.7).
 *
 * <p>Drives the snapshot strategy end-to-end against a filesystem-backed ForSt-RS engine: writes a
 * batch of keys, runs a full incremental checkpoint, asserts a valid {@link
 * ForStRsIncrementalKeyedStateHandle} comes out, then writes more keys and runs a second checkpoint
 * with the first as base — the second's {@code sharedState} list must be non-empty (engine reports
 * SSTs from ckpt 1 as shared with ckpt 2 because compaction has not run between).
 */
class ForStRsSnapshotStrategyTest {

    @Test
    void snapshotProducesValidIncrementalKeyedStateHandle(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                // Seed the DB with a few keys (engine will flush on the checkpoint call).
                for (int i = 0; i < 8; i++) {
                    byte[] k = ("key-" + i).getBytes();
                    byte[] v = ("val-" + i).getBytes();
                    linker.put(db, cf, k, v);
                }

                ForStRsSstRegistry registry = new ForStRsSstRegistry();
                ForStRsSstUploader uploader = new ForStRsSstUploader();
                ForStRsSnapshotStrategy strategy =
                        new ForStRsSnapshotStrategy(
                                linker,
                                db,
                                UUID.randomUUID(),
                                new KeyGroupRange(0, 0),
                                registry,
                                uploader,
                                arena,
                                Map.of("default", 0L));

                MemCheckpointStreamFactory factory =
                        new MemCheckpointStreamFactory(64 * 1024 * 1024);

                // ---- First checkpoint (full — base = 0). ----
                ForStRsSnapshotResources res1 = strategy.syncPrepareResources(1L);
                assertNotNull(res1.getSnapshot(), "sync phase must capture a snapshot");
                assertEquals(0L, res1.getBaseCheckpointId(), "first checkpoint base is 0");

                SnapshotResult<?> result1 =
                        strategy.asyncSnapshot(res1, 1L, 0L, factory, null)
                                .get(new CloseableRegistry());
                assertNotNull(result1);
                ForStRsIncrementalKeyedStateHandle h1 =
                        (ForStRsIncrementalKeyedStateHandle) result1.getJobManagerOwnedSnapshot();
                assertNotNull(h1);
                assertEquals(1L, h1.getCheckpointId());
                assertEquals(0L, h1.getBaseCheckpointId());
                assertNotNull(h1.getMetaDataStateHandle());
                int sharedSize1 = h1.getSharedState().size();
                assertTrue(sharedSize1 >= 1, "ckpt 1 shared state contains the new SSTs");
                int initialRegistrySize = registry.size();
                assertTrue(initialRegistrySize >= 1);

                // ---- Mark ckpt 1 complete + produce a second checkpoint based on it. ----
                strategy.recordCompletedCheckpoint(1L);
                strategy.takePendingRegistrations(1L);

                // Add more keys so ckpt 2 has new + shared SSTs.
                for (int i = 8; i < 24; i++) {
                    byte[] k = ("key-" + i).getBytes();
                    byte[] v = ("val-" + i).getBytes();
                    linker.put(db, cf, k, v);
                }

                ForStRsSnapshotResources res2 = strategy.syncPrepareResources(2L);
                assertEquals(1L, res2.getBaseCheckpointId());
                SnapshotResult<?> result2 =
                        strategy.asyncSnapshot(res2, 2L, 0L, factory, null)
                                .get(new CloseableRegistry());
                ForStRsIncrementalKeyedStateHandle h2 =
                        (ForStRsIncrementalKeyedStateHandle) result2.getJobManagerOwnedSnapshot();
                assertEquals(2L, h2.getCheckpointId());
                assertEquals(1L, h2.getBaseCheckpointId());
                assertTrue(
                        h2.getSharedState().size() >= 1,
                        "ckpt 2 sharedState non-empty (engine reuses ckpt 1's SSTs)");
                // Each shared SST has a deterministic local-path-derived StateHandleID.
                for (HandleAndLocalPath hlp : h2.getSharedState()) {
                    assertNotNull(hlp.getHandle());
                    assertNotNull(hlp.getLocalPath());
                }
            }
        }
    }

    /**
     * E6-H3 regression: the serializer-registry blob must be captured on the mailbox thread
     * during {@code syncPrepareResources}, not later by the async-snapshot worker. The fix
     * pre-reads {@code RegistryBlobProvider#currentBlob()} during the sync phase and stashes
     * the immutable bytes on {@link ForStRsSnapshotResources}; the async phase reads from
     * there. This test verifies the blob is captured at sync time by exposing a provider that
     * records its invocation thread + count and asserting it is called exactly once at sync
     * time (not at async time).
     */
    @Test
    void registryBlobCapturedOnMailboxThreadInSyncPhase(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                for (int i = 0; i < 4; i++) {
                    linker.put(db, cf, ("k" + i).getBytes(), ("v" + i).getBytes());
                }

                ForStRsSstRegistry sstReg = new ForStRsSstRegistry();
                ForStRsSstUploader uploader = new ForStRsSstUploader();
                ForStRsSnapshotStrategy strategy =
                        new ForStRsSnapshotStrategy(
                                linker,
                                db,
                                UUID.randomUUID(),
                                new KeyGroupRange(0, 0),
                                sstReg,
                                uploader,
                                arena,
                                Map.of("default", 0L));

                java.util.concurrent.atomic.AtomicInteger callCount =
                        new java.util.concurrent.atomic.AtomicInteger();
                java.util.concurrent.atomic.AtomicReference<Thread> callerThread =
                        new java.util.concurrent.atomic.AtomicReference<>();
                final byte[] blobBytes = new byte[] {(byte) 0xFE, 0x42, 0x01};
                strategy.setRegistryBlobProvider(
                        () -> {
                            callCount.incrementAndGet();
                            callerThread.set(Thread.currentThread());
                            return blobBytes;
                        });

                Thread mailbox = Thread.currentThread();
                ForStRsSnapshotResources res = strategy.syncPrepareResources(7L);
                assertEquals(
                        1,
                        callCount.get(),
                        "provider must be called exactly once at sync time (mailbox thread)");
                assertEquals(
                        mailbox,
                        callerThread.get(),
                        "provider must be invoked on the mailbox thread, not the async worker");
                assertNotNull(res.getRegistryBlob(), "sync phase must capture the blob");
                assertEquals(blobBytes.length, res.getRegistryBlob().length);

                // Run the async phase on a different thread to ensure provider is NOT
                // re-invoked there (would re-introduce the race). The async phase reads the
                // captured blob from res.getRegistryBlob() directly.
                MemCheckpointStreamFactory factory =
                        new MemCheckpointStreamFactory(64 * 1024 * 1024);
                java.util.concurrent.Callable<SnapshotResult<?>> asyncTask =
                        () -> strategy.asyncSnapshot(res, 7L, 0L, factory, null)
                                .get(new CloseableRegistry());
                java.util.concurrent.ExecutorService pool =
                        java.util.concurrent.Executors.newSingleThreadExecutor();
                try {
                    SnapshotResult<?> sr = pool.submit(asyncTask).get();
                    assertNotNull(sr);
                } finally {
                    pool.shutdownNow();
                }

                assertEquals(
                        1,
                        callCount.get(),
                        "provider must NOT be invoked on the async worker (would race with"
                                + " concurrent register() writes against the live LinkedHashMap)");
            }
        }
    }
}

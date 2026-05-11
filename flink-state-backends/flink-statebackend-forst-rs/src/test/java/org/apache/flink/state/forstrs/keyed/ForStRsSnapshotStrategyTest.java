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
 * ForStRsIncrementalKeyedStateHandle} comes out, then writes more keys and runs a second
 * checkpoint with the first as base — the second's {@code sharedState} list must be non-empty
 * (engine reports SSTs from ckpt 1 as shared with ckpt 2 because compaction has not run between).
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

                MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);

                // ---- First checkpoint (full — base = 0). ----
                ForStRsSnapshotResources res1 = strategy.syncPrepareResources(1L);
                assertNotNull(res1.getManifestPath(), "engine must produce a manifest path");
                assertTrue(
                        res1.getNewSstFiles().size() >= 1,
                        "expected at least one new SST after seed writes");
                assertEquals(
                        0, res1.getSharedSstFiles().size(), "first checkpoint has no shared SSTs");

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
}

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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
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
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire-up test for {@link ForStRsAbstractKeyedStateBackend#snapshot} +
 * {@link ForStRsAbstractKeyedStateBackend#notifyCheckpointComplete} +
 * {@link ForStRsAbstractKeyedStateBackend#notifyCheckpointAborted} (B-Prod-P3 Tasks 3.6 + 3.9).
 *
 * <p>Exercises the snapshot through the abstract backend's public surface (rather than the
 * strategy directly) and verifies the registry's ref-counts behave correctly across
 * complete vs. abort lifecycles.
 */
class ForStRsKeyedBackendCheckpointWireTest {

    @Test
    void snapshotAbortRollsBackRegistryRefs(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);

            ForStRsKeyedStateBackend<String> delegate =
                    new ForStRsKeyedStateBackend<>(
                            arena, linker, db, cf, StringSerializer.INSTANCE, false);

            // Pre-load some data so the engine has SSTs to checkpoint.
            for (int i = 0; i < 8; i++) {
                linker.put(db, cf, ("k-" + i).getBytes(), ("v-" + i).getBytes());
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

            try (CloseableRegistry cr = new CloseableRegistry();
                    ForStRsAbstractKeyedStateBackend<String> backend =
                            new ForStRsAbstractKeyedStateBackend<>(
                                    StringSerializer.INSTANCE,
                                    Thread.currentThread().getContextClassLoader(),
                                    new ExecutionConfig(),
                                    cr,
                                    delegate)) {
                backend.setSnapshotStrategy(strategy, registry);

                // Take a snapshot; run the async future on this thread.
                RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                        backend.snapshot(1L, 0L, factory, null);
                fut.run();
                SnapshotResult<KeyedStateHandle> result = fut.get();
                assertNotNull(result);
                int afterSnapshotSize = registry.size();
                assertTrue(
                        afterSnapshotSize >= 1,
                        "registry should have entries after a successful snapshot");

                // ABORT path: notifyCheckpointAborted must roll back ckpt-1's contributions.
                backend.notifyCheckpointAborted(1L);
                assertEquals(
                        0,
                        registry.size(),
                        "abort rolls back exactly the aborted checkpoint's contributions");
            }

            cf.close();
            db.close();
        }
    }

    @Test
    void snapshotCompleteUpdatesBaseCheckpointId(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);

            ForStRsKeyedStateBackend<String> delegate =
                    new ForStRsKeyedStateBackend<>(
                            arena, linker, db, cf, StringSerializer.INSTANCE, false);

            for (int i = 0; i < 4; i++) {
                linker.put(db, cf, ("k-" + i).getBytes(), ("v-" + i).getBytes());
            }
            ForStRsSstRegistry registry = new ForStRsSstRegistry();
            ForStRsSnapshotStrategy strategy =
                    new ForStRsSnapshotStrategy(
                            linker,
                            db,
                            UUID.randomUUID(),
                            new KeyGroupRange(0, 0),
                            registry,
                            new ForStRsSstUploader(),
                            arena,
                            Map.of("default", 0L));

            try (CloseableRegistry cr = new CloseableRegistry();
                    ForStRsAbstractKeyedStateBackend<String> backend =
                            new ForStRsAbstractKeyedStateBackend<>(
                                    StringSerializer.INSTANCE,
                                    Thread.currentThread().getContextClassLoader(),
                                    new ExecutionConfig(),
                                    cr,
                                    delegate)) {
                backend.setSnapshotStrategy(strategy, registry);
                MemCheckpointStreamFactory factory =
                        new MemCheckpointStreamFactory(64 * 1024 * 1024);

                // Snapshot 1; complete it.
                RunnableFuture<SnapshotResult<KeyedStateHandle>> fut1 =
                        backend.snapshot(1L, 0L, factory, null);
                fut1.run();
                fut1.get();
                backend.notifyCheckpointComplete(1L);
                assertEquals(
                        1L,
                        strategy.getLastCompletedCheckpointId(),
                        "notifyCheckpointComplete must update the strategy's base checkpoint id");
            }

            cf.close();
            db.close();
        }
    }
}

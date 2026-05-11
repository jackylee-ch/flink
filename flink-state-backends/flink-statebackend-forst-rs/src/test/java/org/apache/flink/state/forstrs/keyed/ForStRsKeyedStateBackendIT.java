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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end snapshot+restore IT through the {@link ForStRsAbstractKeyedStateBackend} surface
 * (B-Prod-P4 Task 4.7).
 *
 * <p><b>Scope note (fallback mode).</b> The plan called for a {@code MiniClusterWithClientResource}
 * spin-up running a {@code keyBy(...).process(stateful op)} job, taking a real Flink checkpoint,
 * cancelling, restarting from the checkpoint, and verifying state. That path requires adding {@code
 * flink-test-utils} as a test dependency to this module — and, more importantly, the {@link
 * ForStRsAbstractKeyedStateBackend} skeleton (per its JavaDoc) does not yet implement {@code
 * createOrUpdateInternalState}, {@code create<T>InternalPriorityQueue}, or wire the {@code
 * AbstractStreamOperator}-side hooks Flink would invoke through MiniCluster. Those hookups are
 * explicitly tracked for B-Prod-P5 and beyond.
 *
 * <p>To still verify the P4 contract end-to-end, we drive the same code path the MiniCluster would:
 * the abstract backend's {@link ForStRsAbstractKeyedStateBackend#snapshot} method (which P3 wired)
 * + {@link ForStRsRestoreOperation#restore} (added in this PR), with state writes performed via the
 * L5 delegate's {@code linker.put}. This round-trip exercises the production sync+async snapshot
 * phases, the SST registry, and the restore download/strict-check + rebuild pipeline — everything a
 * MiniCluster job would exercise about the keyed-state-backend half of the contract.
 *
 * <p>When B-Prod-P5 wires {@code createOrUpdateInternalState} into the abstract backend, this file
 * should be expanded with a real {@code MiniClusterExtension} test running a {@code
 * KeyedProcessFunction} that writes ValueState; the round-trip skeleton here is the
 * checkpoint-strategy layer of that future test.
 */
class ForStRsKeyedStateBackendIT {

    @Test
    void backendSnapshotThenRestoreRoundTrip(@TempDir Path tmp) throws Exception {
        // --- Phase 1: open a backend, write some keys, snapshot, capture the handle. ---
        ForStRsIncrementalKeyedStateHandle ckptHandle;
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpen(arena, tmp.resolve("phase1").toString());
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);

            ForStRsKeyedStateBackend<String> delegate =
                    new ForStRsKeyedStateBackend<>(
                            arena, linker, db, cf, StringSerializer.INSTANCE, false);
            // Pre-load 16 keys directly via the L5 linker (mirrors what a stateful operator's
            // KeyedProcessFunction.processElement would do once the createOrUpdateInternalState
            // wiring lands in P5).
            for (int i = 0; i < 16; i++) {
                linker.put(db, cf, ("e2e-k-" + i).getBytes(), ("e2e-v-" + i).getBytes());
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
                RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                        backend.snapshot(42L, 1234L, factory, null);
                fut.run();
                SnapshotResult<KeyedStateHandle> result = fut.get();
                assertNotNull(result);
                ckptHandle =
                        (ForStRsIncrementalKeyedStateHandle) result.getJobManagerOwnedSnapshot();
                assertEquals(42L, ckptHandle.getCheckpointId());
                backend.notifyCheckpointComplete(42L);
            }
            // Backend close cascades to delegate close which already shares ownsResources=false,
            // so we close the cf+db here. (The abstract backend's close() also tries to close the
            // delegate — but ForStRsKeyedStateBackend.close() with ownsResources=false is a no-op
            // beyond cache clearing.)
            cf.close();
            db.close();
        }

        // --- Phase 2: NEW arena + NEW linker — restore via ForStRsRestoreOperation. ---
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            ForStRsRestoreOperation op =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("phase2"),
                            new KeyGroupRange(0, 0),
                            new ForStRsSstRegistry());
            ForStRsRestoreOperation.RestoreResult restored = op.restore(List.of(ckptHandle));
            try {
                // Every key from Phase 1 must read back identical bytes on the restored engine.
                for (int i = 0; i < 16; i++) {
                    byte[] expected = ("e2e-v-" + i).getBytes();
                    byte[] got =
                            linker.get(
                                    restored.getDb(),
                                    restored.getDefaultCf(),
                                    ("e2e-k-" + i).getBytes());
                    assertArrayEquals(
                            expected,
                            got,
                            "E2E roundtrip: key e2e-k-" + i + " missing or corrupted");
                }
                assertEquals(42L, restored.getRestoredCheckpointId());
            } finally {
                restored.getDefaultCf().close();
                restored.getDb().close();
            }
        }
    }
}

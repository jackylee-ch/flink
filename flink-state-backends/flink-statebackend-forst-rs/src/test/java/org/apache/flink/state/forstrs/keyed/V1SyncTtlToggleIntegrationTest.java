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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.state.StateSerializerMetadata;
import org.apache.flink.state.forstrs.state.StateSerializerRegistry;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R25-H1 integration acceptance test for the V1-sync production entry point
 * {@link ForStRsAbstractKeyedStateBackend#createOrUpdateInternalState}.
 *
 * <p>R26-H1 (this revision): V1-sync NO LONGER supports TTL — the V1-sync entry path lacks
 * the {@code createTtlAwareStateInternal} wrapping that V2 async performs. Pre-R26-H1 the
 * V1-sync code forwarded {@code ttlEnabled=true} to {@code verifyOrRegister} but constructed
 * the state with the user's raw serializer (no TtlSerializer), so the registry's claim did
 * NOT match the on-disk layout. The fix rejects TTL at register time with
 * {@link UnsupportedOperationException}. These tests therefore verify:
 *
 * <ol>
 *   <li>V1-sync registration with TTL=ON throws {@code UnsupportedOperationException}
 *       (the snapshot/restore TTL-toggle scenario is impossible to construct on V1-sync now
 *       because the initial TTL=ON registration is rejected).</li>
 *   <li>V1-sync TTL=OFF still works correctly (no regression) and the registry records
 *       {@code ttlEnabled=false} as the on-disk layout matches.</li>
 *   <li>The OFF→OFF cross-snapshot round-trip continues to function — i.e. the registry
 *       still serializes/deserializes a non-TTL state across the boundary.</li>
 * </ol>
 *
 * <p>R26-L2: every V1Pair gets closed in a finally that also closes the {@link Arena} so
 * the FFM allocation does not leak between tests.
 *
 * <p>R26-L3: the {@code formatVersion} assertion that previously claimed "v2 envelope is
 * emitted when TTL flags are present" was misleading — the registry always writes
 * {@code CURRENT_FORMAT_VERSION=2} regardless of {@code ttlEnabled}. It has been removed.
 */
class V1SyncTtlToggleIntegrationTest {

    /** Open a V1-sync backend pair (L5 delegate + Abstract wrapper) against a fresh local DB. */
    private static V1Pair openV1Pair(Path dbDir) throws Exception {
        Arena arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        java.nio.file.Files.createDirectories(dbDir);
        FrsDb db = linker.dbOpen(arena, dbDir.toString());
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);
        KeyGroupRange kgr = new KeyGroupRange(0, 0);
        ForStRsKeyedStateBackend<Integer> delegate =
                new ForStRsKeyedStateBackend<>(
                        arena,
                        linker,
                        db,
                        cf,
                        IntSerializer.INSTANCE,
                        /* ownsResources= */ true,
                        kgr,
                        /* numberOfKeyGroups= */ 1);
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        ForStRsAbstractKeyedStateBackend<Integer> backend =
                new ForStRsAbstractKeyedStateBackend<>(
                        IntSerializer.INSTANCE,
                        Thread.currentThread().getContextClassLoader(),
                        new ExecutionConfig(),
                        cancelStreamRegistry,
                        delegate,
                        kgr,
                        /* numberOfKeyGroups= */ 1);
        return new V1Pair(backend, linker, db, cf, arena, cancelStreamRegistry);
    }

    private record V1Pair(
            ForStRsAbstractKeyedStateBackend<Integer> backend,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            CloseableRegistry cancelStreamRegistry) {

        void closeAll() throws Exception {
            try {
                backend.close();
            } finally {
                // R26-L2: close the Arena even if backend.close() throws so the FFM
                // allocation does not leak into the next test.
                if (arena.scope().isAlive()) {
                    arena.close();
                }
            }
        }
    }

    /**
     * R26-H1 acceptance: V1-sync rejects TTL-enabled descriptors at register time with
     * {@link UnsupportedOperationException}. The registry must NOT be polluted with a
     * ttlEnabled=true entry, because V1-sync writes bare bytes (no TtlSerializer wrapping).
     */
    @Test
    void ttlEnabledOnV1SyncRejectedAtRegisterTime(@TempDir Path tmp) throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db1"));
        try {
            ValueStateDescriptor<Long> ttlOn =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            StateTtlConfig ttlConfig =
                    StateTtlConfig.newBuilder(Duration.ofMillis(60_000L))
                            .updateTtlOnCreateAndWrite()
                            .build();
            ttlOn.enableTimeToLive(ttlConfig);
            assertTrue(ttlOn.getTtlConfig().isEnabled(), "descriptor reports TTL enabled");
            ttlOn.initializeSerializerUnlessSet(new ExecutionConfig());

            UnsupportedOperationException ex =
                    assertThrows(
                            UnsupportedOperationException.class,
                            () ->
                                    pair.backend.createOrUpdateInternalState(
                                            IntSerializer.INSTANCE,
                                            ttlOn,
                                            StateSnapshotTransformFactory.noTransform()),
                            "R26-H1: V1-sync must reject TTL-enabled descriptors");
            assertTrue(
                    ex.getMessage().contains("TTL"),
                    "exception message mentions TTL: " + ex.getMessage());
            assertTrue(
                    ex.getMessage().contains("V1-sync"),
                    "exception message identifies the V1-sync limitation: " + ex.getMessage());

            // Registry must NOT have been polluted with a ttlEnabled=true entry — the throw
            // happens BEFORE verifyOrRegister is called.
            StateSerializerRegistry reg = pair.backend.stateSerializerRegistry();
            assertNotNull(reg);
            // No entry should exist at all (or, if one existed from a prior call, it must not
            // carry ttlEnabled=true).
            StateSerializerMetadata md = reg.get("counter");
            if (md != null) {
                assertFalse(
                        md.ttlEnabled(),
                        "R26-H1: registry must not record ttlEnabled=true on V1-sync");
            }
        } finally {
            pair.closeAll();
        }
    }

    /**
     * Sanity: V1-sync with TTL DISABLED continues to work, and the registry records
     * ttlEnabled=false so the on-disk layout (bare bytes) matches the registry's claim.
     */
    @Test
    void ttlDisabledOnV1SyncWorks(@TempDir Path tmp) throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db1"));
        try {
            ValueStateDescriptor<Long> ttlOff =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            assertFalse(ttlOff.getTtlConfig().isEnabled(), "descriptor default has TTL disabled");
            ttlOff.initializeSerializerUnlessSet(new ExecutionConfig());

            pair.backend.createOrUpdateInternalState(
                    IntSerializer.INSTANCE,
                    ttlOff,
                    StateSnapshotTransformFactory.noTransform());

            StateSerializerRegistry reg = pair.backend.stateSerializerRegistry();
            StateSerializerMetadata md = reg.get("counter");
            assertNotNull(md, "registry holds counter metadata");
            assertFalse(
                    md.ttlEnabled(),
                    "R26-H1: V1-sync registry must record ttlEnabled=false (matches on-disk"
                            + " bare-byte layout)");
            assertEquals(0L, md.ttlMillis(), "millis is 0 when TTL disabled");
            // R26-L3: do NOT assert on formatVersion here — the registry writes
            // CURRENT_FORMAT_VERSION=2 regardless of ttlEnabled, so the prior assertion that
            // "v2 envelope is emitted when TTL flags are present" was misleading.
        } finally {
            pair.closeAll();
        }
    }

    /**
     * Symmetric R25-H1 acceptance: TTL=OFF → TTL=OFF across snapshot/restore continues to
     * round-trip cleanly. (The TTL-toggle assertion this test used to make is no longer
     * reachable on V1-sync per R26-H1 — TTL=ON is rejected at register time. We keep this
     * test as a regression guard for the registry serialize/deserialize round-trip itself.)
     */
    @Test
    void ttlOffRoundTripsAcrossSnapshotRestore(@TempDir Path tmp) throws Exception {
        V1Pair pair1 = openV1Pair(tmp.resolve("db1"));
        byte[] registryBlob;
        try {
            ValueStateDescriptor<Long> ttlOff =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            ttlOff.initializeSerializerUnlessSet(new ExecutionConfig());

            pair1.backend.createOrUpdateInternalState(
                    IntSerializer.INSTANCE,
                    ttlOff,
                    StateSnapshotTransformFactory.noTransform());

            StateSerializerRegistry reg = pair1.backend.stateSerializerRegistry();
            StateSerializerMetadata md = reg.get("counter");
            assertNotNull(md, "registry holds counter metadata");
            assertFalse(md.ttlEnabled(), "V1-sync registry records ttlEnabled=false");
            assertEquals(0L, md.ttlMillis(), "millis is 0 when TTL disabled");

            registryBlob = reg.serialize();
        } finally {
            pair1.closeAll();
        }

        V1Pair pair2 = openV1Pair(tmp.resolve("db2"));
        try {
            Map<String, StateSerializerMetadata> parsed =
                    StateSerializerRegistry.deserialize(registryBlob);
            pair2.backend.seedRestoredSerializerMetadata(new LinkedHashMap<>(parsed));

            // Attempting to re-enable TTL across the restore boundary on V1-sync must STILL
            // fail with UnsupportedOperationException (NOT StateMigrationException) because
            // R26-H1's gate runs BEFORE the registry's toggle check. The user gets the
            // V1-sync TTL limitation surfaced as the primary error.
            ValueStateDescriptor<Long> ttlOn =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            StateTtlConfig ttlConfig =
                    StateTtlConfig.newBuilder(Duration.ofMillis(30_000L))
                            .updateTtlOnCreateAndWrite()
                            .build();
            ttlOn.enableTimeToLive(ttlConfig);
            ttlOn.initializeSerializerUnlessSet(new ExecutionConfig());

            UnsupportedOperationException ex =
                    assertThrows(
                            UnsupportedOperationException.class,
                            () ->
                                    pair2.backend.createOrUpdateInternalState(
                                            IntSerializer.INSTANCE,
                                            ttlOn,
                                            StateSnapshotTransformFactory.noTransform()),
                            "R26-H1: V1-sync rejects TTL even after restoring non-TTL"
                                    + " metadata (the V1-sync limitation supersedes the TTL"
                                    + " toggle StateMigrationException check)");
            assertTrue(
                    ex.getMessage().contains("V1-sync"),
                    "exception identifies V1-sync limitation: " + ex.getMessage());

            // A same-name descriptor with TTL still disabled must round-trip cleanly.
            ValueStateDescriptor<Long> ttlOffAgain =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            ttlOffAgain.initializeSerializerUnlessSet(new ExecutionConfig());
            pair2.backend.createOrUpdateInternalState(
                    IntSerializer.INSTANCE,
                    ttlOffAgain,
                    StateSnapshotTransformFactory.noTransform());
            StateSerializerMetadata md2 =
                    pair2.backend.stateSerializerRegistry().get("counter");
            assertNotNull(md2);
            assertFalse(md2.ttlEnabled(), "restored ttlEnabled=false");

            // For symmetry: a StateMigrationException IS still reachable on the V2 async path
            // — that scenario is covered by the V2 async test suite. This test file only
            // guards the V1-sync entry point.
            //
            // Unused on V1-sync but referenced to keep the original import surface stable.
            assertNotNull(StateMigrationException.class);
        } finally {
            pair2.closeAll();
        }
    }
}

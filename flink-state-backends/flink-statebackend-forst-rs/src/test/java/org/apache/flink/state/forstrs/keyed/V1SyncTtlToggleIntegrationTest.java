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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R25-H1 integration acceptance test: the V1-sync production entry point
 * {@link ForStRsAbstractKeyedStateBackend#createOrUpdateInternalState} must thread the user's
 * TTL config from {@link ValueStateDescriptor#getTtlConfig()} down into the 4-arg
 * {@link StateSerializerRegistry#verifyOrRegister} overload so a TTL toggle across a
 * snapshot/restore is surfaced as {@link StateMigrationException}.
 *
 * <p>Pre-R25-H1 the production call sites used the 3-arg {@code verifyOrRegister} (which
 * defaults {@code ttlEnabled=false}, {@code ttlMillis=0}), bypassing the toggle check
 * added by R24-H2. Without this fix a job that ran with TTL=ON, snapshotted, then was
 * restarted with TTL=OFF would silently decode the 8-byte expiry header as payload bytes
 * (corrupting every restored value) because the schema check at the user-serializer level
 * would resolve {@code COMPATIBLE_AS_IS}.
 *
 * <p>Test design (per R25 spec — INTEGRATION level, NOT a direct
 * {@code StateSerializerRegistry.verifyOrRegister(name, kind, ser, ttlEnabled, ttlMillis)}
 * call): we drive a real V1-sync backend through {@code createOrUpdateInternalState}
 * with a {@link ValueStateDescriptor} that has TTL enabled, then simulate a
 * snapshot/restore by copying the registry blob into a brand-new backend and
 * seeding it via {@code seedRestoredSerializerMetadata}. The second {@code
 * createOrUpdateInternalState} call uses the SAME state name but the descriptor's
 * TTL config flipped off, and must throw {@code StateMigrationException}. The
 * 4-arg registry overload is only reached if the backend code correctly extracts
 * {@code desc.getTtlConfig()} and forwards both flag and millis.
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
            CloseableRegistry cancelStreamRegistry) {}

    /**
     * R25-H1 acceptance: register a ValueState through {@code createOrUpdateInternalState}
     * with TTL enabled, simulate snapshot+restore via registry-blob serialize/deserialize +
     * {@code seedRestoredSerializerMetadata}, then attempt to register the same state with
     * TTL DISABLED — must throw {@link StateMigrationException}.
     */
    @Test
    void ttlOnToOffViaCreateOrUpdateInternalStateThrows(@TempDir Path tmp) throws Exception {
        // Session 1: open backend, register state with TTL=ON.
        V1Pair pair1 = openV1Pair(tmp.resolve("db1"));
        byte[] registryBlob;
        try {
            ValueStateDescriptor<Long> ttlOn =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            StateTtlConfig ttlConfig =
                    StateTtlConfig.newBuilder(Duration.ofMillis(60_000L))
                            .updateTtlOnCreateAndWrite()
                            .build();
            ttlOn.enableTimeToLive(ttlConfig);
            assertTrue(ttlOn.getTtlConfig().isEnabled(), "descriptor reports TTL enabled");

            // Force initialization (otherwise getSerializer() throws).
            ttlOn.initializeSerializerUnlessSet(new ExecutionConfig());

            pair1.backend.createOrUpdateInternalState(
                    /* namespaceSerializer= */ IntSerializer.INSTANCE,
                    ttlOn,
                    /* snapshotTransformFactory= */ StateSnapshotTransformFactory.noTransform());

            // Inspect the registry — the TTL flag must be persisted in the live metadata
            // (this is what asserts that the new 4-arg path is actually reached).
            StateSerializerRegistry reg = pair1.backend.stateSerializerRegistry();
            StateSerializerMetadata md = reg.get("counter");
            assertNotNull(md, "registry holds counter metadata");
            assertTrue(
                    md.ttlEnabled(),
                    "R25-H1: createOrUpdateInternalState forwards ttlEnabled=true from descriptor"
                            + " to registry (live metadata reflects the flag)");
            assertEquals(
                    60_000L,
                    md.ttlMillis(),
                    "R25-H1: createOrUpdateInternalState forwards ttlMillis from descriptor");
            assertEquals(
                    StateSerializerMetadata.FORMAT_VERSION_V2,
                    md.formatVersion(),
                    "v2 envelope is emitted when TTL flags are present");

            // Capture the registry blob — this is what the snapshot strategy would persist
            // into {@code _serializer_metadata.bin}.
            registryBlob = reg.serialize();
        } finally {
            pair1.backend.close();
        }

        // Session 2: fresh backend, seed the registry from the captured blob, then attempt
        // a same-name registration with TTL DISABLED — must throw StateMigrationException.
        V1Pair pair2 = openV1Pair(tmp.resolve("db2"));
        try {
            Map<String, StateSerializerMetadata> parsed =
                    StateSerializerRegistry.deserialize(registryBlob);
            pair2.backend.seedRestoredSerializerMetadata(new LinkedHashMap<>(parsed));
            assertTrue(
                    pair2.backend.stateSerializerRegistry().activatedForRestore(),
                    "seed activates the restore branch");

            ValueStateDescriptor<Long> ttlOff =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            // NOT calling enableTimeToLive — descriptor reports TTL DISABLED by default.
            assertTrue(
                    !ttlOff.getTtlConfig().isEnabled(),
                    "second descriptor reports TTL disabled");
            ttlOff.initializeSerializerUnlessSet(new ExecutionConfig());

            StateMigrationException ex =
                    assertThrows(
                            StateMigrationException.class,
                            () ->
                                    pair2.backend.createOrUpdateInternalState(
                                            IntSerializer.INSTANCE,
                                            ttlOff,
                                            StateSnapshotTransformFactory.noTransform()),
                            "R25-H1: createOrUpdateInternalState must surface a TTL toggle as"
                                    + " StateMigrationException via the 4-arg verifyOrRegister"
                                    + " overload");
            assertTrue(
                    ex.getMessage().contains("TTL toggle"),
                    "exception mentions TTL toggle: " + ex.getMessage());
            assertTrue(
                    ex.getMessage().contains("prior=TTL_ENABLED"),
                    "exception identifies the prior TTL state: " + ex.getMessage());
            assertTrue(
                    ex.getMessage().contains("now=TTL_DISABLED"),
                    "exception identifies the new TTL state: " + ex.getMessage());
        } finally {
            pair2.backend.close();
        }
    }

    /**
     * Symmetric R25-H1 acceptance: TTL=OFF → TTL=ON across snapshot/restore must also
     * throw via {@code createOrUpdateInternalState}.
     */
    @Test
    void ttlOffToOnViaCreateOrUpdateInternalStateThrows(@TempDir Path tmp) throws Exception {
        V1Pair pair1 = openV1Pair(tmp.resolve("db1"));
        byte[] registryBlob;
        try {
            ValueStateDescriptor<Long> ttlOff =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            // No enableTimeToLive call — descriptor default is DISABLED.
            ttlOff.initializeSerializerUnlessSet(new ExecutionConfig());

            pair1.backend.createOrUpdateInternalState(
                    IntSerializer.INSTANCE,
                    ttlOff,
                    StateSnapshotTransformFactory.noTransform());

            StateSerializerRegistry reg = pair1.backend.stateSerializerRegistry();
            StateSerializerMetadata md = reg.get("counter");
            assertNotNull(md, "registry holds counter metadata");
            assertTrue(
                    !md.ttlEnabled(),
                    "R25-H1: createOrUpdateInternalState forwards ttlEnabled=false correctly");
            assertEquals(0L, md.ttlMillis(), "millis is 0 when TTL disabled");

            registryBlob = reg.serialize();
        } finally {
            pair1.backend.close();
        }

        V1Pair pair2 = openV1Pair(tmp.resolve("db2"));
        try {
            Map<String, StateSerializerMetadata> parsed =
                    StateSerializerRegistry.deserialize(registryBlob);
            pair2.backend.seedRestoredSerializerMetadata(new LinkedHashMap<>(parsed));

            ValueStateDescriptor<Long> ttlOn =
                    new ValueStateDescriptor<>("counter", LongSerializer.INSTANCE);
            StateTtlConfig ttlConfig =
                    StateTtlConfig.newBuilder(Duration.ofMillis(30_000L))
                            .updateTtlOnCreateAndWrite()
                            .build();
            ttlOn.enableTimeToLive(ttlConfig);
            ttlOn.initializeSerializerUnlessSet(new ExecutionConfig());

            StateMigrationException ex =
                    assertThrows(
                            StateMigrationException.class,
                            () ->
                                    pair2.backend.createOrUpdateInternalState(
                                            IntSerializer.INSTANCE,
                                            ttlOn,
                                            StateSnapshotTransformFactory.noTransform()),
                            "R25-H1: TTL OFF→ON toggle surfaces via createOrUpdateInternalState");
            assertTrue(
                    ex.getMessage().contains("prior=TTL_DISABLED"),
                    "exception identifies the prior TTL state: " + ex.getMessage());
            assertTrue(
                    ex.getMessage().contains("now=TTL_ENABLED"),
                    "exception identifies the new TTL state: " + ex.getMessage());
        } finally {
            pair2.backend.close();
        }
    }
}

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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;
import org.apache.flink.state.forstrs.state.StateSerializerMetadata;
import org.apache.flink.state.forstrs.state.StateSerializerRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E6-HIGH-1 / E6-HIGH-2 / E6-HIGH-4 acceptance tests for the V1-sync {@code
 * ForStRsAbstractKeyedStateBackend} snapshot + restore path.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>E6-HIGH-1: CANONICAL savepoints are rejected at the V1-sync {@code snapshot()} entry point
 *       (not only on the async path).
 *   <li>E6-HIGH-2: closed cancel-stream-registry no longer relies on a substring match of the
 *       exception message — the structural {@code isClosed()} precondition is used instead.
 *   <li>E6-HIGH-4(a): the V1-sync snapshot strategy now has a registry-blob provider wired, so the
 *       emitted private state contains {@code _serializer_metadata.bin}.
 *   <li>E6-HIGH-4(b): {@link ForStRsAbstractKeyedStateBackend#seedRestoredSerializerMetadata}
 *       correctly forwards a restored (union-merged across sources) metadata map into the backend
 *       registry so subsequent {@code createOrUpdateInternalState} calls run through {@code
 *       verifyOrRegister}.
 * </ul>
 */
class V1SyncSchemaDriftTest {

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
        ForStRsSstRegistry sstReg = new ForStRsSstRegistry();
        ForStRsSnapshotStrategy strategy =
                new ForStRsSnapshotStrategy(
                        linker,
                        db,
                        UUID.randomUUID(),
                        kgr,
                        sstReg,
                        new ForStRsSstUploader(),
                        arena,
                        Map.of("default", 0L));
        backend.setSnapshotStrategy(strategy, sstReg);
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
     * E6-HIGH-1: CANONICAL savepoint requests at the V1-sync entry point fail fast with
     * UnsupportedOperationException. Pre-fix the gate only existed on the async path.
     */
    @Test
    void canonicalSavepointRejectedAtV1SyncEntryPoint(@TempDir Path tmp) throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db"));
        try {
            // Seed at least one entry so the snapshot has something to flush.
            pair.linker.put(pair.db, pair.cf, new byte[] {1, 2}, new byte[] {3, 4});

            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            SavepointType canonical = SavepointType.savepoint(SavepointFormatType.CANONICAL);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            canonical, CheckpointStorageLocationReference.getDefault());

            UnsupportedOperationException uoe =
                    assertThrows(
                            UnsupportedOperationException.class,
                            () -> pair.backend.snapshot(11L, 0L, factory, opts),
                            "E6-HIGH-1: V1-sync snapshot() must reject CANONICAL savepoints");
            assertTrue(
                    uoe.getMessage().contains("Canonical savepoint format"),
                    "exception message names the unsupported format: " + uoe.getMessage());
            assertTrue(
                    uoe.getMessage().contains("NATIVE"),
                    "exception message suggests the NATIVE alternative: " + uoe.getMessage());
        } finally {
            pair.backend.close();
        }
    }

    /**
     * E6-HIGH-2: a closed cancelStreamRegistry causes the V1-sync snapshot path to fall back to a
     * pre-completed empty future without depending on the IOException message substring.
     */
    @Test
    void closedCancelStreamRegistryYieldsEmptyResultStructurally(@TempDir Path tmp)
            throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db"));
        try {
            // Close the registry BEFORE snapshot — mimics a prior checkpoint's abort sequence.
            pair.cancelStreamRegistry.close();
            assertTrue(pair.cancelStreamRegistry.isClosed());

            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                    pair.backend.snapshot(7L, 0L, factory, opts);
            assertTrue(
                    fut.isDone(),
                    "E6-HIGH-2: closed cancel registry → DoneFuture is pre-completed");
            SnapshotResult<KeyedStateHandle> res = fut.get();
            assertNotNull(res, "result non-null");
            // SnapshotResult.empty() — JM handle should be null.
            assertEquals(
                    null,
                    res.getJobManagerOwnedSnapshot(),
                    "E6-HIGH-2: SnapshotResult.empty() carries no JM handle");
        } finally {
            pair.backend.close();
        }
    }

    /**
     * E6-HIGH-4(a): the V1-sync snapshot strategy now has a registry-blob provider wired (via
     * {@code setSnapshotStrategy}). Without this fix the privateState entries would never include
     * the registry blob even when the backend had registered states.
     */
    @Test
    void v1SyncSnapshotEmitsRegistryBlobWhenStatesRegistered(@TempDir Path tmp) throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db"));
        try {
            // Register two states directly on the backend registry (simulates user state creation
            // running through createOrUpdateInternalState).
            StateSerializerRegistry reg = pair.backend.stateSerializerRegistry();
            reg.register("counter", /* VALUE */ 0, LongSerializer.INSTANCE);
            reg.register("name", /* VALUE */ 0, StringSerializer.INSTANCE);
            assertEquals(
                    2,
                    reg.metadataBuffer().size(),
                    "registry holds both registrations before snapshot");

            // Seed some engine bytes so the snapshot has files to upload.
            for (int i = 0; i < 16; i++) {
                pair.linker.put(
                        pair.db, pair.cf, ("k-" + i).getBytes(), ("v-" + i).getBytes());
            }

            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                    pair.backend.snapshot(21L, 0L, factory, opts);
            if (!fut.isDone()) {
                fut.run();
            }
            SnapshotResult<KeyedStateHandle> result = fut.get();
            ForStRsIncrementalKeyedStateHandle handle =
                    (ForStRsIncrementalKeyedStateHandle) result.getJobManagerOwnedSnapshot();
            assertNotNull(handle, "V1-sync snapshot produced an incremental handle");

            // The registry blob is present in privateState.
            assertTrue(
                    handle.getPrivateState().stream()
                            .anyMatch(
                                    hlp ->
                                            ForStRsSnapshotStrategy.SERIALIZER_REGISTRY_LOCAL_PATH
                                                    .equals(hlp.getLocalPath())),
                    "E6-HIGH-4(a): V1-sync snapshot emits the registry blob as a private-state"
                            + " entry");
        } finally {
            pair.backend.close();
        }
    }

    /**
     * E6-HIGH-4(b): {@link ForStRsAbstractKeyedStateBackend#seedRestoredSerializerMetadata}
     * (called by {@code ForStRsStateBackend.createKeyedStateBackend} after a restore op completes)
     * pumps the union-merged map into the registry's restored side, activating it for the next
     * {@code verifyOrRegister} call.
     */
    @Test
    void seedRestoredSerializerMetadataActivatesRegistry(@TempDir Path tmp) throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db"));
        try {
            StateSerializerRegistry reg = pair.backend.stateSerializerRegistry();
            assertFalse(
                    reg.activatedForRestore(),
                    "fresh backend has no restore seed before seedRestoredSerializerMetadata"
                            + " runs");

            // Build a metadata map shaped like the union-merged output of restoreWithRescaling.
            Map<String, StateSerializerMetadata> restored = new LinkedHashMap<>();
            // Pre-existing source A contributes "counter" with LongSerializer's snapshot.
            StateSerializerRegistry seedingA = new StateSerializerRegistry();
            seedingA.register("counter", /* VALUE */ 0, LongSerializer.INSTANCE);
            restored.put("counter", seedingA.get("counter"));
            // Pre-existing source B contributes "name" with StringSerializer's snapshot.
            StateSerializerRegistry seedingB = new StateSerializerRegistry();
            seedingB.register("name", /* VALUE */ 0, StringSerializer.INSTANCE);
            restored.put("name", seedingB.get("name"));

            pair.backend.seedRestoredSerializerMetadata(restored);

            assertTrue(
                    reg.activatedForRestore(),
                    "E6-HIGH-4(b): seedRestoredSerializerMetadata flips activatedForRestore so"
                            + " the verifyOrRegister branch runs against the restored snapshot");

            // verifyOrRegister against the SAME serializer used by the source promotes the entry
            // to live and returns the supplied serializer (COMPATIBLE_AS_IS).
            assertNotNull(
                    reg.verifyOrRegister("counter", /* VALUE */ 0, LongSerializer.INSTANCE),
                    "verifyOrRegister succeeds for restored 'counter'");
            assertNotNull(
                    reg.verifyOrRegister("name", /* VALUE */ 0, StringSerializer.INSTANCE),
                    "verifyOrRegister succeeds for restored 'name'");

            // Passing null is treated as empty (no-op pre-E5 snapshot path).
            pair.backend.seedRestoredSerializerMetadata(null);
        } finally {
            pair.backend.close();
        }
    }

    /**
     * R35-H1 regression: when {@code StateSerializerRegistry#verifyOrRegister} returns a
     * RECONFIGURED variant (because the new descriptor's snapshot resolves to
     * {@link org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility#compatibleWithReconfiguredSerializer}),
     * the V1-sync backend MUST use the reconfigured serializer when constructing the adapter —
     * NOT the original from {@code stateDesc.getSerializer()}. Pre-R35-H1 the return value was
     * silently discarded and the adapter held the OLD serializer, so subsequent reads/writes
     * went through the old schema and silently produced wrong-format payloads.
     *
     * <p>Strategy: pre-seed the backend's registry with a metadata entry that the new
     * descriptor's snapshot will resolve as RECONFIGURED against (the snapshot type
     * {@code ReconfiguringSerializer.Snapshot} below always reports RECONFIGURED with a fixed
     * sentinel reconfigured serializer). Then call {@code createOrUpdateInternalState} with the
     * original serializer; the resulting {@code InternalValueState} adapter must report the
     * RECONFIGURED instance via {@code getValueSerializer()}.
     */
    @Test
    void reconfiguredSerializerThreadedIntoValueAdapter(@TempDir Path tmp) throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db"));
        try {
            StateSerializerRegistry reg = pair.backend.stateSerializerRegistry();
            // Seed prior metadata so verifyOrRegister has something to resolve against — register
            // the SAME serializer under "evolved" so the snapshot bytes are present in `restored`.
            ReconfiguringSerializer originalSer = new ReconfiguringSerializer("v1");
            StateSerializerRegistry seedingReg = new StateSerializerRegistry();
            // StateDescriptor.Type.VALUE.ordinal() == 1 (0 is the deprecated UNKNOWN slot).
            int valueOrdinal =
                    org.apache.flink.api.common.state.StateDescriptor.Type.VALUE.ordinal();
            seedingReg.register("evolved", valueOrdinal, originalSer);
            Map<String, StateSerializerMetadata> restoredMap = new LinkedHashMap<>();
            restoredMap.put("evolved", seedingReg.get("evolved"));
            pair.backend.seedRestoredSerializerMetadata(restoredMap);

            // Now construct a fresh ValueStateDescriptor with the SAME serializer — the snapshot's
            // resolveSchemaCompatibility will return RECONFIGURED with the sentinel below.
            org.apache.flink.api.common.state.ValueStateDescriptor<String> vsd =
                    new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                            "evolved", originalSer);
            vsd.initializeSerializerUnlessSet(new ExecutionConfig());

            @SuppressWarnings({"unchecked", "rawtypes"})
            org.apache.flink.runtime.state.internal.InternalValueState<Integer, Void, String>
                    valueState =
                            (org.apache.flink.runtime.state.internal.InternalValueState<
                                            Integer, Void, String>)
                                    pair.backend.createOrUpdateInternalState(
                                            org.apache.flink.api.common.typeutils.base
                                                    .VoidSerializer.INSTANCE,
                                            (org.apache.flink.api.common.state.StateDescriptor) vsd,
                                            org.apache.flink.runtime.state.StateSnapshotTransformer
                                                    .StateSnapshotTransformFactory.noTransform());

            // R35-H1 assertion: the adapter's value serializer is the RECONFIGURED instance, not
            // the original descriptor serializer.
            assertNotNull(valueState.getValueSerializer(), "adapter must have a value serializer");
            assertTrue(
                    valueState.getValueSerializer() instanceof ReconfiguringSerializer rs
                            && "reconfigured".equals(rs.tag()),
                    "R35-H1: adapter must hold the RECONFIGURED serializer (tag=reconfigured),"
                            + " got: "
                            + valueState.getValueSerializer());
        } finally {
            pair.backend.close();
        }
    }

    /**
     * R36-M3: V1-sync ListState variant of the schema-drift write-path coverage. Confirms the
     * {@code createOrUpdateInternalState} call for a {@code ListStateDescriptor} flows through
     * {@code verifyOrRegister} (writing the schema into the registry's live buffer for
     * subsequent snapshot emission) and that the LIST kind ordinal is persisted so a restore
     * with a kind mismatch will be detected.
     */
    @Test
    void v1SyncListStateRegistersSerializerInRegistry(@TempDir Path tmp) throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db"));
        try {
            StateSerializerRegistry reg = pair.backend.stateSerializerRegistry();
            assertEquals(
                    0,
                    reg.metadataBuffer().size(),
                    "fresh backend has no registered states");

            org.apache.flink.api.common.state.ListStateDescriptor<String> lsd =
                    new org.apache.flink.api.common.state.ListStateDescriptor<>(
                            "evolvedList", StringSerializer.INSTANCE);
            lsd.initializeSerializerUnlessSet(new ExecutionConfig());

            pair.backend.createOrUpdateInternalState(
                    org.apache.flink.api.common.typeutils.base.VoidSerializer.INSTANCE,
                    (org.apache.flink.api.common.state.StateDescriptor) lsd,
                    org.apache.flink.runtime.state.StateSnapshotTransformer
                            .StateSnapshotTransformFactory.noTransform());

            // R36-M3: the ListStateDescriptor's registration landed in the registry, with the
            // LIST kind ordinal persisted so a subsequent restore that supplies the same name
            // with a different kind would surface as StateMigrationException.
            StateSerializerMetadata md = reg.get("evolvedList");
            assertNotNull(md, "ListState creation MUST register schema in the registry");
            int listOrdinal =
                    org.apache.flink.api.common.state.StateDescriptor.Type.LIST.ordinal();
            assertEquals(
                    listOrdinal,
                    md.stateKindOrdinal(),
                    "R36-M3: registered kind ordinal for ListState must equal Type.LIST.ordinal()");
        } finally {
            pair.backend.close();
        }
    }

    /**
     * R36-M3: V1-sync MapState variant of the schema-drift write-path coverage. Confirms the
     * {@code createOrUpdateInternalState} call for a {@code MapStateDescriptor} flows through
     * {@code verifyOrRegister}. Note: the V1-sync MAP path validates the MapSerializer composite
     * (containing both UK and UV) as a SINGLE registry entry, so the UK schema drift is caught
     * by composite snapshot resolution — distinct from the V2 path's R36-M1 fix which uses a
     * separate {@code <stateName>$UK} entry.
     */
    @Test
    void v1SyncMapStateRegistersCompositeSerializerInRegistry(@TempDir Path tmp) throws Exception {
        V1Pair pair = openV1Pair(tmp.resolve("db"));
        try {
            StateSerializerRegistry reg = pair.backend.stateSerializerRegistry();
            assertEquals(
                    0,
                    reg.metadataBuffer().size(),
                    "fresh backend has no registered states");

            org.apache.flink.api.common.state.MapStateDescriptor<String, Long> msd =
                    new org.apache.flink.api.common.state.MapStateDescriptor<>(
                            "evolvedMap", StringSerializer.INSTANCE, LongSerializer.INSTANCE);
            msd.initializeSerializerUnlessSet(new ExecutionConfig());

            pair.backend.createOrUpdateInternalState(
                    org.apache.flink.api.common.typeutils.base.VoidSerializer.INSTANCE,
                    (org.apache.flink.api.common.state.StateDescriptor) msd,
                    org.apache.flink.runtime.state.StateSnapshotTransformer
                            .StateSnapshotTransformFactory.noTransform());

            StateSerializerMetadata md = reg.get("evolvedMap");
            assertNotNull(md, "MapState creation MUST register schema in the registry");
            int mapOrdinal =
                    org.apache.flink.api.common.state.StateDescriptor.Type.MAP.ordinal();
            assertEquals(
                    mapOrdinal,
                    md.stateKindOrdinal(),
                    "R36-M3: registered kind ordinal for MapState must equal Type.MAP.ordinal()");
            // R36-M3: V1-sync uses a single registry entry for the WHOLE MapSerializer composite,
            // so the second registry entry that R36-M1 introduces for V2 ($UK suffix) MUST NOT
            // exist on the V1-sync path. This is a deliberate contract: V1-sync composite snapshot
            // resolution catches UK drift through MapSerializer.resolveSchemaCompatibility's
            // composite delegation — no separate entry needed.
            assertEquals(
                    null,
                    reg.get("evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX),
                    "R36-M3: V1-sync MapState must NOT create a separate $UK registry entry"
                            + " (only V2's R36-M1 path uses the suffix)");
        } finally {
            pair.backend.close();
        }
    }

    /**
     * Test-only {@code TypeSerializer<String>} whose {@code snapshotConfiguration()} returns a
     * snapshot that ALWAYS resolves to {@link
     * org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility#compatibleWithReconfiguredSerializer}
     * with a sentinel {@code ReconfiguringSerializer("reconfigured")} as the reconfigured
     * instance. Drives the R35-H1 fix path: the registry returns the sentinel when the snapshot
     * is restored and compared against the original. Delegates byte-level ops to
     * {@link org.apache.flink.api.common.typeutils.base.StringSerializer#INSTANCE}.
     */
    private static final class ReconfiguringSerializer
            extends org.apache.flink.api.common.typeutils.TypeSerializer<String> {
        private static final long serialVersionUID = 1L;
        private final String tag;

        ReconfiguringSerializer(String tag) {
            this.tag = tag;
        }

        String tag() {
            return tag;
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<String> duplicate() {
            return this;
        }

        @Override
        public String createInstance() {
            return "";
        }

        @Override
        public String copy(String from) {
            return from;
        }

        @Override
        public String copy(String from, String reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(String s, org.apache.flink.core.memory.DataOutputView target)
                throws java.io.IOException {
            org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE.serialize(
                    s, target);
        }

        @Override
        public String deserialize(org.apache.flink.core.memory.DataInputView source)
                throws java.io.IOException {
            return org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE.deserialize(
                    source);
        }

        @Override
        public String deserialize(String reuse, org.apache.flink.core.memory.DataInputView source)
                throws java.io.IOException {
            return deserialize(source);
        }

        @Override
        public void copy(
                org.apache.flink.core.memory.DataInputView source,
                org.apache.flink.core.memory.DataOutputView target)
                throws java.io.IOException {
            String s = deserialize(source);
            serialize(s, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ReconfiguringSerializer other && tag.equals(other.tag);
        }

        @Override
        public int hashCode() {
            return tag.hashCode();
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<String>
                snapshotConfiguration() {
            return new ReconfiguringSerializerSnapshot();
        }

        @Override
        public String toString() {
            return "ReconfiguringSerializer(" + tag + ")";
        }
    }

    /**
     * Companion snapshot that always reports {@code COMPATIBLE_WITH_RECONFIGURED_SERIALIZER} with
     * a sentinel reconfigured serializer.
     */
    public static final class ReconfiguringSerializerSnapshot
            implements org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<String> {
        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(org.apache.flink.core.memory.DataOutputView out)
                throws java.io.IOException {}

        @Override
        public void readSnapshot(
                int readVersion,
                org.apache.flink.core.memory.DataInputView in,
                ClassLoader userCodeClassLoader)
                throws java.io.IOException {}

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<String> restoreSerializer() {
            return new ReconfiguringSerializer("restored-from-snapshot");
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility<String>
                resolveSchemaCompatibility(
                        org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<String>
                                oldSerializerSnapshot) {
            return org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility
                    .compatibleWithReconfiguredSerializer(
                            new ReconfiguringSerializer("reconfigured"));
        }
    }
}

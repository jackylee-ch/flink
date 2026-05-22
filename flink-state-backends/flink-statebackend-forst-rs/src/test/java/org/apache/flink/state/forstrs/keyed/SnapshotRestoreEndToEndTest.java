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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.state.StateSerializerMetadata;
import org.apache.flink.state.forstrs.state.StateSerializerRegistry;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end acceptance test for PR-A4-H4: the async backend now restores from a prior snapshot's
 * {@link KeyedStateHandle} collection instead of opening a fresh empty engine on restart.
 *
 * <p>Sequence under test:
 *
 * <ol>
 *   <li>Open an async backend on a fresh local DB; seed 1024 key/value pairs through the engine's
 *       FFI surface.
 *   <li>Take an incremental snapshot via {@link ForStRsAsyncKeyedStateBackend#snapshot} →
 *       materialise the {@link ForStRsIncrementalKeyedStateHandle}.
 *   <li>Close the source backend.
 *   <li>Open a NEW async backend via {@link ForStRsAsyncKeyedStateBackend#restoreFromHandles}
 *       handing back the snapshot's handle.
 *   <li>Read every seeded key from the restored engine and assert byte-identical round-trip.
 * </ol>
 *
 * <p>Pre-PR: the async-backend path in {@code createAsyncKeyedStateBackend} silently discarded
 * the restored handles and opened an empty DB; this test FAILS pre-PR because step 5 returns null
 * for every key.
 */
class SnapshotRestoreEndToEndTest {

    private static final int ENTRY_COUNT = 1024;

    @Test
    void writeSnapshotCloseReopenAllKeysReadable(@TempDir Path tmp) throws Exception {
        // Step 1: open a source backend + seed entries.
        Arena srcArena = Arena.ofShared();
        ForStRsLinker srcLinker = new ForStRsLinker(srcArena);
        Path srcDbPath = tmp.resolve("src-db");
        java.nio.file.Files.createDirectories(srcDbPath);
        FrsDb srcDb = srcLinker.dbOpen(srcArena, srcDbPath.toString());
        FrsCfHandle srcCf = srcLinker.dbDefaultCf(srcDb, srcArena);
        ForStRsAsyncKeyedStateBackend<Integer> srcBackend =
                new ForStRsAsyncKeyedStateBackend<>(
                        srcArena,
                        srcLinker,
                        srcDb,
                        srcCf,
                        IntSerializer.INSTANCE,
                        new KeyGroupRange(0, 0),
                        /* totalKeyGroups= */ 1,
                        /* ownsResources= */ true);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            srcLinker.put(srcDb, srcCf, key(i), value(i));
        }

        // Step 2: snapshot — materialise the handle.
        MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
        CheckpointOptions opts =
                CheckpointOptions.alignedNoTimeout(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault());

        RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                srcBackend.snapshot(42L, 0L, factory, opts);
        if (!fut.isDone()) {
            fut.run();
        }
        ForStRsIncrementalKeyedStateHandle handle =
                (ForStRsIncrementalKeyedStateHandle) fut.get().getJobManagerOwnedSnapshot();
        assertNotNull(handle, "snapshot returned a non-null incremental handle");
        UUID sourceBackendId = handle.getBackendIdentifier();
        assertNotNull(sourceBackendId, "snapshot backend identifier is populated");

        // Step 3: close the source backend so nothing leaks file handles into the restore.
        srcBackend.close();

        // Step 4: open a NEW async backend via restoreFromHandles. Fresh arena + fresh linker;
        // the restore op downloads SSTs from the in-memory stream factory and reconstructs the
        // engine on disk before opening it. restored.close() closes its arena because
        // ownsResources=true — so we don't wrap restoreArena in try-with-resources.
        Arena restoreArena = Arena.ofShared();
        boolean restoredClosedOk = false;
        try {
            ForStRsLinker restoreLinker = new ForStRsLinker(restoreArena);
            Path restoreDbPath = tmp.resolve("restored-db");
            ForStRsAsyncKeyedStateBackend<Integer> restored =
                    ForStRsAsyncKeyedStateBackend.restoreFromHandles(
                            restoreArena,
                            restoreLinker,
                            IntSerializer.INSTANCE,
                            new KeyGroupRange(0, 0),
                            /* totalKeyGroups= */ 1,
                            restoreDbPath,
                            List.<KeyedStateHandle>of(handle));
            try {
                // Step 5: every seeded key is readable from the restored engine.
                FrsDb restoredDb = fieldDb(restored);
                FrsCfHandle restoredCf = fieldCf(restored);
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    byte[] got = restoreLinker.get(restoredDb, restoredCf, key(i));
                    assertArrayEquals(
                            value(i),
                            got,
                            "restored engine must round-trip key " + i + " byte-identical");
                }

                // The restored backend inherits the source's backend identifier (single-handle,
                // no rescaling fast path).
                assertEquals(
                        sourceBackendId,
                        backendIdField(restored),
                        "no-rescaling restore inherits the source backend identifier so"
                                + " SharedStateRegistry resolves the prior session's SSTs");

                // The restored SST registry was pre-populated by the restore operation so the
                // next incremental snapshot reuses those SSTs.
                assertNotNull(
                        restored.sstRegistryForTesting(),
                        "restoreFromHandles pre-populates the SST registry");
                assertTrue(
                        restored.sstRegistryForTesting().size() >= 1,
                        "restored SST registry has at least one entry from the inherited handle");
            } finally {
                restored.close();
                restoredClosedOk = true;
            }
        } finally {
            // restored.close() already closed restoreArena via ownsResources=true. Only attempt a
            // safety-net close if restored.close() never ran (e.g. exception before its finally).
            if (!restoredClosedOk) {
                try {
                    restoreArena.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    /**
     * E5-HIGH-2: register two states with different serializer snapshots, snapshot the registry
     * via the snapshot strategy's private-state emit, restore, and verify {@link
     * StateSerializerRegistry#verifyOrRegister} succeeds for both (i.e. the parsed restore-side
     * registry sees both prior snapshots and routes them through the COMPATIBLE_AS_IS branch).
     *
     * <p>Why this test, not just a codec round-trip: pre-fix the schema-drift detection was
     * structurally broken because {@code seedFromRestore(new LinkedHashMap<>())} always seeded an
     * empty map and the snapshot path never wrote the registry blob. This test pins BOTH halves:
     * write (snapshot strategy emit) and read (restore op parse + seedFromRestore).
     */
    @Test
    void serializerRegistrySurvivesSnapshotAndRestore(@TempDir Path tmp) throws Exception {
        // Step 1: open a source backend.
        Arena srcArena = Arena.ofShared();
        ForStRsLinker srcLinker = new ForStRsLinker(srcArena);
        Path srcDbPath = tmp.resolve("src-db");
        java.nio.file.Files.createDirectories(srcDbPath);
        FrsDb srcDb = srcLinker.dbOpen(srcArena, srcDbPath.toString());
        FrsCfHandle srcCf = srcLinker.dbDefaultCf(srcDb, srcArena);
        ForStRsAsyncKeyedStateBackend<Integer> srcBackend =
                new ForStRsAsyncKeyedStateBackend<>(
                        srcArena,
                        srcLinker,
                        srcDb,
                        srcCf,
                        IntSerializer.INSTANCE,
                        new KeyGroupRange(0, 0),
                        /* totalKeyGroups= */ 1,
                        /* ownsResources= */ true);

        // Step 2: seed two state-name → serializer registrations with DIFFERENT snapshot kinds.
        // "counter" uses LongSerializer; "name" uses StringSerializer. The registry buffer must
        // carry both entries' opaque {@code TypeSerializerSnapshot} bytes.
        StateSerializerRegistry srcRegistry = srcBackend.stateSerializerRegistry();
        srcRegistry.register("counter", /* VALUE */ 0, LongSerializer.INSTANCE);
        srcRegistry.register("name", /* VALUE */ 0, StringSerializer.INSTANCE);
        assertEquals(
                2,
                srcRegistry.metadataBuffer().size(),
                "source registry holds both registrations before snapshot");

        // Step 3: seed engine data so the snapshot path materialises a real SST + manifest.
        for (int i = 0; i < 128; i++) {
            srcLinker.put(srcDb, srcCf, key(i), value(i));
        }

        // Step 4: snapshot. The strategy's setRegistryBlobProvider hook (wired by
        // ensureSnapshotStrategy) drains the registry into a private-state entry under
        // {@link ForStRsSnapshotStrategy#SERIALIZER_REGISTRY_LOCAL_PATH}.
        MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
        CheckpointOptions opts =
                CheckpointOptions.alignedNoTimeout(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault());
        RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                srcBackend.snapshot(7L, 0L, factory, opts);
        if (!fut.isDone()) {
            fut.run();
        }
        ForStRsIncrementalKeyedStateHandle handle =
                (ForStRsIncrementalKeyedStateHandle) fut.get().getJobManagerOwnedSnapshot();
        assertNotNull(handle);
        // Sanity: the privateState list now carries the registry blob entry.
        assertTrue(
                handle.getPrivateState().stream()
                        .anyMatch(
                                hlp ->
                                        ForStRsSnapshotStrategy.SERIALIZER_REGISTRY_LOCAL_PATH
                                                .equals(hlp.getLocalPath())),
                "E5-HIGH-2: snapshot emits the registry blob as a private-state entry");
        srcBackend.close();

        // Step 5: restore into a fresh backend.
        Arena restoreArena = Arena.ofShared();
        boolean restoredClosedOk = false;
        try {
            ForStRsLinker restoreLinker = new ForStRsLinker(restoreArena);
            Path restoreDbPath = tmp.resolve("restored-db");
            ForStRsAsyncKeyedStateBackend<Integer> restored =
                    ForStRsAsyncKeyedStateBackend.restoreFromHandles(
                            restoreArena,
                            restoreLinker,
                            IntSerializer.INSTANCE,
                            new KeyGroupRange(0, 0),
                            /* totalKeyGroups= */ 1,
                            restoreDbPath,
                            List.<KeyedStateHandle>of(handle));
            try {
                // Step 6: verify the restore-side registry has BOTH entries and
                // verifyOrRegister against the original serializers succeeds for both.
                StateSerializerRegistry restoredRegistry = restored.stateSerializerRegistry();
                assertTrue(
                        restoredRegistry.activatedForRestore(),
                        "restoreFromHandles must seed the registry");
                TypeSerializer<Long> counterEffective =
                        restoredRegistry.verifyOrRegister(
                                "counter", /* VALUE */ 0, LongSerializer.INSTANCE);
                TypeSerializer<String> nameEffective =
                        restoredRegistry.verifyOrRegister(
                                "name", /* VALUE */ 0, StringSerializer.INSTANCE);
                assertNotNull(counterEffective, "counter verifyOrRegister returned a serializer");
                assertNotNull(nameEffective, "name verifyOrRegister returned a serializer");

                // The restore-side registry must hold both names in {@code live} after promotion.
                Map<String, StateSerializerMetadata> postBuffer = restoredRegistry.metadataBuffer();
                assertTrue(
                        postBuffer.containsKey("counter"),
                        "restored+verified registry holds 'counter'");
                assertTrue(
                        postBuffer.containsKey("name"),
                        "restored+verified registry holds 'name'");
            } finally {
                restored.close();
                restoredClosedOk = true;
            }
        } finally {
            if (!restoredClosedOk) {
                try {
                    restoreArena.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    private static byte[] key(int i) {
        return ("k-" + i).getBytes();
    }

    private static byte[] value(int i) {
        return ("v-" + i).getBytes();
    }

    // Reflective accessors — the async backend hides db/cf fields; tests need them to issue
    // raw FFI reads against the restored engine without going through V2 state plumbing.
    private static FrsDb fieldDb(ForStRsAsyncKeyedStateBackend<?> b) throws Exception {
        java.lang.reflect.Field f = ForStRsAsyncKeyedStateBackend.class.getDeclaredField("db");
        f.setAccessible(true);
        return (FrsDb) f.get(b);
    }

    private static FrsCfHandle fieldCf(ForStRsAsyncKeyedStateBackend<?> b) throws Exception {
        java.lang.reflect.Field f =
                ForStRsAsyncKeyedStateBackend.class.getDeclaredField("defaultCf");
        f.setAccessible(true);
        return (FrsCfHandle) f.get(b);
    }

    private static UUID backendIdField(ForStRsAsyncKeyedStateBackend<?> b) throws Exception {
        java.lang.reflect.Field f =
                ForStRsAsyncKeyedStateBackend.class.getDeclaredField("backendIdentifier");
        f.setAccessible(true);
        return (UUID) f.get(b);
    }
}

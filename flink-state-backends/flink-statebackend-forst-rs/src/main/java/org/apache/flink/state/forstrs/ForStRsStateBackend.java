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

package org.apache.flink.state.forstrs;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.ForStRsAbstractKeyedStateBackend;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;
import org.apache.flink.state.forstrs.keyed.ForStRsRestoreOperation;
import org.apache.flink.state.forstrs.keyed.ForStRsSnapshotStrategy;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;

import java.io.File;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * {@link StateBackend} backed by ForSt-RS via JDK 25 FFM.
 *
 * <p><b>SPI entry-point (L5/L6 wired).</b> {@link
 * #createKeyedStateBackend(KeyedStateBackendParameters)} now returns a real {@link
 * ForStRsAbstractKeyedStateBackend} (constructed via {@link ForStRsKeyedStateBackend} delegate), so
 * a Flink user setting {@code state.backend =
 * org.apache.flink.state.forstrs.ForStRsStateBackendFactory} can run keyed-state jobs. Storage URI
 * routing follows {@link ForStRsOptions#storageUri()}: when set, the backend opens via {@link
 * ForStRsLinker#dbOpenRemote OpenDAL}, otherwise via {@link ForStRsLinker#dbOpen} on a unique
 * subdirectory under the TaskManager's tmp working directory.
 *
 * <p>{@link #createOperatorStateBackend(OperatorStateBackendParameters)} delegates to Flink's
 * {@link DefaultOperatorStateBackendBuilder} — the standard pattern for backends whose operator
 * state is a serialized bytestream rather than a KV store.
 *
 * @see ForStRsOptions
 * @see ForStRsLinker
 * @see ForStRsKeyedStateBackend
 */
public class ForStRsStateBackend implements StateBackend {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean supportsAsyncKeyedStateBackend() {
        return true;
    }

    @Override
    public String getName() {
        return "forst-rs";
    }

    @Override
    public <K>
            org.apache.flink.runtime.state.AsyncKeyedStateBackend<K> createAsyncKeyedStateBackend(
                    StateBackend.KeyedStateBackendParameters<K> parameters) throws Exception {
        // E7-H3: assert only one keyed-backend path (V1-sync or async) per operator id. Each
        // backend has its own private StateSerializerRegistry — cross-path use would silently
        // bypass schema-drift detection. Fail loudly here at construction time.
        org.apache.flink.state.forstrs.keyed.ForStRsBackendPathInvariant.recordBackendPath(
                parameters.getOperatorIdentifier(),
                org.apache.flink.state.forstrs.keyed.ForStRsBackendPathInvariant.Path.ASYNC_V2);

        Arena arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        Environment env = parameters.getEnv();
        File tmpRoot = env.getTaskManagerInfo().getTmpWorkingDirectory();
        Path dbRoot = tmpRoot.toPath().resolve("forst-rs-async");
        Files.createDirectories(dbRoot);
        String fileSafeOpId = parameters.getOperatorIdentifier().replaceAll("[^a-zA-Z0-9\\-]", "_");
        Path localDbPath = dbRoot.resolve(fileSafeOpId + "-" + UUID.randomUUID());

        // PR-A4-H4: Flink hands back the prior session's KeyedStateHandles on a job restart
        // (restart-from-checkpoint / restart-from-savepoint / rescaling). Until this PR the async
        // backend silently dropped them and opened a fresh empty engine — snapshots were
        // write-only. Now we route to the restore factory which materialises the engine on disk
        // before opening, mirroring the V1-sync path in `createKeyedStateBackend(...)`.
        Collection<KeyedStateHandle> restoredHandles = parameters.getStateHandles();
        if (restoredHandles != null && !restoredHandles.isEmpty()) {
            try {
                return org.apache.flink.state.forstrs.keyed.ForStRsAsyncKeyedStateBackend
                        .restoreFromHandles(
                                arena,
                                linker,
                                parameters.getKeySerializer(),
                                parameters.getKeyGroupRange(),
                                parameters.getNumberOfKeyGroups(),
                                localDbPath,
                                restoredHandles);
            } catch (Throwable t) {
                // Best-effort tear-down on restore failure: the arena owns the linker; closing
                // it releases every FFM resource the partial restore may have allocated.
                try {
                    arena.close();
                } catch (Throwable ignored) {
                }
                if (t instanceof Exception ex) {
                    throw ex;
                }
                throw new Exception(
                        "ForStRsStateBackend.createAsyncKeyedStateBackend restore failed", t);
            }
        }

        // D5-H1: mirror the restore-path try/catch tear-down (lines 100-122). Before this fix a
        // throw from {@code dbOpen}, {@code dbDefaultCf}, or the backend constructor leaked both
        // the arena and any partially-opened db/cf — the only catch was inside the restore
        // branch. Close in CF→DB→Arena order: cf must be released before db (the engine pins
        // its CF handles to the open db), and arena last because it owns the linker that issued
        // the close calls themselves.
        FrsDb db = null;
        FrsCfHandle cf = null;
        try {
            db = linker.dbOpen(arena, localDbPath.toString());
            cf = linker.dbDefaultCf(db, arena);
            return new org.apache.flink.state.forstrs.keyed.ForStRsAsyncKeyedStateBackend<>(
                    arena,
                    linker,
                    db,
                    cf,
                    parameters.getKeySerializer(),
                    parameters.getKeyGroupRange(),
                    parameters.getNumberOfKeyGroups(),
                    true);
        } catch (Throwable t) {
            if (cf != null) {
                try {
                    cf.close();
                } catch (Throwable ignored) {
                }
            }
            if (db != null) {
                try {
                    db.close();
                } catch (Throwable ignored) {
                }
            }
            try {
                arena.close();
            } catch (Throwable ignored) {
            }
            if (t instanceof Exception ex) {
                throw ex;
            }
            throw new Exception(
                    "ForStRsStateBackend.createAsyncKeyedStateBackend open failed", t);
        }
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            StateBackend.KeyedStateBackendParameters<K> parameters) throws Exception {
        // E7-H3: assert only one keyed-backend path (V1-sync or async) per operator id. Each
        // backend has its own private StateSerializerRegistry — cross-path use would silently
        // bypass schema-drift detection. Fail loudly here at construction time.
        org.apache.flink.state.forstrs.keyed.ForStRsBackendPathInvariant.recordBackendPath(
                parameters.getOperatorIdentifier(),
                org.apache.flink.state.forstrs.keyed.ForStRsBackendPathInvariant.Path.SYNC_V1);

        ForStRsOptions options = new ForStRsOptions();
        Environment env = parameters.getEnv();

        // Resolve a unique local DB path under the TaskManager's tmp working directory.
        // Layout: <tmp>/forst-rs/<operatorIdSanitized>-<uuid>. The sanitisation matches the
        // community ForSt backend so the directory is always a legal filename even when the
        // operator identifier contains slashes or other unfriendly characters.
        String fileSafeOpId = parameters.getOperatorIdentifier().replaceAll("[^a-zA-Z0-9\\-]", "_");
        File tmpRoot = env.getTaskManagerInfo().getTmpWorkingDirectory();
        Path dbRoot = tmpRoot.toPath().resolve("forst-rs");
        Files.createDirectories(dbRoot);
        Path localDbPath = dbRoot.resolve(fileSafeOpId + "-" + UUID.randomUUID());

        Arena arena = Arena.ofShared();
        ForStRsLinker linker = null;
        FrsDb db = null;
        FrsCfHandle cf = null;
        // Shared SST registry — populated by restore (if any) and consumed by the snapshot
        // strategy so previously-uploaded SSTs are reused across post-restore checkpoints.
        ForStRsSstRegistry sstRegistry = new ForStRsSstRegistry();
        try {
            linker = new ForStRsLinker(arena);

            // ---------------- Restore path (Part D — Flink-coordinator restore) ----------------
            // If Flink is launching this backend with a non-empty restored-state collection (job
            // restart-from-checkpoint, restart-from-savepoint, or rescaling), drive the existing
            // ForStRsRestoreOperation to materialize the engine on disk before opening it. The
            // restore op handles both the fast no-rescaling path and the rescaling kg-redistribute
            // path; non-ForSt-RS handle types (e.g. savepoint handles from another backend) are
            // rejected with a typed exception that surfaces back to the JM.
            Collection<KeyedStateHandle> restoredHandles = parameters.getStateHandles();
            String storageUri = options.storageUri();
            boolean useRestore =
                    restoredHandles != null
                            && !restoredHandles.isEmpty()
                            && (storageUri == null || storageUri.isEmpty());

            ForStRsRestoreOperation.RestoreResult restored = null;
            if (useRestore) {
                ForStRsRestoreOperation restoreOp =
                        new ForStRsRestoreOperation(
                                linker,
                                arena,
                                localDbPath,
                                parameters.getKeyGroupRange(),
                                sstRegistry);
                restored = restoreOp.restore(restoredHandles);
                db = restored.getDb();
                cf = restored.getDefaultCf();
            } else if (storageUri != null && !storageUri.isEmpty()) {
                String cacheDir = options.cacheDir();
                if (cacheDir == null || cacheDir.isEmpty()) {
                    // Fall back to a per-backend cache subdirectory next to the local DB path.
                    cacheDir = localDbPath.resolveSibling("cache").toString();
                    Files.createDirectories(Path.of(cacheDir));
                }
                db =
                        linker.dbOpenRemote(
                                arena,
                                storageUri,
                                options.opendalConfigJson(),
                                cacheDir,
                                options.cacheCapacityBytes());
                cf = linker.dbDefaultCf(db, arena);
            } else {
                db = linker.dbOpen(arena, localDbPath.toString());
                cf = linker.dbDefaultCf(db, arena);
            }

            TypeSerializer<K> keySerializer = parameters.getKeySerializer();
            // PR-A3 (S1-6 / E-CRIT-3): pass the real key-group range + number-of-key-groups so
            // V1-sync state routing partitions keys correctly across rescaling boundaries.
            ForStRsKeyedStateBackend<K> delegate =
                    new ForStRsKeyedStateBackend<>(
                            arena,
                            linker,
                            db,
                            cf,
                            keySerializer,
                            /* ownsResources= */ true,
                            parameters.getKeyGroupRange(),
                            parameters.getNumberOfKeyGroups());

            // ForStRsAbstractKeyedStateBackend owns the delegate's lifecycle via close().
            ForStRsAbstractKeyedStateBackend<K> backend =
                    new ForStRsAbstractKeyedStateBackend<>(
                            keySerializer,
                            env.getUserCodeClassLoader().asClassLoader(),
                            env.getExecutionConfig(),
                            parameters.getCancelStreamRegistry(),
                            delegate,
                            parameters.getKeyGroupRange(),
                            parameters.getNumberOfKeyGroups());

            // Wire the snapshot strategy so Flink-triggered checkpoints have somewhere to go.
            // We use the single-CF "default" map (cfId=0) which matches the SingleCfRouter
            // default routing. The SST registry is reused from the restore path so SSTs already
            // materialised on disk are recognised as shared and not re-uploaded next checkpoint.
            ForStRsSnapshotStrategy strategy =
                    new ForStRsSnapshotStrategy(
                            linker,
                            db,
                            UUID.randomUUID(),
                            parameters.getKeyGroupRange(),
                            sstRegistry,
                            new ForStRsSstUploader(),
                            arena,
                            Map.of("default", 0L));
            backend.setSnapshotStrategy(strategy, sstRegistry);
            // E6-HIGH-4(b): seed the per-state serializer metadata into the V1-sync registry so
            // the first {@code createOrUpdateInternalState} for each state name runs through
            // {@code verifyOrRegister} with the restored schema. {@code restored} is null on
            // fresh job starts (no checkpoint to restore from); the no-rescaling path returns the
            // single source handle's blob and the rescaling path returns the union-merged map
            // assembled by {@link ForStRsRestoreOperation#restoreWithRescaling}.
            if (restored != null) {
                backend.seedRestoredSerializerMetadata(restored.getRestoredSerializerMetadata());
            }
            return backend;
        } catch (Throwable t) {
            // Best-effort tear-down on construction failure so we don't leak FFM handles.
            if (cf != null) {
                try {
                    cf.close();
                } catch (Throwable ignored) {
                }
            }
            if (db != null) {
                try {
                    db.close();
                } catch (Throwable ignored) {
                }
            }
            try {
                arena.close();
            } catch (Throwable ignored) {
            }
            if (t instanceof Exception ex) {
                throw ex;
            }
            throw new Exception("ForStRsStateBackend.createKeyedStateBackend failed", t);
        }
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            StateBackend.OperatorStateBackendParameters parameters) throws Exception {
        // ForSt-RS does not store operator state itself — it is a key-value store. Operator state
        // is a serialized bytestream that Flink's default builder handles directly.
        return new DefaultOperatorStateBackendBuilder(
                        Thread.currentThread().getContextClassLoader(),
                        parameters.getEnv().getExecutionConfig(),
                        /* asynchronousSnapshots= */ true,
                        parameters.getStateHandles(),
                        parameters.getCancelStreamRegistry())
                .build();
    }

    /**
     * Phase-D L5 stepping-stone factory: opens an in-memory ForSt-RS engine and returns a {@link
     * ForStRsKeyedStateBackend} bound to it. The returned backend owns the underlying {@link
     * Arena}, {@link ForStRsLinker}, {@link FrsDb} and default {@link FrsCfHandle}; closing it
     * releases all of them.
     *
     * <p>This entry-point predates the L5/L6 SPI wiring and is kept for direct unit tests that want
     * the lean L5-class surface (no Flink runtime context). Production code paths should use {@link
     * #createKeyedStateBackend(KeyedStateBackendParameters)} instead.
     */
    public <K> ForStRsKeyedStateBackend<K> createBasicKeyedBackend(
            TypeSerializer<K> keySerializer) {
        Arena arena = Arena.ofShared();
        try {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpenMemory(arena);
            FrsCfHandle cf;
            try {
                cf = linker.dbDefaultCf(db, arena);
            } catch (RuntimeException e) {
                db.close();
                throw e;
            }
            return new ForStRsKeyedStateBackend<>(arena, linker, db, cf, keySerializer);
        } catch (RuntimeException e) {
            arena.close();
            throw e;
        }
    }
}

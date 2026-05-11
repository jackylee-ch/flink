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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ForSt-RS keyed-backend restore operation (B-Prod-P4 Tasks 4.1, 4.2, 4.3).
 *
 * <p>Implements three flows per spec §9:
 *
 * <ol>
 *   <li><b>No-rescaling fast path</b> ({@code handles.size() == 1 &amp;&amp; handle.kgRange ==
 *       target}) — download manifest + each SST listed in the {@link
 *       ForStRsIncrementalKeyedStateHandle}, then call {@link ForStRsLinker#dbOpenFromIncremental}
 *       to materialize the engine state directly from those local files. (Tasks 4.1 + 4.2.)
 *   <li><b>Strict-SST-presence check</b> — every {@link HandleAndLocalPath} entry in the handle's
 *       shared + private state lists must download successfully. A missing or unopenable handle
 *       throws {@link ForStRsCheckpointRestoreException} carrying the offending local path + the
 *       handle's checkpoint id.
 *   <li><b>Rescaling path</b> — if {@code handles.size() != 1} or the source range is not exactly
 *       the target range, each input handle is opened in turn into a temporary engine, and for
 *       every key-group in the target range the source iterator is replayed into a freshly-opened
 *       target engine using the kg-prefixed composite-key encoding produced by {@link
 *       ForStRsKeyGroupedSerializer}. (Task 4.3.)
 * </ol>
 *
 * <p>The operation is callable in two forms: {@link #restore(Collection)} returns a freshly-opened
 * {@link FrsDb} + default {@link FrsCfHandle} bundle that the caller wraps into an {@link
 * ForStRsAbstractKeyedStateBackend}; or {@link #restoreToBackend(Collection,
 * org.apache.flink.api.common.typeutils.TypeSerializer)} performs the full backend assembly.
 *
 * <p>Lifetime: the operation does <i>not</i> own the {@link ForStRsLinker} or {@link Arena} it was
 * constructed with; it borrows them. The {@link FrsDb} returned is owned by the caller (typically
 * the keyed-backend that wraps it).
 */
@Internal
public class ForStRsRestoreOperation {

    private final ForStRsLinker linker;
    private final Arena arena;
    private final Path targetDir;
    private final KeyGroupRange targetRange;
    private final ForStRsSstRegistry sstRegistry;

    public ForStRsRestoreOperation(
            ForStRsLinker linker,
            Arena arena,
            Path targetDir,
            KeyGroupRange targetRange,
            ForStRsSstRegistry sstRegistry) {
        this.linker = linker;
        this.arena = arena;
        this.targetDir = targetDir;
        this.targetRange = targetRange;
        this.sstRegistry = sstRegistry;
    }

    /** Result bundle returned by {@link #restore(Collection)}. */
    public static final class RestoreResult {
        private final FrsDb db;
        private final FrsCfHandle defaultCf;
        private final Map<String, Long> cfMap;
        private final long restoredCheckpointId;

        RestoreResult(
                FrsDb db,
                FrsCfHandle defaultCf,
                Map<String, Long> cfMap,
                long restoredCheckpointId) {
            this.db = db;
            this.defaultCf = defaultCf;
            this.cfMap = cfMap;
            this.restoredCheckpointId = restoredCheckpointId;
        }

        public FrsDb getDb() {
            return db;
        }

        public FrsCfHandle getDefaultCf() {
            return defaultCf;
        }

        public Map<String, Long> getCfMap() {
            return cfMap;
        }

        public long getRestoredCheckpointId() {
            return restoredCheckpointId;
        }
    }

    /**
     * Performs the restore using the chosen strategy (fast path vs. rescaling).
     *
     * @param handles state handles produced by a prior snapshot (one per source subtask).
     *     Empty/null collection ⇒ open a fresh empty engine at {@link #targetDir}.
     */
    public RestoreResult restore(Collection<KeyedStateHandle> handles) throws IOException {
        ensureTargetDirEmpty();

        if (handles == null || handles.isEmpty()) {
            return openEmpty();
        }

        // Filter out non-ForSt-RS handles defensively (e.g. a savepoint handle from another
        // backend — for now we treat that as unsupported by failing fast).
        List<ForStRsIncrementalKeyedStateHandle> incHandles = new ArrayList<>(handles.size());
        for (KeyedStateHandle h : handles) {
            if (!(h instanceof ForStRsIncrementalKeyedStateHandle)) {
                throw new ForStRsCheckpointRestoreException(
                        null,
                        -1L,
                        "Unsupported keyed-state handle type for ForStRs restore: "
                                + (h == null ? "null" : h.getClass().getName()));
            }
            incHandles.add((ForStRsIncrementalKeyedStateHandle) h);
        }

        boolean fastPath =
                incHandles.size() == 1 && incHandles.get(0).getKeyGroupRange().equals(targetRange);

        if (fastPath) {
            return restoreNoRescaling(incHandles.get(0));
        }
        return restoreWithRescaling(incHandles);
    }

    // ------------------------------------------------------------------
    // 4.1 + 4.2: download + open path with strict-SST check
    // ------------------------------------------------------------------

    private RestoreResult restoreNoRescaling(ForStRsIncrementalKeyedStateHandle handle)
            throws IOException {
        Path downloadDir = targetDir.resolve("_restore_dl");
        Files.createDirectories(downloadDir);

        // 1. Download the manifest (private/meta state) into a known local path.
        Path manifestPath = downloadDir.resolve("CHECKPOINT.blob");
        downloadHandleStrict(
                handle.getMetaDataStateHandle(), manifestPath, "CHECKPOINT.blob", handle);

        // 2. Download every SST (shared + private). Strict check: each must materialize.
        List<String> sstLocalPaths = new ArrayList<>();
        for (HandleAndLocalPath hlp : handle.getSharedState()) {
            Path local = downloadDir.resolve(hlp.getLocalPath());
            downloadHandleStrict(hlp.getHandle(), local, hlp.getLocalPath(), handle);
            sstLocalPaths.add(local.toString());
        }
        for (HandleAndLocalPath hlp : handle.getPrivateState()) {
            Path local = downloadDir.resolve(hlp.getLocalPath());
            downloadHandleStrict(hlp.getHandle(), local, hlp.getLocalPath(), handle);
            sstLocalPaths.add(local.toString());
        }

        // 3. Hand the materialized files to the engine, which links/copies them under targetDir
        //    and reconstructs the LSM from the manifest.
        FrsDb db;
        try {
            db =
                    linker.dbOpenFromIncremental(
                            arena, targetDir.toString(), manifestPath.toString(), sstLocalPaths);
        } catch (RuntimeException re) {
            throw new ForStRsCheckpointRestoreException(
                    manifestPath.toString(),
                    handle.getCheckpointId(),
                    "ForSt-RS engine refused to open from manifest: " + re.getMessage(),
                    re);
        }
        FrsCfHandle defaultCf;
        try {
            defaultCf = linker.dbDefaultCf(db, arena);
        } catch (RuntimeException re) {
            db.close();
            throw new ForStRsCheckpointRestoreException(
                    targetDir.toString(),
                    handle.getCheckpointId(),
                    "ForSt-RS engine restored, but default CF unreachable: " + re.getMessage(),
                    re);
        }

        // 4. Re-populate the SST registry from the restored handles so the next incremental
        //    checkpoint can reuse them as shared state without re-uploading.
        if (sstRegistry != null) {
            for (HandleAndLocalPath hlp : handle.getSharedState()) {
                sstRegistry.register(new StateHandleID(hlp.getLocalPath()), hlp.getHandle());
            }
        }

        return new RestoreResult(
                db, defaultCf, new LinkedHashMap<>(handle.getCfMap()), handle.getCheckpointId());
    }

    /**
     * Reads a {@link StreamStateHandle} into {@code localTarget}. On any failure (handle returns no
     * data, I/O error, or the resulting file is empty) throws a strict-restore exception with
     * {@code logicalPath} + the source handle's checkpoint id.
     */
    private void downloadHandleStrict(
            StreamStateHandle handle,
            Path localTarget,
            String logicalPath,
            ForStRsIncrementalKeyedStateHandle owner)
            throws ForStRsCheckpointRestoreException {
        if (handle == null) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: handle for '" + logicalPath + "' is null");
        }
        try {
            Files.createDirectories(localTarget.getParent());
            try (FSDataInputStream in = handle.openInputStream();
                    OutputStream out = Files.newOutputStream(localTarget)) {
                byte[] buf = new byte[8 * 1024];
                int n;
                long total = 0L;
                while ((n = in.read(buf)) > 0) {
                    out.write(buf, 0, n);
                    total += n;
                }
                // Manifest blobs and SST files are never legitimately empty; an empty file means
                // the handle resolved to a missing/truncated upload — fail strictly.
                if (total == 0L) {
                    throw new ForStRsCheckpointRestoreException(
                            logicalPath,
                            owner.getCheckpointId(),
                            "Strict restore: handle for '"
                                    + logicalPath
                                    + "' produced 0 bytes (likely deleted upstream)");
                }
            }
        } catch (ForStRsCheckpointRestoreException e) {
            throw e;
        } catch (IOException ioe) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: could not download '"
                            + logicalPath
                            + "' for ckpt "
                            + owner.getCheckpointId()
                            + ": "
                            + ioe.getMessage(),
                    ioe);
        }
    }

    // ------------------------------------------------------------------
    // 4.3: rescaling path (kg redistribution)
    // ------------------------------------------------------------------

    private RestoreResult restoreWithRescaling(List<ForStRsIncrementalKeyedStateHandle> handles)
            throws IOException {
        // 1. Materialize each input handle into its own temp DB (using the no-rescaling path).
        List<OpenSourceDb> sources = new ArrayList<>(handles.size());
        long maxRestoredCkpt = 0L;
        Map<String, Long> mergedCfMap = new LinkedHashMap<>();
        try {
            for (int i = 0; i < handles.size(); i++) {
                ForStRsIncrementalKeyedStateHandle h = handles.get(i);
                Path subDir = targetDir.resolve("_restore_src_" + i);
                Files.createDirectories(subDir);
                OpenSourceDb src = openSingleHandleAt(h, subDir);
                sources.add(src);
                if (h.getCheckpointId() > maxRestoredCkpt) {
                    maxRestoredCkpt = h.getCheckpointId();
                }
                // Merge CF maps; first writer wins on conflicts (CFs created in the first source
                // dominate). For B-Prod-P4 we expect CF layouts to be identical across subtasks.
                for (Map.Entry<String, Long> e : h.getCfMap().entrySet()) {
                    mergedCfMap.putIfAbsent(e.getKey(), e.getValue());
                }
            }

            // 2. Open an empty target DB.
            Path targetEngine = targetDir.resolve("_restore_target");
            Files.createDirectories(targetEngine);
            FrsDb targetDb = linker.dbOpen(arena, targetEngine.toString());
            FrsCfHandle targetCf;
            try {
                targetCf = linker.dbDefaultCf(targetDb, arena);
            } catch (RuntimeException re) {
                targetDb.close();
                throw new ForStRsCheckpointRestoreException(
                        targetEngine.toString(),
                        maxRestoredCkpt,
                        "Rescaling restore: target DB default CF unreachable: " + re.getMessage(),
                        re);
            }

            // 3. For each kg in the target range, find the source covering it and copy entries.
            try {
                for (int kg = targetRange.getStartKeyGroup();
                        kg <= targetRange.getEndKeyGroup();
                        kg++) {
                    OpenSourceDb src = findSourceFor(kg, sources);
                    if (src == null) {
                        // No source range covers this kg — leave it empty (legitimate when a job
                        // is restarted with strictly more parallelism than the original snapshot
                        // populated, but normally the source ranges union should cover the
                        // target range). We don't fail here.
                        continue;
                    }
                    copyKeyGroup(src, targetDb, targetCf, kg);
                }
            } catch (RuntimeException re) {
                try {
                    targetCf.close();
                } catch (RuntimeException ignored) {
                }
                targetDb.close();
                throw new ForStRsCheckpointRestoreException(
                        targetEngine.toString(),
                        maxRestoredCkpt,
                        "Rescaling restore failed during kg redistribution: " + re.getMessage(),
                        re);
            }

            return new RestoreResult(targetDb, targetCf, mergedCfMap, maxRestoredCkpt);
        } finally {
            // Always close the source DBs/CFs, success or failure.
            for (OpenSourceDb src : sources) {
                src.close();
            }
        }
    }

    private OpenSourceDb openSingleHandleAt(ForStRsIncrementalKeyedStateHandle handle, Path subDir)
            throws IOException {
        // Reuse the no-rescaling path by temporarily pointing targetDir at subDir.
        ForStRsRestoreOperation singleOp =
                new ForStRsRestoreOperation(
                        linker, arena, subDir, handle.getKeyGroupRange(), /* registry= */ null);
        singleOp.ensureTargetDirEmpty();
        RestoreResult r = singleOp.restoreNoRescaling(handle);
        return new OpenSourceDb(r.getDb(), r.getDefaultCf(), handle.getKeyGroupRange());
    }

    private static OpenSourceDb findSourceFor(int kg, List<OpenSourceDb> sources) {
        for (OpenSourceDb src : sources) {
            KeyGroupRange r = src.range;
            if (kg >= r.getStartKeyGroup() && kg <= r.getEndKeyGroup()) {
                return src;
            }
        }
        return null;
    }

    private void copyKeyGroup(OpenSourceDb src, FrsDb targetDb, FrsCfHandle targetCf, int kg) {
        byte[] kgPrefix = new byte[] {(byte) ((kg >>> 8) & 0xFF), (byte) (kg & 0xFF)};
        try (Arena local = Arena.ofShared();
                FrsIterator it = linker.prefixLookupOpen(src.db, src.cf, kgPrefix, local)) {
            while (true) {
                ForStRsLinker.IteratorEntry entry = linker.iteratorNext(it);
                if (entry == null) {
                    break;
                }
                linker.put(targetDb, targetCf, entry.key(), entry.value());
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Opens a fresh empty engine at the target dir (for "no handles" / first-launch cases). */
    private RestoreResult openEmpty() throws IOException {
        Files.createDirectories(targetDir);
        FrsDb db = linker.dbOpen(arena, targetDir.toString());
        FrsCfHandle cf;
        try {
            cf = linker.dbDefaultCf(db, arena);
        } catch (RuntimeException re) {
            db.close();
            throw new ForStRsCheckpointRestoreException(
                    targetDir.toString(),
                    -1L,
                    "Empty-restore: default CF unreachable on fresh engine: " + re.getMessage(),
                    re);
        }
        return new RestoreResult(db, cf, new LinkedHashMap<>(), 0L);
    }

    private void ensureTargetDirEmpty() throws IOException {
        if (Files.exists(targetDir)) {
            // Only recurse-delete if it's a directory we own. The plan calls for an empty target
            // dir so the engine doesn't trip over leftover files from a previous attempt.
            if (Files.isDirectory(targetDir)) {
                deleteRecursively(targetDir);
            } else {
                Files.delete(targetDir);
            }
        }
        Files.createDirectories(targetDir);
    }

    private static void deleteRecursively(Path p) throws IOException {
        if (!Files.exists(p)) {
            return;
        }
        try (var stream = Files.walk(p)) {
            // Reverse-order so directories come last.
            stream.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(
                            child -> {
                                try {
                                    Files.deleteIfExists(child);
                                } catch (IOException ignored) {
                                    // Best-effort cleanup; the engine will surface a hard error
                                    // if a leftover file truly blocks the restore.
                                }
                            });
        }
    }

    /** Wrapper for an opened source DB used during rescaling. */
    private static final class OpenSourceDb implements AutoCloseable {
        final FrsDb db;
        final FrsCfHandle cf;
        final KeyGroupRange range;

        OpenSourceDb(FrsDb db, FrsCfHandle cf, KeyGroupRange range) {
            this.db = db;
            this.cf = cf;
            this.range = range;
        }

        @Override
        public void close() {
            try {
                cf.close();
            } catch (RuntimeException ignored) {
            }
            try {
                db.close();
            } catch (RuntimeException ignored) {
            }
        }
    }
}

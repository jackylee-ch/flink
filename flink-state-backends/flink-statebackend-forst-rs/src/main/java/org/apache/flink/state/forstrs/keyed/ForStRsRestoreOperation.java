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
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.SstRetryStrategy;
import org.apache.flink.state.forstrs.state.StateSerializerMetadata;
import org.apache.flink.state.forstrs.state.StateSerializerRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ForSt-RS keyed-backend restore operation (B-Prod-P4 Tasks 4.1, 4.2, 4.3).
 *
 * <p>Implements three flows per spec §9:
 *
 * <ol>
 *   <li><b>No-rescaling fast path</b> ({@code handles.size() == 1 &amp;&amp; handle.kgRange ==
 *       target}) — download manifest + each SST listed in the {@link
 *       IncrementalRemoteKeyedStateHandle}, then call {@link ForStRsLinker#dbOpenFromIncremental}
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

    private static final Logger LOG = LoggerFactory.getLogger(ForStRsRestoreOperation.class);
    private static final int TIMER_QUEUE_PREFIX_COLLISION_KEY_GROUP = 0x712F;
    private static final int V2_STATE_PREFIX_COLLISION_KEY_GROUP = 0x6B2F;
    private static final byte[] V2_STATE_PREFIX = new byte[] {'k', '/'};

    private final ForStRsLinker linker;
    private final Arena arena;
    private final Path targetDir;
    private final KeyGroupRange targetRange;
    private final ForStRsSstRegistry sstRegistry;
    private final SstRetryStrategy retryStrategy;
    private final Set<Closeable> activeDownloadStreams = ConcurrentHashMap.newKeySet();

    public ForStRsRestoreOperation(
            ForStRsLinker linker,
            Arena arena,
            Path targetDir,
            KeyGroupRange targetRange,
            ForStRsSstRegistry sstRegistry) {
        this(linker, arena, targetDir, targetRange, sstRegistry, SstRetryStrategy.defaultStrategy());
    }

    /**
     * Test-friendly constructor that injects a {@link SstRetryStrategy}. Production callers should
     * use the no-strategy overload, which applies the default policy (5 retries, exponential
     * backoff 100ms → 30s, jittered). PR-A12.
     */
    public ForStRsRestoreOperation(
            ForStRsLinker linker,
            Arena arena,
            Path targetDir,
            KeyGroupRange targetRange,
            ForStRsSstRegistry sstRegistry,
            SstRetryStrategy retryStrategy) {
        this.linker = linker;
        this.arena = arena;
        this.targetDir = targetDir;
        this.targetRange = targetRange;
        this.sstRegistry = sstRegistry;
        this.retryStrategy = retryStrategy;
    }

    /** Result bundle returned by {@link #restore(Collection)}. */
    public static final class RestoreResult {
        private final FrsDb db;
        private final FrsCfHandle defaultCf;
        private final Map<String, Long> cfMap;
        private final long restoredCheckpointId;
        private final Map<String, StateSerializerMetadata> restoredSerializerMetadata;

        RestoreResult(
                FrsDb db,
                FrsCfHandle defaultCf,
                Map<String, Long> cfMap,
                long restoredCheckpointId,
                Map<String, StateSerializerMetadata> restoredSerializerMetadata) {
            this.db = db;
            this.defaultCf = defaultCf;
            this.cfMap = cfMap;
            this.restoredCheckpointId = restoredCheckpointId;
            this.restoredSerializerMetadata = restoredSerializerMetadata;
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

        /**
         * E5-HIGH-2: per-state serializer metadata parsed from the
         * {@link ForStRsSnapshotStrategy#SERIALIZER_REGISTRY_LOCAL_PATH} private-state entry, if
         * the restored handle carried one. Returns an empty map for pre-E5 snapshots that did not
         * include the registry blob. Never {@code null}.
         */
        public Map<String, StateSerializerMetadata> getRestoredSerializerMetadata() {
            return restoredSerializerMetadata;
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
        List<IncrementalRemoteKeyedStateHandle> incHandles = new ArrayList<>(handles.size());
        for (KeyedStateHandle h : handles) {
            if (!(h instanceof IncrementalRemoteKeyedStateHandle)) {
                // R28-L1: surface the EXPECTED handle type alongside the actual one so an
                // operator triaging a savepoint-restore failure sees the mismatch at a
                // glance and can decide whether to (a) re-checkpoint from the source
                // backend or (b) wire a state-handle conversion layer. Pre-fix the message
                // only reported the actual class name — the operator had to grep the
                // source tree to learn what the ForStRs backend wanted.
                throw new ForStRsCheckpointRestoreException(
                        null,
                        -1L,
                        "Unsupported keyed-state handle type for ForStRs restore: "
                                + (h == null ? "null" : h.getClass().getName())
                                + " (expected "
                                + IncrementalRemoteKeyedStateHandle.class.getName()
                                + "; cross-backend handle types are not yet supported —"
                                + " re-checkpoint from a ForStRs-backed job or convert the"
                                + " handle before restore)");
            }
            incHandles.add((IncrementalRemoteKeyedStateHandle) h);
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

    private RestoreResult restoreNoRescaling(IncrementalRemoteKeyedStateHandle handle)
            throws IOException {
        Path downloadDir = targetDir.resolve("_restore_dl");
        Files.createDirectories(downloadDir);

        // 1. Download the manifest (private/meta state) into a known local path.
        Path manifestPath = downloadDir.resolve("CHECKPOINT.blob");
        downloadHandleStrict(
                handle.getMetaDataStateHandle(), manifestPath, "CHECKPOINT.blob", handle);

        // 2. Download every SST (shared + private). Strict check: each must materialize.
        //    PR-E1 (F5-6 / E-HIGH-2): downloads run in parallel across handles via a bounded
        //    thread pool — each handle's blob is an independent S3/blob-store fetch so
        //    parallelism is wall-clock dominated by the slowest handle, not the sum. Order
        //    of the local-path list is preserved (matches the source-handle iteration order)
        //    because the engine's manifest indexes SSTs by local path and the LSM-reconstruction
        //    step downstream consumes the list as a set, but we keep stable order to make
        //    debugging deterministic.
        // E5-HIGH-2: separate the well-known serializer-registry private-state entry from the
        // engine SST list so it is not handed to {@code dbOpenFromIncremental} (which would treat
        // it as an SST and fail). The entry is downloaded inline (small blob — typically tens of
        // bytes per state), parsed via {@link StateSerializerRegistry#deserialize}, and surfaced
        // through {@link RestoreResult#getRestoredSerializerMetadata}. Pre-E5 snapshots produce
        // an empty privateState list, so {@code restoredSerializerMetadata} stays empty and the
        // restore-side {@code seedFromRestore(empty)} is a no-op.
        HandleAndLocalPath registryEntry = null;
        // FRS-CKPT-NOFLUSH: memtable artifacts ("memtable-cf<id>.arrow") are
        // private state but are NOT SSTs — they must be kept out of the engine
        // SST list (dbOpenFromIncremental would mis-handle them) and instead
        // replayed into the engine after open.
        List<HandleAndLocalPath> memtableArtifacts = new ArrayList<>();
        List<HandleAndLocalPath> sstPrivateState =
                new ArrayList<>(handle.getPrivateState().size());
        for (HandleAndLocalPath hlp : handle.getPrivateState()) {
            String lp = hlp.getLocalPath();
            if (ForStRsSnapshotStrategy.SERIALIZER_REGISTRY_LOCAL_PATH.equals(lp)) {
                registryEntry = hlp;
            } else if (lp.startsWith("memtable-cf") && lp.endsWith(".arrow")) {
                memtableArtifacts.add(hlp);
            } else {
                sstPrivateState.add(hlp);
            }
        }

        List<HandleAndLocalPath> allHlps =
                new ArrayList<>(handle.getSharedState().size() + sstPrivateState.size());
        allHlps.addAll(handle.getSharedState());
        allHlps.addAll(sstPrivateState);
        List<String> sstLocalPaths = parallelDownloadSsts(allHlps, downloadDir, handle);

        // E5-HIGH-2: download + parse the registry blob (if present) before engine open so a
        // corrupt blob fails the restore loudly rather than after the engine is up.
        Map<String, StateSerializerMetadata> restoredSerializerMetadata =
                registryEntry == null
                        ? Collections.emptyMap()
                        : downloadAndParseRegistryBlob(registryEntry, handle);

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

        // FRS-CKPT-NOFLUSH: download the memtable artifacts locally and replay
        // them into the just-opened engine (preserving sequence + op_type),
        // rebuilding the in-RAM state that was checkpointed WITHOUT flushing. A
        // pre-feature / flush-based checkpoint carries no such artifacts → the
        // loop is a no-op and the replay scan finds nothing.
        if (!memtableArtifacts.isEmpty()) {
            try {
                for (HandleAndLocalPath hlp : memtableArtifacts) {
                    downloadHandleStrict(
                            hlp.getHandle(),
                            downloadDir.resolve(hlp.getLocalPath()),
                            hlp.getLocalPath(),
                            handle);
                }
                linker.replayMemtableArtifacts(db, downloadDir.toString());
            } catch (RuntimeException re) {
                db.close();
                throw new ForStRsCheckpointRestoreException(
                        targetDir.toString(),
                        handle.getCheckpointId(),
                        "ForSt-RS memtable-artifact replay failed: " + re.getMessage(),
                        re);
            }
        }

        // 4. Re-populate the SST registry from the restored handles so the next incremental
        //    checkpoint can reuse them as shared state without re-uploading.
        if (sstRegistry != null) {
            for (HandleAndLocalPath hlp : handle.getSharedState()) {
                sstRegistry.register(new StateHandleID(hlp.getLocalPath()), hlp.getHandle());
            }
        }

        // cfMap is intentionally empty: the engine recovers all column-family info FROM THE
        // MANIFEST (CHECKPOINT.blob, the metaStateHandle) inside dbOpenFromIncremental above. The
        // standard Flink IncrementalRemoteKeyedStateHandle does not carry a CF map on the wire (it
        // was an artifact of the now-removed custom ForStRsIncrementalKeyedStateHandle), and no
        // production consumer of RestoreResult.getCfMap() exists.
        return new RestoreResult(
                db,
                defaultCf,
                new LinkedHashMap<>(),
                handle.getCheckpointId(),
                restoredSerializerMetadata);
    }

    /**
     * E5-HIGH-2: download the {@link ForStRsSnapshotStrategy#SERIALIZER_REGISTRY_LOCAL_PATH} blob
     * into memory and parse it via {@link StateSerializerRegistry#deserialize}. The blob is small
     * (typically tens of bytes per registered state) so we keep it fully in-memory rather than
     * spilling to disk like SST handles. Failures are surfaced as
     * {@link ForStRsCheckpointRestoreException} carrying the offending checkpoint id so the
     * runtime routes them through {@code CheckpointFailureManager} consistently with other
     * restore failure paths.
     */
    private Map<String, StateSerializerMetadata> downloadAndParseRegistryBlob(
            HandleAndLocalPath entry, IncrementalRemoteKeyedStateHandle owner)
            throws ForStRsCheckpointRestoreException {
        StreamStateHandle h = entry.getHandle();
        if (h == null) {
            throw new ForStRsCheckpointRestoreException(
                    entry.getLocalPath(),
                    owner.getCheckpointId(),
                    "Strict restore: serializer-registry handle for '"
                            + entry.getLocalPath()
                            + "' is null");
        }
        byte[] blob;
        try (FSDataInputStream in = h.openInputStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[4 * 1024];
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
            blob = out.toByteArray();
        } catch (IOException ioe) {
            throw new ForStRsCheckpointRestoreException(
                    entry.getLocalPath(),
                    owner.getCheckpointId(),
                    "Strict restore: failed reading serializer-registry blob '"
                            + entry.getLocalPath()
                            + "': "
                            + ioe.getMessage(),
                    ioe);
        }
        if (blob.length == 0) {
            throw new ForStRsCheckpointRestoreException(
                    entry.getLocalPath(),
                    owner.getCheckpointId(),
                    "Strict restore: serializer-registry blob '"
                            + entry.getLocalPath()
                            + "' resolved to 0 bytes");
        }
        try {
            return StateSerializerRegistry.deserialize(blob);
        } catch (IOException ioe) {
            throw new ForStRsCheckpointRestoreException(
                    entry.getLocalPath(),
                    owner.getCheckpointId(),
                    "Strict restore: serializer-registry blob '"
                            + entry.getLocalPath()
                            + "' is malformed: "
                            + ioe.getMessage(),
                    ioe);
        }
    }

    /**
     * Reads a {@link StreamStateHandle} into {@code localTarget}. On any failure (handle returns no
     * data, I/O error, or the resulting file is empty) throws a strict-restore exception with
     * {@code logicalPath} + the source handle's checkpoint id.
     *
     * <p>The download is wrapped in {@link SstRetryStrategy} (PR-A12): transient I/O faults are
     * retried with exponential backoff so a single S3 5xx does not fail the restore. The "zero
     * bytes" guard is treated as a permanent error and is NOT retried — re-downloading a
     * known-empty handle just slows the failure by O(retries × backoff).
     */
    private void downloadHandleStrict(
            StreamStateHandle handle,
            Path localTarget,
            String logicalPath,
            IncrementalRemoteKeyedStateHandle owner)
            throws ForStRsCheckpointRestoreException {
        if (handle == null) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: handle for '" + logicalPath + "' is null");
        }
        try {
            Files.createDirectories(localTarget.getParent());
            retryStrategy.execute(
                    "download " + logicalPath,
                    () -> {
                        // Open fresh source + destination streams per attempt. The local target
                        // file is truncated (CREATE + TRUNCATE_EXISTING is the default for
                        // Files.newOutputStream) so a half-written attempt is discarded cleanly.
                        FSDataInputStream in = null;
                        OutputStream out = null;
                        try {
                            in = handle.openInputStream();
                            out = Files.newOutputStream(localTarget);
                            activeDownloadStreams.add(in);
                            activeDownloadStreams.add(out);
                            byte[] buf = new byte[8 * 1024];
                            int n;
                            long total = 0L;
                            while ((n = in.read(buf)) > 0) {
                                if (Thread.currentThread().isInterrupted()) {
                                    throw new InterruptedIOException(
                                            "Interrupted while downloading " + logicalPath);
                                }
                                out.write(buf, 0, n);
                                total += n;
                            }
                            // Manifest blobs and SST files are never legitimately empty; an
                            // empty file means the handle resolved to a missing/truncated
                            // upload — fail strictly. Wrap in a non-IOException so the retry
                            // strategy doesn't loop on it. We re-throw as a checked exception
                            // inside the IoOperation by raising a custom IOException
                            // subclass that the outer catch in this method re-wraps without
                            // adding another retry layer.
                            if (total == 0L) {
                                throw new EmptyHandleException(logicalPath);
                            }
                            return null;
                        } finally {
                            if (in != null) {
                                activeDownloadStreams.remove(in);
                            }
                            if (out != null) {
                                activeDownloadStreams.remove(out);
                            }
                            IOException failure = null;
                            if (out != null) {
                                try {
                                    out.close();
                                } catch (IOException e) {
                                    failure = e;
                                }
                            }
                            if (in != null) {
                                try {
                                    in.close();
                                } catch (IOException e) {
                                    if (failure == null) {
                                        failure = e;
                                    } else {
                                        failure.addSuppressed(e);
                                    }
                                }
                            }
                            if (failure != null) {
                                throw failure;
                            }
                        }
                    });
        } catch (EmptyHandleException ehe) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: handle for '"
                            + logicalPath
                            + "' produced 0 bytes (likely deleted upstream)");
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

    private void closeActiveDownloadStreams() {
        for (Closeable c : activeDownloadStreams) {
            try {
                c.close();
            } catch (IOException ignored) {
                // Best-effort unblock for restore cancellation/failure.
            } finally {
                activeDownloadStreams.remove(c);
            }
        }
    }

    static Path resolveSafeRestoreLocalPath(
            Path downloadDir, String logicalPath, IncrementalRemoteKeyedStateHandle owner)
            throws ForStRsCheckpointRestoreException {
        if (logicalPath == null || logicalPath.isBlank()) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: SST local path must not be null or blank");
        }
        if (logicalPath.indexOf('/') >= 0 || logicalPath.indexOf('\\') >= 0) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: SST local path must be a single file name: " + logicalPath);
        }
        Path relative;
        try {
            relative = Path.of(logicalPath);
        } catch (RuntimeException re) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: malformed SST local path '" + logicalPath + "'",
                    re);
        }
        if (relative.isAbsolute()
                || relative.getFileName() == null
                || ".".equals(relative.toString())
                || "..".equals(relative.toString())) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: SST local path must be a relative file name: "
                            + logicalPath);
        }
        Path root = downloadDir.toAbsolutePath().normalize();
        Path resolved = root.resolve(relative).normalize();
        if (!resolved.startsWith(root)
                || resolved.getParent() == null
                || !resolved.getParent().equals(root)) {
            throw new ForStRsCheckpointRestoreException(
                    logicalPath,
                    owner.getCheckpointId(),
                    "Strict restore: SST local path escapes restore directory: " + logicalPath);
        }
        return resolved;
    }

    private static void rejectTimerPrefixCollisionOnRescale(
            List<IncrementalRemoteKeyedStateHandle> handles)
            throws ForStRsCheckpointRestoreException {
        for (IncrementalRemoteKeyedStateHandle handle : handles) {
            if (handle.getKeyGroupRange().contains(TIMER_QUEUE_PREFIX_COLLISION_KEY_GROUP)) {
                throw new ForStRsCheckpointRestoreException(
                        "key-group-" + TIMER_QUEUE_PREFIX_COLLISION_KEY_GROUP,
                        handle.getCheckpointId(),
                        "Rescaling restore is unsafe for ForSt-RS checkpoints whose source "
                                + "key-group range contains "
                                + TIMER_QUEUE_PREFIX_COLLISION_KEY_GROUP
                                + ": regular-state rows with prefix bytes 0x71/0x2f collide "
                                + "with the legacy timer queue marker \"q/\". Restore must use "
                                + "the same key-group assignment or a versioned timer key format.");
            }
        }
    }

    private static void rejectV2PrefixCollisionOnRescale(
            List<IncrementalRemoteKeyedStateHandle> handles)
            throws ForStRsCheckpointRestoreException {
        for (IncrementalRemoteKeyedStateHandle handle : handles) {
            if (handle.getKeyGroupRange().contains(V2_STATE_PREFIX_COLLISION_KEY_GROUP)) {
                throw new ForStRsCheckpointRestoreException(
                        "key-group-" + V2_STATE_PREFIX_COLLISION_KEY_GROUP,
                        handle.getCheckpointId(),
                        "Rescaling restore is unsafe for ForSt-RS checkpoints whose source "
                                + "key-group range contains "
                                + V2_STATE_PREFIX_COLLISION_KEY_GROUP
                                + ": regular-state rows with prefix bytes 0x6b/0x2f collide "
                                + "with the Async v2 state prefix \"k/\". Restore must use the "
                                + "same key-group assignment or a versioned v2 key format.");
            }
        }
    }

    /**
     * Sentinel IOException that signals "handle returned 0 bytes" — distinct from a transient I/O
     * error. Extends {@link java.io.FileNotFoundException} so the {@link
     * SstRetryStrategy#DEFAULT_TRANSIENT_PREDICATE} classifies it as <i>non-transient</i>: the
     * retry loop short-circuits on the first occurrence rather than re-downloading a
     * known-truncated handle 5 more times.
     */
    private static final class EmptyHandleException extends java.io.FileNotFoundException {
        private static final long serialVersionUID = 1L;

        EmptyHandleException(String logicalPath) {
            super("0-byte download for " + logicalPath);
        }
    }

    // ------------------------------------------------------------------
    // 4.3: rescaling path (kg redistribution)
    // ------------------------------------------------------------------

    private RestoreResult restoreWithRescaling(List<IncrementalRemoteKeyedStateHandle> handles)
            throws IOException {
        rejectTimerPrefixCollisionOnRescale(handles);
        rejectV2PrefixCollisionOnRescale(handles);

        // 1. Materialize each input handle into its own temp DB (using the no-rescaling path).
        //    PR-E1 (F5-6 / E-HIGH-2): handles materialize in parallel via a bounded thread
        //    pool — each handle's S3/blob download + dbOpenFromIncremental is fully
        //    independent. We preserve insertion order of the `sources` list so the
        //    `findSourceFor(kg)` lookup remains deterministic.
        long maxRestoredCkpt = 0L;
        Map<String, Long> mergedCfMap = new LinkedHashMap<>();
        OpenSourceDb[] sourcesArr = new OpenSourceDb[handles.size()];
        ExecutorService restoreExec =
                newRestoreExecutor(
                        Math.min(handles.size(), Runtime.getRuntime().availableProcessors()),
                        "forstrs-restore-rescaling");
        // A7-M3: track successful materialization so a partial failure during the futures
        // join can close already-opened native sources before propagating. The outer {@code
        // finally { restoreExec.shutdown(); }} on its own does NOT close OpenSourceDb
        // instances, and the post-try resource-management block (which builds {@code
        // sources} and closes them on its own finally) is unreachable when we throw.
        boolean joinSucceeded = false;
        // A8-H6 / D8-H3: hold the futures list at method scope so the finally block can
        // iterate any slots that completed AFTER we decided to fail (shutdownNow interrupts
        // in-flight workers, but a Callable that already returned an OpenSourceDb before the
        // interrupt landed will still hold a native FrsDb / FrsCfHandle. Without inspecting
        // those slots post-termination, those handles leak.
        List<Future<OpenSourceDb>> futuresOuter = new ArrayList<>(handles.size());
        try {
            List<Future<OpenSourceDb>> futures = futuresOuter;
            for (int i = 0; i < handles.size(); i++) {
                final IncrementalRemoteKeyedStateHandle h = handles.get(i);
                final Path subDir = targetDir.resolve("_restore_src_" + i);
                futures.add(
                        restoreExec.submit(
                                (Callable<OpenSourceDb>)
                                        () -> {
                                            Files.createDirectories(subDir);
                                            return openSingleHandleAt(h, subDir);
                                        }));
                if (h.getCheckpointId() > maxRestoredCkpt) {
                    maxRestoredCkpt = h.getCheckpointId();
                }
                // No CF-map merge: Flink's built-in IncrementalRemoteKeyedStateHandle does not
                // carry a CF map, and the engine recovers all column-family info per source from
                // each source's manifest inside openSingleHandleAt -> dbOpenFromIncremental.
                // mergedCfMap stays empty (non-load-bearing, surfaced only via RestoreResult).
            }
            for (int i = 0; i < futures.size(); i++) {
                try {
                    sourcesArr[i] = futures.get(i).get();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ForStRsCheckpointRestoreException(
                            null,
                            handles.get(i).getCheckpointId(),
                            "Interrupted while materializing rescaling source " + i,
                            ie);
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (cause instanceof ForStRsCheckpointRestoreException) {
                        throw (ForStRsCheckpointRestoreException) cause;
                    }
                    if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new ForStRsCheckpointRestoreException(
                            null,
                            handles.get(i).getCheckpointId(),
                            "Materializing rescaling source " + i + " failed: " + cause,
                            cause);
                }
            }
            joinSucceeded = true;
        } finally {
            if (!joinSucceeded) {
                // A8-H6 / D8-H3: A7-M3 left {@code shutdown()} here, which only stops accepting
                // new tasks — in-flight workers past the failure index continue running and may
                // open additional {@link OpenSourceDb} instances. Since the join loop has
                // already iterated past those slots, the native handles would leak. Force
                // interrupt with {@code shutdownNow()} + {@code awaitTermination(5s)} (mirrors
                // E5-HIGH-2 in the parallel SST download path) and ONLY THEN sweep the futures
                // array to close any sources that managed to complete despite the interrupt.
                restoreExec.shutdownNow();
                try {
                    restoreExec.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                // After termination, any future that finished (isDone && !isCancelled) holds
                // an OpenSourceDb the join loop never saw. Close them.
                for (int i = 0; i < futuresOuter.size(); i++) {
                    if (sourcesArr[i] != null) {
                        // Already discovered by the join loop — close below in the unified sweep.
                        continue;
                    }
                    Future<OpenSourceDb> f = futuresOuter.get(i);
                    if (f.isDone() && !f.isCancelled()) {
                        try {
                            sourcesArr[i] = f.get();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } catch (ExecutionException ignored) {
                            // Worker failed — nothing to close for this slot.
                        }
                    }
                }
                // Close eagerly so native FrsDb / FrsCfHandle leak is contained. The
                // post-try resource-management block (which builds {@code sources} and closes
                // them on its own finally) is unreachable when we throw.
                for (OpenSourceDb s : sourcesArr) {
                    if (s != null) {
                        try {
                            s.close();
                        } catch (RuntimeException ignored) {
                            // close() is already best-effort; swallow secondary close errors
                            // so the original cause propagates unobstructed.
                        }
                    }
                }
            } else {
                // Success path: drain the executor gracefully.
                restoreExec.shutdown();
            }
        }
        List<OpenSourceDb> sources = new ArrayList<>(handles.size());
        for (OpenSourceDb s : sourcesArr) {
            if (s != null) {
                sources.add(s);
            }
        }

        // R31-M1 + R32-L3: pre-flight overlap detection. {@link #findSourceFor} returns the
        // FIRST source whose KeyGroupRange covers a given kg, so two sources that both claim
        // the same kg would silently lose half the data on restore (the second source's
        // contents for the shared kgs would never be copied because the first source already
        // matched). The earlier check at checkpoint time SHOULD have produced disjoint ranges,
        // but operator bugs or hand-edited metadata can produce overlaps; failing loudly here
        // prevents corrupt restores.
        //
        // R32-L3: replace the O(N^2) all-pairs comparison with a sort-by-startKeyGroup + linear
        // sweep (O(N log N)). After sorting by start, we track {@code maxEndSoFar} — the maximum
        // endKeyGroup seen across all already-visited ranges. For the current range b, if
        // {@code b.start <= maxEndSoFar}, then b overlaps the range that contributed
        // {@code maxEndSoFar} (which we remember via {@code maxEndOwnerIdx}). This correctly
        // catches the "nested" case (e.g. [0,100], [10,20], [50,60]) that a simple adjacent-pair
        // check would miss. We sort INDICES, not the sources list itself, so error messages
        // preserve the original ordering callers expect.
        Integer[] sortedIdx = new Integer[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            sortedIdx[i] = i;
        }
        java.util.Arrays.sort(
                sortedIdx,
                (x, y) ->
                        Integer.compare(
                                sources.get(x).range.getStartKeyGroup(),
                                sources.get(y).range.getStartKeyGroup()));
        int maxEndSoFar = Integer.MIN_VALUE;
        int maxEndOwnerIdx = -1;
        for (int k = 0; k < sortedIdx.length; k++) {
            int curr = sortedIdx[k];
            KeyGroupRange b = sources.get(curr).range;
            if (k > 0 && b.getStartKeyGroup() <= maxEndSoFar) {
                KeyGroupRange a = sources.get(maxEndOwnerIdx).range;
                String msg =
                        "Overlapping source ranges: indices "
                                + maxEndOwnerIdx
                                + " ("
                                + a
                                + ") and "
                                + curr
                                + " ("
                                + b
                                + ") both claim kg "
                                + b.getStartKeyGroup();
                // Close any opened sources before throwing — they own native handles.
                for (OpenSourceDb s : sources) {
                    try {
                        s.close();
                    } catch (Throwable ignored) {
                        // best-effort during cleanup
                    }
                }
                throw new ForStRsCheckpointRestoreException(
                        targetDir.toString(), maxRestoredCkpt, msg);
            }
            if (b.getEndKeyGroup() > maxEndSoFar) {
                maxEndSoFar = b.getEndKeyGroup();
                maxEndOwnerIdx = curr;
            }
        }

        try {

            rejectAsyncV2StateRowsOnRescale(sources, maxRestoredCkpt);

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
            // E-H9: also group target kgs by source so we can do a SINGLE
            // timer-row scan per source (timer rows live under a state-name-
            // varying QUEUE_NS_MARKER prefix, so per-kg scans would replay
            // every timer row once per target kg — quadratic in numKgs ×
            // numTimerRows on large jobs).
            java.util.Map<OpenSourceDb, java.util.BitSet> targetKgsBySrc =
                    new java.util.IdentityHashMap<>();
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
                    copyKeyGroupRegularState(src, targetDb, targetCf, kg);
                    targetKgsBySrc
                            .computeIfAbsent(
                                    src,
                                    k -> new java.util.BitSet(targetRange.getEndKeyGroup() + 1))
                            .set(kg);
                }
                // E-H9: ONE timer-row scan per source, route each row to
                // its embedded kg if that kg is in the target set for
                // this source.
                for (java.util.Map.Entry<OpenSourceDb, java.util.BitSet> e :
                        targetKgsBySrc.entrySet()) {
                    copyTimerRowsForSourceBatch(e.getKey(), targetDb, targetCf, e.getValue());
                }
            } catch (ForStRsCheckpointRestoreException cre) {
                try {
                    targetCf.close();
                } catch (RuntimeException ignored) {
                }
                targetDb.close();
                throw cre;
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

            // E6-HIGH-4(b) / E7-H2: union-merge the per-source serializer metadata so the target
            // backend's {@link StateSerializerRegistry} sees the schema snapshot for every state
            // present in any source, regardless of which subtask uploaded it. De-duped by state
            // name — Flink's contract is that schema for a given state name is uniform across the
            // keyed-stream operator instance, so any collision must carry byte-identical {@link
            // StateSerializerMetadata}. Pre-E5 source handles contribute empty maps and are
            // absorbed without changing the result.
            //
            // E7-H2: the previous implementation used {@code putAll} (last-writer-wins). Because
            // source-handle iteration order is not guaranteed deterministic across restore
            // retries, last-writer-wins meant the "winning" metadata for any genuine collision
            // could vary between restore attempts — schema-drift detection might fire-or-not-fire
            // non-deterministically. The fix ASSERTS intra-collision equality via {@link
            // StateSerializerMetadata#equals}: a mismatch fails loudly with an {@link
            // IOException} naming the offending state, regardless of iteration order. When all
            // collisions match the value is well-defined and deterministic by construction —
            // restore retries see the same metadata for every state name. Insertion-ordered
            // {@link LinkedHashMap} keeps the entry order stable for review and tests.
            try {
                Map<String, StateSerializerMetadata> unionedMetadata = new LinkedHashMap<>();
                for (OpenSourceDb src : sources) {
                    if (src.serializerMetadata == null || src.serializerMetadata.isEmpty()) {
                        continue;
                    }
                    for (Map.Entry<String, StateSerializerMetadata> e :
                            src.serializerMetadata.entrySet()) {
                        String name = e.getKey();
                        StateSerializerMetadata incoming = e.getValue();
                        StateSerializerMetadata existing = unionedMetadata.get(name);
                        if (existing == null) {
                            unionedMetadata.put(name, incoming);
                        } else if (!existing.equals(incoming)) {
                            throw new IOException(
                                    "schema mismatch across restore handles for state "
                                            + name
                                            + ": found incompatible metadata"
                                            + " (rescaling union-merge requires every source-handle to"
                                            + " contribute byte-identical StateSerializerMetadata for"
                                            + " the same state name)");
                        }
                        // else: equal — no-op, deterministic regardless of iteration order.
                    }
                }
                return new RestoreResult(
                        targetDb,
                        targetCf,
                        mergedCfMap,
                        maxRestoredCkpt,
                        unionedMetadata.isEmpty() ? Collections.emptyMap() : unionedMetadata);
            } catch (IOException | RuntimeException e) {
                try {
                    targetCf.close();
                } catch (RuntimeException ignored) {
                }
                targetDb.close();
                throw e;
            }
        } finally {
            // Always close the source DBs/CFs, success or failure.
            for (OpenSourceDb src : sources) {
                src.close();
            }
        }
    }

    private OpenSourceDb openSingleHandleAt(IncrementalRemoteKeyedStateHandle handle, Path subDir)
            throws IOException {
        // Reuse the no-rescaling path by temporarily pointing targetDir at subDir.
        ForStRsRestoreOperation singleOp =
                new ForStRsRestoreOperation(
                        linker, arena, subDir, handle.getKeyGroupRange(), /* registry= */ null);
        singleOp.ensureTargetDirEmpty();
        RestoreResult r = singleOp.restoreNoRescaling(handle);
        // E6-HIGH-4(b): forward the per-source serializer metadata so the rescaling caller can
        // union-merge across all sources before seeding the backend's registry.
        return new OpenSourceDb(
                r.getDb(),
                r.getDefaultCf(),
                handle.getKeyGroupRange(),
                r.getRestoredSerializerMetadata());
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

    /**
     * Maximum number of entries staged per FFI batch in {@link #copyKeyGroup}. Chosen to balance
     * per-call FFM overhead amortization against transient off-heap memory pressure. With an
     * average key + value size of ~64 B each, 4096 entries ≈ 512 KiB of staging memory.
     */
    static final int COPY_KG_BATCH_SIZE = 4096;

    /**
     * Initial off-heap data-region capacity per batch. Auto-grows via {@link Arena} allocation
     * when an individual entry exceeds the remaining capacity; sized so the common Q-test
     * workload (≤ 64 B keys + ≤ 64 B values × 4096 = 512 KiB) lands within the first allocation.
     */
    private static final long COPY_KG_DATA_INITIAL_CAP = 512L * 1024L;

    /**
     * Copies all entries that fall into key-group {@code kg} from the source DB into the target DB
     * using batched, vectorized FFI calls — replaces the legacy per-record {@code linker.put(...)}
     * loop. PR-E1 (F5-6 / E-HIGH-2): for large rescale operations the per-record FFM crossing was
     * the wall-clock dominator (4.6 µs × N puts); the batched path drops to ~250 ns per entry
     * because the keys/values for a whole batch cross the FFI boundary in one invocation.
     *
     * <p>Layout (matches {@link ForStRsLinker#vectorizedBatchPut}):
     *
     * <ul>
     *   <li>{@code keyOffsets}: {@code (n+1) × int4}; offsets into {@code keyData} where
     *       entry {@code i}'s key lives at {@code [keyOffsets[i], keyOffsets[i+1])}.
     *   <li>{@code valOffsets}: same shape against {@code valData}.
     * </ul>
     *
     * <p>The implementation re-uses one off-heap staging arena per key-group; the arena is
     * released when {@code copyKeyGroup} returns, so transient memory pressure stays bounded
     * by the largest single key-group being copied.
     */
    /**
     * E-H9: regular-state-only per-kg copy. Timer rows are no longer
     * handled here — the caller batches all timer rows for a source DB
     * via {@link #copyTimerRowsForSourceBatch} after the per-kg loop
     * so each source's QUEUE_NS_MARKER prefix is scanned ONCE rather
     * than once per target kg.
     */
    private void copyKeyGroupRegularState(
            OpenSourceDb src, FrsDb targetDb, FrsCfHandle targetCf, int kg) {
        byte[] kgPrefix = new byte[] {(byte) ((kg >>> 8) & 0xFF), (byte) (kg & 0xFF)};
        try (Arena stagingArena = Arena.ofShared();
                FrsIterator it = linker.prefixLookupOpen(src.db, src.cf, kgPrefix, stagingArena)) {
            BatchPutStaging batch =
                    new BatchPutStaging(stagingArena, COPY_KG_BATCH_SIZE, COPY_KG_DATA_INITIAL_CAP);
            while (true) {
                ForStRsLinker.IteratorEntry entry = linker.iteratorNext(it);
                if (entry == null) {
                    break;
                }
                batch.append(entry.key(), entry.value());
                if (batch.size() >= COPY_KG_BATCH_SIZE) {
                    flushCopyKeyGroupBatch(batch, targetDb, targetCf);
                }
            }
            flushCopyKeyGroupBatch(batch, targetDb, targetCf);
        }
    }

    private void rejectAsyncV2StateRowsOnRescale(List<OpenSourceDb> sources, long checkpointId)
            throws ForStRsCheckpointRestoreException {
        for (OpenSourceDb src : sources) {
            try (Arena scanArena = Arena.ofShared();
                    FrsIterator it =
                            linker.prefixLookupOpen(src.db, src.cf, V2_STATE_PREFIX, scanArena)) {
                ForStRsLinker.IteratorEntry entry = linker.iteratorNext(it);
                if (entry != null) {
                    throw new ForStRsCheckpointRestoreException(
                            "prefix-k/",
                            checkpointId,
                            "Rescaling restore is unsafe for Async v2 ForSt-RS state rows: "
                                    + "keys encoded with prefix \"k/\" do not carry a leading "
                                    + "key-group prefix, so copyKeyGroupRegularState cannot "
                                    + "redistribute them accurately. Restore without rescaling "
                                    + "or migrate to a key-group-prefixed v2 key format.");
                }
            }
        }
    }

    /**
     * E-H1 + E-H9: scans the source DB's QUEUE_NS_MARKER prefix ONCE and
     * routes each row to the target if its embedded kg is in
     * {@code targetKgsForSrc}. Pre-E-H9 the same scan ran once per
     * target kg, making restore O(numTargetKgs × numTimerRowsInSource)
     * — quadratic on jobs with many timers.
     */
    private void copyTimerRowsForSourceBatch(
            OpenSourceDb src,
            FrsDb targetDb,
            FrsCfHandle targetCf,
            java.util.BitSet targetKgsForSrc)
            throws ForStRsCheckpointRestoreException {
        byte[] marker =
                org.apache.flink.state.forstrs.timer.ForStRsKeyGroupedInternalPriorityQueue
                        .QUEUE_NS_MARKER;
        try (Arena stagingArena = Arena.ofShared();
                FrsIterator it =
                        linker.prefixLookupOpen(src.db, src.cf, marker, stagingArena)) {
            BatchPutStaging batch =
                    new BatchPutStaging(
                            stagingArena, COPY_KG_BATCH_SIZE, COPY_KG_DATA_INITIAL_CAP);
            while (true) {
                ForStRsLinker.IteratorEntry entry = linker.iteratorNext(it);
                if (entry == null) {
                    break;
                }
                byte[] key = entry.key();
                int sepIdx = -1;
                for (int i = marker.length; i < key.length; i++) {
                    if (key[i] == (byte) '/') {
                        sepIdx = i;
                        break;
                    }
                }
                int rowKg = extractTimerRowKeyGroupOrThrow(key, marker, sepIdx);
                if (!src.range.contains(rowKg)) {
                    throw new ForStRsCheckpointRestoreException(
                            "timer-row-key-group",
                            -1L,
                            "Timer row declares key-group "
                                    + rowKg
                                    + " but the source handle range is "
                                    + src.range
                                    + ". Refusing rescale restore because the checkpoint metadata "
                                    + "and timer key encoding disagree.");
                }
                if (rowKg < 0
                        || rowKg >= targetKgsForSrc.length()
                        || !targetKgsForSrc.get(rowKg)) {
                    continue;
                }
                batch.append(key, entry.value());
                if (batch.size() >= COPY_KG_BATCH_SIZE) {
                    flushCopyKeyGroupBatch(batch, targetDb, targetCf);
                }
            }
            flushCopyKeyGroupBatch(batch, targetDb, targetCf);
        }
    }

    static int extractTimerRowKeyGroupOrThrow(byte[] key, byte[] marker, int sepIdx) {
        if (sepIdx <= marker.length || sepIdx + 1 + 2 + Long.BYTES > key.length) {
            throw new IllegalArgumentException(
                    "Malformed ForSt-RS timer key under q/ prefix: missing state-name separator,"
                            + " key-group, or timestamp bytes; length="
                            + key.length
                            + ", sepIdx="
                            + sepIdx);
        }
        for (int i = 0; i < marker.length; i++) {
            if (key[i] != marker[i]) {
                throw new IllegalArgumentException("Malformed ForSt-RS timer key prefix");
            }
        }
        int rowHi = key[sepIdx + 1] & 0xFF;
        int rowLo = key[sepIdx + 2] & 0xFF;
        return (rowHi << 8) | rowLo;
    }

    /**
     * PR-E1 test seam: flushes the accumulated {@link BatchPutStaging} via the vectorized batch
     * FFI. Production impl forwards to {@link ForStRsLinker#vectorizedBatchPut}; tests may
     * override to count calls and assert zero per-record {@code linker.put} crossings.
     */
    protected void flushCopyKeyGroupBatch(
            BatchPutStaging batch, FrsDb targetDb, FrsCfHandle targetCf) {
        batch.flush(linker, targetDb, targetCf);
    }

    /**
     * Off-heap staging buffer that accumulates (key, value) pairs into the offsets+data layout
     * accepted by {@link ForStRsLinker#vectorizedBatchPut}, then drains them in one FFI call.
     *
     * <p>NOT thread-safe — used single-threaded inside {@link #copyKeyGroup}. The {@link Arena}
     * is supplied by the caller (shared with the iterator); offsets/data segments are allocated
     * once at construction; data segments grow when an entry doesn't fit (rare in practice).
     */
    static final class BatchPutStaging {
        private final Arena arena;
        private final int capacity;
        private MemorySegment keyOffsets; // (capacity + 1) × int4
        private MemorySegment valOffsets; // (capacity + 1) × int4
        private MemorySegment keyData;
        private MemorySegment valData;
        private long keyDataCap;
        private long valDataCap;
        private long keyDataUsed;
        private long valDataUsed;
        private int n;

        BatchPutStaging(Arena arena, int capacity, long initialDataCap) {
            this.arena = arena;
            this.capacity = capacity;
            this.keyOffsets = arena.allocate((long) (capacity + 1) * Integer.BYTES);
            this.valOffsets = arena.allocate((long) (capacity + 1) * Integer.BYTES);
            this.keyDataCap = initialDataCap;
            this.valDataCap = initialDataCap;
            this.keyData = arena.allocate(keyDataCap);
            this.valData = arena.allocate(valDataCap);
            // offsets[0] = 0 (offsets[i] is start of row i, offsets[i+1] is end of row i).
            this.keyOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
            this.valOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
        }

        int size() {
            return n;
        }

        /**
         * Appends one (key, value) entry. Bytes are copied into the off-heap data regions
         * exactly once. Must NOT be called when {@link #size()} == capacity — the caller
         * ({@link #copyKeyGroup}) flushes first.
         */
        void append(byte[] key, byte[] value) {
            if (n >= capacity) {
                throw new IllegalStateException(
                        "BatchPutStaging full (capacity=" + capacity + "); caller must flush first");
            }
            long kStart = keyDataUsed;
            long vStart = valDataUsed;
            ensureKeyDataCap(kStart + key.length);
            ensureValDataCap(vStart + value.length);
            MemorySegment.copy(key, 0, keyData, ValueLayout.JAVA_BYTE, kStart, key.length);
            MemorySegment.copy(value, 0, valData, ValueLayout.JAVA_BYTE, vStart, value.length);
            keyDataUsed = kStart + key.length;
            valDataUsed = vStart + value.length;
            // offsets[n+1] = end of row n
            keyOffsets.set(ValueLayout.JAVA_INT, (long) (n + 1) * Integer.BYTES, (int) keyDataUsed);
            valOffsets.set(ValueLayout.JAVA_INT, (long) (n + 1) * Integer.BYTES, (int) valDataUsed);
            n++;
        }

        /**
         * Flushes the accumulated batch via {@link ForStRsLinker#vectorizedBatchPut}. A flush of
         * an empty buffer is a no-op (legitimate at end-of-iterator). Resets state for reuse.
         */
        void flush(ForStRsLinker linker, FrsDb targetDb, FrsCfHandle targetCf) {
            if (n == 0) {
                return;
            }
            linker.vectorizedBatchPut(targetDb, targetCf, keyOffsets, keyData, valOffsets, valData, n);
            // Reset for the next batch. Offsets[0] stays 0; everything else is overwritten.
            n = 0;
            keyDataUsed = 0;
            valDataUsed = 0;
        }

        private void ensureKeyDataCap(long needed) {
            if (needed <= keyDataCap) {
                return;
            }
            long newCap = Math.max(keyDataCap * 2L, needed);
            MemorySegment grown = arena.allocate(newCap);
            MemorySegment.copy(keyData, 0L, grown, 0L, keyDataUsed);
            keyData = grown;
            keyDataCap = newCap;
        }

        private void ensureValDataCap(long needed) {
            if (needed <= valDataCap) {
                return;
            }
            long newCap = Math.max(valDataCap * 2L, needed);
            MemorySegment grown = arena.allocate(newCap);
            MemorySegment.copy(valData, 0L, grown, 0L, valDataUsed);
            valData = grown;
            valDataCap = newCap;
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
        return new RestoreResult(db, cf, new LinkedHashMap<>(), 0L, Collections.emptyMap());
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
        // R31-L2 + R32-M2: collect per-entry failures into a suppressed list and log
        // at WARN, then re-scan to determine whether the leftover is real.
        //
        // R31-L2 (history): pre-fix the catch was a silent {@code ignored}, so a
        // partially-cleaned restore directory would surface as a confusing engine
        // error later (e.g. "engine refused to open: stale MANIFEST present").
        // R31-L2's throw-on-first-IOException turned WARN-only into throw-on-flake.
        //
        // R32-M2: throwing on transient flakes (Windows AV scanner holds a handle,
        // remote-FS retry-able EBUSY, GC pause that re-locks the file) aborted the
        // restore for a no-op cleanup that would have succeeded on retry. Fix:
        // (1) complete the loop and collect failures as before, (2) re-scan after
        // the loop, (3) throw ONLY if the re-scan still finds entries. This keeps
        // the loud-on-real-leftover semantics while tolerating transient FS latency.
        java.util.List<IOException> suppressed = new java.util.ArrayList<>();
        try (var stream = Files.walk(p)) {
            // Reverse-order so directories come last.
            stream.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(
                            child -> {
                                try {
                                    Files.deleteIfExists(child);
                                } catch (IOException e) {
                                    LOG.warn(
                                            "deleteRecursively: failed to delete '{}'; continuing",
                                            child,
                                            e);
                                    suppressed.add(e);
                                }
                            });
        }
        if (suppressed.isEmpty()) {
            return;
        }
        // R32-M2: re-scan. If the loop's failures were transient (AV scanner /
        // FS retry / GC), the entries may already be gone now. Only the entries
        // still on disk count as a real cleanup failure.
        java.util.List<Path> stillPresent = new java.util.ArrayList<>();
        if (Files.exists(p)) {
            try (var stream2 = Files.walk(p)) {
                stream2.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(
                                child -> {
                                    if (!child.equals(p) && Files.exists(child)) {
                                        try {
                                            // One more best-effort delete in case the FS just
                                            // caught up; only record what's STILL present after.
                                            Files.deleteIfExists(child);
                                            if (Files.exists(child)) {
                                                stillPresent.add(child);
                                            }
                                        } catch (IOException ignored) {
                                            stillPresent.add(child);
                                        }
                                    }
                                });
            }
        }
        if (stillPresent.isEmpty()) {
            LOG.warn(
                    "deleteRecursively: {} transient failures cleared on re-scan; continuing",
                    suppressed.size());
            return;
        }
        // Real leftover — surface the head IOException with the rest chained as
        // suppressed so callers / unit tests can introspect the root cause.
        IOException head = suppressed.get(0);
        for (int i = 1; i < suppressed.size(); i++) {
            head.addSuppressed(suppressed.get(i));
        }
        LOG.warn(
                "deleteRecursively: re-scan still found {} entries under '{}' after best-effort"
                        + " cleanup; failing restore",
                stillPresent.size(),
                p);
        throw head;
    }

    /**
     * Downloads each SST in {@code hlps} into {@code downloadDir} in parallel. PR-E1 (F5-6 /
     * E-HIGH-2): the bottleneck is per-handle network I/O, so a small bounded thread pool
     * (sized to {@code min(cores, n_handles)}) drops wall-clock restore time from O(Σ
     * download_time) to O(max(download_time)) without spawning unbounded threads.
     *
     * <p>Order of the returned list of local paths matches the input order, so the engine's
     * LSM reconstruction (which is order-insensitive but easier to debug deterministically)
     * sees a stable list. Strict-presence is enforced: any failed download triggers a
     * {@link ForStRsCheckpointRestoreException} carrying the offending logical path.
     */
    private List<String> parallelDownloadSsts(
            List<HandleAndLocalPath> hlps,
            Path downloadDir,
            IncrementalRemoteKeyedStateHandle owner)
            throws IOException {
        if (hlps.isEmpty()) {
            return new ArrayList<>();
        }
        String[] resolved = new String[hlps.size()];
        int parallelism = Math.min(hlps.size(), Runtime.getRuntime().availableProcessors());
        if (parallelism <= 1) {
            // Serial fallback — keeps single-handle paths simple and zero-overhead.
            for (int i = 0; i < hlps.size(); i++) {
                HandleAndLocalPath hlp = hlps.get(i);
                Path local = resolveSafeRestoreLocalPath(downloadDir, hlp.getLocalPath(), owner);
                downloadHandleStrict(hlp.getHandle(), local, hlp.getLocalPath(), owner);
                resolved[i] = local.toString();
            }
            return new ArrayList<>(java.util.Arrays.asList(resolved));
        }
        ExecutorService dl = newRestoreExecutor(parallelism, "forstrs-restore-sst-download");
        boolean success = false;
        try {
            List<Future<String>> futures = new ArrayList<>(hlps.size());
            for (int i = 0; i < hlps.size(); i++) {
                final HandleAndLocalPath hlp = hlps.get(i);
                final Path local = resolveSafeRestoreLocalPath(downloadDir, hlp.getLocalPath(), owner);
                futures.add(
                        dl.submit(
                                (Callable<String>)
                                        () -> {
                                            downloadHandleStrict(
                                                    hlp.getHandle(),
                                                    local,
                                                    hlp.getLocalPath(),
                                                    owner);
                                            return local.toString();
                                        }));
            }
            for (int i = 0; i < futures.size(); i++) {
                try {
                    resolved[i] = futures.get(i).get();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ForStRsCheckpointRestoreException(
                            hlps.get(i).getLocalPath(),
                            owner.getCheckpointId(),
                            "Interrupted while downloading " + hlps.get(i).getLocalPath(),
                            ie);
                } catch (ExecutionException ee) {
                    Throwable cause = ee.getCause();
                    if (cause instanceof ForStRsCheckpointRestoreException) {
                        throw (ForStRsCheckpointRestoreException) cause;
                    }
                    if (cause instanceof IOException) {
                        throw (IOException) cause;
                    }
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new ForStRsCheckpointRestoreException(
                            hlps.get(i).getLocalPath(),
                            owner.getCheckpointId(),
                            "Parallel SST download for '"
                                    + hlps.get(i).getLocalPath()
                                    + "' failed: "
                                    + cause,
                            cause);
                }
            }
            success = true;
        } finally {
            // E4-HIGH-2: on the exception path, force-interrupt in-flight S3 downloads instead of
            // letting them run to completion. {@code shutdown()} only stops accepting new tasks
            // but lets queued + in-flight workers continue — for a multi-GB SST download from S3
            // that means seconds-to-minutes of wasted bandwidth after restore has already
            // decided to fail. {@code shutdownNow()} sends an interrupt to each worker thread so
            // the blocking {@code OpenDal}/HTTP read aborts promptly.
            if (success) {
                dl.shutdown();
            } else {
                closeActiveDownloadStreams();
                dl.shutdownNow();
                try {
                    dl.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return new ArrayList<>(java.util.Arrays.asList(resolved));
    }

    /**
     * Creates a fresh bounded thread pool with daemon worker threads. We use a dedicated
     * executor (instead of {@link java.util.concurrent.ForkJoinPool#commonPool()}) so that
     * blocking S3/blob I/O does not starve common-pool consumers in the same JVM. The
     * executor is shut down by the caller via try/finally; daemon threads prevent shutdown
     * lag from delaying JVM exit on test-runner crashes.
     */
    private static ExecutorService newRestoreExecutor(int parallelism, String name) {
        AtomicInteger seq = new AtomicInteger();
        ThreadFactory tf =
                r -> {
                    Thread t = new Thread(r, name + "-" + seq.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                };
        return Executors.newFixedThreadPool(Math.max(1, parallelism), tf);
    }

    /** Wrapper for an opened source DB used during rescaling. */
    private static final class OpenSourceDb implements AutoCloseable {
        final FrsDb db;
        final FrsCfHandle cf;
        final KeyGroupRange range;
        // E6-HIGH-4(b): per-source serializer metadata extracted from the source handle's
        // private-state registry blob. Used by {@link #restoreWithRescaling} to union-merge
        // metadata across all source handles so the target backend's {@link
        // StateSerializerRegistry} sees the same {@code restoredMetadata} regardless of how many
        // sources contributed key groups. Empty map for pre-E5 source handles that did not carry
        // the registry blob.
        final Map<String, StateSerializerMetadata> serializerMetadata;

        OpenSourceDb(
                FrsDb db,
                FrsCfHandle cf,
                KeyGroupRange range,
                Map<String, StateSerializerMetadata> serializerMetadata) {
            this.db = db;
            this.cf = cf;
            this.range = range;
            this.serializerMetadata = serializerMetadata;
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

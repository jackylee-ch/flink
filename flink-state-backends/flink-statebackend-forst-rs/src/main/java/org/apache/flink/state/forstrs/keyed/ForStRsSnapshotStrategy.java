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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsSnapshot;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ForSt-RS snapshot strategy (B-Prod-P3 Tasks 3.4 + 3.5).
 *
 * <p>Implements Flink 2.2.0's {@link SnapshotStrategy} contract:
 *
 * <ul>
 *   <li><b>Sync phase</b> ({@link #syncPrepareResources(long)}) — captures an engine snapshot
 *       (pinning compaction at the current seq). This is O(1) and non-blocking so it does not stall
 *       the data path during checkpoint barriers.
 *   <li><b>Async phase</b> ({@link #asyncSnapshot}) — runs in a virtual thread, invokes {@link
 *       ForStRsLinker#createIncrementalCheckpointAt} which flushes memtables and writes a manifest
 *       blob + reports (new SSTs, shared SSTs) for this checkpoint relative to the previous {@code
 *       lastCheckpointId}. Then uploads the manifest + new SSTs via {@link ForStRsSstUploader},
 *       registers the new SSTs in the local {@link ForStRsSstRegistry}, and assembles a {@link
 *       ForStRsIncrementalKeyedStateHandle}. Shared SSTs from prior checkpoints are looked up from
 *       the registry rather than re-uploaded. The returned {@link SnapshotResult} carries the
 *       JM-owned handle (no task-local copy in v1).
 * </ul>
 *
 * <p>Thread model: sync phase runs on the task thread (must be fast — we issue 1 FFI call:
 * dbSnapshot), async phase dispatches one virtual thread per file via the uploader, then the
 * orchestrating {@link SnapshotStrategy.SnapshotResultSupplier} blocks on the join. {@link
 * CloseableRegistry} registration is no-op for v1 because each upload future already self-cleans on
 * completion; cancellation hooks land in P4 alongside the restore wiring.
 */
@Internal
public class ForStRsSnapshotStrategy
        implements SnapshotStrategy<KeyedStateHandle, ForStRsSnapshotResources> {

    /** Layout: data ptr (8) + len (8) + reserved (8) for FrsBytes; here for path/list pointers. */
    private static final long PTR = ValueLayout.ADDRESS.byteSize();

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final UUID backendIdentifier;
    private final KeyGroupRange keyGroupRange;
    private final ForStRsSstRegistry sstRegistry;
    private final ForStRsSstUploader uploader;
    private final Arena nativeArena;
    private final Map<String, Long> cfMap;

    /**
     * The previous successfully-completed checkpoint id. Updated by {@link
     * #recordCompletedCheckpoint(long)} when notifyCheckpointComplete fires; used as {@code
     * base_checkpoint_id} for the next sync-phase capture.
     */
    private final AtomicLong lastCompletedCheckpointId = new AtomicLong(0L);

    /**
     * Per-checkpoint list of {@link HandleAndLocalPath} entries registered by the async phase.
     * Consumed by the backend's {@code notifyCheckpointAborted} to roll back ref-counts of the
     * aborted checkpoint without touching ref-counts contributed by other (completed) checkpoints.
     */
    private final java.util.concurrent.ConcurrentHashMap<Long, List<HandleAndLocalPath>>
            pendingRegistrations = new java.util.concurrent.ConcurrentHashMap<>();

    public ForStRsSnapshotStrategy(
            ForStRsLinker linker,
            FrsDb db,
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            ForStRsSstRegistry sstRegistry,
            ForStRsSstUploader uploader,
            Arena nativeArena,
            Map<String, Long> cfMap) {
        this.linker = linker;
        this.db = db;
        this.backendIdentifier = backendIdentifier;
        this.keyGroupRange = keyGroupRange;
        this.sstRegistry = sstRegistry;
        this.uploader = uploader;
        this.nativeArena = nativeArena;
        this.cfMap = new LinkedHashMap<>(cfMap);
    }

    /**
     * Notifier hook — wire-called by the keyed-backend's {@code notifyCheckpointComplete} so the
     * next sync-phase uses this checkpoint id as its {@code base_checkpoint_id}. Strictly
     * monotonic: completion of a smaller id is ignored (out-of-order completes are rare but Flink
     * permits them).
     */
    public void recordCompletedCheckpoint(long checkpointId) {
        lastCompletedCheckpointId.accumulateAndGet(checkpointId, Math::max);
    }

    /** Test accessor — returns the strategy's last-completed checkpoint id. */
    public long getLastCompletedCheckpointId() {
        return lastCompletedCheckpointId.get();
    }

    @Override
    public ForStRsSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        // Step 1: capture an engine snapshot (pinning the seq). This is O(1) and non-blocking.
        FrsSnapshot snapshot = linker.dbSnapshot(db, nativeArena);

        // Step 2: record the base checkpoint id for the async phase. The actual
        // createIncrementalCheckpointAt call (which flushes memtables and computes
        // new/shared SST lists) is deferred to the async phase so it does NOT block
        // the task thread during checkpoint barriers. The snapshot pins all versions
        // at the captured seq — concurrent writes do not affect correctness.
        long baseCheckpointId = lastCompletedCheckpointId.get();

        return new ForStRsSnapshotResources(linker, db, snapshot, checkpointId, baseCheckpointId);
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            ForStRsSnapshotResources resources,
            long checkpointId,
            long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions) {
        return (CloseableRegistry registry) -> {
            try {
                return doAsyncSnapshot(resources, streamFactory);
            } finally {
                // Whether success or failure, release the engine snapshot + result struct now —
                // the uploaded handles + Java IncrementalKeyedStateHandle below carry zero
                // dependency on the engine-side native memory after this point.
                resources.release();
            }
        };
    }

    /**
     * Test/backend accessor — returns and removes the per-checkpoint registration list so the
     * keyed-backend can roll back ref-counts on abort. Returns {@code null} if no registrations for
     * that id are tracked (already consumed or never tracked).
     */
    public List<HandleAndLocalPath> takePendingRegistrations(long checkpointId) {
        return pendingRegistrations.remove(checkpointId);
    }

    private SnapshotResult<KeyedStateHandle> doAsyncSnapshot(
            ForStRsSnapshotResources resources, CheckpointStreamFactory streamFactory)
            throws Exception {
        // ---- Compute the incremental checkpoint (flush + SST enumeration). ----
        // This was moved out of the sync phase to avoid blocking the task thread
        // during checkpoint barriers. The snapshot pins all versions at the captured
        // seq so concurrent writes do not affect correctness.
        MemorySegment result = nativeArena.allocate(32);
        try {
            linker.createIncrementalCheckpointAt(
                    db,
                    resources.getSnapshot(),
                    resources.getCheckpointId(),
                    resources.getBaseCheckpointId(),
                    result);
        } catch (RuntimeException re) {
            throw re;
        }

        Path manifestPath = readCString(result, 0L);
        List<Path> newSstFiles = readSstList(result, PTR);
        List<Path> sharedSstFiles = readSstList(result, 2 * PTR);

        // Free the result struct now that we've marshalled the data into Java objects.
        try {
            linker.dbIncrementalCheckpointResultFree(result);
        } catch (RuntimeException ignored) {
            // Idempotent on the native side.
        }

        // ---- Upload manifest (private state) under EXCLUSIVE scope. ----
        CompletableFuture<StreamStateHandle> manifestFut =
                uploader.upload(manifestPath, streamFactory, CheckpointedStateScope.EXCLUSIVE);

        // ---- Upload each new SST under SHARED scope (eligible for cross-checkpoint sharing). ----
        List<CompletableFuture<HandleAndLocalPath>> newSstFuts =
                new ArrayList<>(newSstFiles.size());
        for (Path p : newSstFiles) {
            String localPath = p.getFileName().toString();
            CompletableFuture<HandleAndLocalPath> f =
                    uploader.upload(p, streamFactory, CheckpointedStateScope.SHARED)
                            .thenApply(h -> HandleAndLocalPath.of(h, localPath));
            newSstFuts.add(f);
        }

        // ---- Resolve shared SSTs from the registry (already uploaded by a prior ckpt). ----
        List<HandleAndLocalPath> sharedHandles = new ArrayList<>();
        for (Path p : sharedSstFiles) {
            String localPath = p.getFileName().toString();
            StateHandleID id = new StateHandleID(localPath);
            StreamStateHandle h =
                    sstRegistry
                            .get(id)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Shared SST not in registry: "
                                                            + localPath
                                                            + " (engine reported it as shared but "
                                                            + "no prior checkpoint registered it)"));
            sharedHandles.add(HandleAndLocalPath.of(h, localPath));
            // Bump ref-count for this checkpoint's reference.
            sstRegistry.register(id, h);
        }

        // ---- Wait for all uploads. ----
        StreamStateHandle metaHandle = manifestFut.get();
        // Track the per-checkpoint registrations so notifyCheckpointAborted can roll back
        // exactly this checkpoint's ref-count contribution without disturbing baseline shared
        // state from previously-completed checkpoints.
        List<HandleAndLocalPath> thisCheckpointRegistrations = new ArrayList<>();
        for (CompletableFuture<HandleAndLocalPath> f : newSstFuts) {
            HandleAndLocalPath hlp = f.get();
            sharedHandles.add(hlp);
            sstRegistry.register(new StateHandleID(hlp.getLocalPath()), hlp.getHandle());
            thisCheckpointRegistrations.add(hlp);
        }
        // Also include the shared SSTs: their ref-counts were bumped above so abort must roll those
        // back as well.
        for (Path p : sharedSstFiles) {
            String localPath = p.getFileName().toString();
            sstRegistry
                    .get(new StateHandleID(localPath))
                    .ifPresent(
                            h ->
                                    thisCheckpointRegistrations.add(
                                            HandleAndLocalPath.of(h, localPath)));
        }
        pendingRegistrations.put(resources.getCheckpointId(), thisCheckpointRegistrations);
        // Manifest is private — kept off the shared list. (Carried as the metaStateHandle below.)

        // ---- Build the keyed state handle. ----
        // Private state for v1 is empty (manifest is the dedicated metaStateHandle slot per
        // Flink's incremental contract; the engine writes only one file per checkpoint that we
        // treat as private). We keep the privateState list reserved for future per-ckpt artefacts.
        ForStRsIncrementalKeyedStateHandle handle =
                new ForStRsIncrementalKeyedStateHandle(
                        backendIdentifier,
                        keyGroupRange,
                        resources.getCheckpointId(),
                        resources.getBaseCheckpointId(),
                        /* sharedState= */ sharedHandles,
                        /* privateState= */ List.of(),
                        metaHandle,
                        cfMap);
        return SnapshotResult.of(handle);
    }

    // ------------------------------------------------------------------
    // Helpers — read FrsLiveFileList contents through the FFM ABI.
    // ------------------------------------------------------------------

    /** Reads a NUL-terminated UTF-8 C string referenced at {@code resultStruct[off]}. */
    private static Path readCString(MemorySegment resultStruct, long off) {
        MemorySegment ptr = resultStruct.get(ValueLayout.ADDRESS, off);
        if (ptr.address() == 0L) {
            return null;
        }
        // Re-interpret the unbounded native pointer so getString can walk to the NUL byte.
        MemorySegment reinterpreted = ptr.reinterpret(Long.MAX_VALUE);
        return Paths.get(reinterpreted.getString(0L));
    }

    /**
     * Reads a {@code FrsLiveFileList*} stored at {@code resultStruct[off]} and walks its inner
     * {@code files} array, extracting each file's {@code path}. Other fields (size/seq/level/cf)
     * are ignored for now — we only need the absolute path for upload.
     */
    private static List<Path> readSstList(MemorySegment resultStruct, long off) {
        MemorySegment listPtr = resultStruct.get(ValueLayout.ADDRESS, off);
        if (listPtr.address() == 0L) {
            return List.of();
        }
        // FrsLiveFileList layout: files (8) + count (8) + manifest_size (8) = 24 bytes.
        MemorySegment listStruct = listPtr.reinterpret(24L);
        MemorySegment filesPtr = listStruct.get(ValueLayout.ADDRESS, 0L);
        long count = listStruct.get(ValueLayout.JAVA_LONG, 8L);
        if (filesPtr.address() == 0L || count == 0L) {
            return List.of();
        }
        // FrsLiveFile layout: path (8) + size (8) + sequence (8) + level (1) + pad (7) + cf (8)
        // = 40 bytes per entry.
        long entryBytes = 40L;
        MemorySegment filesArr = filesPtr.reinterpret(count * entryBytes);
        List<Path> out = new ArrayList<>((int) count);
        for (long i = 0; i < count; i++) {
            MemorySegment pathPtr = filesArr.get(ValueLayout.ADDRESS, i * entryBytes);
            if (pathPtr.address() == 0L) {
                continue;
            }
            String path = pathPtr.reinterpret(Long.MAX_VALUE).getString(0L);
            out.add(Paths.get(path));
        }
        return out;
    }
}

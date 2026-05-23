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
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
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
import org.apache.flink.state.forstrs.state.StateSerializerRegistry;

import java.io.IOException;
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

    /**
     * E5-HIGH-2: well-known local-path name for the serialized {@link StateSerializerRegistry}
     * blob, stored under {@code privateState}. The restore path scans for this exact path to
     * separate the registry blob from any future per-checkpoint private artefacts.
     */
    public static final String SERIALIZER_REGISTRY_LOCAL_PATH = "_serializer_metadata.bin";

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final UUID backendIdentifier;
    private final KeyGroupRange keyGroupRange;
    private final ForStRsSstRegistry sstRegistry;
    private final ForStRsSstUploader uploader;
    private final Arena nativeArena;
    private final Map<String, Long> cfMap;

    /**
     * E5-HIGH-2: hook that returns the current registry blob bytes at snapshot time. The blob is
     * uploaded as a private-state {@link HandleAndLocalPath} entry under {@link
     * #SERIALIZER_REGISTRY_LOCAL_PATH}; restore parses it and re-seeds the next session's
     * registry so {@code verifyOrRegister} can detect schema drift. Defaults to "no registry
     * wired" (null) — emitted privateState stays empty and behavior matches pre-fix snapshots.
     * Tests can leave this null to keep existing checkpoint snapshots intact.
     */
    private volatile RegistryBlobProvider registryBlobProvider = null;

    /** Supplier callback used by {@link #setRegistryBlobProvider}. */
    @FunctionalInterface
    public interface RegistryBlobProvider {
        /**
         * Returns the current registry blob bytes to embed in the snapshot, or {@code null} if no
         * states are registered (in which case no private-state entry is emitted).
         */
        byte[] currentBlob() throws IOException;
    }

    /**
     * Wire a supplier that returns the registry blob at snapshot time. Called once by the keyed
     * backend immediately after the strategy is constructed; subsequent calls overwrite. Setting
     * to {@code null} disables emit (used by tests that don't have a backend registry).
     */
    public void setRegistryBlobProvider(RegistryBlobProvider provider) {
        this.registryBlobProvider = provider;
    }

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
     *
     * <p>E7-H1: Flink may retry an async snapshot for the same {@code checkpointId} after a partial
     * failure. The first attempt may already have bumped registry ref-counts for some SSTs before
     * throwing. An unconditional {@code put} on the second attempt would REPLACE the first
     * attempt's list — only the second list would be rolled back on abort, leaking the first
     * attempt's ref-bumps. The writer at the end of {@link #doAsyncSnapshot} therefore uses {@link
     * java.util.concurrent.ConcurrentHashMap#merge(Object, Object, java.util.function.BiFunction)}
     * to APPEND any pre-existing list for the same id rather than overwrite it. Retries thus
     * contribute additional rollback entries; a single {@code notifyCheckpointAborted} sweeps
     * every ref-bump from every attempt for that checkpoint.
     */
    private final java.util.concurrent.ConcurrentHashMap<Long, List<HandleAndLocalPath>>
            pendingRegistrations = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * E8-H2: per-checkpoint "aborted" markers. Populated by
     * {@link #takePendingRegistrationsForAbort} (called from {@code notifyCheckpointAborted})
     * the moment the abort handler begins draining the rollback list. The async-snapshot
     * worker checks this set under the list's monitor in {@link #appendAndRegister}: if the
     * marker is set the worker skips both the {@code sstRegistry.register} call and the list
     * append, leaving no orphan ref-count bumps for the abort handler to chase.
     *
     * <p>This closes the residual race left after E7-H1: an abort that fires AFTER the
     * rollback list has been installed in {@link #pendingRegistrations} but DURING the
     * register loop would, pre-fix, leave subsequent register-bumps unrolled-back. With the
     * marker plus the monitor-guarded re-check, every register-after-take is detected and
     * skipped before the ref-count is bumped.
     *
     * <p>We keep this as a {@link java.util.concurrent.ConcurrentHashMap} keyed by checkpoint
     * id (Set semantics, value is a no-op {@code Boolean}) so the marker can be inspected
     * across the {@code doAsyncSnapshot} worker thread and the {@code notifyCheckpointAborted}
     * thread without further locking. Cleared by {@code recordCompletedCheckpoint} (success
     * path) so the map does not grow unboundedly.
     */
    private final java.util.concurrent.ConcurrentHashMap<Long, Boolean> abortedCheckpoints =
            new java.util.concurrent.ConcurrentHashMap<>();

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
        // E8-H2: completion path also clears any stale abort marker so the map stays bounded.
        // A completed checkpoint cannot also be aborted, but a previously-marked id may
        // legitimately recur (Flink's restart-from-savepoint can reuse ids in some adapter
        // chains) — the explicit clear keeps semantics tight.
        clearAbortMarker(checkpointId);
    }

    /** Test accessor — returns the strategy's last-completed checkpoint id. */
    public long getLastCompletedCheckpointId() {
        return lastCompletedCheckpointId.get();
    }

    @Override
    public ForStRsSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        // Step 1: capture an engine snapshot (pinning the seq). This is O(1) and non-blocking.
        FrsSnapshot snapshot = linker.dbSnapshot(db, nativeArena);

        // A7-H3: from here through the ForStRsSnapshotResources construction we MUST
        // close the engine snapshot on any throw — otherwise no SnapshotResources is
        // constructed → release() is never called → the snapshot pins the source seq
        // until process exit, which blocks compaction. provider.currentBlob() in
        // particular can race with concurrent register() writes and surface a
        // ConcurrentModificationException; without this guard, that escape leaks the
        // native snapshot handle.
        try {
            // Step 2: record the base checkpoint id for the async phase. The actual
            // createIncrementalCheckpointAt call (which flushes memtables and computes
            // new/shared SST lists) is deferred to the async phase so it does NOT block
            // the task thread during checkpoint barriers. The snapshot pins all versions
            // at the captured seq — concurrent writes do not affect correctness.
            long baseCheckpointId = lastCompletedCheckpointId.get();

            // E6-H3: capture the serializer-registry blob on the mailbox thread (the sync
            // phase runs in-mailbox-turn during checkpoint barrier alignment). The
            // async-snapshot worker would otherwise call {@code provider.currentBlob()} on
            // the worker thread — which iterates the live {@link
            // org.apache.flink.state.forstrs.state.StateSerializerRegistry} LinkedHashMap,
            // racing with concurrent {@code register()} writes also on the mailbox thread
            // (via {@code verifyOrRegister}). Pulling the bytes here, while the mailbox
            // holds the turn, makes the snapshot immutable / thread-safe by the time the
            // worker reads it.
            byte[] capturedRegistryBlob = null;
            RegistryBlobProvider provider = registryBlobProvider;
            if (provider != null) {
                capturedRegistryBlob = provider.currentBlob();
            }

            return new ForStRsSnapshotResources(
                    linker, db, snapshot, checkpointId, baseCheckpointId, capturedRegistryBlob);
        } catch (Throwable t) {
            try {
                snapshot.close();
            } catch (Throwable closeErr) {
                t.addSuppressed(closeErr);
            }
            throw t;
        }
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
                return doAsyncSnapshot(resources, streamFactory, checkpointOptions);
            } finally {
                // Whether success or failure, release the engine snapshot + result struct now —
                // the uploaded handles + Java IncrementalKeyedStateHandle below carry zero
                // dependency on the engine-side native memory after this point.
                resources.release();
            }
        };
    }

    /**
     * Test/backend accessor — returns and removes the per-checkpoint registration list. This
     * variant is the COMPLETION-path take (called from {@code notifyCheckpointComplete}); it
     * does NOT install the abort marker and the returned list is dropped (the registry
     * ref-counts persist because the SSTs are now committed). Returns {@code null} if no
     * registrations for that id are tracked.
     *
     * <p>For the abort path use {@link #takePendingRegistrationsForAbort} which additionally
     * installs the marker so any still-running async-snapshot worker self-rolls-back its
     * pending bumps.
     */
    public List<HandleAndLocalPath> takePendingRegistrations(long checkpointId) {
        List<HandleAndLocalPath> list = pendingRegistrations.remove(checkpointId);
        if (list == null) {
            return null;
        }
        // Snapshot under the list's monitor so any in-flight {@link #appendAndRegister} (which
        // holds the same monitor across register + add) completes before we copy. Returning
        // the same {@code list} reference is unsafe — a subsequent worker append could
        // observably mutate the caller's view. The shallow copy is O(N) and bounded by the
        // per-checkpoint registration count.
        synchronized (list) {
            return new ArrayList<>(list);
        }
    }

    /**
     * E8-H2: abort-path take. Installs the abort marker BEFORE removing the list so any
     * still-running async-snapshot worker — when it next reaches the {@link #appendAndRegister}
     * monitor-protected re-check — observes the marker and self-rolls-back any
     * {@code sstRegistry.register} call it has already issued in this attempt.
     *
     * <p>The marker install happens before {@code pendingRegistrations.remove} so a worker
     * that observes the missing list cannot be racing ahead of the marker — once the worker
     * checks {@code abortedCheckpoints}, it MUST see the install.
     *
     * <p>The list snapshot is taken under the list's monitor so any in-flight
     * {@link #appendAndRegister} synchronized block completes (its register + add is atomic
     * relative to this snapshot) before we copy.
     *
     * <p>Post-abort drain: after the initial take, the worker may have a pending
     * {@code synchronized(installedList)} block still mid-flight (it acquired the monitor
     * BEFORE our snapshot — we waited on it before the copy). Once we release, that worker
     * MUST observe the marker on its under-monitor re-check and skip the (register, add)
     * pair — leaving no late entries to drain. Callers can additionally invoke
     * {@link #drainLatePendingRegistrations(long, List)} as a defensive second sweep.
     */
    public List<HandleAndLocalPath> takePendingRegistrationsForAbort(long checkpointId) {
        // E8-H2: install the abort marker BEFORE removing the list.
        abortedCheckpoints.put(checkpointId, Boolean.TRUE);
        List<HandleAndLocalPath> list = pendingRegistrations.remove(checkpointId);
        if (list == null) {
            return null;
        }
        synchronized (list) {
            return new ArrayList<>(list);
        }
    }

    /**
     * E8-H2 post-abort drain: defensive second sweep called by the abort handler after
     * processing the initial {@link #takePendingRegistrationsForAbort} snapshot. Returns and
     * removes any rollback entries that landed in {@code pendingRegistrations} for
     * {@code checkpointId} between the initial take and this call.
     *
     * <p>Under the current {@link #appendAndRegister} design, the worker re-checks the abort
     * marker UNDER the list's monitor before it adds — so a worker that ran AFTER the abort
     * marker install will skip the append entirely and no late entry can appear. This drain
     * is therefore expected to be a no-op (returning an empty list) on every call; the second
     * sweep exists to defend against future refactors that might re-introduce a window.
     */
    public List<HandleAndLocalPath> drainLatePendingRegistrations(long checkpointId) {
        List<HandleAndLocalPath> late = pendingRegistrations.remove(checkpointId);
        if (late == null) {
            return java.util.Collections.emptyList();
        }
        synchronized (late) {
            List<HandleAndLocalPath> copy = new ArrayList<>(late);
            late.clear();
            return copy;
        }
    }

    /**
     * E8-H2: clear the abort marker for {@code checkpointId}. Called by
     * {@link #recordCompletedCheckpoint} so the marker map does not grow unboundedly across
     * the lifetime of a long-running job. Idempotent.
     */
    private void clearAbortMarker(long checkpointId) {
        abortedCheckpoints.remove(checkpointId);
    }

    /**
     * E8-H2: register an SST handle and atomically append the rollback entry to the per-checkpoint
     * list, holding the list's monitor across BOTH halves. The same monitor is acquired by
     * {@link #takePendingRegistrations} when it snapshots the list — so the abort handler's view
     * is atomic with respect to the {@code (register, list.add)} pair: it either sees the
     * register-bump AND the rollback entry, or neither.
     *
     * <p>The pre-fix order (register first, then merge the list afterwards) left a window where
     * an abort that fired AFTER register but BEFORE the list append would see no rollback entry
     * for the bump → ref-count leaked forever. By holding the monitor across both calls, the
     * abort handler's {@code synchronized(list)} snapshot is guaranteed to observe both halves
     * atomically.
     *
     * <p>Self-rollback on observed abort: an abort that fires BEFORE we acquire the monitor
     * means {@code abortedCheckpoints.containsKey} returns true on our early probe — we skip
     * the whole {@code (register, append)} pair and the canonical rollback list (already drained
     * by the abort handler) requires no action from us. After we acquire the monitor we
     * re-check: an abort that snuck in between the probe and the monitor acquisition will be
     * caught here and we self-unregister to balance our own bump (because the abort handler's
     * snapshot was taken under the same monitor — either we hold it or they do, never both, so
     * if we observe the marker AFTER taking the monitor, the handler must have taken it BEFORE
     * us → their snapshot did not see our bump → we must self-unregister).
     *
     * <p>Dedupes by {@link StateHandleID} via {@code registeredIds}: if the engine reports the
     * same localPath twice (e.g. in BOTH newSstFiles and sharedSstFiles), we register once and
     * record one rollback entry — preserving the 1:1 invariant between register-bumps owned by
     * this checkpoint attempt and rollback entries that abort will unregister.
     */
    private void appendAndRegister(
            List<HandleAndLocalPath> installedList,
            java.util.LinkedHashSet<StateHandleID> registeredIds,
            long checkpointId,
            StateHandleID id,
            StreamStateHandle handle) {
        // Dedupe: same id seen earlier in this attempt or carried over from a prior retry
        // attempt — skip both register and append (the prior register's rollback entry is
        // already on the list).
        if (!registeredIds.add(id)) {
            return;
        }
        // Fast-path abort probe: if the abort handler has already installed the marker we
        // skip the register entirely. (The canonical rollback list has been drained.)
        if (abortedCheckpoints.containsKey(checkpointId)) {
            return;
        }
        // E8-H2 invariant: register + list.add happen atomically under the list's monitor.
        // takePendingRegistrations acquires the same monitor when it copies the list — so the
        // abort handler's snapshot is atomic with respect to this pair: it sees both halves
        // or neither. There is no window where register completes but the rollback entry is
        // missing from the snapshot.
        synchronized (installedList) {
            // Re-check the abort marker under the monitor: if the abort handler installed the
            // marker AND acquired the monitor BEFORE us, we observe the marker here and have
            // a guarantee that the handler's snapshot did not include our (yet-to-be-added)
            // entry. Skip both register and append.
            if (abortedCheckpoints.containsKey(checkpointId)) {
                return;
            }
            sstRegistry.register(id, handle);
            installedList.add(HandleAndLocalPath.of(handle, id.getKeyString()));
        }
    }

    /**
     * Test-only helper for the E7-H1 retry-semantics regression: re-install a previously-taken
     * list back under the same id so a subsequent async-snapshot call exercises the {@code merge}
     * path against a non-empty existing entry. Real Flink does not call this — the merge fires
     * naturally because the first attempt never reached {@code takePendingRegistrations} before
     * the retry kicked off.
     */
    @org.apache.flink.annotation.VisibleForTesting
    public void takePendingRegistrationsForTestsReinstall(
            long checkpointId, List<HandleAndLocalPath> list) {
        pendingRegistrations.put(checkpointId, new ArrayList<>(list));
        // E8-H2: the test harness used {@link #takePendingRegistrations} to peek at the first
        // attempt's list — that call set the abort marker as a side effect. The retry-semantics
        // test simulates a real retry where the first attempt finished WITHOUT an abort, so
        // clear the marker before the second attempt's appendAndRegister loop runs (otherwise
        // every register-bump self-rolls-back and the merge contract under test never fires).
        clearAbortMarker(checkpointId);
    }

    private SnapshotResult<KeyedStateHandle> doAsyncSnapshot(
            ForStRsSnapshotResources resources,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions)
            throws Exception {
        // R35-H2: branch on the checkpoint type's sharing strategy. Pre-R35-H2 the sync phase
        // set {@code baseCheckpointId = lastCompletedCheckpointId.get()} unconditionally, so a
        // Flink-requested FULL_CHECKPOINT (FORWARD) or CANONICAL savepoint (NO_SHARING) was
        // emitted as an INCREMENTAL handle that referenced prior-checkpoint SSTs under SHARED
        // scope. A subsequent retention-policy cleanup of those prior incrementals could then
        // delete the SSTs the "full" handle depended on — silently corrupting restore from the
        // supposed self-contained snapshot.
        //
        // Two-tier gate driven by {@link SharingFilesStrategy}:
        //  - FORWARD (FULL_CHECKPOINT) and NO_SHARING (CANONICAL savepoint) BOTH disallow
        //    reusing files from older snapshots — set {@code effectiveBaseCheckpointId = 0}
        //    so the engine emits every reachable SST as NEW (no SHARED references to prior).
        //  - NO_SHARING additionally disallows future snapshots from sharing these files —
        //    upload SSTs under EXCLUSIVE scope so the snapshot is fully self-contained and
        //    SharedStateRegistry won't ref-count them as a shared resource.
        //  - FORWARD_BACKWARD (default CHECKPOINT) keeps the pre-R35-H2 incremental contract
        //    (base=lastCompleted, SSTs under SHARED scope).
        //
        // Defensive: legacy test harness call-sites pass {@code null} for {@code checkpointOptions}
        // — treat null as "incremental" (default FORWARD_BACKWARD), matching pre-R35-H2 behaviour.
        org.apache.flink.runtime.checkpoint.SnapshotType.SharingFilesStrategy strategy =
                checkpointOptions == null
                        ? org.apache.flink.runtime.checkpoint.SnapshotType.SharingFilesStrategy
                                .FORWARD_BACKWARD
                        : checkpointOptions.getCheckpointType().getSharingFilesStrategy();
        boolean noPriorDependence =
                strategy
                                == org.apache.flink.runtime.checkpoint.SnapshotType
                                        .SharingFilesStrategy.FORWARD
                        || strategy
                                == org.apache.flink.runtime.checkpoint.SnapshotType
                                        .SharingFilesStrategy.NO_SHARING;
        boolean fullCheckpoint =
                strategy
                        == org.apache.flink.runtime.checkpoint.SnapshotType.SharingFilesStrategy
                                .NO_SHARING;
        long effectiveBaseCheckpointId =
                noPriorDependence ? 0L : resources.getBaseCheckpointId();

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
                    effectiveBaseCheckpointId,
                    result);
        } catch (RuntimeException re) {
            throw re;
        }

        Path manifestPath = readCString(result, 0L);
        List<Path> newSstFiles = readSstList(result, PTR);
        List<Path> sharedSstFiles = readSstList(result, 2 * PTR);

        // Free the result struct now that we've marshalled the data into Java objects.
        //
        // R31-L1: {@code frs_incremental_checkpoint_result_free} is documented as idempotent on
        // the Rust side — it CAS-flips a "freed" flag inside the boxed result and short-circuits
        // when re-invoked. Catching {@link RuntimeException} is belt-and-braces for two cases:
        // (a) a future ABI change that introduces a fallible variant we don't know about yet,
        // and (b) a JVM-side wrapper exception thrown during the FFI bind. Either way, swallowing
        // is safe because the worst-case (failed free) is a leak of the result struct (which is
        // bounded — one per checkpoint) and never a use-after-free of the manifest path /
        // sstFiles lists we already marshalled into Java arrays above. An integration test that
        // double-invokes free() is intentionally deferred — the native idempotency is covered by
        // the Rust-side unit test in {@code checkpoint::tests::result_free_idempotent}.
        try {
            linker.dbIncrementalCheckpointResultFree(result);
        } catch (RuntimeException ignored) {
            // Idempotent on the native side.
        }

        // ---- Upload manifest (private state) under EXCLUSIVE scope. ----
        CompletableFuture<StreamStateHandle> manifestFut =
                uploader.upload(manifestPath, streamFactory, CheckpointedStateScope.EXCLUSIVE);

        // ---- Upload each new SST. ----
        // R35-H2: under {@link SharingFilesStrategy#NO_SHARING} (FULL_CHECKPOINT and CANONICAL
        // savepoints) the snapshot MUST be self-contained — every SST is emitted under EXCLUSIVE
        // scope so neither this snapshot nor a later one will adopt these files into a shared
        // ref-counted set. Under the incremental contract (FORWARD / FORWARD_BACKWARD) the
        // pre-R35-H2 SHARED scope is preserved so cross-checkpoint sharing continues to work.
        CheckpointedStateScope newSstScope =
                fullCheckpoint
                        ? CheckpointedStateScope.EXCLUSIVE
                        : CheckpointedStateScope.SHARED;
        List<CompletableFuture<HandleAndLocalPath>> newSstFuts =
                new ArrayList<>(newSstFiles.size());
        for (Path p : newSstFiles) {
            String localPath = p.getFileName().toString();
            CompletableFuture<HandleAndLocalPath> f =
                    uploader.upload(p, streamFactory, newSstScope)
                            .thenApply(h -> HandleAndLocalPath.of(h, localPath));
            newSstFuts.add(f);
        }

        // ---- E8-H2: install the per-checkpoint rollback list in pendingRegistrations BEFORE
        // any sstRegistry.register() call. If notifyCheckpointAborted fires between an earlier
        // attempt's residual register and this attempt's first register, the abort handler can
        // observe THIS attempt's empty list and add its rollback no-op without leaking. Each
        // (register, add) pair below holds the list's monitor so a concurrently-running
        // takePendingRegistrations snapshots a consistent view.
        //
        // E7-M3: dedupe the rollback list by StateHandleID. If the engine ever reports the
        // same localPath in BOTH newSstFiles and sharedSstFiles (or duplicates within one
        // list), the registration loops above would each fire — bumping the refcount twice
        // — but if the rollback list also recorded both, notifyCheckpointAborted would
        // unregister() the same id twice, while a follow-up notifyCheckpointComplete on a
        // sibling checkpoint that legitimately registered the same shared SST has only one
        // matching unregister() to balance. Net result: under abort the refcount goes
        // negative or the entry gets prematurely deleted from the registry, taking the
        // shared SST out from under a still-live checkpoint. The LinkedHashSet on
        // StateHandleID guarantees 1:1 between rollback entries and register() bumps that
        // belong to THIS checkpoint while preserving deterministic order.
        long checkpointId = resources.getCheckpointId();
        List<HandleAndLocalPath> thisCheckpointRegistrations = new ArrayList<>();
        // E7-H1: append to (don't overwrite) any prior attempt's rollback list for the same
        // checkpoint id. A partially-failed earlier attempt may have already bumped ref-counts —
        // those entries must remain rollback-able when the abort eventually fires.
        //
        // E8-H2: do the merge BEFORE the register loop. The pre-fix code ran the loop FIRST
        // and merged AFTER — leaving a window in which abort would observe a missing entry
        // and skip rollback for register-bumps already issued. With the merge done up front,
        // any abort that arrives during the loop will at minimum observe this attempt's
        // (initially empty) entry and add its rollback to that list as the loop proceeds.
        List<HandleAndLocalPath> installedList =
                pendingRegistrations.merge(
                        checkpointId,
                        thisCheckpointRegistrations,
                        (existing, additions) -> {
                            existing.addAll(additions);
                            return existing;
                        });
        // installedList is the canonical list now owned by the map: either this attempt's
        // empty list (no prior attempt) or the prior attempt's list with our (empty)
        // additions appended. Subsequent appendRegistration calls go through it so the abort
        // handler reads via takePendingRegistrations sees every entry from EITHER attempt.
        java.util.LinkedHashSet<StateHandleID> registeredIds = new java.util.LinkedHashSet<>();
        // Seed the dedupe set with any IDs ALREADY in the installed list — a prior retry
        // attempt may have already registered them; we must not register them again here
        // (the registry refcount would double-bump and a single rollback per id would not
        // cancel both).
        for (HandleAndLocalPath prior : installedList) {
            registeredIds.add(new StateHandleID(prior.getLocalPath()));
        }

        // ---- Resolve shared SSTs from the registry (already uploaded by a prior ckpt). ----
        // R35-H2: with {@code effectiveBaseCheckpointId == 0} (FULL_CHECKPOINT or CANONICAL
        // savepoint), the engine has no prior checkpoint to reference against, so
        // {@code sharedSstFiles} MUST be empty — every reachable SST should land in
        // {@code newSstFiles}. We assert this defensively; a non-empty list under
        // no-prior-dependence would mean the engine ignored the base id and produced a
        // self-inconsistent "full" handle that still depended on the registry.
        if (noPriorDependence && !sharedSstFiles.isEmpty()) {
            throw new IllegalStateException(
                    "R35-H2 invariant violated: full snapshot (strategy="
                            + strategy
                            + ") requested with effectiveBaseCheckpointId=0, but engine reported "
                            + sharedSstFiles.size()
                            + " shared SST(s). All SSTs must be emitted as NEW; "
                            + "FULL_CHECKPOINT must not reference prior incremental SSTs.");
        }
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
            // E8-H2: bump ref-count for this checkpoint's reference, recording the rollback
            // BEFORE the register call so abort can observe it. Self-rollback fires if the
            // abort marker has already been set.
            appendAndRegister(installedList, registeredIds, checkpointId, id, h);
        }

        // ---- Wait for all uploads. ----
        StreamStateHandle metaHandle = manifestFut.get();
        for (CompletableFuture<HandleAndLocalPath> f : newSstFuts) {
            HandleAndLocalPath hlp = f.get();
            sharedHandles.add(hlp);
            StateHandleID id = new StateHandleID(hlp.getLocalPath());
            appendAndRegister(installedList, registeredIds, checkpointId, id, hlp.getHandle());
        }
        // Manifest is private — kept off the shared list. (Carried as the metaStateHandle below.)

        // ---- E5-HIGH-2: emit the StateSerializerRegistry blob as a private-state entry. ----
        // The blob is a small in-memory byte[] produced by {@link StateSerializerRegistry#serialize}.
        // We write it directly into a fresh {@link CheckpointStateOutputStream} (EXCLUSIVE scope —
        // schema registry is checkpoint-local, never shared) and bundle the resulting
        // StreamStateHandle into privateState under the well-known local path.
        //
        // E6-H3: the blob bytes were captured on the mailbox thread during
        // {@link #syncPrepareResources}, NOT read here. Calling
        // {@code provider.currentBlob()} on this async-worker thread races with concurrent
        // {@code register()} writes against the live LinkedHashMap (CME / torn read →
        // corrupt registry blob). {@link ForStRsSnapshotResources#getRegistryBlob()} returns
        // the immutable snapshot taken sync-phase. If no provider was wired (test-only
        // construction) or the registry was empty the entry is skipped — older checkpoints
        // that don't carry the blob remain readable by the restore-side guard, which treats
        // absence as "no metadata seeded" (matches pre-fix behavior).
        List<HandleAndLocalPath> privateStateEntries = List.of();
        byte[] registryBlob = resources.getRegistryBlob();
        if (registryBlob != null && registryBlob.length > 0) {
            StreamStateHandle registryHandle = uploadRegistryBlob(registryBlob, streamFactory);
            privateStateEntries =
                    List.of(HandleAndLocalPath.of(registryHandle, SERIALIZER_REGISTRY_LOCAL_PATH));
        }

        // ---- Build the keyed state handle. ----
        // The manifest is the dedicated metaStateHandle slot per Flink's incremental contract; the
        // engine writes a single manifest file per checkpoint, distinct from the serializer
        // registry blob now carried under privateState (E5-HIGH-2).
        //
        // R35-H2: record the EFFECTIVE base checkpoint id on the handle (0L for NO_SHARING /
        // FULL_CHECKPOINT) so any handle consumer (restore, SharedStateRegistry registration,
        // retention bookkeeping) sees a self-consistent "full snapshot" rather than a "depends
        // on prior checkpoint" descriptor.
        ForStRsIncrementalKeyedStateHandle handle =
                new ForStRsIncrementalKeyedStateHandle(
                        backendIdentifier,
                        keyGroupRange,
                        resources.getCheckpointId(),
                        effectiveBaseCheckpointId,
                        /* sharedState= */ sharedHandles,
                        /* privateState= */ privateStateEntries,
                        metaHandle,
                        cfMap);
        return SnapshotResult.of(handle);
    }

    /**
     * E5-HIGH-2: write a small in-memory byte[] (the serialized {@link StateSerializerRegistry}
     * blob) into a fresh {@link CheckpointStateOutputStream} and return the resulting
     * {@link StreamStateHandle}. EXCLUSIVE scope because the registry blob is checkpoint-local —
     * SharedStateRegistry sharing is for SSTs that span checkpoints, not per-ckpt metadata.
     */
    private StreamStateHandle uploadRegistryBlob(byte[] blob, CheckpointStreamFactory streamFactory)
            throws IOException {
        try (CheckpointStateOutputStream out =
                streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE)) {
            out.write(blob, 0, blob.length);
            return out.closeAndGetHandle();
        }
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

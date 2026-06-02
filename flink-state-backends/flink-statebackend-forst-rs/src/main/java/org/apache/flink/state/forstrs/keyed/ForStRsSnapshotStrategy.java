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
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
    /**
     * R36-L2: supplier (re-)read on every {@link #doAsyncSnapshot} entry. Pre-fix we held a
     * defensive copy of the cfMap snapshot taken at strategy-construction time — any CF added
     * after the strategy was wired (e.g. a state created post-restore) would be invisible to
     * later checkpoints, silently emitting a stale cfMap on the handle. Switching to a
     * {@link Supplier} re-snapshots on each invocation, so we always reflect the backend's
     * current CF universe at the moment the checkpoint runs.
     */
    private final Supplier<Map<String, Long>> cfMapSupplier;

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
     * FRS-CKPT-NOFLUSH: when enabled, {@link #doAsyncSnapshot} captures the LIVE memtable as
     * per-CF Arrow-IPC private-state artifacts ({@code memtable-cf<id>.arrow}) and uses the
     * non-flushing checkpoint variant, instead of force-flushing the memtable into a fresh L0 SST
     * on every checkpoint. The force-flush path mints an L0 SST each interval faster than
     * size-only compaction drains it, so on heavy-join workloads (q7) the per-probe L0 fan-out
     * grows unbounded and throughput decays. Capturing the memtable as a side artifact stops the
     * L0 minting; {@link ForStRsRestoreOperation} already recognizes + replays these artifacts.
     *
     * <p>Read live per snapshot from the system property {@code forst.rs.checkpoint.noflush}
     * (default off, so behavior is byte-identical to the flushing path unless explicitly opted
     * in). A non-null {@link #noFlushOverride} (set by tests) takes precedence.
     */
    private volatile Boolean noFlushOverride = null;

    /**
     * Test hook: force the no-flush checkpoint mode on ({@code true}) or off ({@code false}),
     * bypassing the {@code forst.rs.checkpoint.noflush} system property. {@code null} restores
     * property-driven behavior.
     */
    public void setNoFlushCheckpointForTesting(Boolean noFlush) {
        this.noFlushOverride = noFlush;
    }

    private boolean isNoFlushCheckpoint() {
        Boolean override = this.noFlushOverride;
        return override != null ? override : Boolean.getBoolean("forst.rs.checkpoint.noflush");
    }

    /** Best-effort recursive delete of a local staging directory; never throws. */
    private static void deleteDirQuietly(Path dir) {
        if (dir == null) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = java.nio.file.Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    java.nio.file.Files.deleteIfExists(p);
                                } catch (IOException ignored) {
                                    // best-effort
                                }
                            });
        } catch (IOException | RuntimeException ignored) {
            // best-effort: staging dir lives under the OS temp dir and is bounded per checkpoint.
        }
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

    /** Completed-checkpoint contribution ledger, released on checkpoint subsumption. */
    private final java.util.concurrent.ConcurrentHashMap<Long, List<HandleAndLocalPath>>
            completedRegistrations = new java.util.concurrent.ConcurrentHashMap<>();

    private final Object checkpointRegistrationLock = new Object();

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

    /**
     * Back-compat constructor: a static {@link Map} is wrapped in a {@link Supplier} that
     * returns a defensive copy on each call so the caller-supplied map cannot be mutated by
     * later writes. Prefer the supplier overload for new call sites — it lets the backend
     * surface CFs added after strategy construction.
     */
    public ForStRsSnapshotStrategy(
            ForStRsLinker linker,
            FrsDb db,
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            ForStRsSstRegistry sstRegistry,
            ForStRsSstUploader uploader,
            Arena nativeArena,
            Map<String, Long> cfMap) {
        this(
                linker,
                db,
                backendIdentifier,
                keyGroupRange,
                sstRegistry,
                uploader,
                nativeArena,
                staticCfMapSupplier(cfMap));
    }

    /**
     * R36-L2 primary constructor: caller passes a {@link Supplier} that is re-invoked on every
     * {@link #doAsyncSnapshot} call so the emitted handle reflects the live CF universe at the
     * moment of the checkpoint, not the universe at strategy-construction time.
     */
    public ForStRsSnapshotStrategy(
            ForStRsLinker linker,
            FrsDb db,
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            ForStRsSstRegistry sstRegistry,
            ForStRsSstUploader uploader,
            Arena nativeArena,
            Supplier<Map<String, Long>> cfMapSupplier) {
        this.linker = linker;
        this.db = db;
        this.backendIdentifier = backendIdentifier;
        this.keyGroupRange = keyGroupRange;
        this.sstRegistry = sstRegistry;
        this.uploader = uploader;
        this.nativeArena = nativeArena;
        this.cfMapSupplier = cfMapSupplier;
    }

    private static Supplier<Map<String, Long>> staticCfMapSupplier(Map<String, Long> cfMap) {
        // Snapshot the input once so the supplier is stable under caller-side mutation; copy on
        // each call so the strategy itself cannot observe surprising in-place edits.
        Map<String, Long> frozen = new LinkedHashMap<>(cfMap);
        return () -> new LinkedHashMap<>(frozen);
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

    public List<HandleAndLocalPath> completeCheckpoint(long checkpointId) {
        synchronized (checkpointRegistrationLock) {
            recordCompletedCheckpoint(checkpointId);
            List<HandleAndLocalPath> completed = takePendingRegistrations(checkpointId);
            if (completed != null && !completed.isEmpty()) {
                completedRegistrations.merge(
                        checkpointId,
                        completed,
                        (oldList, newList) -> {
                            ArrayList<HandleAndLocalPath> merged =
                                    new ArrayList<>(oldList.size() + newList.size());
                            merged.addAll(oldList);
                            merged.addAll(newList);
                            return merged;
                        });
            }
            return completed;
        }
    }

    public List<HandleAndLocalPath> takeCompletedRegistrationsForSubsumed(long checkpointId) {
        synchronized (checkpointRegistrationLock) {
            List<HandleAndLocalPath> list = completedRegistrations.remove(checkpointId);
            if (list == null) {
                return java.util.Collections.emptyList();
            }
            synchronized (list) {
                return new ArrayList<>(list);
            }
        }
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
            SnapshotUploadTracker uploadTracker = new SnapshotUploadTracker();
            boolean registeredTracker = false;
            try {
                if (registry != null) {
                    registry.registerCloseable(uploadTracker);
                    registeredTracker = true;
                }
                SnapshotResult<KeyedStateHandle> result =
                        doAsyncSnapshot(
                                resources, streamFactory, checkpointOptions, uploadTracker);
                if (isCheckpointAborted(checkpointId)) {
                    throw new CancellationException(
                            "Checkpoint " + checkpointId + " was aborted during async snapshot");
                }
                uploadTracker.commit();
                if (registeredTracker) {
                    registry.unregisterCloseable(uploadTracker);
                }
                return result;
            } catch (Throwable t) {
                rollbackPendingRegistrationsForFailedSnapshot(checkpointId);
                if (registeredTracker && registry != null) {
                    registry.unregisterCloseable(uploadTracker);
                }
                uploadTracker.close();
                throw t;
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
        synchronized (checkpointRegistrationLock) {
            if (completedRegistrations.containsKey(checkpointId)) {
                return null;
            }
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
        List<HandleAndLocalPath> late;
        synchronized (checkpointRegistrationLock) {
            if (completedRegistrations.containsKey(checkpointId)) {
                return java.util.Collections.emptyList();
            }
            late = pendingRegistrations.remove(checkpointId);
        }
        if (late == null) {
            return java.util.Collections.emptyList();
        }
        synchronized (late) {
            List<HandleAndLocalPath> copy = new ArrayList<>(late);
            late.clear();
            return copy;
        }
    }

    void rollbackPendingRegistrationsForFailedSnapshot(long checkpointId) {
        List<HandleAndLocalPath> rollback;
        synchronized (checkpointRegistrationLock) {
            rollback = pendingRegistrations.remove(checkpointId);
        }
        if (rollback == null) {
            return;
        }
        List<HandleAndLocalPath> copy;
        synchronized (rollback) {
            copy = new ArrayList<>(rollback);
            rollback.clear();
        }
        for (HandleAndLocalPath h : copy) {
            sstRegistry.unregister(new StateHandleID(h.getLocalPath()));
        }
        if (!abortedCheckpoints.containsKey(checkpointId)) {
            clearAbortMarker(checkpointId);
        }
    }

    public boolean isCheckpointAborted(long checkpointId) {
        return abortedCheckpoints.containsKey(checkpointId);
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
        // fail the in-flight snapshot. Silently skipping the register would let the async phase
        // return a handle whose shared SSTs have no local registry contribution.
        if (abortedCheckpoints.containsKey(checkpointId)) {
            throw new CancellationException(
                    "Checkpoint " + checkpointId + " was aborted before SST registration");
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
                throw new CancellationException(
                        "Checkpoint " + checkpointId + " was aborted during SST registration");
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
            CheckpointOptions checkpointOptions,
            SnapshotUploadTracker uploadTracker)
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

        // ---- FRS-CKPT-NOFLUSH: optionally capture the LIVE memtable as Arrow artifacts. ----
        // MVCC ordering: capture the memtable BEFORE the checkpoint enumerates the SST set — both
        // pinned at resources.getSnapshot()'s sequence. If a background flush fires in the window,
        // the flushed rows land in BOTH the artifact and a new L0 SST; replay preserves (key, seq)
        // so the duplicate is idempotent (newest-seq-wins) — a dup, never a loss. The reverse
        // order would risk losing rows that flushed out of the memtable after enumeration.
        boolean noFlush = isNoFlushCheckpoint();
        Path memtableStagingDir = null;
        if (noFlush) {
            memtableStagingDir =
                    java.nio.file.Files.createTempDirectory(
                            "frs-ckpt-memtable-" + resources.getCheckpointId() + "-");
            linker.snapshotMemtablesToDir(
                    db, resources.getSnapshot(), memtableStagingDir.toString());
        }

        // ---- Compute the incremental checkpoint (SST enumeration; flush only when !noFlush). ----
        // This was moved out of the sync phase to avoid blocking the task thread
        // during checkpoint barriers. The snapshot pins all versions at the captured
        // seq so concurrent writes do not affect correctness.
        MemorySegment result = nativeArena.allocate(32);
        try {
            if (noFlush) {
                linker.createIncrementalCheckpointAtNoflush(
                        db,
                        resources.getSnapshot(),
                        resources.getCheckpointId(),
                        effectiveBaseCheckpointId,
                        result);
            } else {
                linker.createIncrementalCheckpointAt(
                        db,
                        resources.getSnapshot(),
                        resources.getCheckpointId(),
                        effectiveBaseCheckpointId,
                        result);
            }
        } catch (RuntimeException re) {
            deleteDirQuietly(memtableStagingDir);
            throw re;
        }

        Path manifestPath = readCString(result, 0L);
        List<Path> newSstFiles = readSstList(result, PTR);
        List<Path> sharedSstFiles = readSstList(result, 2 * PTR);

        // ---- R36-M2: hoist the no-prior-dependence invariant check BEFORE any side-effects.
        // Pre-fix the check ran AFTER uploader.upload was queued for the manifest + new SSTs
        // AND after the pendingRegistrations.merge installed a rollback list. On an engine bug
        // (sharedSstFiles non-empty under effectiveBaseCheckpointId=0) we threw IllegalStateException
        // from the middle of the method, leaving orphan upload futures running into S3 and an
        // orphan rollback list in pendingRegistrations that no abort handler would ever drain.
        // Asserting immediately after createIncrementalCheckpointAt + readSstList lets us bail
        // out before either side-effect, leaving the state machine clean.
        if (noPriorDependence && !sharedSstFiles.isEmpty()) {
            throw new IllegalStateException(
                    "R35-H2 invariant violated: full snapshot (strategy="
                            + strategy
                            + ") requested with effectiveBaseCheckpointId=0, but engine reported "
                            + sharedSstFiles.size()
                            + " shared SST(s). All SSTs must be emitted as NEW; "
                            + "FULL_CHECKPOINT must not reference prior incremental SSTs.");
        }

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
                trackedUpload(
                        manifestPath,
                        streamFactory,
                        CheckpointedStateScope.EXCLUSIVE,
                        uploadTracker);

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
                    trackedUpload(p, streamFactory, newSstScope, uploadTracker)
                            .thenApply(h -> HandleAndLocalPath.of(h, localPath));
            newSstFuts.add(f);
        }

        // ---- FRS-CKPT-NOFLUSH: upload the captured memtable artifacts under EXCLUSIVE scope. ----
        // Memtable artifacts are checkpoint-local (each checkpoint captures its own live memtable;
        // they are never shared across checkpoints), so they belong on privateState under
        // EXCLUSIVE scope — exactly like the manifest + registry blob. The restore side filters
        // them by the memtable-cf<id>.arrow local-path convention and replays them after open.
        List<CompletableFuture<HandleAndLocalPath>> memtableArtFuts = new ArrayList<>();
        if (noFlush && memtableStagingDir != null) {
            try (java.util.stream.Stream<Path> arts =
                    java.nio.file.Files.list(memtableStagingDir)) {
                List<Path> artFiles =
                        arts.filter(
                                        p -> {
                                            String n = p.getFileName().toString();
                                            return n.startsWith("memtable-cf")
                                                    && n.endsWith(".arrow");
                                        })
                                .sorted()
                                .collect(java.util.stream.Collectors.toList());
                for (Path art : artFiles) {
                    String localPath = art.getFileName().toString();
                    memtableArtFuts.add(
                            trackedUpload(
                                            art,
                                            streamFactory,
                                            CheckpointedStateScope.EXCLUSIVE,
                                            uploadTracker)
                                    .thenApply(h -> HandleAndLocalPath.of(h, localPath)));
                }
            } catch (IOException ioe) {
                deleteDirQuietly(memtableStagingDir);
                throw ioe;
            }
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
        // R35-H2: the no-prior-dependence sharedSstFiles invariant was already asserted above
        // (R36-M2) immediately after createIncrementalCheckpointAt returned — well before any
        // uploader.upload or pendingRegistrations.merge side-effect, so a violating engine state
        // throws without leaving orphan S3 uploads or orphan rollback lists behind.
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
        // R36-H1: under NO_SHARING (fullCheckpoint=true) SSTs were already uploaded under
        // EXCLUSIVE scope by R35-H2, AND the handle MUST be self-contained — the SSTs cannot
        // participate in cross-checkpoint sharing because the snapshot's whole point is to be
        // independent of any other checkpoint. Pre-R36-H1 we placed them in sharedHandles,
        // which made AbstractIncrementalStateHandle.registerSharedStates() register every
        // localPath via SharedStateRegistryKey on JM-side registration; a future incremental
        // ckpt that re-emits the same localPath would then collide on the registry key
        // (legitimate sharing turning into an unintended adoption of the prior NO_SHARING
        // upload). Route them through privateState instead — registerSharedStates iterates
        // sharedState only, so no registration fires; discardState drops them per-checkpoint
        // along with the manifest + registry blob.
        //
        // Additionally, the local sstRegistry / pendingRegistrations.appendAndRegister bumps
        // are skipped for NO_SHARING SSTs: those entries exist to track cross-checkpoint
        // reuse via takePendingRegistrations on abort, but a NO_SHARING SST has no reuse
        // path — keeping it in the local registry would either leak ref-counts forever or
        // confuse a sibling incremental that happens to emit the same localPath later.
        //
        // For all OTHER strategies (FORWARD / FORWARD_BACKWARD) SSTs were uploaded SHARED
        // and the original sharedHandles + appendAndRegister flow is preserved.
        StreamStateHandle metaHandle = manifestFut.get();
        List<HandleAndLocalPath> noSharingPrivateSsts =
                fullCheckpoint ? new ArrayList<>(newSstFuts.size()) : null;
        for (CompletableFuture<HandleAndLocalPath> f : newSstFuts) {
            HandleAndLocalPath hlp = f.get();
            if (fullCheckpoint) {
                noSharingPrivateSsts.add(hlp);
            } else {
                sharedHandles.add(hlp);
                StateHandleID id = new StateHandleID(hlp.getLocalPath());
                appendAndRegister(installedList, registeredIds, checkpointId, id, hlp.getHandle());
            }
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
        // R36-H1: privateStateEntries collects (a) the optional registry-metadata blob and
        // (b) any NO_SHARING SSTs we diverted off sharedHandles above. Order: SSTs first,
        // then the registry blob (deterministic for the restore-side scan that splits on the
        // well-known {@link #SERIALIZER_REGISTRY_LOCAL_PATH} name).
        List<HandleAndLocalPath> privateStateEntries;
        if (noSharingPrivateSsts != null && !noSharingPrivateSsts.isEmpty()) {
            privateStateEntries = new ArrayList<>(noSharingPrivateSsts.size() + 1);
            privateStateEntries.addAll(noSharingPrivateSsts);
        } else {
            privateStateEntries = new ArrayList<>(1);
        }
        byte[] registryBlob = resources.getRegistryBlob();
        if (registryBlob != null && registryBlob.length > 0) {
            StreamStateHandle registryHandle = uploadRegistryBlob(registryBlob, streamFactory);
            uploadTracker.trackHandle(registryHandle);
            privateStateEntries.add(
                    HandleAndLocalPath.of(registryHandle, SERIALIZER_REGISTRY_LOCAL_PATH));
        }

        // ---- FRS-CKPT-NOFLUSH: resolve + attach memtable artifacts, then clean up staging. ----
        // f.get() blocks until each artifact is fully uploaded by the uploader; only then is it
        // safe to delete the local staging dir the engine wrote them into. The artifacts join
        // privateState alongside the registry blob and any NO_SHARING SSTs.
        if (!memtableArtFuts.isEmpty()) {
            try {
                for (CompletableFuture<HandleAndLocalPath> f : memtableArtFuts) {
                    privateStateEntries.add(f.get());
                }
            } finally {
                deleteDirQuietly(memtableStagingDir);
            }
        } else {
            deleteDirQuietly(memtableStagingDir);
        }

        // ---- Build the keyed state handle. ----
        // The manifest is the dedicated metaStateHandle slot per Flink's incremental contract; the
        // engine writes a single manifest file per checkpoint, distinct from the serializer
        // registry blob now carried under privateState (E5-HIGH-2).
        //
        // FRS-CKPT-HANDLE-MIGRATION (2026-06-01): emit the STANDARD Flink {@link
        // IncrementalRemoteKeyedStateHandle} rather than the custom {@code
        // ForStRsIncrementalKeyedStateHandle}. {@link ForStRsRestoreOperation} was already
        // migrated to accept only the standard handle; producing the custom subtype here left a
        // snapshot→restore round-trip break (restore rejected the type at its instanceof gate).
        // The two custom-only fields are intentionally dropped:
        //   - baseCheckpointId: incremental file-sharing across checkpoints is driven by Flink's
        //     SharedStateRegistry (shared SSTs are registered by local path and de-duplicated
        //     against prior checkpoints), so the explicit base-id descriptor is redundant. The
        //     `effectiveBaseCheckpointId` computed above still gates the strict full-vs-incremental
        //     sharing logic; it simply is not persisted on the handle.
        //   - cfMap: the engine recovers all column-family info FROM THE MANIFEST at restore time
        //     (see ForStRsRestoreOperation), so the CF map is no longer carried on the wire — no
        //     production consumer of it remains.
        IncrementalRemoteKeyedStateHandle handle =
                new IncrementalRemoteKeyedStateHandle(
                        backendIdentifier,
                        keyGroupRange,
                        resources.getCheckpointId(),
                        /* sharedState= */ sharedHandles,
                        /* privateState= */ privateStateEntries,
                        metaHandle);
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

    private CompletableFuture<StreamStateHandle> trackedUpload(
            Path file,
            CheckpointStreamFactory streamFactory,
            CheckpointedStateScope scope,
            SnapshotUploadTracker uploadTracker) {
        CompletableFuture<StreamStateHandle> future = uploader.upload(file, streamFactory, scope);
        uploadTracker.track(future);
        return future;
    }

    /**
     * Tracks async upload futures/handles until the checkpoint handle is fully built. Closing the
     * tracker means the checkpoint has failed or been cancelled, so all pending futures are
     * cancelled and every already-created handle is discarded. Once {@link #commit()} runs, Flink's
     * returned snapshot owns the handles and subsequent registry closure must not discard them.
     */
    private static final class SnapshotUploadTracker implements java.io.Closeable {
        private final Object lock = new Object();
        private final List<CompletableFuture<StreamStateHandle>> futures = new ArrayList<>();
        private final List<StreamStateHandle> handles = new ArrayList<>();
        private boolean closed;
        private boolean committed;

        void track(CompletableFuture<StreamStateHandle> future) {
            synchronized (lock) {
                if (closed && !committed) {
                    future.cancel(true);
                    return;
                }
                futures.add(future);
            }
            future.whenComplete(
                    (handle, failure) -> {
                        if (handle == null) {
                            return;
                        }
                        boolean discardNow;
                        synchronized (lock) {
                            discardNow = closed && !committed;
                            if (!discardNow) {
                                handles.add(handle);
                            }
                        }
                        if (discardNow) {
                            discardQuietly(handle);
                        }
                    });
        }

        void trackHandle(StreamStateHandle handle) {
            boolean discardNow;
            synchronized (lock) {
                discardNow = closed && !committed;
                if (!discardNow) {
                    handles.add(handle);
                }
            }
            if (discardNow) {
                discardQuietly(handle);
            }
        }

        void commit() {
            synchronized (lock) {
                if (closed) {
                    throw new CancellationException(
                            "ForSt-RS snapshot upload tracker was closed before commit");
                }
                committed = true;
                futures.clear();
                handles.clear();
            }
        }

        @Override
        public void close() {
            List<CompletableFuture<StreamStateHandle>> futuresToCancel;
            List<StreamStateHandle> handlesToDiscard;
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                if (committed) {
                    return;
                }
                futuresToCancel = new ArrayList<>(futures);
                handlesToDiscard = new ArrayList<>(handles);
                handles.clear();
            }
            for (CompletableFuture<StreamStateHandle> future : futuresToCancel) {
                future.cancel(true);
            }
            for (StreamStateHandle handle : handlesToDiscard) {
                discardQuietly(handle);
            }
        }

        private static void discardQuietly(StreamStateHandle handle) {
            try {
                handle.discardState();
            } catch (Exception ignored) {
                // Best-effort cleanup for failed/cancelled checkpoint uploads.
            }
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
     *
     * <p><b>R38-L1 — NUL-terminator FFI contract:</b> the inner {@code pathPtr} is reinterpreted as
     * an unbounded segment so {@code getString(0L)} can walk to the first NUL byte. This
     * <em>trusts</em> the Rust side ({@code FrsLiveFile.path}) to NUL-terminate every UTF-8 path
     * it emits — a missing NUL would read past the allocation and either crash the JVM or leak
     * arbitrary native bytes into the returned path. The proper fix is to extend the FFI
     * struct to carry an explicit length and switch to {@code reinterpret(len).asByteBuffer()};
     * deferred because the change is invasive (cbindgen + every consumer). DO NOT remove the
     * NUL-termination guarantee on the Rust side without first landing the length-prefixed
     * variant of the struct.
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

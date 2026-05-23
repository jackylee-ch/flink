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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.v2.internal.InternalKeyedState;
import org.apache.flink.state.forstrs.VectorizedExecutor;
import org.apache.flink.state.forstrs.exec.IterLifetimeWatchdog;
import org.apache.flink.state.forstrs.exec.SlotArenaScope;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsAbi;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.metrics.DispatchMetrics;
import org.apache.flink.state.forstrs.state.ForStRsAggregatingStateV2;
import org.apache.flink.state.forstrs.state.ForStRsAsyncAggregatingStateV2;
import org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2;
import org.apache.flink.state.forstrs.state.ForStRsAsyncReducingStateV2;
import org.apache.flink.state.forstrs.state.ForStRsMapStateV2;
import org.apache.flink.state.forstrs.state.ForStRsReducingStateV2;
import org.apache.flink.state.forstrs.state.ForStRsValueStateV2;
import org.apache.flink.state.forstrs.state.StateSerializerRegistry;
import org.apache.flink.state.forstrs.state.ttl.TtlAwareValueStateV2;
import org.apache.flink.state.forstrs.state.ttl.TtlClock;
import org.apache.flink.state.forstrs.state.ttl.TtlSerializer;
import org.apache.flink.state.forstrs.state.ttl.TtlValue;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;
import org.apache.flink.state.forstrs.timer.ForStRsKeyGroupedInternalPriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Internal
public class ForStRsAsyncKeyedStateBackend<K> implements AsyncKeyedStateBackend<K> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ForStRsAsyncKeyedStateBackend.class);

    private static final long DEFAULT_SLOT_TURN_BYTES = 8L * 1024 * 1024;
    private static final long DEFAULT_SLOT_CACHE_BYTES = 64L * 1024 * 1024;

    private final Arena arena;
    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle defaultCf;
    private final TypeSerializer<K> keySerializer;
    private final KeyGroupRange keyGroupRange;
    private final int totalKeyGroups;
    private final boolean ownsResources;

    /**
     * Timer-service backing-store selector. HEAP uses Flink's in-memory {@link
     * HeapPriorityQueueSetFactory}; FORSTRS uses the engine-backed {@link
     * ForStRsKeyGroupedInternalPriorityQueue}.
     *
     * <p>D-R4-1: default is {@code FORSTRS} — the new batched off-heap variant which post-PR-B*
     * lands 1.x× faster than HEAP on Q11/Q12 with the engine-backed batched flush. The earlier
     * {@code HEAP} default was the V1 workaround documented in {@code
     * project_q12_heap_timer_beats_forst} before the timer queue's pending-buffer drain was
     * batched; that workaround is no longer needed (see {@link #pickTimerFactory}, which now
     * defaults the system property to {@code FORSTRS}).
     *
     * <p>Override via {@code -Dforst.rs.timer-service.factory=HEAP} for compatibility with
     * snapshots taken before the batched-engine timer landed, or for benchmarks that need the
     * heap fallback.
     */
    private enum TimerServiceFactory {
        HEAP,
        FORSTRS
    }

    private static TimerServiceFactory pickTimerFactory() {
        String prop =
                System.getProperty("forst.rs.timer-service.factory", "FORSTRS").trim().toUpperCase();
        return "HEAP".equals(prop) ? TimerServiceFactory.HEAP : TimerServiceFactory.FORSTRS;
    }

    // ---------------------------------------------------------------------
    // PR-M2: single-writer-thread invariant for keyed-state registries.
    //
    // All registry fields below ({@link #stateCache}, {@link #managedExecutors}, and every
    // {@code registered*StatesV2}/{@code registered*States} list) are mutated and iterated
    // exclusively from the operator's mailbox thread. Flink's async-V2 contract guarantees:
    //   • {@code setup(...)} / {@code getOrCreateKeyedState} / {@code createStateExecutor}
    //     are invoked from {@code AbstractStreamOperator.initializeState} (mailbox).
    //   • {@code snapshot(id, ts, factory, options)} is invoked from {@code
    //     prepareSnapshotPreBarrier} / checkpoint barrier alignment (mailbox).
    //   • Subsequent record-driven {@code getOrCreateKeyedState} calls (lazy state creation
    //     on first use) run from {@code processElement}, also on the mailbox thread.
    //
    // The async snapshot strategy's worker thread (PHASE 3 in {@link #snapshot}) does NOT
    // touch these registries — it operates on the engine handle (FFI) and the SST registry.
    // Therefore plain {@code ArrayList}/{@code HashMap}/{@code HashSet} is correct here; no
    // {@code CopyOnWriteArrayList} or {@code ConcurrentHashMap} is required.
    //
    // EXCEPTION: {@link #registeredTimerQueues} uses {@code CopyOnWriteArrayList} because
    // the engine-backed timer queue's {@code create(...)} entry point (which adds to the
    // registry) is callable from non-mailbox threads in the engine-FORSTRS timer-factory
    // mode (which is opt-in, not default).
    // ---------------------------------------------------------------------
    private StateRequestHandler stateRequestHandler;
    private final Map<String, InternalKeyedState<K, ?, ?>> stateCache = new HashMap<>();
    private final Set<VectorizedExecutor> managedExecutors = new HashSet<>();

    /**
     * Registry of live ReducingState V2 instances (umbrella spec §3 Trace E).
     *
     * <p>Each instance is registered on construction so that {@link #snapshot} can call {@code
     * flushOnBarrier()} to drain dirty RMW accumulators before the engine snapshot runs.
     */
    private final List<ForStRsReducingStateV2<?>> registeredReducingStates = new ArrayList<>();

    /**
     * Registry of live AggregatingState V2 instances (umbrella spec §3 Trace E).
     *
     * <p>Each instance is registered on construction so that {@link #snapshot} can call {@code
     * flushOnBarrier()} to drain dirty RMW accumulators before the engine snapshot runs.
     */
    private final List<ForStRsAggregatingStateV2<?, ?, ?>> registeredAggregatingStates =
            new ArrayList<>();

    /**
     * PR-C3 (V12 / B3-H1): registry of live async-V2 ReducingState instances. Each instance is
     * registered when the backend creates it so that {@link #snapshot} can call {@code
     * flushOnBarrier()} to drain the RMW accumulator cache before the engine snapshot runs.
     */
    private final List<ForStRsAsyncReducingStateV2<?, ?, ?>> registeredAsyncReducingStates =
            new ArrayList<>();

    /**
     * PR-C3 (V12 / B3-H2): registry of live async-V2 AggregatingState instances. Each instance is
     * registered when the backend creates it so that {@link #snapshot} can call {@code
     * flushOnBarrier()} to drain the RMW accumulator cache before the engine snapshot runs.
     */
    private final List<ForStRsAsyncAggregatingStateV2<?, ?, ?, ?, ?>>
            registeredAsyncAggregatingStates = new ArrayList<>();

    /**
     * PR-C1 (V2-8 / Z3-6 / C-H5): registry of live MapStateV2 instances so that {@link #snapshot}
     * drains every state's off-heap staging buffer to the engine BEFORE the snapshot reads from
     * the engine. Trace E barrier-drain semantics; mirrors the V1-sync {@code statebuf} flush hook
     * in {@code ForStRsKeyedStateBackend.flushValueStateBuffers} (commit b3b9d7f2a6c).
     */
    private final List<ForStRsMapStateV2<?, ?, ?, ?>> registeredMapStatesV2 = new ArrayList<>();

    /**
     * PR-C2: registry of {@link ForStRsAsyncListStateV2} instances configured with an off-heap
     * {@link org.apache.flink.state.forstrs.state.ListStateArrowBuffer}. Snapshot pre-hook drains
     * each instance's buffer via {@code frs_vec_merge_append_batch} BEFORE the engine snapshot is
     * read, so accumulated single-element {@code asyncAdd} chunks are durable.
     */
    private final List<ForStRsAsyncListStateV2<?, ?, ?>> registeredListStatesV2 = new ArrayList<>();

    /**
     * PR-A1: registry of engine-backed timer queues created by {@link #create}. Snapshot pre-hook
     * (Phase 1) calls {@link ForStRsKeyGroupedInternalPriorityQueue#flushPendingToEngine} on each
     * registered queue so the pending timer-buffer is drained to the engine memtable before the
     * snapshot strategy enumerates files. Mirrors the V1-sync pattern in {@link
     * ForStRsAbstractKeyedStateBackend#engineTimerQueues}.
     */
    private final List<ForStRsKeyGroupedInternalPriorityQueue<?>> registeredTimerQueues =
            new CopyOnWriteArrayList<>();

    /**
     * PR-A1: backend identifier published in {@link
     * org.apache.flink.runtime.state.IncrementalKeyedStateHandle#getBackendIdentifier}. Stable for
     * the lifetime of this backend instance; on restore the same identifier MUST be carried over so
     * SharedStateRegistry can resolve the SST handles. Generated lazily because tests construct
     * backends without a restore handle and we don't want the cost on the constructor's hot path.
     */
    private volatile UUID backendIdentifier;

    /**
     * PR-A1: lazily-constructed snapshot strategy. Built on the first {@link #snapshot} call so
     * tests that never invoke snapshot don't pay the cost. Reuses the same {@code
     * ForStRsSnapshotStrategy} machinery as the V1-sync backend — that strategy drives {@code
     * frs_create_incremental_checkpoint_at} via FFM, uploads the manifest + new SSTs, and returns
     * a {@link ForStRsIncrementalKeyedStateHandle}.
     */
    @Nullable private volatile ForStRsSnapshotStrategy snapshotStrategy;

    /**
     * PR-A1: SST registry shared between the snapshot strategy and {@link
     * #notifyCheckpointComplete}/{@link #notifyCheckpointAborted}. Manages ref-counts for shared
     * (cross-checkpoint) SST handles. Lazily initialized alongside {@link #snapshotStrategy}.
     */
    @Nullable private volatile ForStRsSstRegistry sstRegistry;

    /**
     * PR-A1: dedicated cancel-stream registry passed to {@link SnapshotStrategyRunner} so the
     * checkpoint coordinator can abort an in-flight async snapshot via {@link
     * SnapshotStrategyRunner#snapshot}. Owned by this backend; closed in {@link #dispose}.
     */
    private final CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

    private SlotArenaScope slotArenaScope;
    private IterLifetimeWatchdog iterWatchdog;
    private boolean disposed = false;

    /**
     * R15-H1: set once {@link #close()} begins so {@link #snapshot} can short-circuit any racing
     * checkpoint request rather than registering a fresh future against an arena that is about
     * to close. Pure best-effort — the contractually correct race is for the coordinator to
     * stop issuing checkpoints to a backend that is being torn down, but if it does we still
     * must not enqueue work that the arena-close will then UAF.
     */
    private volatile boolean closing = false;

    /**
     * R15-H1: registry of in-flight async-snapshot {@link RunnableFuture}s. Populated in {@link
     * #snapshot} when the snapshot strategy runs ASYNCHRONOUSLY (periodic checkpoint path); the
     * future self-removes from this set when it completes (success or failure).
     *
     * <p>{@link #close()} iterates this set and either (a) waits up to {@link
     * #CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS}ms per future for completion, or (b) calls
     * {@code cancel(true)} if the future is still incomplete after the timeout. Only after every
     * outstanding future is resolved does {@code close()} proceed to {@code arena.close()},
     * which closes the native arena backing every {@link
     * org.apache.flink.state.forstrs.ffm.FrsSnapshot} held by a snapshot worker.
     *
     * <p>Pre-fix, {@code close()} aborted the cancel-stream registry but did not await
     * outstanding workers — a worker that had not yet executed {@code resources.release()}
     * could call {@code snapshot.close()} on a {@code MemorySegment} whose backing
     * {@code nativeArena} was already closed, surfacing as a UAF on the native FrsSnapshot
     * struct (R15-H1 root cause).
     *
     * <p>{@link ConcurrentHashMap#newKeySet()} provides lock-free add/remove suitable for the
     * mailbox-thread {@code snapshot()} and worker-thread completion callback to share without
     * synchronization on the backend instance.
     */
    private final Set<RunnableFuture<?>> outstandingSnapshots =
            ConcurrentHashMap.newKeySet();

    /** R15-H1: per-future await budget on {@link #close()}. 5s matches the spec. */
    private static final long CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS = 5_000L;

    /**
     * E8-H4: identity of the (JobID, operatorIdentifier) slot this backend occupies in
     * {@link ForStRsBackendPathInvariant}. Captured at factory time and used on {@link
     * #dispose()} to release the slot so a subsequent job redeploy / restart on the same
     * operator can re-register without a false-positive cross-path violation. {@code null} if
     * the factory did not wire the identity (tests, non-runtime construction).
     */
    private org.apache.flink.api.common.JobID backendPathJobId;

    private String backendPathOperatorId;

    /**
     * E8-H4: wire the {@link ForStRsBackendPathInvariant} identity so {@link #dispose()} can
     * release the slot. Called once by the factory site immediately after construction.
     */
    public void setBackendPathIdentity(
            org.apache.flink.api.common.JobID jobId, String operatorIdentifier) {
        this.backendPathJobId = jobId;
        this.backendPathOperatorId = operatorIdentifier;
    }

    /**
     * Tracks the current key on the operator's mailbox thread (PR-A4 / S1-5 fix).
     *
     * <p>Flink's async-state V2 routes per-record keys through {@link RecordContext}; the runtime
     * invokes {@link #switchContext(RecordContext)} on the mailbox thread before dispatching state
     * requests for a record. We capture the current key here so the engine-backed timer queue can
     * derive the correct {@code currentKeyGroup} on each {@code peek/poll/advance} — without this,
     * the queue saw only timers in the constant {@code keyGroupRange.getStartKeyGroup()} (the
     * E2-CRIT-2 bug).
     *
     * <p>Read by an inline {@link InternalKeyContext} view passed to {@link
     * ForStRsKeyGroupedInternalPriorityQueue}.
     */
    @Nullable private volatile K currentKey;

    /**
     * E6-M1 / E7-M1: volatile for the same publication guarantees as {@link #currentKey}. The
     * {@code currentKeyGroup} is mutated on the mailbox thread (via {@code switchContext}) but
     * read from the snapshot pre-flush lambda — when that lambda runs off-mailbox (e.g.
     * async-snapshot prepare), a non-volatile int could observe a stale group via the JMM (no
     * happens-before edge between the mailbox writer and the snapshot reader without the
     * volatile). A stale group routes the timer queue's peek/poll into the wrong key-group
     * slice — the E2-CRIT-2 family of bugs in latent form.
     */
    private volatile int currentKeyGroup = -1;

    /**
     * View of this backend's per-record key/key-group state as an {@link InternalKeyContext}.
     * Constructed lazily in {@link #internalKeyContext()} so it can be injected into the timer
     * queue. The view is read-only for the queue's purposes (writes occur via {@code
     * switchContext}).
     */
    @Nullable private InternalKeyContext<K> internalKeyContext;

    /**
     * Per-backend dispatch metrics (umbrella spec §1 §c, component 8).
     *
     * <p>Placeholder: initialized with {@link UnregisteredMetricsGroup} until the backend
     * constructor is extended to accept a real {@link MetricGroup} from the Flink runtime. Phase P5
     * will inject MetricGroup via constructor once the backend is fully integrated.
     */
    private final DispatchMetrics dispatchMetrics;

    /**
     * PR-A11 (E3-HIGH-4): registry of {@code TypeSerializerSnapshot}s for every registered keyed
     * state. Populated on first {@code getOrCreateKeyedState} for a given state name; PR-A1's
     * snapshot path will drain this into the checkpoint blob (see {@link StateSerializerRegistry}
     * for the format spec).
     *
     * <p>On restore (after PR-A1 lands and calls {@link StateSerializerRegistry#seedFromRestore}),
     * the next session's {@code getOrCreateKeyedState} verifies the new {@code TypeSerializer}
     * against the persisted snapshot via {@link
     * org.apache.flink.api.common.typeutils.TypeSerializerSnapshot#resolveSchemaCompatibility} and
     * throws {@link org.apache.flink.util.StateMigrationException} on incompatibility.
     */
    private final StateSerializerRegistry stateSerializerRegistry = new StateSerializerRegistry();

    /**
     * PR-A7 (S1-12): processing-time clock used by the TTL decorators wrapping per-state-type
     * results when {@code desc.getTtlConfig().isEnabled()}. Default {@link TtlClock#SYSTEM};
     * overridable from tests via {@link #setTtlClockForTesting(TtlClock)}.
     */
    private volatile TtlClock ttlClock = TtlClock.SYSTEM;

    /** PR-A7 test hook: replace the TTL clock with a deterministic source. */
    @VisibleForTesting
    public void setTtlClockForTesting(TtlClock clock) {
        this.ttlClock = clock;
    }

    public ForStRsAsyncKeyedStateBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle defaultCf,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            boolean ownsResources) {
        this(
                arena,
                linker,
                db,
                defaultCf,
                keySerializer,
                keyGroupRange,
                keyGroupRange.getNumberOfKeyGroups(),
                ownsResources);
    }

    public ForStRsAsyncKeyedStateBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle defaultCf,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            int totalKeyGroups,
            boolean ownsResources) {
        FrsAbi.verifyAgainst(linker::frsAbiVersion);
        this.arena = arena;
        this.linker = linker;
        this.db = db;
        this.defaultCf = defaultCf;
        this.keySerializer = keySerializer;
        this.keyGroupRange = keyGroupRange;
        this.totalKeyGroups = totalKeyGroups;
        this.ownsResources = ownsResources;
        this.slotArenaScope =
                SlotArenaScope.openForSlot(DEFAULT_SLOT_TURN_BYTES, DEFAULT_SLOT_CACHE_BYTES);
        this.iterWatchdog = new IterLifetimeWatchdog(slotArenaScope);
        this.iterWatchdog.start();
        // Phase P5: replace UnregisteredMetricsGroup with real MetricGroup from the
        // TaskExecutorEnvironment / RuntimeEnvironment once the backend is fully integrated.
        this.dispatchMetrics = new DispatchMetrics(new UnregisteredMetricsGroup());
    }

    /**
     * PR-A4-H4: restore-aware factory. Materializes the engine from {@code restoredHandles} (via
     * {@link ForStRsRestoreOperation}'s parallel-restore path — PR-E1), then returns a fresh async
     * backend wired to the restored {@link FrsDb} + default CF + SST registry + backend identifier
     * so subsequent incremental snapshots correctly link against the restored shared SSTs.
     *
     * <p>Before this method existed the async backend was write-only: PR-A1 landed the WRITE side
     * of the snapshot machinery, but {@link
     * org.apache.flink.runtime.state.StateBackend#createKeyedStateBackend} on a restart was
     * opening a fresh empty engine on disk — every restored handle was silently discarded. This
     * factory closes that gap by mirroring the V1-sync {@code
     * ForStRsStateBackend.createKeyedStateBackend} restore branch onto the async path.
     *
     * <p>The factory:
     *
     * <ol>
     *   <li>Constructs {@link ForStRsRestoreOperation} bound to a fresh {@link
     *       ForStRsSstRegistry}.
     *   <li>Calls {@link ForStRsRestoreOperation#restore} which downloads each handle's manifest +
     *       SSTs in parallel (PR-E1) and invokes {@link ForStRsLinker#dbOpenFromIncremental} to
     *       reconstruct the LSM from the manifest.
     *   <li>Re-publishes the restored handle's backend identifier so {@link
     *       org.apache.flink.runtime.state.SharedStateRegistry} can resolve the post-restore
     *       incremental snapshot's shared-SST handles. If multiple handles are present (rescaling)
     *       we mint a fresh identifier — the SSTs were reshuffled across key groups so the
     *       resulting engine is logically a new lineage.
     *   <li>Seeds the {@link StateSerializerRegistry} so the first {@code getOrCreateKeyedState}
     *       call in the new session verifies the new {@link TypeSerializer} against the persisted
     *       snapshot. The serializer-metadata blob persistence in {@code snapshot()} is still
     *       documented TODO (see snapshot() Javadoc), so this seed is best-effort: pre-A11
     *       snapshots carry no metadata and the registry stays empty (writes are correct on a
     *       fresh registration). When PR-A11-emit lands, this hook fires verification.
     * </ol>
     *
     * @param arena owned arena that survives the returned backend
     * @param linker FFI linker bound to {@code arena}
     * @param keySerializer key serializer
     * @param keyGroupRange the target key-group range for this subtask
     * @param totalKeyGroups total number of key groups in the job
     * @param localDbPath empty local directory to materialize the engine into
     * @param restoredHandles handles produced by a prior {@code snapshot()}; must be non-empty
     *     ({@link ForStRsStateBackend#createAsyncKeyedStateBackend} routes empty-handle cases
     *     through the no-restore path)
     * @throws IOException on download or engine-open failure
     */
    public static <K> ForStRsAsyncKeyedStateBackend<K> restoreFromHandles(
            Arena arena,
            ForStRsLinker linker,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            int totalKeyGroups,
            Path localDbPath,
            Collection<KeyedStateHandle> restoredHandles)
            throws IOException {
        if (restoredHandles == null || restoredHandles.isEmpty()) {
            throw new IllegalArgumentException(
                    "restoreFromHandles requires a non-empty handle collection; route empty-restore"
                            + " through the regular constructor + dbOpen() path");
        }
        ForStRsSstRegistry restoredSstRegistry = new ForStRsSstRegistry();
        ForStRsRestoreOperation restoreOp =
                new ForStRsRestoreOperation(
                        linker, arena, localDbPath, keyGroupRange, restoredSstRegistry);
        // A5-M1: accumulate db + cf references separately from {@code restored} so the catch
        // path can close them BEFORE the caller (createAsyncKeyedStateBackend) closes the
        // arena. Pre-fix, a throw inside the backend constructor or any of the post-restore
        // wiring leaked the engine-side FrsDb + default CF; the caller's catch path only
        // closed the arena, which dropped the FFI shared-segment ownership but did NOT issue
        // {@code dbClose}/{@code cfClose} on the open engine handle.
        FrsDb db = null;
        FrsCfHandle cf = null;
        try {
            ForStRsRestoreOperation.RestoreResult restored = restoreOp.restore(restoredHandles);
            db = restored.getDb();
            cf = restored.getDefaultCf();

            ForStRsAsyncKeyedStateBackend<K> backend =
                    new ForStRsAsyncKeyedStateBackend<>(
                            arena,
                            linker,
                            db,
                            cf,
                            keySerializer,
                            keyGroupRange,
                            totalKeyGroups,
                            /* ownsResources= */ true);

            // Preserve the source backend identifier when restoring from a single non-rescaled
            // handle so SharedStateRegistry resolves the prior session's shared SSTs. On
            // rescaling (multiple source handles or differing source range) we leave
            // backendIdentifier null and let ensureSnapshotStrategy mint a fresh UUID — the
            // restored LSM is a new lineage.
            UUID inherited = inheritBackendIdentifier(restoredHandles, keyGroupRange);
            if (inherited != null) {
                backend.backendIdentifier = inherited;
            }

            // Repopulate the SST registry from the restored handles so post-restore
            // incremental snapshots reuse SSTs already on S3 without re-uploading.
            backend.adoptSstRegistry(restoredSstRegistry);

            // E5-HIGH-2: seed the serializer registry from the restored handle's
            // {@code _serializer_metadata.bin} private-state entry. ForStRsRestoreOperation has
            // already parsed the blob (via StateSerializerRegistry.deserialize) so we just hand
            // the decoded map straight to the registry. Pre-E5 snapshots carry no blob and
            // restoreOp returns an empty map — seedFromRestore stays a no-op in that case, which
            // matches the documented "fresh state in this session" branch of verifyOrRegister.
            backend.stateSerializerRegistry.seedFromRestore(
                    new LinkedHashMap<>(restored.getRestoredSerializerMetadata()));

            return backend;
        } catch (Throwable t) {
            // Best-effort tear-down on restore failure. Order matters: CF closes BEFORE DB
            // because the engine pins CF handles to the open db. Arena ownership is the
            // caller's; we leave it for createAsyncKeyedStateBackend's catch to close so we
            // don't double-close on the propagated exception path.
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
            if (t instanceof IOException io) {
                throw io;
            }
            if (t instanceof RuntimeException re) {
                throw re;
            }
            if (t instanceof Error err) {
                throw err;
            }
            throw new IOException("ForStRsAsyncKeyedStateBackend.restoreFromHandles failed", t);
        }
    }

    /**
     * Returns the source backend identifier when {@code handles} is a single
     * {@link ForStRsIncrementalKeyedStateHandle} whose key-group range exactly matches the target
     * — i.e. the no-rescaling fast path. Otherwise returns {@code null} so the caller mints a
     * fresh identifier.
     */
    @Nullable
    private static UUID inheritBackendIdentifier(
            Collection<KeyedStateHandle> handles, KeyGroupRange target) {
        if (handles.size() != 1) {
            return null;
        }
        KeyedStateHandle only = handles.iterator().next();
        if (!(only instanceof ForStRsIncrementalKeyedStateHandle)) {
            return null;
        }
        ForStRsIncrementalKeyedStateHandle inc = (ForStRsIncrementalKeyedStateHandle) only;
        if (!inc.getKeyGroupRange().equals(target)) {
            return null;
        }
        return inc.getBackendIdentifier();
    }

    /**
     * Pre-populate the lazy SST registry slot so the first {@code snapshot()} reuses it instead of
     * minting a fresh empty one. Called from {@link #restoreFromHandles} after {@link
     * ForStRsRestoreOperation} has registered the restored shared-SST entries. Idempotent and only
     * effective before the first {@code snapshot()} call.
     */
    private void adoptSstRegistry(ForStRsSstRegistry restoredRegistry) {
        // ensureSnapshotStrategy synchronizes on `this`; align our lazy-init under the same lock.
        synchronized (this) {
            if (this.sstRegistry == null) {
                this.sstRegistry = restoredRegistry;
            }
        }
    }

    @Override
    public void setup(@Nonnull StateRequestHandler h) {
        this.stateRequestHandler = h;
    }

    /**
     * PR-A4 / S1-5 fix: capture the current key whenever Flink's async-state runtime switches the
     * mailbox context to a new record. The captured key is consumed by the
     * engine-backed-timer-queue's {@code currentKeyGroupSupplier} so peek/poll/advance route to the
     * correct key group instead of the constant {@code keyGroupRange.getStartKeyGroup()}.
     *
     * <p>Invoked from the mailbox thread; reads from {@link #currentKey} on that same thread are
     * race-free. The volatile qualifier guards against the snapshot pre-flush path (which can be
     * triggered via {@code Snapshotable.snapshot} from a different worker before the mailbox owns
     * the queue again).
     */
    @Override
    public void switchContext(@Nullable RecordContext<K> context) {
        if (context == null) {
            this.currentKey = null;
            this.currentKeyGroup = -1;
        } else {
            this.currentKey = context.getKey();
            this.currentKeyGroup = context.getKeyGroup();
        }
    }

    /**
     * Returns an {@link InternalKeyContext} view of this backend's current key + key group, lazily
     * constructed on first request. The view is read by the engine-backed-timer queue and is
     * safe to call from the mailbox thread.
     */
    @VisibleForTesting
    InternalKeyContext<K> internalKeyContext() {
        InternalKeyContext<K> view = internalKeyContext;
        if (view != null) {
            return view;
        }
        view =
                new InternalKeyContext<K>() {
                    @Override
                    public K getCurrentKey() {
                        return currentKey;
                    }

                    @Override
                    public int getCurrentKeyGroupIndex() {
                        return currentKeyGroup;
                    }

                    @Override
                    public int getNumberOfKeyGroups() {
                        return totalKeyGroups;
                    }

                    @Override
                    public KeyGroupRange getKeyGroupRange() {
                        return keyGroupRange;
                    }

                    @Override
                    public void setCurrentKey(@Nonnull K newKey) {
                        ForStRsAsyncKeyedStateBackend.this.currentKey = newKey;
                    }

                    @Override
                    public void setCurrentKeyGroupIndex(int newKeyGroupIndex) {
                        ForStRsAsyncKeyedStateBackend.this.currentKeyGroup = newKeyGroupIndex;
                    }
                };
        internalKeyContext = view;
        return view;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N, S extends State, SV> S getOrCreateKeyedState(
            N ns, TypeSerializer<N> nsSer, StateDescriptor<SV> desc) throws Exception {
        InternalKeyedState<K, ?, ?> existing = stateCache.get(desc.getStateId());
        if (existing != null) {
            return (S) existing;
        }
        // PR-A11 (E3-HIGH-4): persist the user's TypeSerializerSnapshot alongside the engine's
        // state data so the next session can detect schema drift. On a restored session (after
        // PR-A1 wires seedFromRestore), this verify call routes COMPATIBLE_AS_IS through and
        // throws StateMigrationException on INCOMPATIBLE per Flink's standard contract.
        // For now, on a fresh session, this is equivalent to a write-only register call.
        stateSerializerRegistry.verifyOrRegister(
                desc.getStateId(), desc.getType().ordinal(), desc.getSerializer());
        // PR-A7 (S1-12): if the descriptor has TTL enabled, wrap the inner state in a TTL
        // decorator. Pre-A7 the TtlConfig was silently dropped on the floor and TTL never fired.
        // STORAGE FORMAT BREAK: TTL-enabled state cells now carry an 8-byte expiry prefix;
        // enabling TTL on existing non-TTL data is not snapshot-compatible.
        S created;
        if (desc.getTtlConfig() != null && desc.getTtlConfig().isEnabled()) {
            created = createTtlAwareStateInternal(ns, nsSer, desc);
        } else {
            created = createStateInternal(ns, nsSer, desc);
        }
        stateCache.put(desc.getStateId(), (InternalKeyedState<K, ?, ?>) created);
        return created;
    }

    /**
     * PR-A7 (S1-12): build a TTL-decorated state. Only {@code VALUE} is supported in this PR;
     * other state types throw {@link UnsupportedOperationException} with a follow-on PR pointer
     * — this is intentional and replaces the pre-A7 silent-drop behavior with a loud failure.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <N, S extends State, SV> S createTtlAwareStateInternal(
            N ns, TypeSerializer<N> nsSer, StateDescriptor<SV> desc) throws Exception {
        String name = desc.getStateId();
        switch (desc.getType()) {
            case VALUE:
                {
                    TypeSerializer<SV> userSerializer = desc.getSerializer();
                    TtlSerializer<SV> ttlSerializer = new TtlSerializer<>(userSerializer);
                    // Construct the inner state with the TtlSerializer (so the engine sees
                    // [long expiry][value bytes] as the cell payload) and wrap it.
                    ForStRsValueStateV2<K, N, TtlValue<SV>> innerState =
                            new ForStRsValueStateV2<>(
                                    stateRequestHandler,
                                    name,
                                    keySerializer,
                                    nsSer,
                                    ttlSerializer);
                    TtlAwareValueStateV2<K, N, SV> wrapped =
                            new TtlAwareValueStateV2<>(innerState, desc.getTtlConfig(), ttlClock);
                    return (S) wrapped;
                }
            case MAP:
            case LIST:
            case REDUCING:
            case AGGREGATING:
                throw new UnsupportedOperationException(
                        "PR-A7: TTL is wired for ValueState V2 in this PR. "
                                + desc.getType()
                                + " state TTL is deferred to follow-on PR (decorator + per-entry "
                                + "expiry filter on iteration paths). State name: "
                                + name
                                + ". Disable TTL or use ValueState in the interim.");
            default:
                throw new UnsupportedOperationException("Unsupported state type: " + desc.getType());
        }
    }

    /**
     * PR-A11 (E3-HIGH-4): expose the registry for PR-A1's snapshot/restore wiring. Until PR-A1
     * lands the registry is consulted only on the write path; tests use this accessor to
     * pre-seed restored metadata and exercise the read/verify branch.
     */
    @VisibleForTesting
    public StateSerializerRegistry stateSerializerRegistry() {
        return stateSerializerRegistry;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    @Override
    public <N, S extends InternalKeyedState, SV> S createStateInternal(
            @Nonnull N ns, @Nonnull TypeSerializer<N> nsSer, @Nonnull StateDescriptor<SV> desc)
            throws Exception {
        String name = desc.getStateId();
        // PR-A2 (S1-4 / E2-CRIT-1): forward `nsSer` to every V2 state constructor so the
        // composite storage key includes the request namespace. Pre-A2 keys lacked the namespace
        // suffix, causing all windows for the same (key, stateName) to collide on one cell.
        // NOTE: this is a hard binary format break vs v3.x snapshots — restoring an old
        // snapshot will surface as missing keys (loud nulls), not silent state corruption.
        // See RELEASE-NOTES: "v4.0 keyed-state binary format is incompatible with v3.x".
        switch (desc.getType()) {
            case VALUE:
                return (S)
                        new ForStRsValueStateV2<>(
                                stateRequestHandler,
                                name,
                                keySerializer,
                                nsSer,
                                desc.getSerializer());
            case MAP:
                var mapDesc = (MapStateDescriptor<?, ?>) desc;
                // PR-C1: hand linker/db/cf to the MapStateV2 so it can stage writes in its
                // off-heap MapStateArrowBuffer and drain via linker.batchPut on flush. Register
                // the instance so the snapshot pre-hook drains every live MapState V2.
                ForStRsMapStateV2<K, N, ?, ?> mapStateV2 =
                        new ForStRsMapStateV2<>(
                                stateRequestHandler,
                                name,
                                keySerializer,
                                nsSer,
                                mapDesc.getUserKeySerializer(),
                                mapDesc.getSerializer(),
                                linker,
                                db,
                                defaultCf);
                registeredMapStatesV2.add(mapStateV2);
                return (S) mapStateV2;
            case LIST:
                var listDesc = (ListStateDescriptor<?>) desc;
                // V3.2 (V20 sub-spec §5): register the ListState name with every managed
                // executor so the per-batch classifier routes LIST_ADD via APPEND_MERGE.
                for (VectorizedExecutor exec : managedExecutors) {
                    exec.registerListState(name);
                }
                // PR-C2: hand linker/db/cf + a fresh per-state ListStateArrowBuffer to the
                // AsyncListStateV2 so the classifier's APPEND_MERGE routing goes through the
                // off-heap fast path (skips the heap-byte[] AppendMergeBatchBuffer). Register
                // the instance so the snapshot pre-hook drains its accumulator.
                org.apache.flink.state.forstrs.state.ListStateArrowBuffer listBuf =
                        new org.apache.flink.state.forstrs.state.ListStateArrowBuffer();
                ForStRsAsyncListStateV2<K, N, ?> listStateV2 =
                        new ForStRsAsyncListStateV2<>(
                                stateRequestHandler,
                                name,
                                keySerializer,
                                nsSer,
                                listDesc.getSerializer(),
                                listBuf,
                                linker,
                                db,
                                defaultCf);
                registeredListStatesV2.add(listStateV2);
                return (S) listStateV2;
            case REDUCING:
                return (S) createReducingState(name, nsSer, desc);
            case AGGREGATING:
                return (S) createAggregatingState(name, nsSer, desc);
            default:
                throw new UnsupportedOperationException("Unsupported: " + desc.getType());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <N> ForStRsAsyncReducingStateV2<K, N, ?> createReducingState(
            String name, TypeSerializer<N> nsSer, StateDescriptor<?> desc) {
        ReducingStateDescriptor reducingDesc = (ReducingStateDescriptor) desc;
        ForStRsAsyncReducingStateV2<K, N, ?> state =
                new ForStRsAsyncReducingStateV2<>(
                        stateRequestHandler,
                        name,
                        keySerializer,
                        nsSer,
                        reducingDesc.getSerializer(),
                        reducingDesc.getReduceFunction());
        // PR-C3 (V12 / B3-H1): wire the production flush handler so accumulators captured by the
        // RMW cache are actually durable on checkpoint. Closes A4-H2 — the previous default of
        // {@code (k,v) -> {}} silently discarded every cached accumulator at snapshot time. We use
        // the direct {@code linker.put}/{@code linker.delete} path (engine is the durability
        // target) instead of synthesizing StateRequest objects, because the original
        // RecordContext is gone at flush time and a synthetic context would only carry the
        // operator key — exactly what {@code linker.put} already takes as a raw key.
        state.setFlushHandler(this::rmwFlushToEngine);
        registeredAsyncReducingStates.add(state);
        return state;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <N> ForStRsAsyncAggregatingStateV2<K, N, ?, ?, ?> createAggregatingState(
            String name, TypeSerializer<N> nsSer, StateDescriptor<?> desc) {
        AggregatingStateDescriptor aggDesc = (AggregatingStateDescriptor) desc;
        ForStRsAsyncAggregatingStateV2<K, N, ?, ?, ?> state =
                new ForStRsAsyncAggregatingStateV2<>(
                        stateRequestHandler,
                        name,
                        keySerializer,
                        nsSer,
                        aggDesc.getSerializer(),
                        aggDesc.getAggregateFunction());
        // PR-C3 (V12 / B3-H2): wire the production flush handler — see createReducingState above
        // for the A4-H2 rationale.
        state.setFlushHandler(this::rmwFlushToEngine);
        registeredAsyncAggregatingStates.add(state);
        return state;
    }

    /**
     * Production flush handler for {@link ForStRsAsyncReducingStateV2} and
     * {@link ForStRsAsyncAggregatingStateV2} RMW caches (A4-H2 fix).
     *
     * <p>Called once per dirty cache entry during {@code flushOnBarrier()} on the snapshot mailbox
     * thread (Trace E PHASE 1.d), with the composite-key bytes and the serialized accumulator
     * bytes. {@code null}/empty accumulator bytes mean "cleared" — route to a DELETE; otherwise
     * PUT directly through the engine via the FFM linker. Bypassing the classifier here is
     * intentional and safe: the snapshot pre-flush has already drained the in-flight executor
     * batches in PHASE 1.a, and PHASE 2 will run {@code flushDirty()} again to fold these direct
     * PUTs into the SST being snapshotted in PHASE 3.
     */
    private void rmwFlushToEngine(byte[] key, byte[] value) {
        if (value == null || value.length == 0) {
            linker.delete(db, defaultCf, key);
        } else {
            linker.put(db, defaultCf, key, value);
        }
    }

    @Nonnull
    @Override
    public StateExecutor createStateExecutor() {
        var e = new VectorizedExecutor(linker, db, defaultCf, arena);
        e.setDispatchMetrics(dispatchMetrics);
        managedExecutors.add(e);
        return e;
    }

    /** Returns the per-backend dispatch metrics. Exposed for testing and monitoring integration. */
    public DispatchMetrics dispatchMetrics() {
        return dispatchMetrics;
    }

    /**
     * Registers a {@link ForStRsReducingStateV2} instance so that {@link #snapshot} includes it in
     * the Trace E barrier drain (§3 Trace E).
     *
     * <p>Called by {@code ForStRsReducingStateV2} on construction when a backend reference is
     * available. V1: callers pass {@code this} backend reference after construction.
     *
     * @param state the ReducingState V2 instance to register
     */
    public void registerReducingState(ForStRsReducingStateV2<?> state) {
        registeredReducingStates.add(state);
    }

    /**
     * Registers a {@link ForStRsAggregatingStateV2} instance so that {@link #snapshot} includes it
     * in the Trace E barrier drain (§3 Trace E).
     *
     * <p>Called by {@code ForStRsAggregatingStateV2} on construction when a backend reference is
     * available. V1: callers pass {@code this} backend reference after construction.
     *
     * @param state the AggregatingState V2 instance to register
     */
    public void registerAggregatingState(ForStRsAggregatingStateV2<?, ?, ?> state) {
        registeredAggregatingStates.add(state);
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    /**
     * Takes a checkpoint snapshot (PR-A1 / PR-A8 / PR-A9).
     *
     * <h3>Trace E: RMW barrier drain + engine snapshot sequence</h3>
     *
     * <pre>
     *   PHASE 1 — flush in-flight V2 dispatch + per-state buffers + timers:
     *     executor.flushDirty()         // memtable → L0 SST via FFM linker.flush(db)
     *     mapStateV2.flushOffHeapBuffer()  // PR-C1: drain MapStateArrowBuffer staging
     *     listStateV2.flushPreSnapshot()   // PR-C2: drain ListStateArrowBuffer accumulator
     *     reducingV2.flushOnBarrier()      // PR-C3 (V1 + V2): drain RMW cache
     *     aggregatingV2.flushOnBarrier()   // PR-C3 (V1 + V2): drain RMW cache
     *     timerQueue.flushPendingToEngine()  // engine-backed timer pending-buffer
     *
     *   PHASE 2 — second flushDirty to drain the PUTs enqueued by RMW cache flushes.
     *
     *   PHASE 3 — engine snapshot via FFI:
     *     ForStRsSnapshotStrategy.syncPrepareResources(id)  // dbSnapshot pin
     *     → asyncSnapshot returns SnapshotResultSupplier   // uploads SSTs, returns handle
     *
     *   PHASE 4 — branch on SnapshotType (PR-A9):
     *     CheckpointType.CHECKPOINT       → incremental (SHARED scope SSTs)
     *     CheckpointType.FULL_CHECKPOINT  → incremental (engine handles strategy)
     *     SavepointType.savepoint(...)    → incremental + TODO canonical-format follow-on PR
     *     SavepointType.terminate(...)    → SYNC_SAVEPOINT semantics (PR-A8): full drain await
     * </pre>
     *
     * <h3>PR-A8 stop --savepoint correctness</h3>
     *
     * <p>When the runtime issues a {@code stop --savepoint} command, it sets the checkpoint
     * options to {@link SavepointType#isSynchronous() synchronous} (TERMINATE / SUSPEND). Until
     * this PR, that flag was ignored and the snapshot pre-flush ran identically to a periodic
     * checkpoint — meaning any in-flight V2 state requests racing the snapshot would be lost on
     * restart. Now we make the sync path:
     *
     * <ol>
     *   <li>run the multi-phase drain twice (already the path) AND
     *   <li>execute the snapshot strategy via {@link SnapshotExecutionType#SYNCHRONOUS} so the
     *       returned future is pre-run before this method returns. The mailbox thread therefore
     *       blocks until every state mutation up to the barrier is durable.
     * </ol>
     *
     * <h3>PR-A9 CheckpointOptions branching</h3>
     *
     * <p>{@link CheckpointOptions#getCheckpointType()} now selects the {@link
     * org.apache.flink.runtime.state.CheckpointedStateScope} via the strategy's {@link
     * org.apache.flink.runtime.checkpoint.SnapshotType.SharingFilesStrategy}. For a savepoint, the
     * V1.1 implementation still emits {@link ForStRsIncrementalKeyedStateHandle} — emitting
     * canonical Flink savepoint format (loadable by community ForSt) is a follow-on PR; we
     * preserve the type information on the handle so restore can verify compatibility.
     *
     * <h3>What is NOT yet drained (TODOs for follow-on PRs)</h3>
     *
     * <ul>
     *   <li>{@code StateSerializerRegistry.metadataBuffer} — PR-A11 staged the registry but the
     *       snapshot does not yet serialize the registry blob into the metaHandle. Restore-side
     *       PR-A11 still uses {@code seedFromRestore}. Follow-on: emit
     *       {@code stateSerializerRegistry.serialize()} as a private-state entry.
     *   <li>{@link #ttlClock} state — PR-A7 TTL decorator writes per-cell expiry timestamps to
     *       the engine value layout, so the clock itself is stateless; nothing extra to persist.
     * </ul>
     */
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long id, long ts, @Nonnull CheckpointStreamFactory f, @Nonnull CheckpointOptions o)
            throws Exception {
        SnapshotType ctype = o.getCheckpointType();
        boolean isSavepoint = ctype.isSavepoint();
        boolean isSync = isSavepoint && ((SavepointType) ctype).isSynchronous();

        // E5-HIGH-1 / E6-HIGH-1: Canonical savepoint format ({@link SavepointFormatType#CANONICAL},
        // the {@link SavepointFormatType#DEFAULT}) requires emitting a backend-portable handle that
        // community ForSt / RocksDB can load. ForSt-RS only emits its incremental native format
        // today, so silently accepting a canonical savepoint request would produce a non-portable
        // handle that operators discover only at restore time. Fail fast at the request site with
        // a clear remediation: the operator must explicitly opt into NATIVE format (either via
        // the CLI {@code --type native} flag or by passing {@link SavepointFormatType#NATIVE}
        // programmatically) until the canonical-emit PR lands. The guard is delegated to {@link
        // ForStRsSavepointGuards} so the V1-sync {@code ForStRsAbstractKeyedStateBackend.snapshot}
        // entry point uses the identical contract — E6-HIGH-1 fixed the V1-sync regression where
        // the gate only existed in this async path.
        //
        // E4-HIGH-1: For NATIVE savepoints we still emit a {@link ForStRsIncrementalKeyedStateHandle}
        // — the format is backend-native by definition, so this is correct. We retain the WARN so
        // operators can see savepoint events in the log; only the canonical-handle gap throws.
        ForStRsSavepointGuards.rejectCanonicalSavepoint(o);
        if (isSavepoint) {
            LOG.warn(
                    "ForStRsAsyncKeyedStateBackend: NATIVE savepoint requested for checkpoint"
                            + " id={} (synchronous={}). The emitted KeyedStateHandle is the"
                            + " incremental ForSt-RS native format — durable and restorable by"
                            + " ForSt-RS itself, but not loadable by other backends. For"
                            + " cross-backend portability, the canonical-emit PR is still TODO.",
                    id,
                    isSync);
        }

        // ============================================================
        // PHASE 1 — drain in-flight V2 dispatch + state buffers + timers
        // ============================================================
        // PHASE 1.a: flush all in-flight batches already queued in managed executors. After this
        // call returns the engine's memtable has been folded to an L0 SST (PR-A1 made flushDirty
        // call linker.flush) so the snapshot strategy's file enumeration is complete.
        managedExecutors.forEach(VectorizedExecutor::flushDirty);

        // PHASE 1.b: PR-C1 — drain each MapStateV2's off-heap staging buffer to the engine via
        // batchPut + tombstone deletes BEFORE the engine snapshot reads. Mirrors the V1-sync
        // statebuf flush hook (commit b3b9d7f2a6c).
        for (ForStRsMapStateV2<?, ?, ?, ?> ms : registeredMapStatesV2) {
            ms.flushOffHeapBuffer();
        }
        // PHASE 1.c: PR-C2 — drain ListStateV2 off-heap accumulator via a single {@code
        // frs_vec_merge_append_batch} FFI. Without this hook, accumulated single-element
        // {@code asyncAdd} chunks would still be in the off-heap arena at snapshot time and
        // would be lost on restore.
        for (ForStRsAsyncListStateV2<?, ?, ?> ls : registeredListStatesV2) {
            ls.flushPreSnapshot();
        }

        // PHASE 1.d: drain RMW caches (Reducing/Aggregating V1 + V2). Each {@code
        // flushOnBarrier()} serializes accumulated reduce/aggregate results and enqueues PUT
        // requests to the classifier — those PUTs are picked up by PHASE 2's flushDirty pass.
        for (ForStRsReducingStateV2<?> s : registeredReducingStates) {
            s.flushOnBarrier();
        }
        for (ForStRsAggregatingStateV2<?, ?, ?> s : registeredAggregatingStates) {
            s.flushOnBarrier();
        }
        for (ForStRsAsyncReducingStateV2<?, ?, ?> s : registeredAsyncReducingStates) {
            s.flushOnBarrier();
        }
        for (ForStRsAsyncAggregatingStateV2<?, ?, ?, ?, ?> s : registeredAsyncAggregatingStates) {
            s.flushOnBarrier();
        }

        // PHASE 1.e: PR-A1 — drain each engine-backed timer queue's pending-buffer to the engine.
        // Mirrors the V1-sync engineTimerQueues drain in
        // ForStRsAbstractKeyedStateBackend.snapshot() (spec invariant #4 in the batched-timer
        // design). HEAP timer-factory mode registers no queues and this loop is a no-op.
        for (ForStRsKeyGroupedInternalPriorityQueue<?> q : registeredTimerQueues) {
            q.flushPendingToEngine();
        }

        // ============================================================
        // PHASE 2 — second flushDirty to drain the PUTs enqueued by RMW cache flushes
        // ============================================================
        managedExecutors.forEach(VectorizedExecutor::flushDirty);

        // ============================================================
        // PHASE 3 — engine snapshot via FFI through ForStRsSnapshotStrategy
        // ============================================================
        // Lazily construct the strategy on first snapshot. We pass a synthetic single-CF map
        // ("default" → 0) because the async-V2 path keeps every state in the default column
        // family; multi-CF wiring is a follow-on PR (V20).
        ForStRsSnapshotStrategy strategy = ensureSnapshotStrategy();
        try {
            // R15-H1: if {@link #close()} is in progress, refuse to enqueue new snapshot work
            // — the arena that backs every FrsSnapshot is about to close, and any future we
            // register here would race the arena teardown.
            if (closing) {
                return DoneFuture.of(SnapshotResult.empty());
            }
            // PR-A8: SYNC_SAVEPOINT semantics. Synchronous execution blocks until the future is
            // pre-run, so by the time this method returns every state mutation up to the barrier
            // is durable on S3. For periodic checkpoints we use ASYNCHRONOUS so the mailbox
            // thread continues processing records while the upload completes in a virtual
            // thread.
            SnapshotExecutionType execType =
                    isSync ? SnapshotExecutionType.SYNCHRONOUS : SnapshotExecutionType.ASYNCHRONOUS;
            RunnableFuture<SnapshotResult<KeyedStateHandle>> future =
                    new SnapshotStrategyRunner<>(
                                    isSavepoint
                                            ? "ForStRs-async-savepoint"
                                            : "ForStRs-async-incremental-snapshot",
                                    strategy,
                                    cancelStreamRegistry,
                                    execType)
                            .snapshot(id, ts, f, o);
            // R15-H1: register the future so {@link #close()} can await its completion before
            // closing the native arena. For SYNCHRONOUS savepoints the future is already done
            // by the time {@code .snapshot()} returns (SnapshotStrategyRunner pre-runs the
            // FutureTask), so tracking is harmless (close() sees done=true and skips the
            // await). For ASYNCHRONOUS checkpoints the worker thread runs concurrently with
            // the mailbox and MUST be awaited at close() to prevent the
            // R15-H1 use-after-free on the nativeArena.
            return trackSnapshot(future);
        } catch (IOException e) {
            // The cancelStreamRegistry may already be closed if a prior checkpoint's async phase
            // failed or the task is being cancelled. In that case the SnapshotStrategyRunner
            // cannot register its cancellation hook and throws "Cannot register Closeable,
            // registry is already closed." This is the only documented benign path: the
            // coordinator already gave up on this checkpoint, so a pre-completed empty future
            // unblocks the mailbox without hanging the job. Every other IOException — S3
            // failures, manifest write failures, FFI errors surfaced as IOException — MUST
            // propagate so the checkpoint coordinator's failedCheckpoints counter increments
            // and `tolerable-failed-checkpoints` accounting fires (A4-H3 fix).
            //
            // E5-HIGH-3: precondition the empty-result fallback on
            // {@code cancelStreamRegistry.isClosed()} rather than substring-matching the
            // exception message. The previous match on {@code "registry is already closed"}
            // was brittle: a downstream change to {@link CloseableRegistry}'s rejection
            // message (or a translated locale) would have silently flipped real failures into
            // {@code SnapshotResult.empty()} and bypassed checkpoint-failure accounting.
            // {@link AbstractAutoCloseableRegistry#isClosed()} is the structural precondition
            // and is stable across Flink versions.
            if (cancelStreamRegistry.isClosed()) {
                return DoneFuture.of(SnapshotResult.empty());
            }
            throw e;
        }
        // Other Exception subtypes (RuntimeException / FFI panics surfaced as Exception via
        // FrsBackendException) propagate without catch: they signal real backend failure and
        // MUST be observed by the coordinator. A4-H3: pre-fix, the prior catch(Exception) swallow
        // returned SnapshotResult.empty() which Flink interpreted as a successful empty snapshot
        // bypassing tolerable-failed-checkpoints accounting. The contract is now: the snapshot
        // method throws on real failure, returns a RunnableFuture on success — and the runtime
        // routes the throw to CheckpointFailureManager.handleCheckpointException.
    }

    /**
     * PR-A1: lazily construct the snapshot strategy + SST registry. Thread-safe via
     * double-checked locking on {@link #snapshotStrategy}.
     */
    private ForStRsSnapshotStrategy ensureSnapshotStrategy() {
        ForStRsSnapshotStrategy s = snapshotStrategy;
        if (s != null) {
            return s;
        }
        synchronized (this) {
            s = snapshotStrategy;
            if (s != null) {
                return s;
            }
            if (backendIdentifier == null) {
                backendIdentifier = UUID.randomUUID();
            }
            // PR-A4-H4: if a restore pre-populated the SST registry (via adoptSstRegistry), reuse
            // it so the first post-restore incremental snapshot recognises the inherited shared
            // SSTs and skips re-uploading them. Otherwise mint a fresh registry as before.
            ForStRsSstRegistry reg = this.sstRegistry;
            if (reg == null) {
                reg = new ForStRsSstRegistry();
            }
            // V1 wiring: default uploader retry policy. PR-A12 will inject a configured
            // AsyncRetryStrategy here for bounded S3 retries.
            ForStRsSstUploader uploader = new ForStRsSstUploader();
            // Single default CF for the async-V2 path (V20 multi-CF wiring lands in a follow-on).
            Map<String, Long> cfMap = new LinkedHashMap<>();
            cfMap.put("default", 0L);
            s =
                    new ForStRsSnapshotStrategy(
                            linker,
                            db,
                            backendIdentifier,
                            keyGroupRange,
                            reg,
                            uploader,
                            arena,
                            cfMap);
            // E5-HIGH-2: wire the serializer-registry blob provider so each snapshot's
            // privateState carries the current schema metadata. Restore reads it back via
            // ForStRsRestoreOperation + seedFromRestore so the next session can detect schema
            // drift across the snapshot/restore boundary.
            s.setRegistryBlobProvider(stateSerializerRegistry::serialize);
            this.sstRegistry = reg;
            this.snapshotStrategy = s;
            return s;
        }
    }

    /**
     * PR-A1 test accessor — exposes the lazily-constructed snapshot strategy so tests can verify
     * the engine-FFI integration without forcing a real S3 upload. Returns {@code null} before
     * the first {@link #snapshot} call.
     */
    @VisibleForTesting
    @Nullable
    public ForStRsSnapshotStrategy snapshotStrategyForTesting() {
        return snapshotStrategy;
    }

    /**
     * PR-A1 test accessor — exposes the SST registry. Returns {@code null} before the first
     * {@link #snapshot} call.
     */
    @VisibleForTesting
    @Nullable
    public ForStRsSstRegistry sstRegistryForTesting() {
        return sstRegistry;
    }

    @Override
    public void notifyCheckpointComplete(long id) {
        // PR-A1: tell the strategy so the next snapshot uses this id as base_checkpoint_id for
        // its incremental delta calculation. Idempotent and monotonic — accumulateAndGet picks
        // the max of (existing, id).
        ForStRsSnapshotStrategy s = snapshotStrategy;
        if (s != null) {
            s.recordCompletedCheckpoint(id);
        }
        managedExecutors.forEach(VectorizedExecutor::flushDirty);
    }

    @Override
    public void notifyCheckpointAborted(long id) {
        // PR-A1: roll back the SST registry ref-count contribution of the aborted checkpoint.
        // Only entries registered specifically for this checkpoint are decremented — completed
        // checkpoints' baseline shared SSTs remain intact.
        ForStRsSnapshotStrategy s = snapshotStrategy;
        ForStRsSstRegistry reg = sstRegistry;
        if (s != null && reg != null) {
            var regs = s.takePendingRegistrationsForAbort(id);
            if (regs != null) {
                for (var hlp : regs) {
                    reg.unregister(
                            new org.apache.flink.runtime.state.StateHandleID(hlp.getLocalPath()));
                }
            }
            // E8-H2 post-abort drain: catch any entries that landed after the initial take.
            var late = s.drainLatePendingRegistrations(id);
            for (var hlp : late) {
                reg.unregister(
                        new org.apache.flink.runtime.state.StateHandleID(hlp.getLocalPath()));
            }
        }
    }

    @Override
    public void notifyCheckpointSubsumed(long id) {}

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String n, @Nonnull TypeSerializer<T> s) {
        return create(n, s, false);
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String n,
                    @Nonnull TypeSerializer<T> s,
                    boolean allowFutureMetadataUpdates) {
        if (pickTimerFactory() == TimerServiceFactory.HEAP) {
            // HEAP timer factory — see TimerServiceFactory javadoc. Engine-backed timer queue
            // remains available via -Dforst.rs.timer-service.factory=FORSTRS.
            return new HeapPriorityQueueSetFactory(keyGroupRange, totalKeyGroups, 128).create(n, s);
        }
        // PR-A4 / S1-5 (E2-CRIT-2 fix): plumb the backend's InternalKeyContext into the queue so
        // peek/poll/advance route to the current key's key group, not a constant. The view reads
        // {@code currentKey} (captured on the mailbox thread by {@link #switchContext}) and hashes
        // it via {@link KeyGroupRangeAssignment#assignToKeyGroup}; if no current key is set (e.g.
        // before any record has arrived) it falls back to {@code keyGroupRange.getStartKeyGroup()}
        // — same as the legacy constant behaviour.
        ForStRsKeyGroupedInternalPriorityQueue<T> queue =
                new ForStRsKeyGroupedInternalPriorityQueue<>(
                        linker,
                        db,
                        defaultCf,
                        arena,
                        n,
                        s,
                        element -> {
                            if (element
                                    instanceof
                                    org.apache.flink.streaming.api.operators.InternalTimer<?, ?>
                                            timer) {
                                return timer.getTimestamp();
                            }
                            return 0L;
                        },
                        internalKeyContext(),
                        totalKeyGroups,
                        keyGroupRange);
        // PR-A1: register so snapshot()'s Phase 1.e drains the pending-buffer to the engine
        // before the FFI checkpoint enumerates files.
        registeredTimerQueues.add(queue);
        return queue;
    }

    @Override
    public String getBackendTypeIdentifier() {
        return "forst-rs-async";
    }

    /**
     * Returns the per-slot Arena scope. Throws {@link IllegalStateException} if called after {@link
     * #dispose()} so stale callers fail loudly.
     */
    public SlotArenaScope slotArenaScope() {
        if (slotArenaScope == null) {
            throw new IllegalStateException("Backend disposed");
        }
        return slotArenaScope;
    }

    @Override
    public void dispose() {
        if (disposed) {
            return;
        }
        disposed = true;
        managedExecutors.forEach(VectorizedExecutor::flushDirty);
        managedExecutors.forEach(VectorizedExecutor::shutdown);
        managedExecutors.clear();
        // D5-H2: release each MapStateV2's MapStateCache arena BEFORE clearing the state cache
        // and dropping registry references. Pre-fix, the cache's Arena.ofShared() (and its 5
        // off-heap segments) survived to JVM exit — one perma-leak per V2 MapState instance.
        for (ForStRsMapStateV2<?, ?, ?, ?> ms : registeredMapStatesV2) {
            try {
                ms.close();
            } catch (Throwable ignored) {
                // Per-state close is best-effort on dispose; a failure here must not block
                // subsequent state tear-down (engine close, arena close).
            }
        }
        registeredMapStatesV2.clear();
        stateCache.clear();
        if (iterWatchdog != null) {
            iterWatchdog.stop();
            iterWatchdog = null;
        }
        if (slotArenaScope != null) {
            slotArenaScope.closeSlot();
            slotArenaScope = null;
        }
        // PR-A1: close the cancel-stream registry so any in-flight async snapshots are aborted.
        try {
            cancelStreamRegistry.close();
        } catch (IOException ignored) {
            // best-effort close on dispose
        }
        // E8-H4: release the path-invariant slot so a subsequent job redeploy / restart on the
        // same (jobId, operatorIdentifier) can re-register without a false-positive cross-path
        // block. Best-effort — never throws to the caller.
        if (backendPathOperatorId != null) {
            try {
                ForStRsBackendPathInvariant.removeBackendPath(
                        backendPathJobId, backendPathOperatorId);
            } catch (Throwable ignored) {
            }
            backendPathOperatorId = null;
            backendPathJobId = null;
        }
    }

    @Override
    public void close() throws IOException {
        // R15-H1: mark closing BEFORE dispose() / arena.close() so any in-flight
        // {@link #snapshot} request observes the flag and returns an empty future instead of
        // enqueuing work against the arena that is about to close.
        closing = true;
        // R15-H1: await every outstanding async-snapshot future BEFORE dispose() (which closes
        // the cancel-stream registry) and BEFORE arena.close() (which would UAF on any in-flight
        // worker's FrsSnapshot close).
        awaitOutstandingSnapshots();
        dispose();
        if (ownsResources) {
            try {
                defaultCf.close();
            } catch (Exception ignored) {
            }
            try {
                db.close();
            } catch (Exception ignored) {
            }
            try {
                arena.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * R15-H1: wraps a snapshot future so it self-removes from {@link #outstandingSnapshots} on
     * completion. The wrapper is a thin {@link RunnableFuture} forwarder; the only added
     * behaviour is the {@code finally} block on {@link RunnableFuture#run()} that removes the
     * inner future from the registry once {@code get()} would no longer block.
     *
     * <p>Note: SnapshotStrategyRunner's FutureTask is pre-run inline for SYNCHRONOUS execution
     * (savepoints), so by the time we wrap it the inner future is already done. In that case
     * {@link #run()} on the wrapper is a no-op + remove, which is harmless.
     */
    private RunnableFuture<SnapshotResult<KeyedStateHandle>> trackSnapshot(
            RunnableFuture<SnapshotResult<KeyedStateHandle>> inner) {
        outstandingSnapshots.add(inner);
        // If the runner already pre-ran the inner future (SYNCHRONOUS path) it is already done
        // and the worker-thread completion hook will never fire — remove the entry eagerly so
        // {@link #close()}'s await does not spin on a done future.
        if (inner.isDone()) {
            outstandingSnapshots.remove(inner);
            return inner;
        }
        return new RunnableFuture<SnapshotResult<KeyedStateHandle>>() {
            @Override
            public void run() {
                try {
                    inner.run();
                } finally {
                    outstandingSnapshots.remove(inner);
                }
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                try {
                    return inner.cancel(mayInterruptIfRunning);
                } finally {
                    outstandingSnapshots.remove(inner);
                }
            }

            @Override
            public boolean isCancelled() {
                return inner.isCancelled();
            }

            @Override
            public boolean isDone() {
                return inner.isDone();
            }

            @Override
            public SnapshotResult<KeyedStateHandle> get()
                    throws InterruptedException, ExecutionException {
                try {
                    return inner.get();
                } finally {
                    outstandingSnapshots.remove(inner);
                }
            }

            @Override
            public SnapshotResult<KeyedStateHandle> get(long timeout, @Nonnull TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                try {
                    return inner.get(timeout, unit);
                } finally {
                    if (inner.isDone()) {
                        outstandingSnapshots.remove(inner);
                    }
                }
            }
        };
    }

    /**
     * R15-H1: drain {@link #outstandingSnapshots} before tearing down the native arena. For
     * each outstanding future:
     *
     * <ol>
     *   <li>If already done, remove and continue.
     *   <li>Otherwise, attempt {@code cancel(true)} to interrupt the worker, then {@code
     *       get(CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS, MILLISECONDS)} to block until the worker
     *       returns. The timeout caps how long {@code close()} can block on a wedged worker;
     *       on timeout we drop the future and proceed (the worker may still touch the arena
     *       briefly, but the contract here is best-effort safety, not a hard guarantee — the
     *       coordinator should never issue a checkpoint to a closing backend).
     * </ol>
     *
     * <p>All exceptions are swallowed: {@code close()} must not throw on shutdown.
     */
    private void awaitOutstandingSnapshots() {
        if (outstandingSnapshots.isEmpty()) {
            return;
        }
        // Snapshot to a local list so concurrent removals from the worker-completion hook do
        // not surprise us mid-iteration.
        List<RunnableFuture<?>> pending = new ArrayList<>(outstandingSnapshots);
        for (RunnableFuture<?> f : pending) {
            if (f.isDone()) {
                outstandingSnapshots.remove(f);
                continue;
            }
            try {
                f.cancel(true);
            } catch (Throwable ignored) {
                // best-effort cancel; some snapshot strategies are uninterruptible mid-upload
            }
            try {
                f.get(CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn(
                        "ForStRsAsyncKeyedStateBackend.close: interrupted while awaiting"
                                + " outstanding async-snapshot future — proceeding to arena"
                                + " teardown anyway",
                        e);
                break;
            } catch (TimeoutException e) {
                LOG.warn(
                        "ForStRsAsyncKeyedStateBackend.close: outstanding async-snapshot did"
                                + " not complete within {}ms after cancel(true) — proceeding"
                                + " to arena teardown; the worker may UAF on its FrsSnapshot"
                                + " close (R15-H1 best-effort fallback)",
                        CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS);
            } catch (Throwable ignored) {
                // ExecutionException / cancellation surfaced via get(); benign — the worker
                // has terminated, which is all we needed before closing the arena.
            } finally {
                outstandingSnapshots.remove(f);
            }
        }
    }
}

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
import org.apache.flink.api.common.state.StateTtlConfig;
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
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
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
import java.util.concurrent.CompletableFuture;
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

    /**
     * FRS-TIMER-CF (2026-06-07): dedicated column family for engine-backed timers. DEFAULT OFF
     * (enable with {@code -Dforst.rs.timer.cf=1}).
     *
     * <p>Timers (the {@link ForStRsKeyGroupedInternalPriorityQueue} backing store) can live in their
     * own CF, named {@link #TIMER_CF_NAME}, instead of sharing {@code defaultCf} with window/join
     * state. The hypothesis was that this removes q11/q12 timer read-amplification by avoiding
     * interleaving with state SSTs.
     *
     * <p><b>REFUTED as a perf lever (2026-06-07).</b> An initial single A/B pair measured q11
     * 529s→411s, but that was variance: a faithful default-on reproduction measured 518s and 457s
     * (61s spread), and TIMER_DIAG showed the dedicated CF leaves the timer-scan access pattern
     * unchanged — refills 5357 vs 5291, entriesPerRefill 989 vs 993 vs the shared-CF baseline. The
     * q11/q12 cost is diffuse LSM range-scan read-amplification over the GROWING timer state (the
     * same q4/q9-class engine gap, see {@link ForStRsKeyGroupedInternalPriorityQueue#readRangeIntoCache}),
     * NOT timer/state interleaving or delete tombstones — so a separate CF removes nothing. The
     * capability is retained behind the flag (it is correct and fully checkpoint-safe) for future
     * timer-CF experiments such as targeted compaction; it is not on the production path.
     *
     * <p>The feature is checkpoint/restore-safe when enabled. The engine captures every non-default
     * CF in a checkpoint
     * — its SSTs (version is global, tagged by {@code cf_id}), its descriptor (carried in the
     * manifest blob), and, under no-flush checkpoints, its live memtable as a per-CF Arrow-IPC
     * artifact ({@code memtable-cf<id>.arrow}). On restore, {@code open_from_checkpoint}
     * re-registers the CF preserving its {@code cf_id} and replays its memtable artifact, so the
     * timer CF already exists when the timer queue is (re)created. We therefore resolve it via
     * {@link ForStRsLinker#dbOpenOrCreateCf} — create on a fresh DB, open by name on restore.
     *
     * <p>Resolved lazily on the first {@link #create} call and cached so multiple timer services
     * (e.g. event-time + processing-time) within one backend share the single CF handle. Closed in
     * {@link #releaseNativeResources()}. Set {@code -Dforst.rs.timer.cf=0} to disable and fall back
     * to the legacy shared {@code defaultCf} (e.g. to restore a pre-timer-CF snapshot whose timer
     * rows live in the default CF).
     */
    private static final String TIMER_CF_NAME = "frs_timers";

    private FrsCfHandle timerCf;
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
    /** STAGE-1: the two-regime switch (null unless FRS_RS_EXECUTOR=two-regime). */
    @javax.annotation.Nullable
    private org.apache.flink.state.forstrs.exec.RegimeSwitch twoRegimeSwitch;

    /**
     * STAGE-1 L→H seal (design §3.2, H=1-or-N simplification): runs on the MAILBOX at the
     * 0→1 outstanding transition — nothing in flight, so the synchronous vectorized drains
     * are race-free by construction. Off-heap batched FFI throughout (spec §8).
     */
    private void flushAllStagingForRegimeTransition() {
        for (org.apache.flink.state.forstrs.state.ForStRsMapStateV2<?, ?, ?, ?> s :
                registeredMapStatesV2) {
            s.flushOffHeapBuffer();
        }
        for (org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2<?, ?, ?> s :
                registeredListStatesV2) {
            s.flushPreSnapshot();
        }
        for (ForStRsAsyncReducingStateV2<?, ?, ?> s : registeredAsyncReducingStates) {
            s.flushOnBarrier();
        }
        for (ForStRsAsyncAggregatingStateV2<?, ?, ?, ?, ?> s : registeredAsyncAggregatingStates) {
            s.flushOnBarrier();
        }
    }

    // M1/PR-1: parallel executors (routing or coordinated, per FRS_RS_EXECUTOR); their worker
    // VectorizedExecutors live in managedExecutors, but the router/coordinator owns the worker
    // threads + arenas and must be shut down in dispose().
    private final java.util.List<org.apache.flink.runtime.asyncprocessing.StateExecutor>
            routingExecutors = new java.util.ArrayList<>();

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
     * R22-H1: guards {@link #releaseNativeResources()} so the close()→dispose() chain (close()
     * calls dispose() before doing its own native-release block, and dispose() now also calls
     * releaseNativeResources()) does not double-close db / defaultCf / arena. The first caller
     * that flips false→true performs the native frees; subsequent callers no-op.
     */
    private final java.util.concurrent.atomic.AtomicBoolean nativeReleased =
            new java.util.concurrent.atomic.AtomicBoolean(false);

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
     * R16-H1: lock that gates the publish of new {@link #outstandingSnapshots} entries against the
     * {@link #closing} flag flip. The previous (R15-H1) sequence — read {@code closing}, run a long
     * sync prep, then add to {@code outstandingSnapshots} — was a TOCTOU: a concurrent {@link
     * #close()} could flip {@code closing=true} between the check and the add, observe an EMPTY
     * outstanding set, and proceed to {@code arena.close()} while the in-flight snapshot's
     * {@link org.apache.flink.state.forstrs.ffm.FrsSnapshot} still referenced the arena (UAF
     * reintroduced).
     *
     * <p>Fix: register a {@code CompletableFuture} placeholder under this lock BEFORE running the
     * long sync prep, also under the same lock check that {@code closing == false}. {@link
     * #close()} takes the same lock to flip {@code closing}, so it is mutually exclusive with the
     * snapshot-register window. After release, the long sync prep runs unsynchronized; if
     * {@link #close()} then runs concurrently it observes the placeholder in the outstanding set
     * and awaits it before {@code arena.close()}.
     */
    private final Object closeLock = new Object();

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
        if (!(only instanceof IncrementalRemoteKeyedStateHandle)) {
            return null;
        }
        IncrementalRemoteKeyedStateHandle inc = (IncrementalRemoteKeyedStateHandle) only;
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
        //
        // R25-H1: extract TTL from the descriptor and forward to the 4-arg overload so the
        // registry can compare {@code ttlEnabled}/{@code ttlMillis} against the persisted
        // metadata on restore. Pre-R25-H1 the 3-arg overload defaulted ttlEnabled=false and
        // any TTL toggle on a restored state silently decoded the 8-byte expiry header as
        // payload bytes. The descriptor's getTtlConfig() is @Nonnull and returns
        // {@code StateTtlConfig.DISABLED} when TTL is not configured, so the null guard
        // below is defensive.
        StateTtlConfig ttlConfig = desc.getTtlConfig();
        boolean ttlEnabled = ttlConfig != null && ttlConfig.isEnabled();
        long ttlMillis = ttlEnabled ? ttlConfig.getTimeToLive().toMillis() : 0L;
        // R26-M1: validate state-type-vs-TTL BEFORE touching the registry. Pre-R26-M1 the
        // {@code verifyOrRegister} call committed {@code ttlEnabled=true} into the registry
        // for every state type, then the switch in {@code createTtlAwareStateInternal} below
        // threw {@code UnsupportedOperationException} for MAP/LIST/REDUCING/AGGREGATING. The
        // registry mutation persisted across the throw — a subsequent retry (or a snapshot
        // taken before the catch ran) carried a {@code ttlEnabled=true} entry for state that
        // never had TTL wrapping applied, the same registry-vs-on-disk-layout mismatch that
        // R26-H1 fixes on the V1-sync path. Move the unsupported-state-type check up so the
        // registry only ever records {@code ttlEnabled=true} for state types whose
        // construction actually applies TtlSerializer wrapping.
        if (ttlEnabled) {
            switch (desc.getType()) {
                case VALUE:
                    break; // supported — createTtlAwareStateInternal wraps with TtlSerializer
                case MAP:
                case LIST:
                case REDUCING:
                case AGGREGATING:
                    throw new UnsupportedOperationException(
                            "PR-A7: TTL is wired for ValueState V2 in this PR. "
                                    + desc.getType()
                                    + " state TTL is deferred to follow-on PR (decorator + "
                                    + "per-entry expiry filter on iteration paths). State name: "
                                    + desc.getStateId()
                                    + ". Disable TTL or use ValueState in the interim.");
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported state type for TTL: " + desc.getType());
            }
        }
        // R35-H1: capture the (possibly reconfigured) serializer returned by the registry. When
        // the new descriptor resolves as {@code COMPATIBLE_WITH_RECONFIGURED_SERIALIZER}, the
        // registry returns the reconfigured TypeSerializer instance; discarding it here and
        // passing the raw {@code desc.getSerializer()} to the V2 state constructor caused all
        // subsequent reads/writes to go through the OLD serializer schema, silently producing
        // wrong-format payloads on the engine.
        @SuppressWarnings("unchecked")
        TypeSerializer<SV> activeSerializer =
                (TypeSerializer<SV>)
                        stateSerializerRegistry.verifyOrRegister(
                                desc.getStateId(),
                                desc.getType().ordinal(),
                                desc.getSerializer(),
                                ttlEnabled,
                                ttlMillis);
        // PR-A7 (S1-12): if the descriptor has TTL enabled, wrap the inner state in a TTL
        // decorator. Pre-A7 the TtlConfig was silently dropped on the floor and TTL never fired.
        // STORAGE FORMAT BREAK: TTL-enabled state cells now carry an 8-byte expiry prefix;
        // enabling TTL on existing non-TTL data is not snapshot-compatible.
        S created;
        if (desc.getTtlConfig() != null && desc.getTtlConfig().isEnabled()) {
            created = createTtlAwareStateInternal(ns, nsSer, desc, activeSerializer);
        } else {
            created = createStateInternal(ns, nsSer, desc, activeSerializer);
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
            N ns,
            TypeSerializer<N> nsSer,
            StateDescriptor<SV> desc,
            TypeSerializer<SV> activeSerializer)
            throws Exception {
        String name = desc.getStateId();
        switch (desc.getType()) {
            case VALUE:
                {
                    // R35-H1: wrap the (possibly reconfigured) serializer returned by the
                    // registry, not the original {@code desc.getSerializer()}. The TtlSerializer
                    // composes around the active user serializer so reads / writes go through
                    // the post-reconfiguration schema even after schema-evolution restore.
                    TypeSerializer<SV> userSerializer = activeSerializer;
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
    @Override
    public <N, S extends InternalKeyedState, SV> S createStateInternal(
            @Nonnull N ns, @Nonnull TypeSerializer<N> nsSer, @Nonnull StateDescriptor<SV> desc)
            throws Exception {
        // R36-L1: fail-loud. The 3-arg overload would silently bypass
        // {@link StateSerializerRegistry#verifyOrRegister} — pre-fix it delegated to the 4-arg
        // overload with {@code desc.getSerializer()}, so any caller that reached this entry
        // point (Flink-internal TTL plumbing, runtime adapters, or a future refactor of
        // {@code getOrCreateKeyedState}) would create state through the OLD serializer schema
        // after a schema-evolution restore. Schema drift would surface only as silently wrong
        // payloads at the first read. Throwing here makes the design constraint explicit:
        // every state creation MUST first run through the registry to obtain the active
        // serializer; callers must use {@link #getOrCreateKeyedState} (which does the
        // verification + TTL gating + caching) or the private 4-arg overload from within this
        // class.
        throw new IllegalStateException(
                "createStateInternal(ns, nsSer, desc) must not be called directly — it would"
                        + " bypass StateSerializerRegistry#verifyOrRegister and silently use the"
                        + " un-validated descriptor serializer. Route through getOrCreateKeyedState"
                        + " so schema-drift detection runs first. State name: "
                        + desc.getStateId()
                        + ", kind: "
                        + desc.getType());
    }

    /**
     * R35-H1 overload: build a V2 state using the supplied {@code activeSerializer} rather than
     * reading {@code desc.getSerializer()} directly. {@code activeSerializer} is the return value
     * of {@link StateSerializerRegistry#verifyOrRegister} — i.e. the original serializer when the
     * descriptor was compatible-as-is or after-migration, or the RECONFIGURED variant when the
     * registry resolved to {@link
     * org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility#compatibleWithReconfiguredSerializer}.
     * Pre-R35-H1 the registry result was discarded and the engine read/wrote through the OLD
     * serializer schema, silently producing wrong-format payloads after a schema-evolution restore.
     *
     * <p>For composite descriptors (LIST, MAP) the active serializer is the FULL ListSerializer /
     * MapSerializer composite; we extract the inner element / user-k-v components from it so the
     * reconfigured branches propagate the new schema. For singleton-value descriptors (VALUE,
     * REDUCING, AGGREGATING) the active serializer replaces {@code desc.getSerializer()} directly.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    private <N, S extends InternalKeyedState, SV> S createStateInternal(
            @Nonnull N ns,
            @Nonnull TypeSerializer<N> nsSer,
            @Nonnull StateDescriptor<SV> desc,
            @Nonnull TypeSerializer<SV> activeSerializer)
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
                // R35-H1: use the active (possibly reconfigured) value serializer.
                return (S)
                        new ForStRsValueStateV2<>(
                                stateRequestHandler,
                                name,
                                keySerializer,
                                nsSer,
                                activeSerializer);
            case MAP:
                var mapDesc = (MapStateDescriptor<?, ?>) desc;
                // R35-H1: V2 {@link MapStateDescriptor} extends {@code StateDescriptor<UV>}, so
                // {@code desc.getSerializer()} is the user-VALUE serializer (NOT a composite),
                // and {@code activeSerializer} carries the (possibly reconfigured) value schema.
                //
                // R36-M1: validate the user-key serializer through the same registry as the
                // value serializer, using the synthetic registry key {@code "<stateName>$UK"}
                // and the synthetic kind {@link StateSerializerRegistry#KIND_USER_KEY}. Pre-fix
                // the UK serializer was passed through {@code as-is} — a schema-evolution of
                // the user-key type would resolve as compatible-but-different on disk and the
                // restored MapState would silently read garbage on every lookup. The UK runs
                // through the same {@code verifyOrRegister} contract as any other serializer:
                // (a) fresh-state → register so the next snapshot persists the UK schema;
                // (b) restored-state → resolveSchemaCompatibility comparison fires, with
                //     RECONFIGURED variants threaded into the state constructor and
                //     INCOMPATIBLE surfaced as StateMigrationException matching Flink's contract.
                // We don't TTL-tag the UK entry: MapState TTL prefixes the VALUE (R26-M1
                // refuses TTL on MapState anyway in the current code), so the UK serializer
                // does not change shape under TTL toggle.
                @SuppressWarnings("unchecked")
                TypeSerializer<Object> userKeySerializerRaw =
                        (TypeSerializer<Object>) mapDesc.getUserKeySerializer();
                TypeSerializer<Object> activeUserKeySerializer =
                        stateSerializerRegistry.verifyOrRegister(
                                name + StateSerializerRegistry.USER_KEY_SUFFIX,
                                StateSerializerRegistry.KIND_USER_KEY,
                                userKeySerializerRaw,
                                /* ttlEnabled= */ false,
                                /* ttlMillis= */ 0L);
                // PR-C1: hand linker/db/cf to the MapStateV2 so it can stage writes in its
                // off-heap MapStateArrowBuffer and drain via linker.batchPut on flush. Register
                // the instance so the snapshot pre-hook drains every live MapState V2.
                ForStRsMapStateV2<K, N, ?, ?> mapStateV2 =
                        new ForStRsMapStateV2<>(
                                stateRequestHandler,
                                name,
                                keySerializer,
                                nsSer,
                                activeUserKeySerializer,
                                activeSerializer,
                                linker,
                                db,
                                defaultCf);
                registeredMapStatesV2.add(mapStateV2);
                if (twoRegimeSwitch != null) {
                    mapStateV2.setRegimeSwitch(twoRegimeSwitch);
                }
                return (S) mapStateV2;
            case LIST:
                // V3.2 (V20 sub-spec §5): register the ListState name with every managed
                // executor so the per-batch classifier routes LIST_ADD via APPEND_MERGE.
                for (VectorizedExecutor exec : managedExecutors) {
                    exec.registerListState(name);
                }
                // R35-H1: registry validated the ListSerializer composite — pass it through so
                // ForStRsAsyncListStateV2 sees the (possibly reconfigured) element schema.
                // PR-C2: hand linker/db/cf + a fresh per-state ListStateArrowBuffer to the
                // AsyncListStateV2 so the classifier's APPEND_MERGE routing goes through the
                // off-heap fast path (skips the heap-byte[] AppendMergeBatchBuffer). Register
                // the instance so the snapshot pre-hook drains its accumulator.
                // B-SPIKE (per-batch-buffer-ownership §2): no per-state staging accumulator
                // under the pipelined executor — the worker-side flushIfDirty drain would race
                // mailbox appends. Null buffer = the heap APPEND_MERGE path via the classifier's
                // per-batch-private AppendMergeBatchBuffer (recordAppendMergeOffHeap returns
                // null → caller falls back; flushPreSnapshot no-ops).
                org.apache.flink.state.forstrs.state.ListStateArrowBuffer listBuf =
                        org.apache.flink.state.forstrs.state.ForStRsMapStateV2
                                        .legacyPipelinedActive()
                                ? null
                                : new org.apache.flink.state.forstrs.state.ListStateArrowBuffer();
                ForStRsAsyncListStateV2<K, N, ?> listStateV2 =
                        new ForStRsAsyncListStateV2<>(
                                stateRequestHandler,
                                name,
                                keySerializer,
                                nsSer,
                                activeSerializer,
                                listBuf,
                                linker,
                                db,
                                defaultCf);
                registeredListStatesV2.add(listStateV2);
                if (twoRegimeSwitch != null) {
                    listStateV2.setRegimeSwitch(twoRegimeSwitch);
                }
                return (S) listStateV2;
            case REDUCING:
                return (S) createReducingState(name, nsSer, desc, activeSerializer);
            case AGGREGATING:
                return (S) createAggregatingState(name, nsSer, desc, activeSerializer);
            default:
                throw new UnsupportedOperationException("Unsupported: " + desc.getType());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <N> ForStRsAsyncReducingStateV2<K, N, ?> createReducingState(
            String name,
            TypeSerializer<N> nsSer,
            StateDescriptor<?> desc,
            TypeSerializer<?> activeSerializer) {
        ReducingStateDescriptor reducingDesc = (ReducingStateDescriptor) desc;
        // R35-H1: use the registry-validated active serializer rather than
        // {@code reducingDesc.getSerializer()} so schema-reconfigured types flow through.
        ForStRsAsyncReducingStateV2<K, N, ?> state =
                new ForStRsAsyncReducingStateV2<>(
                        stateRequestHandler,
                        name,
                        keySerializer,
                        nsSer,
                        activeSerializer,
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
        if (twoRegimeSwitch != null) {
            state.setRegimeSwitch(twoRegimeSwitch);
        }
        return state;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <N> ForStRsAsyncAggregatingStateV2<K, N, ?, ?, ?> createAggregatingState(
            String name,
            TypeSerializer<N> nsSer,
            StateDescriptor<?> desc,
            TypeSerializer<?> activeSerializer) {
        AggregatingStateDescriptor aggDesc = (AggregatingStateDescriptor) desc;
        // R35-H1: thread the registry-validated active accumulator serializer through.
        ForStRsAsyncAggregatingStateV2<K, N, ?, ?, ?> state =
                new ForStRsAsyncAggregatingStateV2<>(
                        stateRequestHandler,
                        name,
                        keySerializer,
                        nsSer,
                        activeSerializer,
                        aggDesc.getAggregateFunction());
        // PR-C3 (V12 / B3-H2): wire the production flush handler — see createReducingState above
        // for the A4-H2 rationale.
        state.setFlushHandler(this::rmwFlushToEngine);
        registeredAsyncAggregatingStates.add(state);
        if (twoRegimeSwitch != null) {
            state.setRegimeSwitch(twoRegimeSwitch);
        }
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
        // M1: parallel async-state executor (env FRS_RS_PARALLEL_EXECUTOR=1). Routes each batch to
        // one of N single-thread VectorizedExecutor workers so disjoint-key batches run concurrently
        // + the mailbox overlaps the next batch (the documented "in-flight depth > 1" fix). Workers
        // register into managedExecutors so snapshot/flush/shutdown reach them.
        // OPT-01 (2026-06-09): async-offload executor is now the DEFAULT (opt-out with
        // FRS_RS_PARALLEL_EXECUTOR=0). The two correctness blockers are FIXED: (1) the deadlock — via
        // the deadlock-free SYNCHRONOUS key-group-affine RoutingStateExecutor; (2) the windowed-join
        // under-emit — via cache-off-when-parallel (the single-threaded MapStateCache races under
        // parallel; cache-off is the proven-correct parallel config, coupled in ForStRsMapStateV2).
        // VERIFIED correct + neutral-or-faster across families under parallel+cache-off: q3 36.6s
        // (light, exact), q8 windowed-join (correct band), q11 318.9→135.7s (2.35×, 92M exact, PASSES
        // 0.82×RocksDB), q12 50.6→46.5s (cache-benefiting got FASTER, 92M exact — cache-off did NOT
        // rob it). Full q0–q22 e2e sweep is the confirmation gate; reversible via =0.
        // REVERTED to OPT-IN (2026-06-10): default-on robs cache-benefiting throughput-bound joins.
        // OPT-01 (parallel+cache-off) is correctness-safe and a big win for DRAIN-TAIL-bound queries
        // (q11 318.9→135.7s = 2.35×; q12 faster) but it forces cache-off, and q9 — a per-probe-read-
        // bound join that RELIES on the MapStateCache — does NOT get q11's tail win and is
        // neutral-to-SLOWER (DNF 79.2M@1284s vs depth-1 ~84.5M@1204s). Helping q11 while robbing q9 is
        // "rob Peter to pay Paul" — forbidden. So OPT-01 stays OPT-IN (FRS_RS_PARALLEL_EXECUTOR=1);
        // default = depth-1 + cache (correct + fast for ALL). A true default needs the cache KEPT
        // under parallel (per-worker cache pinned to the key-group's worker thread) — multi-PR.
        // PR-1 executor selection (2026-06-10):
        //   FRS_RS_EXECUTOR=coordinated → CoordinatedStateExecutor (non-blocking ForSt model;
        //                                  target default once the Task-5 benchmark gates pass)
        //   FRS_RS_EXECUTOR=routing     → RoutingStateExecutor (the measured synchronous opt-in)
        //   FRS_RS_EXECUTOR=inline      → depth-1 VectorizedExecutor (kill switch)
        // Back-compat: FRS_RS_PARALLEL_EXECUTOR=1 (the old opt-in) == routing.
        // Default REMAINS inline until the PR-1 Task-5 gates (q8 band, q17 ≤85s, q11, q20, q9
        // no-regress) pass on 8c/32g; the flip is PR-1 Task 6.
        String mode = System.getenv("FRS_RS_EXECUTOR");
        if (mode != null) {
            mode = mode.trim();
        }
        if (mode == null || mode.isEmpty()) {
            String legacy = System.getenv("FRS_RS_PARALLEL_EXECUTOR");
            mode = (legacy != null && legacy.trim().equals("1")) ? "routing" : "inline";
        }
        switch (mode) {
            case "adaptive":
                // PR-1 adaptive: iter-free batches inline (q17-class keeps depth-1 speed),
                // iter batches → blocking parallel fan-out (q11/q20/q7-class wins). Both
                // synchronous → lockstep semantics, the proven-correct regime.
                org.apache.flink.state.forstrs.exec.RoutingStateExecutor a =
                        new org.apache.flink.state.forstrs.exec.RoutingStateExecutor(
                                linker, db, defaultCf, dispatchMetrics, managedExecutors::add, true);
                routingExecutors.add(a);
                return a;
            case "coordinated":
                org.apache.flink.state.forstrs.exec.CoordinatedStateExecutor c =
                        org.apache.flink.state.forstrs.exec.CoordinatedStateExecutor.create(
                                linker, db, defaultCf, dispatchMetrics, managedExecutors::add);
                routingExecutors.add(c);
                return c;
            case "routing":
                org.apache.flink.state.forstrs.exec.RoutingStateExecutor r =
                        new org.apache.flink.state.forstrs.exec.RoutingStateExecutor(
                                linker, db, defaultCf, dispatchMetrics, managedExecutors::add);
                routingExecutors.add(r);
                return r;
            case "two-regime":
                // STAGE-1 (design 2026-06-11 §3): LIGHT inline-when-pipeline-empty (q17-class
                // keeps depth-1 speed) / HEAVY non-blocking kg-FIFO dispatch. Same ctor as
                // routing-async; the mode string activates RegimeSwitch (twoRegimeMode()).
                org.apache.flink.state.forstrs.exec.RoutingStateExecutor tr =
                        new org.apache.flink.state.forstrs.exec.RoutingStateExecutor(
                                linker,
                                db,
                                defaultCf,
                                dispatchMetrics,
                                managedExecutors::add,
                                false,
                                true);
                routingExecutors.add(tr);
                this.twoRegimeSwitch = tr.regimeSwitch();
                tr.regimeSwitch().setOnHeavyTransition(this::flushAllStagingForRegimeTransition);
                return tr;
            case "routing-async":
                // FRS-ROUTING-ASYNC (2026-06-11): routing's kg-affine worker FIFOs WITHOUT the
                // mailbox latch — executeBatchRequests returns a truthful incomplete future and
                // the AEC pipelines (fullyLoaded() caps depth). Motivated by the q9@100M perf
                // profile: TM averaged 1.7/8 cores under blocking routing (latency-bound, not
                // CPU-bound); the latch was the cap. No mailbox-inline execution, so the
                // 2026-06-10 statebuf-overtake corruption mechanism cannot occur.
                org.apache.flink.state.forstrs.exec.RoutingStateExecutor ra =
                        new org.apache.flink.state.forstrs.exec.RoutingStateExecutor(
                                linker,
                                db,
                                defaultCf,
                                dispatchMetrics,
                                managedExecutors::add,
                                false,
                                true);
                routingExecutors.add(ra);
                return ra;
            default:
                var e = new VectorizedExecutor(linker, db, defaultCf, arena);
                e.setDispatchMetrics(dispatchMetrics);
                managedExecutors.add(e);
                return e;
        }
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
        // R17-H1: PHASE 0 — atomic check-and-publish placeholder under {@link #closeLock}
        // ============================================================
        // The placeholder MUST be published BEFORE PHASE 1 because every subsequent phase
        // touches the native arena via flushDirty / off-heap drains / FFI batchPut / FFI
        // snapshot. Pre-fix (R16-H1), the placeholder was added only at the start of PHASE 3,
        // leaving PHASES 1-2 unguarded: a close() racing inside that window observed an empty
        // outstandingSnapshots set, flipped closing=true, and ran arena.close() concurrently
        // with the in-flight FFI work — UAF reintroduced.
        //
        // Pattern: the placeholder is a no-op {@link RunnableFuture}. {@link #close()} flips
        // {@code closing=true} under the same lock and then awaits every outstanding entry;
        // the placeholder blocks the await until snapshot() reaches its publish-and-retire
        // point (PHASE 3 — actual future installed, placeholder removed). The wrapped
        // {@link CompletableFuture} is completed there so any racing await returns immediately.
        //
        // Snapshot/close mutual-exclusion invariant: snapshot() either (a) sees closing=true
        // and short-circuits to DoneFuture.of(empty) — no native work — OR (b) successfully
        // publishes the placeholder, in which case close() awaits it before arena.close().
        CompletableFuture<Void> placeholder = new CompletableFuture<>();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> placeholderFuture =
                new PlaceholderRunnableFuture(placeholder);
        synchronized (closeLock) {
            if (closing) {
                return DoneFuture.of(SnapshotResult.empty());
            }
            outstandingSnapshots.add(placeholderFuture);
        }
        try {
            // ============================================================
            // PHASE 1 — drain in-flight V2 dispatch + state buffers + timers
            // ============================================================
            // PHASE 1.a: FRS-CKPT-NOFLUSH (2026-06-01) — DO NOT fold the memtable
            // to an L0 SST here. The strategy's async phase now captures the live
            // memtable as a seq-bounded Arrow-IPC artifact (no-flush incremental
            // checkpoint), keeping it RAM-resident + unfragmented for reads (the
            // ckpt-ON heavy-join fix). V2 dispatch has already applied every
            // in-flight batch to the engine memtable synchronously (see
            // VectorizedExecutor.flushDirty's contract note), so there is nothing
            // to drain on the Java side here; the state-buffer drains below
            // (PHASE 1.b/1.c/timers) put all remaining mutations into the memtable
            // BEFORE the snapshot pin in PHASE 3, so the seq-bounded artifact
            // captures them. (dispose() still flushes on shutdown — that path is
            // unchanged.)

            // R28-H2: every drain loop wraps the per-state flush in try/Throwable so a throw
            // from one state instance cannot strand the remaining states' buffers in the
            // pre-snapshot arena. The MapStateV2 dispose() loop (line ~1531) already uses this
            // pattern (D5-H2 + R25-M2); the snapshot drain MUST too because a partial drain
            // produces a silently truncated checkpoint:
            //   * MapStateV2 off-heap buffer = the user's pending mutations
            //   * ListStateV2 accumulator   = appended values not yet folded
            //   * Reducing/Aggregating cache = pending RMW results
            //   * timer queues               = scheduled-but-not-yet-engine-side timers
            // Losing any of these silently breaks at-least-once / exactly-once guarantees on
            // restore. The first failure is captured and rethrown after every state has been
            // attempted; subsequent failures become suppressed exceptions so the operator sees
            // the full failure surface in the first task-fail report.
            //
            // Best-effort drain pattern (R25-M2): warn-log each failure with the state name,
            // remember the first failure as the root cause, and addSuppressed for the rest.
            // RuntimeException is rethrown at the end so the snapshot strategy still sees a
            // failed pre-flush.
            Throwable drainFail = null;

            // PHASE 1.b: PR-C1 — drain each MapStateV2's off-heap staging buffer to the engine
            // via batchPut + tombstone deletes BEFORE the engine snapshot reads. Mirrors the
            // V1-sync statebuf flush hook (commit b3b9d7f2a6c).
            for (ForStRsMapStateV2<?, ?, ?, ?> ms : registeredMapStatesV2) {
                try {
                    ms.flushOffHeapBuffer();
                } catch (Throwable t) {
                    LOG.warn(
                            "PHASE 1.b drain failed for MapStateV2 instance #{}: continuing with"
                                    + " remaining states (first failure recorded as root cause)",
                            System.identityHashCode(ms),
                            t);
                    if (drainFail == null) {
                        drainFail = t;
                    } else {
                        drainFail.addSuppressed(t);
                    }
                }
            }
            // PHASE 1.c: PR-C2 — drain ListStateV2 off-heap accumulator via a single {@code
            // frs_vec_merge_append_batch} FFI. Without this hook, accumulated single-element
            // {@code asyncAdd} chunks would still be in the off-heap arena at snapshot time and
            // would be lost on restore.
            for (ForStRsAsyncListStateV2<?, ?, ?> ls : registeredListStatesV2) {
                try {
                    ls.flushPreSnapshot();
                } catch (Throwable t) {
                    LOG.warn(
                            "PHASE 1.c drain failed for ListStateV2 '{}': continuing with"
                                    + " remaining states",
                            ls.getStateName(),
                            t);
                    if (drainFail == null) {
                        drainFail = t;
                    } else {
                        drainFail.addSuppressed(t);
                    }
                }
            }

            // PHASE 1.d: drain RMW caches (Reducing/Aggregating V1 + V2). Each {@code
            // flushOnBarrier()} serializes accumulated reduce/aggregate results and enqueues PUT
            // requests to the classifier — those PUTs are picked up by PHASE 2's flushDirty
            // pass.
            for (ForStRsReducingStateV2<?> s : registeredReducingStates) {
                try {
                    s.flushOnBarrier();
                } catch (Throwable t) {
                    LOG.warn(
                            "PHASE 1.d drain failed for ReducingStateV2 instance #{}: continuing",
                            System.identityHashCode(s),
                            t);
                    if (drainFail == null) {
                        drainFail = t;
                    } else {
                        drainFail.addSuppressed(t);
                    }
                }
            }
            for (ForStRsAggregatingStateV2<?, ?, ?> s : registeredAggregatingStates) {
                try {
                    s.flushOnBarrier();
                } catch (Throwable t) {
                    LOG.warn(
                            "PHASE 1.d drain failed for AggregatingStateV2 instance #{}:"
                                    + " continuing",
                            System.identityHashCode(s),
                            t);
                    if (drainFail == null) {
                        drainFail = t;
                    } else {
                        drainFail.addSuppressed(t);
                    }
                }
            }
            for (ForStRsAsyncReducingStateV2<?, ?, ?> s : registeredAsyncReducingStates) {
                try {
                    s.flushOnBarrier();
                } catch (Throwable t) {
                    LOG.warn(
                            "PHASE 1.d drain failed for AsyncReducingStateV2 instance #{}:"
                                    + " continuing",
                            System.identityHashCode(s),
                            t);
                    if (drainFail == null) {
                        drainFail = t;
                    } else {
                        drainFail.addSuppressed(t);
                    }
                }
            }
            for (ForStRsAsyncAggregatingStateV2<?, ?, ?, ?, ?> s :
                    registeredAsyncAggregatingStates) {
                try {
                    s.flushOnBarrier();
                } catch (Throwable t) {
                    LOG.warn(
                            "PHASE 1.d drain failed for AsyncAggregatingStateV2 instance #{}:"
                                    + " continuing",
                            System.identityHashCode(s),
                            t);
                    if (drainFail == null) {
                        drainFail = t;
                    } else {
                        drainFail.addSuppressed(t);
                    }
                }
            }

            // PHASE 1.e: PR-A1 — drain each engine-backed timer queue's pending-buffer to the
            // engine. Mirrors the V1-sync engineTimerQueues drain in
            // ForStRsAbstractKeyedStateBackend.snapshot() (spec invariant #4 in the
            // batched-timer design). HEAP timer-factory mode registers no queues and this loop
            // is a no-op.
            for (ForStRsKeyGroupedInternalPriorityQueue<?> q : registeredTimerQueues) {
                try {
                    q.flushPendingToEngine();
                } catch (Throwable t) {
                    LOG.warn(
                            "PHASE 1.e drain failed for timer queue instance #{}: continuing",
                            System.identityHashCode(q),
                            t);
                    if (drainFail == null) {
                        drainFail = t;
                    } else {
                        drainFail.addSuppressed(t);
                    }
                }
            }

            // R28-H2: rethrow the aggregated drain failure after every state has been given a
            // chance to flush. The snapshot strategy never sees a half-drained state.
            if (drainFail != null) {
                throw new RuntimeException(
                        "Snapshot pre-drain failed (first failure shown; later failures"
                                + " attached as suppressed)",
                        drainFail);
            }

            // ============================================================
            // PHASE 2 — FRS-CKPT-NOFLUSH: no second flush either. The PUTs
            // enqueued by the PHASE 1.b/1.c RMW/state-buffer drains are already in
            // the engine memtable (V2 dispatch applies synchronously); they are
            // captured by the seq-bounded memtable artifact in PHASE 3, not folded
            // to an SST.
            // ============================================================

            // ============================================================
            // PHASE 3 — engine snapshot via FFI through ForStRsSnapshotStrategy
            // ============================================================
            // Lazily construct the strategy on first snapshot. We pass a synthetic single-CF
            // map ("default" → 0) because the async-V2 path keeps every state in the default
            // column family; multi-CF wiring is a follow-on PR (V20).
            ForStRsSnapshotStrategy strategy = ensureSnapshotStrategy();

            // PR-A8: SYNC_SAVEPOINT semantics. Synchronous execution blocks until the future is
            // pre-run, so by the time this method returns every state mutation up to the
            // barrier is durable on S3. For periodic checkpoints we use ASYNCHRONOUS so the
            // mailbox thread continues processing records while the upload completes in a
            // virtual thread.
            SnapshotExecutionType execType =
                    isSync
                            ? SnapshotExecutionType.SYNCHRONOUS
                            : SnapshotExecutionType.ASYNCHRONOUS;
            RunnableFuture<SnapshotResult<KeyedStateHandle>> future =
                    new SnapshotStrategyRunner<>(
                                    isSavepoint
                                            ? "ForStRs-async-savepoint"
                                            : "ForStRs-async-incremental-snapshot",
                                    strategy,
                                    cancelStreamRegistry,
                                    execType)
                            .snapshot(id, ts, f, o);
            // R15-H1 + R16-H1 + R17-H1: register the future so {@link #close()} can await its
            // completion before closing the native arena. For SYNCHRONOUS savepoints the future
            // is already done by the time {@code .snapshot()} returns (SnapshotStrategyRunner
            // pre-runs the FutureTask), so tracking is harmless (close() sees done=true and
            // skips the await). For ASYNCHRONOUS checkpoints the worker thread runs
            // concurrently with the mailbox and MUST be awaited at close() to prevent the
            // R15-H1 use-after-free on the nativeArena. The placeholder is removed only AFTER
            // the actual future is installed in the set, so close() never sees an empty
            // outstanding set between the two operations.
            RunnableFuture<SnapshotResult<KeyedStateHandle>> tracked = trackSnapshot(future);
            // Publish-then-retire ordering: actual future is now in outstandingSnapshots (or
            // already done and removed), so it is safe to drop the placeholder.
            outstandingSnapshots.remove(placeholderFuture);
            placeholder.complete(null);
            return tracked;
        } catch (IOException e) {
            // R16-H1 + R17-H1: release the placeholder on any throw path (PHASE 1 drain
            // failures, PHASE 2 flush failures, PHASE 3 strategy errors) so close()'s await
            // does not block on a dead snapshot attempt.
            outstandingSnapshots.remove(placeholderFuture);
            placeholder.complete(null);
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
        } catch (Throwable t) {
            // R16-H1 + R17-H1: any non-IOException throw (PHASE 1/2/3 drain or flush failures,
            // RuntimeException, FFI panics, Errors) must also release the placeholder so
            // close()'s await does not block on a dead attempt. The throw still propagates so
            // the coordinator's tolerable-failed-checkpoints accounting observes the real
            // failure (A4-H3 contract preserved).
            outstandingSnapshots.remove(placeholderFuture);
            placeholder.complete(null);
            throw t;
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
            s.completeCheckpoint(id);
        }
        // R38-M2: do NOT call {@code managedExecutors.forEach(flushDirty)} here.
        // A flushDirty AFTER the snapshot manifest is already on durable
        // storage produces SSTs that are orphaned w.r.t. the just-completed
        // manifest — they can only be picked up by the next checkpoint, and
        // until then sit on disk inflating local-disk usage with no caller
        // benefit. flushDirty is exclusively a snapshot-pre-hook (driven
        // from {@link ForStRsSnapshotStrategy} during `syncPrepareResources`).
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
                    unregisterAndDiscardEvictedSst(reg, hlp);
                }
            }
            // E8-H2 post-abort drain: catch any entries that landed after the initial take.
            var late = s.drainLatePendingRegistrations(id);
            for (var hlp : late) {
                unregisterAndDiscardEvictedSst(reg, hlp);
            }
        }
    }

    private static void unregisterAndDiscardEvictedSst(
            ForStRsSstRegistry reg,
            org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath hlp) {
        var evicted =
                reg.unregisterAndGetEvicted(
                        new org.apache.flink.runtime.state.StateHandleID(hlp.getLocalPath()));
        evicted.ifPresent(
                h -> {
                    try {
                        h.discardState();
                    } catch (Exception ignored) {
                        // Best-effort cleanup for aborted checkpoint uploads.
                    }
                });
    }

    @Override
    public void notifyCheckpointSubsumed(long id) {
        ForStRsSnapshotStrategy s = snapshotStrategy;
        ForStRsSstRegistry reg = sstRegistry;
        if (s == null || reg == null) {
            return;
        }
        for (var hlp : s.takeCompletedRegistrationsForSubsumed(id)) {
            unregisterAndDiscardEvictedSst(reg, hlp);
        }
    }

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
        // FRS-TIMER-CF: optionally route engine timers to a dedicated, checkpoint-safe CF (see
        // timerCf field). DEFAULT OFF — CF isolation was REFUTED as a perf lever (2026-06-07): with
        // the dedicated CF on, TIMER_DIAG showed identical refills (5357 vs 5291) and entriesPerRefill
        // (989 vs 993) to the shared-CF baseline, i.e. the timer-scan access pattern is unchanged. The
        // q11/q12 cost is diffuse LSM range-scan read-amp over the GROWING timer state (the same
        // q4/q9-class engine gap), not timer/state interleaving — so a separate CF removes nothing.
        // The capability is kept behind -Dforst.rs.timer.cf=1 (fully checkpoint-safe, see field
        // javadoc + the engine round-trip test) for future timer-CF experiments. Production uses the
        // legacy shared defaultCf, matching the verified all-pass baseline.
        FrsCfHandle queueCf = defaultCf;
        if ("1".equals(System.getProperty("forst.rs.timer.cf", "0"))) {
            if (timerCf == null) {
                timerCf = linker.dbOpenOrCreateCf(db, arena, TIMER_CF_NAME);
            }
            queueCf = timerCf;
        }
        ForStRsKeyGroupedInternalPriorityQueue<T> queue =
                new ForStRsKeyGroupedInternalPriorityQueue<>(
                        linker,
                        db,
                        queueCf,
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
        // R18-M2: dispose() is a legitimate Flink lifecycle entry point that closes native
        // resources (slotArenaScope, cancelStreamRegistry, MapStateV2 arenas, watchdog).
        // Pre-fix, only close() drained outstanding async snapshots — a direct dispose() call
        // (e.g., from AbstractStreamOperator#dispose on task cancellation that did not go
        // through close()) tore down the arena while in-flight snapshot workers were still
        // touching it (R15-H1 UAF re-opened).
        //
        // Idempotency: close() also calls awaitOutstandingSnapshots() before invoking
        // dispose(). The second call here observes an empty set (the first await drained it)
        // and returns immediately — no double-await cost. Direct-dispose callers (bypassing
        // close()) get the same UAF protection.
        //
        // Flip closing=true under closeLock so any racing snapshot() short-circuits — same
        // semantics as close(). Safe to flip multiple times.
        synchronized (closeLock) {
            closing = true;
        }
        // R24-H1 (parity with sync backend): wrap the awaitOutstandingSnapshots + executor
        // flush/shutdown + state-cache teardown phase in a try/finally so any throw is
        // captured AND the native-resource release (slotArenaScope, cancelStreamRegistry,
        // backendPathInvariant, releaseNativeResources) ALWAYS runs. Pre-fix, a throw in any
        // of the early steps (e.g. {@link VectorizedExecutor#flushDirty} surfacing a snapshot
        // pre-flush failure, or a per-state {@code ms.close()} throwing because its arena was
        // already torn down by a racing slot exit) leaked the slot arena, cancel-stream
        // registry, and most importantly db/defaultCf/arena because {@code disposed=true} at
        // the top of dispose() blocks any retry through the close()→dispose() path.
        //
        // R23-M1 extension: the original R23-M1 fix moved the native release into a shared
        // helper but kept it OUTSIDE the try/finally. This change now puts the entire
        // teardown chain inside a finally so the order is preserved AND the native release
        // is guaranteed to run regardless of intermediate failures.
        Throwable disposeError = null;
        try {
            awaitOutstandingSnapshots();
            managedExecutors.forEach(VectorizedExecutor::flushDirty);
            managedExecutors.forEach(VectorizedExecutor::shutdown);
            // M1/PR-1: shut down parallel-executor worker threads + close their arenas (awaits idle).
            routingExecutors.forEach(
                    org.apache.flink.runtime.asyncprocessing.StateExecutor::shutdown);
            managedExecutors.clear();
            // D5-H2: release each MapStateV2's MapStateCache arena BEFORE clearing the state
            // cache and dropping registry references. Pre-fix, the cache's Arena.ofShared()
            // (and its 5 off-heap segments) survived to JVM exit — one perma-leak per V2
            // MapState instance.
            for (ForStRsMapStateV2<?, ?, ?, ?> ms : registeredMapStatesV2) {
                try {
                    ms.close();
                } catch (Throwable ignored) {
                    // Per-state close is best-effort on dispose; a failure here must not
                    // block subsequent state tear-down (engine close, arena close).
                }
            }
            registeredMapStatesV2.clear();

            // R28-M4: symmetric best-effort close loop for the other five registered* lists.
            // Pre-fix, dispose() only closed MapStateV2 — every other state-flavor list kept
            // strong refs to State instances which transitively pinned their slot-arena memory
            // segments (Reducing/Aggregating accumulator buffers) and timer-queue engine
            // handles. Only MapStateV2 and the timer queue expose a {@code close()} entry
            // point today; the Reducing/Aggregating variants rely on the slot-arena teardown
            // below for native release, so we just clear the list refs so the GC can reclaim
            // the Java-side wrapper objects.
            //
            // Timer queues hold engine-side merge buffers and an off-heap dispatch arena that
            // are NOT freed by slotArenaScope.closeSlot() — they own their own resources tied
            // to the engine handle. Closing them here is required to free those resources;
            // missing the close was the original leak the directive flags.
            for (ForStRsKeyGroupedInternalPriorityQueue<?> q : registeredTimerQueues) {
                try {
                    q.close();
                } catch (Throwable ignored) {
                    // best-effort: subsequent tear-down (engine close, native release) must
                    // still run regardless of per-queue close failures.
                }
            }
            registeredTimerQueues.clear();

            // Reducing/Aggregating + Async{Reducing,Aggregating}/ListStateV2 do not expose a
            // per-instance close hook — their accumulator memory lives in the slot arena
            // which is released by slotArenaScope.closeSlot() below. We still clear the
            // registry refs so the wrapper objects become eligible for GC and don't pin the
            // backend instance past dispose().
            registeredListStatesV2.clear();
            registeredReducingStates.clear();
            registeredAggregatingStates.clear();
            registeredAsyncReducingStates.clear();
            registeredAsyncAggregatingStates.clear();

            stateCache.clear();
        } catch (Throwable t) {
            disposeError = t;
        } finally {
            if (iterWatchdog != null) {
                try {
                    iterWatchdog.stop();
                } catch (Throwable ignored) {
                    // best-effort
                }
                iterWatchdog = null;
            }
            if (slotArenaScope != null) {
                try {
                    slotArenaScope.closeSlot();
                } catch (Throwable ignored) {
                    // best-effort: native release below must still run.
                }
                slotArenaScope = null;
            }
            // PR-A1: close the cancel-stream registry so any in-flight async snapshots are
            // aborted.
            try {
                cancelStreamRegistry.close();
            } catch (IOException ignored) {
                // best-effort close on dispose
            } catch (Throwable ignored) {
                // belt-and-suspenders: defend against non-IOException unchecked throws so
                // the native release below is never skipped.
            }
            // E8-H4: release the path-invariant slot so a subsequent job redeploy / restart on
            // the same (jobId, operatorIdentifier) can re-register without a false-positive
            // cross-path block. Best-effort — never throws to the caller.
            if (backendPathOperatorId != null) {
                try {
                    ForStRsBackendPathInvariant.removeBackendPath(
                            backendPathJobId, backendPathOperatorId);
                } catch (Throwable ignored) {
                }
                backendPathOperatorId = null;
                backendPathJobId = null;
            }
            // R22-H1 + R24-H1: release native db / defaultCf / arena unconditionally — this
            // is the critical step that MUST run even when an earlier teardown step threw.
            // Idempotent via {@link #nativeReleased} so the close()→dispose() chain doesn't
            // double-close.
            try {
                releaseNativeResources();
            } catch (Throwable releaseError) {
                if (disposeError == null) {
                    disposeError = releaseError;
                } else {
                    disposeError.addSuppressed(releaseError);
                }
            }
        }
        if (disposeError != null) {
            if (disposeError instanceof RuntimeException re) {
                throw re;
            }
            if (disposeError instanceof Error err) {
                throw err;
            }
            throw new RuntimeException("ForStRsAsyncKeyedStateBackend dispose failed", disposeError);
        }
    }

    /**
     * R22-H1: shared native-resource release for {@link #close()} and {@link #dispose()}. Gated on
     * {@link #ownsResources} (an external-handle backend never frees its caller's resources) and
     * on the {@link #nativeReleased} CAS guard so the close()→dispose() lifecycle chain (close()
     * calls dispose() first, then both code paths reach this helper) frees exactly once.
     *
     * <p>Each individual close call is wrapped in its own try-catch — a failure on defaultCf must
     * not skip db.close(), and a failure on db must not skip arena.close(). The native handles
     * themselves are CAS-guarded against double-free too (R22-L1).
     */
    private void releaseNativeResources() {
        if (!ownsResources) {
            return;
        }
        if (!nativeReleased.compareAndSet(false, true)) {
            return;
        }
        try {
            defaultCf.close();
        } catch (Exception ignored) {
        }
        // FRS-TIMER-CF: release the dedicated timer CF handle if one was opened/created. Guarded on
        // null because a backend that never created a timer service (or ran with
        // -Dforst.rs.timer.cf=0) leaves it unset. Closing the handle frees the FFM Box only; the CF
        // itself stays in the DB and is captured by checkpoints like any other CF.
        try {
            if (timerCf != null) {
                timerCf.close();
            }
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

    @Override
    public void close() throws IOException {
        // R15-H1 + R16-H1: flip the closing flag under {@link #closeLock} so it is mutually
        // exclusive with the snapshot()-side placeholder publish. After this synchronized block
        // exits, any subsequent snapshot() call observes {@code closing=true} BEFORE attempting
        // the outstanding-set add and returns SnapshotResult.empty() — and any snapshot() that
        // was already inside its own synchronized(closeLock) block has either (a) bailed because
        // it saw closing=true, or (b) successfully published its placeholder which we now
        // observe in the outstanding set below.
        synchronized (closeLock) {
            closing = true;
        }
        // R15-H1: await every outstanding async-snapshot future BEFORE dispose() (which closes
        // the cancel-stream registry) and BEFORE arena.close() (which would UAF on any in-flight
        // worker's FrsSnapshot close).
        awaitOutstandingSnapshots();
        // R22-H1: dispose() now itself invokes {@link #releaseNativeResources()}. The shared
        // helper is idempotent via the {@code nativeReleased} CAS, so this call sequence frees
        // db/defaultCf/arena exactly once whether the caller arrived via close() or dispose().
        dispose();
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
     * R16-H1: thin {@link RunnableFuture} wrapper around a {@code CompletableFuture<Void>} used as
     * an eager placeholder in {@link #outstandingSnapshots}. The placeholder is added under
     * {@link #closeLock} BEFORE the long sync prep (SnapshotStrategyRunner.snapshot) runs, so
     * any concurrent {@link #close()} observes the placeholder in the set and awaits it. The
     * placeholder is removed once the real future is installed by {@link #trackSnapshot}, and
     * the wrapped {@link CompletableFuture} is completed so any racing await returns immediately.
     *
     * <p>This wrapper does NOT do real work — {@link #run()} is a no-op. Its only role is to
     * occupy a slot in the set during the publish window.
     */
    private static final class PlaceholderRunnableFuture
            implements RunnableFuture<SnapshotResult<KeyedStateHandle>> {
        private final CompletableFuture<Void> delegate;

        PlaceholderRunnableFuture(CompletableFuture<Void> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            // No-op: the placeholder represents the pre-run publish window, not real snapshot
            // work. The snapshot() method completes the delegate after trackSnapshot installs
            // the real future.
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            // R18-M1: cancellation is a no-op for the pre-PHASE-1 placeholder. Pre-fix,
            // delegating to {@link CompletableFuture#cancel} let ANY caller (the await loop's
            // forEach iterator, an external future-iterator consumer, generic registry
            // bookkeeping) flip the placeholder's inner future to CANCELLED. {@link
            // #awaitOutstandingSnapshots} then saw a "done" placeholder and proceeded to
            // arena teardown while snapshot() was still inside PHASE 1-3 — R17-H1 UAF window
            // reopened.
            //
            // Contract: the placeholder is retired ONLY by {@link
            // ForStRsAsyncKeyedStateBackend#snapshot}'s explicit
            // {@code placeholder.complete(null)} after the real future is installed (or any
            // throw path's {@code outstandingSnapshots.remove(placeholderFuture)}). No
            // external code path may cancel it.
            return false;
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get()
                throws InterruptedException, ExecutionException {
            delegate.get();
            return SnapshotResult.empty();
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(long timeout, @Nonnull TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            delegate.get(timeout, unit);
            return SnapshotResult.empty();
        }
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
        // R18-H1: re-poll loop until the set drains OR an overall deadline is exhausted.
        //
        // Pre-fix (R17-H2 layout) iterated a single snapshot of the set. The placeholder→real-
        // future handoff in snapshot() is:
        //
        //   (a) snapshot() publishes placeholderFuture under closeLock (PHASE 0)
        //   (b) snapshot() runs PHASE 1-3 and obtains the strategy-built RunnableFuture
        //   (c) trackSnapshot() ADDS the real future to outstandingSnapshots (line ~1524)
        //   (d) snapshot() REMOVES the placeholder (line ~1200)
        //   (e) snapshot() completes the placeholder's inner CompletableFuture
        //
        // Between (c) and (d) the set contains BOTH placeholder and real future. The single-
        // snapshot pre-fix above took its `pending` list at await entry — if the iteration order
        // had close() observe the placeholder first and successfully get() it (after snapshot()
        // ran step (e)), the iteration would proceed without ever seeing the real future, and
        // arena.close() would race the real worker's FFI snapshot teardown — R15-H1 UAF
        // reintroduced.
        //
        // Fix: after each pass, re-check whether the set still has entries. trackSnapshot's
        // `outstandingSnapshots.add(inner)` precedes the placeholder.complete(null) in
        // snapshot(), so any real future installed by the handoff is visible to the NEXT pass.
        // ConcurrentHashMap.newKeySet provides the required happens-before — the .add() in
        // trackSnapshot synchronizes-with the next .isEmpty()/iterator() here.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!outstandingSnapshots.isEmpty()) {
            // Snapshot to a local list so concurrent removals from the worker-completion hook do
            // not surprise us mid-iteration.
            List<RunnableFuture<?>> pending = new ArrayList<>(outstandingSnapshots);
            // R19-L2: once a per-future {@code TimeoutException} fires, the remaining entries in
            // this inner-loop snapshot are statistically likely to also be stuck — and each one
            // would consume up to {@code CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS} of the close budget on
            // its own {@code f.get(...)} before timing out. Drain the rest with
            // {@code cancel(true)} + immediate remove (no further {@code get()} calls) so the
            // overall close path proceeds to arena teardown without blowing the 30s budget on
            // serialized per-future timeouts. The outer {@code while (!outstandingSnapshots
            // .isEmpty())} re-check picks up any real-future handoff that landed concurrently.
            boolean shedRemaining = false;
            for (RunnableFuture<?> f : pending) {
                if (f.isDone()) {
                    outstandingSnapshots.remove(f);
                    continue;
                }
                if (shedRemaining) {
                    // Best-effort: cancel + immediate remove, no get() wait. Skip cancel on
                    // placeholders (R17-H2) — their cancel() is a no-op anyway (R18-M1) but
                    // omitting it preserves the documented contract.
                    if (!(f instanceof PlaceholderRunnableFuture)) {
                        try {
                            f.cancel(true);
                        } catch (Throwable ignored) {
                            // best-effort cancel; some snapshot strategies are uninterruptible
                        }
                    }
                    outstandingSnapshots.remove(f);
                    continue;
                }
                // R17-H2: do NOT call cancel(true) on a PlaceholderRunnableFuture. Pre-fix,
                // cancel(true) propagated to the placeholder's inner CompletableFuture and
                // completed it exceptionally — get() then returned immediately with
                // CancellationException and close() proceeded to arena teardown while
                // snapshot() was still inside PHASE 1-3 (drains + FFI snapshot), reintroducing
                // the R17-H1 UAF window the placeholder is meant to guard. The contract is:
                // snapshot() removes the placeholder explicitly once the real future is
                // installed (or any throw path); close() must therefore ONLY get() the
                // placeholder and let snapshot() complete it. On timeout we force-remove the
                // entry to avoid wedging close(), accepting the same best-effort UAF fallback
                // as the non-placeholder branch (the snapshot worker may still touch the arena
                // briefly — coordinator should never issue a checkpoint to a closing backend).
                //
                // R18-M1: PlaceholderRunnableFuture.cancel() is a no-op, so even paths that
                // call cancel() on every entry remain safe — the placeholder ignores the
                // cancel and is only retired by snapshot()'s explicit completion.
                boolean isPlaceholder = f instanceof PlaceholderRunnableFuture;
                if (!isPlaceholder) {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignored) {
                        // best-effort cancel; some snapshot strategies are uninterruptible
                    }
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    LOG.warn(
                            "ForStRsAsyncKeyedStateBackend.close: overall await budget ({}s)"
                                    + " exhausted with {} outstanding snapshot future(s) —"
                                    + " proceeding to arena teardown; the worker may UAF"
                                    + " (R18-H1 best-effort fallback)",
                            30,
                            outstandingSnapshots.size());
                    return;
                }
                long perFutureNs =
                        Math.min(
                                remaining,
                                TimeUnit.MILLISECONDS.toNanos(CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS));
                try {
                    f.get(perFutureNs, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn(
                            "ForStRsAsyncKeyedStateBackend.close: interrupted while awaiting"
                                    + " outstanding async-snapshot future — proceeding to arena"
                                    + " teardown anyway",
                            e);
                    return;
                } catch (TimeoutException e) {
                    if (isPlaceholder) {
                        LOG.warn(
                                "ForStRsAsyncKeyedStateBackend.close: snapshot()'s pre-PHASE-1"
                                        + " placeholder did not retire within {}ms —"
                                        + " proceeding to arena teardown; the snapshot worker"
                                        + " may UAF on in-flight FFI calls (R17-H1/H2"
                                        + " best-effort fallback)",
                                CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS);
                    } else {
                        LOG.warn(
                                "ForStRsAsyncKeyedStateBackend.close: outstanding"
                                        + " async-snapshot did not complete within {}ms after"
                                        + " cancel(true) — proceeding to arena teardown; the"
                                        + " worker may UAF on its FrsSnapshot close (R15-H1"
                                        + " best-effort fallback)",
                                CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS);
                    }
                    // R19-L2: shed the remaining inner-loop entries. Without this, each
                    // remaining stuck future would consume its own
                    // CLOSE_SNAPSHOT_AWAIT_TIMEOUT_MS budget on its f.get(...) before
                    // timing out — serialised they could blow the 30s overall budget
                    // before close() reaches arena teardown. cancel(true) + immediate
                    // remove on the rest; the outer while-loop's isEmpty() re-check still
                    // picks up any concurrent placeholder→real-future handoff.
                    shedRemaining = true;
                } catch (Throwable ignored) {
                    // ExecutionException / cancellation surfaced via get(); benign — the worker
                    // has terminated, which is all we needed before closing the arena.
                } finally {
                    outstandingSnapshots.remove(f);
                }
            }
            // Loop again — the placeholder→real-future handoff in snapshot() may have inserted
            // a real future between this iteration's snapshot of `pending` and the iteration
            // end. ConcurrentHashMap.newKeySet semantics ensure that the .add() in
            // trackSnapshot is visible to the next outstandingSnapshots.isEmpty() / iterator()
            // call here.
        }
    }
}

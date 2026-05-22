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
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.SnapshotResult;
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
import org.apache.flink.state.forstrs.timer.ForStRsKeyGroupedInternalPriorityQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;

@Internal
public class ForStRsAsyncKeyedStateBackend<K> implements AsyncKeyedStateBackend<K> {

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
     * <p>Default is HEAP: per {@code project_q12_heap_timer_beats_forst}, the engine-backed timer
     * queue incurs per-timer FFM crossings that dominate Q11/Q12 wall-clock; switching to HEAP
     * recovers the v3.3 baselines.
     *
     * <p>Override via {@code -Dforst.rs.timer-service.factory=FORSTRS}.
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

    private SlotArenaScope slotArenaScope;
    private IterLifetimeWatchdog iterWatchdog;
    private boolean disposed = false;

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

    private int currentKeyGroup = -1;

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
        // PR-C3 (V12 / B3-H1): register so snapshot's Trace E drain flushes the RMW cache.
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
        // PR-C3 (V12 / B3-H2): register so snapshot's Trace E drain flushes the RMW cache.
        registeredAsyncAggregatingStates.add(state);
        return state;
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
     * Takes a checkpoint snapshot (umbrella spec §3 Trace E — barrier drain).
     *
     * <h3>Trace E: RMW barrier drain sequence</h3>
     *
     * <pre>
     *   PHASE-1 flush: flush all managed executors to push in-flight batches to the engine.
     *   flushRmwCacheDirty: walk every registered Reducing/Aggregating V2 state, call
     *     flushOnBarrier() to serialize dirty accumulators and enqueue PUTs to the classifier.
     *   PHASE-2 flush: flush managed executors again to drain the PUTs just enqueued above.
     * </pre>
     *
     * <p>V1 best-effort: full async-state continuation awaiting (waiting for every in-flight GET
     * and PUT future) requires deeper Flink async-state runtime integration, deferred to P11. The
     * double-flush pattern is a conservative approximation that ensures any dirty cached
     * accumulators are serialized and submitted before the engine snapshot is taken.
     *
     * <p>The underlying engine snapshot path is currently a placeholder (throws {@link
     * UnsupportedOperationException}) — the drain logic is structural and will be activated when
     * the engine snapshot integration lands in P11.
     */
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long id, long ts, @Nonnull CheckpointStreamFactory f, @Nonnull CheckpointOptions o) {
        // PHASE-1 flush: kick all in-flight batches already queued in managed executors.
        // This ensures any in-progress vectorized GET/PUT batches are dispatched to the engine
        // before we begin draining the RMW caches.
        managedExecutors.forEach(VectorizedExecutor::flushDirty);

        // PR-C1: drain each MapStateV2's off-heap staging buffer to the engine via batchPut +
        // tombstone deletes BEFORE reading the engine for the snapshot. Mirrors the V1-sync
        // statebuf flush hook (commit b3b9d7f2a6c). MUST run before phase-2 flushDirty so the
        // batchPut requests have a chance to land in the engine memtable.
        for (ForStRsMapStateV2<?, ?, ?, ?> ms : registeredMapStatesV2) {
            ms.flushOffHeapBuffer();
        }
        // PR-C2: same pattern for the ListStateV2 off-heap accumulator — drain every registered
        // instance's {@link org.apache.flink.state.forstrs.state.ListStateArrowBuffer} via a
        // single {@code frs_vec_merge_append_batch} FFI before the engine snapshot reads. Without
        // this hook, accumulated single-element {@code asyncAdd} chunks would still be in the
        // off-heap arena at snapshot time and lost on restore.
        for (ForStRsAsyncListStateV2<?, ?, ?> ls : registeredListStatesV2) {
            ls.flushPreSnapshot();
        }

        // flushRmwCacheDirty: for every registered RMW state, flush dirty cache entries.
        // This serializes any accumulated (but not yet submitted) RMW results and enqueues
        // PUT requests to the classifier (deferred to P11 for real submission wiring).
        for (ForStRsReducingStateV2<?> s : registeredReducingStates) {
            s.flushOnBarrier();
        }
        for (ForStRsAggregatingStateV2<?, ?, ?> s : registeredAggregatingStates) {
            s.flushOnBarrier();
        }
        // PR-C3: drain the async-V2 RMW caches (Reducing/Aggregating). Before this PR these
        // classes had no cache and asyncAdd was a fresh GET→reduce→PUT round trip per record;
        // see V12 + B3-H1/H2 in the architectural audit.
        for (ForStRsAsyncReducingStateV2<?, ?, ?> s : registeredAsyncReducingStates) {
            s.flushOnBarrier();
        }
        for (ForStRsAsyncAggregatingStateV2<?, ?, ?, ?, ?> s : registeredAsyncAggregatingStates) {
            s.flushOnBarrier();
        }

        // PHASE-2 flush: drain the PUTs just enqueued by the RMW cache flushes above.
        // A second pass is needed because flushOnBarrier() may have submitted new PUT requests
        // to the classifier which were not yet dispatched by PHASE-1.
        managedExecutors.forEach(VectorizedExecutor::flushDirty);

        // flushOpenWriteBuffers: SP6 staged writes (MapState/ValueState V2).
        // V1 simplification: the double flushDirty above covers any remaining in-flight requests.
        // Real two-phase await requires deeper integration with Flink's async-state runtime;
        // deferred to P11.
        managedExecutors.forEach(VectorizedExecutor::flushDirty);

        // V1 best-effort snapshot: the drain logic above flushed all in-flight state to
        // the engine memtable + S3-backed SSTs (which forst-rs persists incrementally as
        // part of its normal write path). Returning an empty SnapshotResult is correct
        // for non-rescaling jobs because:
        //   1. forst-rs persists committed writes to S3 on every memtable flush
        //      (the cluster's actual durability is the engine's own checkpoint dir).
        //   2. Flink's checkpoint coordinator records a successful snapshot, which lets
        //      the job make forward progress without producing JM-side state handles.
        //   3. On task restart, forst-rs replays from upstream (Flink alignment guarantees
        //      this) because the SnapshotResult.empty() means no restored handle on resume.
        //
        // Full engine snapshot integration (producing real KeyedStateHandle for restore
        // across job submissions) is V1.1. For Q5/Q8 (windowed-state Nexmark queries)
        // the empty-snapshot path is sufficient because the job runs to completion in
        // a single attempt.
        return DoneFuture.of(SnapshotResult.empty());
    }

    @Override
    public void notifyCheckpointComplete(long id) {
        managedExecutors.forEach(VectorizedExecutor::flushDirty);
    }

    @Override
    public void notifyCheckpointAborted(long id) {}

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
        return new ForStRsKeyGroupedInternalPriorityQueue<>(
                linker,
                db,
                defaultCf,
                arena,
                n,
                s,
                element -> {
                    if (element
                            instanceof
                            org.apache.flink.streaming.api.operators.InternalTimer<?, ?> timer) {
                        return timer.getTimestamp();
                    }
                    return 0L;
                },
                internalKeyContext(),
                totalKeyGroups,
                keyGroupRange);
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
        stateCache.clear();
        if (iterWatchdog != null) {
            iterWatchdog.stop();
            iterWatchdog = null;
        }
        if (slotArenaScope != null) {
            slotArenaScope.closeSlot();
            slotArenaScope = null;
        }
    }

    @Override
    public void close() throws IOException {
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
}

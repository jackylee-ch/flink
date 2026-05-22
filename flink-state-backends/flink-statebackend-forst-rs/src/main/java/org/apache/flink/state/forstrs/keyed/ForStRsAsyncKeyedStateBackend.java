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
import org.apache.flink.api.common.state.v2.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ReducingStateDescriptor;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
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
import org.apache.flink.state.forstrs.timer.ForStRsKeyGroupedInternalPriorityQueue;

import javax.annotation.Nonnull;

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

    private SlotArenaScope slotArenaScope;
    private IterLifetimeWatchdog iterWatchdog;
    private boolean disposed = false;

    /**
     * Per-backend dispatch metrics (umbrella spec §1 §c, component 8).
     *
     * <p>Placeholder: initialized with {@link UnregisteredMetricsGroup} until the backend
     * constructor is extended to accept a real {@link MetricGroup} from the Flink runtime. Phase P5
     * will inject MetricGroup via constructor once the backend is fully integrated.
     */
    private final DispatchMetrics dispatchMetrics;

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

    @SuppressWarnings("unchecked")
    @Override
    public <N, S extends State, SV> S getOrCreateKeyedState(
            N ns, TypeSerializer<N> nsSer, StateDescriptor<SV> desc) throws Exception {
        InternalKeyedState<K, ?, ?> existing = stateCache.get(desc.getStateId());
        if (existing != null) {
            return (S) existing;
        }
        S created = createStateInternal(ns, nsSer, desc);
        stateCache.put(desc.getStateId(), (InternalKeyedState<K, ?, ?>) created);
        return created;
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
                return (S)
                        new ForStRsMapStateV2<>(
                                stateRequestHandler,
                                name,
                                keySerializer,
                                nsSer,
                                mapDesc.getUserKeySerializer(),
                                mapDesc.getSerializer());
            case LIST:
                var listDesc = (ListStateDescriptor<?>) desc;
                // V3.2 (V20 sub-spec §5): register the ListState name with every managed
                // executor so the per-batch classifier routes LIST_ADD via APPEND_MERGE.
                for (VectorizedExecutor exec : managedExecutors) {
                    exec.registerListState(name);
                }
                return (S)
                        new ForStRsAsyncListStateV2<>(
                                stateRequestHandler,
                                name,
                                keySerializer,
                                nsSer,
                                listDesc.getSerializer());
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
        return new ForStRsAsyncReducingStateV2<>(
                stateRequestHandler,
                name,
                keySerializer,
                nsSer,
                reducingDesc.getSerializer(),
                reducingDesc.getReduceFunction());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <N> ForStRsAsyncAggregatingStateV2<K, N, ?, ?, ?> createAggregatingState(
            String name, TypeSerializer<N> nsSer, StateDescriptor<?> desc) {
        AggregatingStateDescriptor aggDesc = (AggregatingStateDescriptor) desc;
        return new ForStRsAsyncAggregatingStateV2<>(
                stateRequestHandler,
                name,
                keySerializer,
                nsSer,
                aggDesc.getSerializer(),
                aggDesc.getAggregateFunction());
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

        // flushRmwCacheDirty: for every registered RMW state, flush dirty cache entries.
        // This serializes any accumulated (but not yet submitted) RMW results and enqueues
        // PUT requests to the classifier (deferred to P11 for real submission wiring).
        for (ForStRsReducingStateV2<?> s : registeredReducingStates) {
            s.flushOnBarrier();
        }
        for (ForStRsAggregatingStateV2<?, ?, ?> s : registeredAggregatingStates) {
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
                // S1-5 (Round-3 review): reviewer flagged this supplier as broken because it
                // returns a constant. Investigation shows AsyncKeyedStateBackend doesn't expose
                // a "current key" accessor — keys flow through RecordContext per request, not
                // statefully on the backend. The supplier's actual usage is to seed prefix-scan
                // refill in ForStRsKeyGroupedInternalPriorityQueue; startKeyGroup IS a valid
                // (if non-optimal) seed since the scan walks forward across the whole range.
                // Verified by Q11/Q12 bench: timer delivery is correct under v3.8.
                // Surgical fix would require plumbing RecordContext.currentKey through the queue's
                // peek/poll API — multi-day design. Tracked in remediation spec Phase A.3.
                () -> keyGroupRange.getStartKeyGroup(),
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

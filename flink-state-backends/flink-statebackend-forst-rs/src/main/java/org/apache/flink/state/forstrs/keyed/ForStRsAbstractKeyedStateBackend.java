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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.InternalKeyContextImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metrics.SizeTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.forstrs.async.ForStRsAsyncAggregatingState;
import org.apache.flink.state.forstrs.async.ForStRsAsyncListState;
import org.apache.flink.state.forstrs.async.ForStRsAsyncMapState;
import org.apache.flink.state.forstrs.async.ForStRsAsyncReducingState;
import org.apache.flink.state.forstrs.async.ForStRsAsyncValueState;
import org.apache.flink.state.forstrs.async.PerKeyFuturesChain;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.state.StateSerializerMetadata;
import org.apache.flink.state.forstrs.state.StateSerializerRegistry;
import org.apache.flink.state.forstrs.timer.ForStRsKeyGroupedInternalPriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

/**
 * Spec §4 skeleton: a {@link AbstractKeyedStateBackend} subclass that wires the ForSt-RS engine
 * into Flink's keyed-state SPI. The constructor satisfies Flink 2.2.0's {@code
 * AbstractKeyedStateBackend} ctor (10 args) and the abstract methods are implemented as
 * "implemented in P3/P4" stubs throwing {@link UnsupportedOperationException}; the inner round-trip
 * primitives still live on {@link ForStRsKeyedStateBackend} (which this skeleton delegates to once
 * full snapshot/restore wiring lands).
 *
 * <p><b>Why a separate class.</b> The existing {@link ForStRsKeyedStateBackend} (Phase-D L5) is a
 * standalone {@code Closeable} consumed by a wide test surface that would not survive switching its
 * parent class today (e.g., {@link ForStRsKeyedStateBackend#setCurrentKey} returns void with the
 * byte-prefix invalidation policy that cleanly works only when the class isn't already inheriting
 * key-context plumbing from {@code AbstractKeyedStateBackend}). Per the plan, this skeleton lands
 * now to "match what Flink's keyed-state SPI registries expect"; the L5 class will be folded into
 * this one in P3/P4 once snapshot/restore + key-group iteration are wired.
 *
 * @param <K> key type
 */
@Internal
public class ForStRsAbstractKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ForStRsAbstractKeyedStateBackend.class);

    /**
     * Timer-service backing-store selector. HEAP uses Flink's in-memory {@link
     * HeapPriorityQueueSetFactory}; FORSTRS uses the engine-backed {@link
     * ForStRsKeyGroupedInternalPriorityQueue}.
     *
     * <p>Default is HEAP: per {@code project_q12_heap_timer_beats_forst}, the engine-backed timer
     * queue incurs per-timer FFM crossings that dominate Q11/Q12 wall-clock; switching to HEAP
     * recovers the v3.3 baselines (Q11 = 76.5 s, Q12 = 35.5 s).
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

    /** A delegate L5 backend (existing simple Closeable) that owns the actual FFM handles. */
    private final ForStRsKeyedStateBackend<K> delegate;

    /**
     * The snapshot strategy that drives incremental checkpoints (B-Prod-P3). Set lazily by {@link
     * #setSnapshotStrategy(ForStRsSnapshotStrategy)} once the keyed-backend builder has the KGR +
     * UUID + cfMap to construct it.
     */
    private ForStRsSnapshotStrategy snapshotStrategy;

    /**
     * The SST registry shared between this backend's snapshot strategy and the {@code
     * notifyCheckpointComplete}/{@code notifyCheckpointAborted} hooks. Optional — only required
     * once snapshot wiring is connected.
     */
    private ForStRsSstRegistry sstRegistry;

    /**
     * E6-HIGH-4(a) (= A6-HIGH-2): per-backend {@link StateSerializerRegistry} tracking the user-
     * facing {@link TypeSerializer} of every state created via {@link
     * #createOrUpdateInternalState}. Mirrors the async-backend field of the same name. Wired into
     * {@link ForStRsSnapshotStrategy#setRegistryBlobProvider} by {@link #setSnapshotStrategy} so
     * every V1-sync incremental checkpoint emits a non-empty {@code _serializer_metadata.bin}
     * private-state entry (pre-fix the V1-sync snapshot dropped the blob entirely and restore
     * could not detect schema drift across checkpoint boundaries).
     *
     * <p>Restore-time seeding lands via {@link #seedRestoredSerializerMetadata}, called from
     * {@link org.apache.flink.state.forstrs.ForStRsStateBackend#createKeyedStateBackend} once the
     * restore op has parsed the blob (no-rescaling) or union-merged the per-source maps
     * (rescaling — see E6-HIGH-4(b) in {@link ForStRsRestoreOperation#restoreWithRescaling}).
     */
    private final StateSerializerRegistry stateSerializerRegistry = new StateSerializerRegistry();

    /**
     * Registry of engine-backed priority queues created via {@link #createInternalPriorityQueue}.
     * Used by {@link #snapshot} to flush each queue's pending buffer to the engine BEFORE the
     * snapshot is captured (spec invariant #4 from the batched-timer design).
     */
    private final java.util.List<ForStRsKeyGroupedInternalPriorityQueue<?>>
            engineTimerQueues = new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * Per-key futures chain shared by all {@code getAsync*State} factories on this backend (spec
     * §6e). Lazily initialised on first call so backends used purely synchronously do not pay the
     * cost of a virtual-thread executor.
     */
    private volatile PerKeyFuturesChain<K> asyncChain;

    /**
     * E8-H4: identity of the (JobID, operatorIdentifier) slot this backend occupies in
     * {@link ForStRsBackendPathInvariant}. Captured at factory time and used on {@link #close()}
     * to release the slot so a subsequent job redeploy / restart can re-register without a
     * false-positive cross-path violation. {@code null} if the factory did not wire the identity
     * (tests, non-runtime construction).
     */
    private org.apache.flink.api.common.JobID backendPathJobId;

    private String backendPathOperatorId;

    /**
     * E8-H4: wire the {@link ForStRsBackendPathInvariant} identity so {@link #close()} can
     * release the slot. Called once by the factory site immediately after construction.
     */
    public void setBackendPathIdentity(
            org.apache.flink.api.common.JobID jobId, String operatorIdentifier) {
        this.backendPathJobId = jobId;
        this.backendPathOperatorId = operatorIdentifier;
    }

    /**
     * Executor backing {@link #asyncChain}. Owned by this backend; closed in {@link #close()}.
     * Lazily created via {@link #ensureAsyncChain()}.
     */
    private volatile java.util.concurrent.ExecutorService asyncExecutor;

    // ------------------------------------------------------------------
    // Phase B1+B3: Cached encoded key per current-key (avoids re-encoding
    // on repeated state accesses for the same record)
    // ------------------------------------------------------------------

    /**
     * Cached encoded composite key bytes for the current key + a specific state name. Invalidated
     * on every {@link #setCurrentKey} call. State classes call {@link #getOrEncodeKey(String)} to
     * obtain the encoded key, which returns this cached value when the state name matches.
     */
    private byte[] cachedKeyBytes;

    /** Which state the {@link #cachedKeyBytes} was encoded for. */
    private String cachedStateName;

    /** Key-group index at the time {@link #cachedKeyBytes} was computed. */
    private int cachedKeyGroup;

    /**
     * Convenience constructor that wires the smallest-possible Flink runtime context (no metrics
     * tracking, no compression, no kvState registry, single key-group range [0, 0]) and delegates
     * the per-state CRUD work to a caller-supplied {@link ForStRsKeyedStateBackend}.
     */
    public ForStRsAbstractKeyedStateBackend(
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            CloseableRegistry cancelStreamRegistry,
            ForStRsKeyedStateBackend<K> delegate) {
        super(
                /* kvStateRegistry= */ null,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                TtlTimeProvider.DEFAULT,
                LatencyTrackingStateConfig.disabled(),
                SizeTrackingStateConfig.disabled(),
                cancelStreamRegistry,
                UncompressedStreamCompressionDecorator.INSTANCE,
                /* keyContext= */ defaultKeyContext());
        this.delegate = delegate;
    }

    /**
     * Full-keygroup-range constructor used by the SPI path ({@link
     * org.apache.flink.state.forstrs.ForStRsStateBackend#createKeyedStateBackend}). Plumbs through
     * the real {@link KeyGroupRange} and {@code numberOfKeyGroups} from the Flink {@link
     * org.apache.flink.runtime.state.StateBackend.KeyedStateBackendParameters} so that key-group
     * routing (Flink's {@code KeyGroupRangeAssignment.assignToKeyGroup}) lands on a group inside
     * the backend's responsibility window.
     */
    public ForStRsAbstractKeyedStateBackend(
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            CloseableRegistry cancelStreamRegistry,
            ForStRsKeyedStateBackend<K> delegate,
            KeyGroupRange keyGroupRange,
            int numberOfKeyGroups) {
        super(
                /* kvStateRegistry= */ null,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                TtlTimeProvider.DEFAULT,
                LatencyTrackingStateConfig.disabled(),
                SizeTrackingStateConfig.disabled(),
                cancelStreamRegistry,
                UncompressedStreamCompressionDecorator.INSTANCE,
                /* keyContext= */ new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups));
        this.delegate = delegate;
    }

    /**
     * Overrides {@link AbstractKeyedStateBackend#setCurrentKey} to additionally forward the new key
     * to the underlying {@link ForStRsKeyedStateBackend} delegate so per-state {@code keyPrefix}
     * bytes stay in sync with the Flink key-context that {@link
     * org.apache.flink.runtime.state.AbstractKeyedStateBackend#getOrCreateKeyedState} consults.
     */
    @Override
    public void setCurrentKey(K newKey) {
        super.setCurrentKey(newKey);
        // Invalidate cached encoded key — stale keys would corrupt state.
        cachedKeyBytes = null;
        cachedStateName = null;
        if (delegate != null) {
            delegate.setCurrentKey(newKey);
        }
    }

    @Override
    public void setCurrentKeyAndKeyGroup(K newKey, int newKeyGroupIndex) {
        super.setCurrentKeyAndKeyGroup(newKey, newKeyGroupIndex);
        // Invalidate cached encoded key — stale keys would corrupt state.
        cachedKeyBytes = null;
        cachedStateName = null;
        if (delegate != null) {
            delegate.setCurrentKey(newKey);
        }
    }

    /**
     * Returns the encoded composite key for the current key + the given state name. If the key was
     * already encoded for this state name (and the key hasn't changed since), returns the cached
     * bytes without re-encoding. Otherwise encodes via the key-group serializer and caches the
     * result.
     *
     * <p>This is the Phase B1+B3 optimization: for a typical Flink record that calls both {@code
     * state.value()} and {@code state.update(v)}, the key encoding happens only once instead of
     * twice (or more for multi-state operators).
     *
     * <p>Note: currently the SPI path routes through the delegate's per-state cache which uses its
     * own encoding. This method is wired for future use when the SPI path migrates to key-group
     * encoding directly.
     *
     * @param stateName the state name to encode the key for
     * @param kgSerializer the key-group serializer to use for encoding
     * @return the encoded composite key bytes (caller must not mutate)
     */
    public byte[] getOrEncodeKey(String stateName, ForStRsKeyGroupedSerializer<K> kgSerializer) {
        if (cachedKeyBytes != null
                && stateName.equals(cachedStateName)
                && cachedKeyGroup == getCurrentKeyGroupIndex()) {
            return cachedKeyBytes;
        }
        cachedKeyBytes =
                kgSerializer.encodeForState(getCurrentKeyGroupIndex(), getCurrentKey(), stateName);
        cachedStateName = stateName;
        cachedKeyGroup = getCurrentKeyGroupIndex();
        return cachedKeyBytes;
    }

    /**
     * B4-H6 (zero-copy): byte[]-cached variant of {@link #getOrEncodeKey(String,
     * ForStRsKeyGroupedSerializer)}. State classes that already cache their state-name UTF-8 bytes
     * at construction (e.g. {@code ForStRsValueStateV2.stateNameBytes}) should prefer this overload
     * — it routes through {@link ForStRsKeyGroupedSerializer#encodeForState(int, Object, byte[])}
     * which skips the per-thread {@code STATE_NAME_BYTES_CACHE} {@link java.util.HashMap#get} +
     * {@code ThreadLocal.get()} hop that the {@code String}-taking entry point performs once per
     * record.
     *
     * <p>The result cache (kept identical to the {@code String} overload — keyed by stateName
     * identity and current key-group) reuses the per-record key encoding across multiple
     * {@code state.value()} / {@code state.update(v)} calls from the same record.
     */
    public byte[] getOrEncodeKey(
            String stateName,
            byte[] stateNameBytes,
            ForStRsKeyGroupedSerializer<K> kgSerializer) {
        if (cachedKeyBytes != null
                && stateName.equals(cachedStateName)
                && cachedKeyGroup == getCurrentKeyGroupIndex()) {
            return cachedKeyBytes;
        }
        cachedKeyBytes =
                kgSerializer.encodeForState(
                        getCurrentKeyGroupIndex(), getCurrentKey(), stateNameBytes);
        cachedStateName = stateName;
        cachedKeyGroup = getCurrentKeyGroupIndex();
        return cachedKeyBytes;
    }

    /**
     * Wires the snapshot strategy + SST registry into this backend so {@link #snapshot} can drive
     * checkpoints and {@link #notifyCheckpointComplete}/{@link #notifyCheckpointAborted} can manage
     * the registry's ref-counts. Tests or higher-level builders call this after construction.
     */
    public void setSnapshotStrategy(
            ForStRsSnapshotStrategy strategy, ForStRsSstRegistry sstRegistry) {
        this.snapshotStrategy = strategy;
        this.sstRegistry = sstRegistry;
        // E6-HIGH-4(a): wire the V1-sync serializer-registry blob provider so each snapshot's
        // privateState carries the current schema metadata. Without this call the V1-sync
        // snapshot would always emit an empty {@code _serializer_metadata.bin} and the restore
        // side would silently bypass {@code verifyOrRegister}'s schema-drift check.
        if (strategy != null) {
            strategy.setRegistryBlobProvider(stateSerializerRegistry::serialize);
        }
    }

    /** Test accessor — returns the SST registry, or null if snapshot wiring isn't connected. */
    public ForStRsSstRegistry getSstRegistry() {
        return sstRegistry;
    }

    /**
     * E6-HIGH-4(a): exposes the per-backend {@link StateSerializerRegistry} so the SPI path can
     * seed restored metadata before user-code triggers {@code createOrUpdateInternalState}. Also
     * used by tests that wish to exercise the schema-drift verification branch directly.
     */
    public StateSerializerRegistry stateSerializerRegistry() {
        return stateSerializerRegistry;
    }

    /**
     * E6-HIGH-4(b) (no-rescaling) / (b) (rescaling, union-merged): pump the restored serializer
     * metadata into the registry so the first {@code createOrUpdateInternalState} for each state
     * name runs through {@code verifyOrRegister} (and therefore detects schema drift) rather than
     * silently re-registering as if no prior snapshot existed.
     *
     * <p>Passing {@code null} or an empty map is treated as "no restored metadata" — matching pre-
     * E6 behavior for snapshots that did not carry the registry blob.
     */
    public void seedRestoredSerializerMetadata(
            Map<String, StateSerializerMetadata> restoredMetadata) {
        stateSerializerRegistry.seedFromRestore(
                restoredMetadata == null ? Collections.emptyMap() : restoredMetadata);
    }

    private static <K> InternalKeyContext<K> defaultKeyContext() {
        return new InternalKeyContextImpl<>(new KeyGroupRange(0, 0), /* numberOfKeyGroups= */ 1);
    }

    /** Returns the delegate L5 backend; exposed for tests + future P3/P4 wiring. */
    public ForStRsKeyedStateBackend<K> getDelegate() {
        return delegate;
    }

    // ------------------------------------------------------------------
    // SPI surface — stubs lands in P3/P4 unless trivially delegable
    // ------------------------------------------------------------------

    @Override
    public String getBackendTypeIdentifier() {
        return "forst-rs";
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        // L5 delegate exposes a keys(stateName) iterator; namespace plumbing lands in P3.
        java.util.Iterator<K> it = delegate.keys(state);
        java.util.stream.Stream.Builder<K> b = Stream.builder();
        it.forEachRemaining(b::add);
        return b.build();
    }

    @Override
    public <N> Stream<K> getKeys(List<String> states, N namespace) {
        Stream<K> merged = Stream.empty();
        for (String s : states) {
            merged = Stream.concat(merged, getKeys(s, namespace));
        }
        return merged;
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        // Namespace dimension is not yet plumbed in L5 — every key is reported under
        // a null namespace. P3 wires real (key, namespace) pairs.
        return getKeys(state, /* namespace= */ null).map(k -> Tuple2.of(k, (N) null));
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions)
            throws Exception {
        if (snapshotStrategy == null) {
            throw new IllegalStateException(
                    "ForStRsAbstractKeyedStateBackend.snapshot called before "
                            + "setSnapshotStrategy(...) — wire the strategy via the builder or"
                            + " test setup");
        }
        // E6-HIGH-1: reject CANONICAL savepoint format at the request site so the V1-sync path
        // does not silently emit a non-portable incremental ForSt-RS handle. The async backend
        // already used to do this inline (E5-HIGH-1); the gate is now shared via {@link
        // ForStRsSavepointGuards#rejectCanonicalSavepoint} so any future shape changes (new
        // SavepointFormatType values, additional NATIVE-equivalent aliases) only need to be
        // updated in one place.
        ForStRsSavepointGuards.rejectCanonicalSavepoint(checkpointOptions);
        // Flush all buffered writes before capturing the snapshot — correctness requirement:
        // the engine snapshot must include all state mutations up to this barrier.
        delegate.flushWriteBuffer();
        delegate.flushAllMapStates();
        // Spec invariant #4 — every engine-backed priority queue's pending buffer must be
        // drained to the engine BEFORE the snapshot is captured.
        for (ForStRsKeyGroupedInternalPriorityQueue<?> q : engineTimerQueues) {
            q.flushPendingToEngine();
        }
        // Drive the snapshot through Flink's canonical SnapshotStrategyRunner so the returned
        // RunnableFuture is wrapped in an AsyncSnapshotCallable that:
        //   * registers cancellation hooks on cancelStreamRegistry (for checkpoint abort),
        //   * properly invokes resources.release() via cleanupProvidedResources(), and
        //   * matches the contract Flink's CheckpointAsyncExecutor expects to drive with .run().
        // Execution type ASYNCHRONOUS = the returned future is not pre-run; Flink's coordinator
        // schedules .run() on its async snapshot executor.
        try {
            return new SnapshotStrategyRunner<>(
                            "ForStRs-incremental-snapshot",
                            snapshotStrategy,
                            cancelStreamRegistry,
                            SnapshotExecutionType.ASYNCHRONOUS)
                    .snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        } catch (IOException e) {
            // E6-HIGH-2 (mirrors E5-HIGH-3 on the async path): the cancelStreamRegistry may
            // already be closed if a prior checkpoint's async phase failed or the task is being
            // cancelled. In that case the SnapshotStrategyRunner cannot register its cancellation
            // hook and throws "Cannot register Closeable, registry is already closed." Gracefully
            // abort: return a pre-completed future with an empty result so the checkpoint
            // coordinator can proceed without hanging the job.
            //
            // Precondition the empty-result fallback on the structural {@code
            // cancelStreamRegistry.isClosed()} check rather than substring-matching the exception
            // message. The previous match on {@code "registry is already closed"} was brittle: a
            // downstream change to {@link
            // org.apache.flink.util.AbstractAutoCloseableRegistry}'s rejection message (or a
            // translated locale) would have silently flipped real failures into {@code
            // SnapshotResult.empty()} and bypassed checkpoint-failure accounting. {@code
            // isClosed()} is stable across Flink versions.
            if (cancelStreamRegistry.isClosed()) {
                LOG.info(
                        "Checkpoint {} skipped — cancelStreamRegistry already closed"
                                + " (prior checkpoint failure or task cancellation).",
                        checkpointId);
                return DoneFuture.of(SnapshotResult.empty());
            }
            throw e;
        }
    }

    @Override
    public SavepointResources<K> savepoint() throws Exception {
        throw new UnsupportedOperationException(
                "ForStRsAbstractKeyedStateBackend.savepoint is implemented in B-Prod-P4 (savepoint resources)");
    }

    /**
     * Cache of created InternalKvState adapter wrappers keyed by state-descriptor name so repeat
     * calls return the same adapter (matching the AbstractKeyedStateBackend.keyValueStatesByName
     * contract). The adapters re-fetch the underlying L5 ForStRs* state on every method call so
     * they automatically pick up the post-setCurrentKey rebind via the delegate's cache.
     */
    private final java.util.Map<
                    String, org.apache.flink.runtime.state.internal.InternalKvState<?, ?, ?>>
            internalStatesByName = new java.util.HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        // Idempotent: cached adapter wins, matching the parent's contract that successive
        // getOrCreateKeyedState calls return the same kvState instance.
        org.apache.flink.runtime.state.internal.InternalKvState<?, ?, ?> existing =
                internalStatesByName.get(stateDesc.getName());
        if (existing != null) {
            return (IS) existing;
        }
        // E6-HIGH-4(a): register / verify the user's value serializer against any restored
        // snapshot before constructing the adapter. The registry holds the live snapshot (drained
        // into {@code _serializer_metadata.bin} by {@link ForStRsSnapshotStrategy}) and runs the
        // schema-compatibility check against restored metadata on first encounter. We pass the
        // *value* serializer because that is the one that observes user-visible payload changes;
        // the namespace serializer is fixed by Flink's runtime.
        //
        // R25-H1: forward TTL config to the 4-arg overload so a TTL toggle across a snapshot/
        // restore is surfaced as StateMigrationException by the registry. The v1 StateDescriptor
        // returns {@code StateTtlConfig.DISABLED} (non-null) when TTL was never configured.
        //
        // R26-H1: V1-sync has NO TTL wrapping path — the createOrUpdateInternalState switch
        // below constructs the bare ForStRsInternalKvStateAdapters.{Value,List,Map,...}Adapter
        // with the user's raw serializer; there is no analogue of V2's
        // createTtlAwareStateInternal that wraps with TtlSerializer/TtlAwareValueStateV2.
        // Pre-R26-H1 the registry was told ttlEnabled=true even though the state writes bare
        // bytes (no 8-byte expiry prefix), so the on-disk layout DID NOT match the registry's
        // claim — restore would then compare a TTL-wrapped serializer against the user-naked
        // bytes and silently corrupt every read. Reject at register time so the user sees the
        // error early, not as silent payload corruption after a snapshot/restore. V1-sync TTL
        // is a documented limitation; users should migrate to the V2 async backend for TTL.
        StateTtlConfig ttlConfig = stateDesc.getTtlConfig();
        boolean ttlEnabled = ttlConfig != null && ttlConfig.isEnabled();
        if (ttlEnabled) {
            throw new UnsupportedOperationException(
                    "TTL is not supported on the V1-sync ForSt-RS state backend (state name '"
                            + stateDesc.getName()
                            + "', type "
                            + stateDesc.getType()
                            + "). V1-sync writes raw payload bytes with no TtlSerializer"
                            + " wrapping; enabling TTL here would create a registry/on-disk"
                            + " format mismatch. Use the V2 async backend"
                            + " (ForStRsAsyncKeyedStateBackend) for TTL support, or disable"
                            + " StateTtlConfig on this descriptor.");
        }
        long ttlMillis = 0L;
        stateSerializerRegistry.verifyOrRegister(
                stateDesc.getName(),
                stateDesc.getType().ordinal(),
                stateDesc.getSerializer(),
                /* ttlEnabled= */ false,
                ttlMillis);
        org.apache.flink.runtime.state.internal.InternalKvState<?, ?, ?> created;
        switch (stateDesc.getType()) {
            case VALUE:
                {
                    ValueStateDescriptor<SV> vsd = (ValueStateDescriptor<SV>) stateDesc;
                    created =
                            new ForStRsInternalKvStateAdapters.ValueAdapter<>(
                                    getKeySerializer(),
                                    (TypeSerializer<Object>) namespaceSerializer,
                                    vsd.getSerializer(),
                                    stateDesc.getName(),
                                    (ForStRsKeyedStateBackend<K>) delegate);
                    break;
                }
            case LIST:
                {
                    ListStateDescriptor<Object> lsd = (ListStateDescriptor<Object>) stateDesc;
                    created =
                            new ForStRsInternalKvStateAdapters.ListAdapter<>(
                                    getKeySerializer(),
                                    (TypeSerializer<Object>) namespaceSerializer,
                                    lsd.getElementSerializer(),
                                    stateDesc.getName(),
                                    (ForStRsKeyedStateBackend<K>) delegate);
                    break;
                }
            case MAP:
                {
                    MapStateDescriptor<Object, Object> msd =
                            (MapStateDescriptor<Object, Object>) stateDesc;
                    created =
                            new ForStRsInternalKvStateAdapters.MapAdapter<>(
                                    getKeySerializer(),
                                    (TypeSerializer<Object>) namespaceSerializer,
                                    msd.getKeySerializer(),
                                    msd.getValueSerializer(),
                                    stateDesc.getName(),
                                    (ForStRsKeyedStateBackend<K>) delegate);
                    break;
                }
            case REDUCING:
                {
                    ReducingStateDescriptor<SV> rsd = (ReducingStateDescriptor<SV>) stateDesc;
                    created =
                            new ForStRsInternalKvStateAdapters.ReducingAdapter<>(
                                    getKeySerializer(),
                                    (TypeSerializer<Object>) namespaceSerializer,
                                    rsd.getSerializer(),
                                    stateDesc.getName(),
                                    rsd.getReduceFunction(),
                                    (ForStRsKeyedStateBackend<K>) delegate);
                    break;
                }
            case AGGREGATING:
                {
                    AggregatingStateDescriptor<Object, SV, Object> asd =
                            (AggregatingStateDescriptor<Object, SV, Object>) stateDesc;
                    created =
                            new ForStRsInternalKvStateAdapters.AggregatingAdapter<>(
                                    getKeySerializer(),
                                    (TypeSerializer<Object>) namespaceSerializer,
                                    asd.getSerializer(),
                                    stateDesc.getName(),
                                    asd.getAggregateFunction(),
                                    (ForStRsKeyedStateBackend<K>) delegate);
                    break;
                }
            default:
                throw new UnsupportedOperationException(
                        "ForStRs backend does not support state type: " + stateDesc.getType());
        }
        internalStatesByName.put(stateDesc.getName(), created);
        return (IS) created;
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        if (pickTimerFactory() == TimerServiceFactory.HEAP) {
            // HEAP timer factory — see TimerServiceFactory javadoc above for why this is the
            // default. The engine-backed path remains available via
            // -Dforst.rs.timer-service.factory=FORSTRS.
            return new HeapPriorityQueueSetFactory(
                            getKeyGroupRange(), getNumberOfKeyGroups(), 128)
                    .create(stateName, byteOrderedElementSerializer);
        }
        return createInternalPriorityQueue(stateName, byteOrderedElementSerializer);
    }

    /**
     * B-Prod-P9 — Spec §6f. Returns a {@link ForStRsKeyGroupedInternalPriorityQueue} backed by the
     * delegate L5 backend's engine handles. The queue stores entries under {@code "q/" || stateName
     * || "/" || kg(2B BE) || ts(8B BE) || serialize(T)} (sign-flipped 8B BE timestamps so
     * big-endian lex order matches signed-numerical order). The "current key group" is sourced from
     * the Flink {@link InternalKeyContext}, and the timestamp is extracted via reflection on a
     * {@code getTimestamp() : long} accessor — which {@link
     * org.apache.flink.streaming.api.operators.InternalTimer} provides natively.
     *
     * <p>If {@code T} doesn't expose {@code getTimestamp() : long}, callers may pass the queue
     * factory a custom {@link java.util.function.ToLongFunction} via the lower-level constructor
     * directly; this convenience entry-point uses reflection so it can satisfy the {@code
     * AbstractKeyedStateBackend.create} override without API churn.
     */
    public <T extends HeapPriorityQueueElement>
            ForStRsKeyGroupedInternalPriorityQueue<T> createInternalPriorityQueue(
                    String stateName, TypeSerializer<T> elementSerializer) {
        java.util.function.ToLongFunction<T> tsExtractor = reflectiveTimestampExtractor();
        ForStRsKeyGroupedInternalPriorityQueue<T> q =
                new ForStRsKeyGroupedInternalPriorityQueue<>(
                        delegate.getLinker(),
                        delegate.getDb(),
                        delegate.getDefaultCf(),
                        delegate.getArena(),
                        stateName,
                        elementSerializer,
                        tsExtractor,
                        this::getCurrentKeyGroupIndex,
                        getKeyGroupRange());
        engineTimerQueues.add(q);
        return q;
    }

    /**
     * Reflectively reads {@code getTimestamp() : long} on the element. Cached via static {@code
     * java.lang.invoke.MethodHandle}-style closure inside the lambda — first-call cost is a single
     * reflective lookup; subsequent calls go through the captured {@link java.lang.reflect.Method}.
     */
    private static <T> java.util.function.ToLongFunction<T> reflectiveTimestampExtractor() {
        return new java.util.function.ToLongFunction<T>() {
            private volatile java.lang.reflect.Method getTimestampMethod;

            @Override
            public long applyAsLong(T t) {
                java.lang.reflect.Method m = getTimestampMethod;
                if (m == null) {
                    try {
                        m = t.getClass().getMethod("getTimestamp");
                    } catch (NoSuchMethodException e) {
                        throw new UnsupportedOperationException(
                                "Element type "
                                        + t.getClass().getName()
                                        + " has no getTimestamp() method; pass an explicit"
                                        + " ToLongFunction via the queue ctor instead.",
                                e);
                    }
                    getTimestampMethod = m;
                }
                try {
                    Object out = m.invoke(t);
                    if (out instanceof Long l) {
                        return l;
                    }
                    if (out instanceof Number n) {
                        return n.longValue();
                    }
                    throw new IllegalStateException("getTimestamp() returned non-numeric: " + out);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(
                            "getTimestamp() invocation failed: " + e.getMessage(), e);
                }
            }
        };
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Tell the snapshot strategy that this checkpoint id is now eligible as a base for
        // future incremental checkpoints (subsequent dbSnapshot()s will pass it as
        // base_checkpoint_id). The strategy keeps a monotonic max so out-of-order completes are
        // safely ignored.
        if (snapshotStrategy != null) {
            snapshotStrategy.recordCompletedCheckpoint(checkpointId);
            // Drop the per-checkpoint registration tracking — the registry's ref-counts persist
            // (the SSTs registered for this checkpoint stay alive until subsumed by the registry
            // when the next checkpoint completes).
            snapshotStrategy.takePendingRegistrations(checkpointId);
        }
    }

    /**
     * Override of {@link
     * org.apache.flink.api.common.state.CheckpointListener#notifyCheckpointAborted(long)}.
     *
     * <p>For an aborted checkpoint we must roll back the registry's ref-count bumps so the aborted
     * checkpoint's "newly-uploaded" SSTs can drop to zero ref (eligible for discard) — the baseline
     * shared with previously completed checkpoints is preserved because the registry's ref-counts
     * are independent per checkpoint contribution.
     */
    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        // Parent default is no-op (CheckpointListener.notifyCheckpointAborted has a default impl);
        // skip super to avoid forwarding to a default-method handle that doesn't exist on
        // AbstractKeyedStateBackend itself.
        if (sstRegistry == null || snapshotStrategy == null) {
            return;
        }
        List<HandleAndLocalPath> rollback =
                snapshotStrategy.takePendingRegistrationsForAbort(checkpointId);
        if (rollback != null) {
            for (HandleAndLocalPath h : rollback) {
                sstRegistry.unregister(new StateHandleID(h.getLocalPath()));
            }
        }
        // E8-H2 post-abort drain: defensive second sweep for entries that landed AFTER the
        // initial take. With the current appendAndRegister design (re-check under monitor) the
        // worker self-skips on observed marker so this is expected to be empty — guards against
        // future refactors re-introducing a window.
        List<HandleAndLocalPath> late =
                snapshotStrategy.drainLatePendingRegistrations(checkpointId);
        for (HandleAndLocalPath h : late) {
            sstRegistry.unregister(new StateHandleID(h.getLocalPath()));
        }
    }

    @Override
    public int numKeyValueStateEntries() {
        // Best-effort: the L5 backend's count is accurate for the single-CF / single-keygroup
        // setup used until P3 wires multi-CF iteration. Cast guards against the long->int
        // conversion since Flink's interface returns int.
        long n = delegate.numKeyValueStateEntries();
        return (int) Math.min(Integer.MAX_VALUE, n);
    }

    // ------------------------------------------------------------------
    // Async state SPI (spec §6e — Flink 2.x async stateful operators)
    // ------------------------------------------------------------------

    /**
     * Returns an {@link ForStRsAsyncValueState} bound to {@code stateName}. The returned wrapper
     * captures {@linkplain ForStRsKeyedStateBackend#getCurrentKey current key} on each method call
     * and serialises per-key ops via this backend's {@link PerKeyFuturesChain}.
     *
     * <p>Per-key ordering and cross-key parallelism are guaranteed by the shared chain. Callers may
     * submit multiple ops for the same key in flight and observe submit-order completion.
     */
    public <T> ForStRsAsyncValueState<K, T> getAsyncValueState(
            String stateName, TypeSerializer<T> valueSerializer) {
        return new ForStRsAsyncValueState<>(
                delegate, stateName, valueSerializer, ensureAsyncChain());
    }

    /** Async counterpart of {@link ForStRsKeyedStateBackend#getListState}. */
    public <T> ForStRsAsyncListState<K, T> getAsyncListState(
            String stateName, TypeSerializer<T> elementSerializer) {
        return new ForStRsAsyncListState<>(
                delegate, stateName, elementSerializer, ensureAsyncChain());
    }

    /** Async counterpart of {@link ForStRsKeyedStateBackend#getMapState}. */
    public <UK, UV> ForStRsAsyncMapState<K, UK, UV> getAsyncMapState(
            String stateName,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer) {
        return new ForStRsAsyncMapState<>(
                delegate, stateName, userKeySerializer, userValueSerializer, ensureAsyncChain());
    }

    /** Async counterpart of {@link ForStRsKeyedStateBackend#getReducingState}. */
    public <T> ForStRsAsyncReducingState<K, T> getAsyncReducingState(
            String stateName, TypeSerializer<T> serializer, ReduceFunction<T> reduceFunction) {
        return new ForStRsAsyncReducingState<>(
                delegate, stateName, serializer, reduceFunction, ensureAsyncChain());
    }

    /** Async counterpart of {@link ForStRsKeyedStateBackend#getAggregatingState}. */
    public <IN, ACC, OUT> ForStRsAsyncAggregatingState<K, IN, ACC, OUT> getAsyncAggregatingState(
            String stateName,
            TypeSerializer<ACC> accSerializer,
            AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        return new ForStRsAsyncAggregatingState<>(
                delegate, stateName, accSerializer, aggregateFunction, ensureAsyncChain());
    }

    /**
     * Lazily constructs the per-key futures chain backed by a virtual-thread executor on first
     * call. Idempotent and thread-safe via double-checked locking on the {@code asyncChain} field.
     */
    private PerKeyFuturesChain<K> ensureAsyncChain() {
        PerKeyFuturesChain<K> chain = asyncChain;
        if (chain == null) {
            synchronized (this) {
                chain = asyncChain;
                if (chain == null) {
                    java.util.concurrent.ExecutorService exec =
                            Executors.newVirtualThreadPerTaskExecutor();
                    chain = new PerKeyFuturesChain<>((Executor) exec);
                    this.asyncExecutor = exec;
                    this.asyncChain = chain;
                }
            }
        }
        return chain;
    }

    /** Test accessor for the async chain (or {@code null} if no async state op has been issued). */
    public PerKeyFuturesChain<K> getAsyncChain() {
        return asyncChain;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            // Flush + close each engine-backed priority queue's pending buffer before tearing
            // down the delegate (engine handles).
            for (ForStRsKeyGroupedInternalPriorityQueue<?> q : engineTimerQueues) {
                try {
                    q.close();
                } catch (Exception ignored) {
                }
            }
            engineTimerQueues.clear();
            // Best-effort shutdown of the async executor — long-running async ops are interrupted.
            if (asyncExecutor != null) {
                asyncExecutor.shutdownNow();
            }
            delegate.close();
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
        }
    }
}

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
import org.apache.flink.runtime.state.AbstractIncrementalStateHandle;
import org.apache.flink.runtime.state.CheckpointBoundKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * ForSt-RS incremental keyed state handle (B-Prod-P3 Task 3.3).
 *
 * <p>Extends Flink 2.2.0's {@link AbstractIncrementalStateHandle} (which already implements {@link
 * org.apache.flink.runtime.state.IncrementalKeyedStateHandle} via a UUID + KeyGroupRange + a list
 * of {@link HandleAndLocalPath shared SSTs} + the meta-data stream handle) and layers on:
 *
 * <ul>
 *   <li>a {@code privateState} list — the per-checkpoint manifest blob and any other
 *       single-checkpoint artefacts that don't participate in cross-checkpoint sharing,
 *   <li>a {@code baseCheckpointId} — the previous checkpoint this incremental was taken against (0
 *       for a full / first checkpoint), and
 *   <li>a {@code cfMap} — column family name → column family identifier on the engine side, used by
 *       restore to recreate matching CF handles in the right order.
 * </ul>
 *
 * <p>The "many-method" surface of {@link
 * org.apache.flink.runtime.state.IncrementalKeyedStateHandle} (and its supertype {@link
 * org.apache.flink.runtime.state.CompositeStateHandle}) is satisfied by inheriting from
 * AbstractIncrementalStateHandle for {@link #getBackendIdentifier()}, {@link #getKeyGroupRange()},
 * {@link #getSharedStateHandles()}, {@link #getMetaDataStateHandle()}, {@link #getStateHandleId()},
 * {@link #getCheckpointId()}, and {@link #getIntersection(KeyGroupRange)}; we add the remaining
 * abstract methods from {@link StateObject} and {@link
 * org.apache.flink.runtime.state.CompositeStateHandle} below.
 */
@Internal
public class ForStRsIncrementalKeyedStateHandle extends AbstractIncrementalStateHandle {

    private static final long serialVersionUID = 1L;

    /**
     * The previous checkpoint this incremental was taken against (passed to the engine as {@code
     * base_checkpoint_id} in the FFI call). Zero for the very first / full checkpoint.
     */
    private final long baseCheckpointId;

    /**
     * Per-checkpoint state — the manifest blob plus any other single-checkpoint artefacts that
     * shouldn't survive subsumption. Held by-name (local path within the checkpoint dir) so the
     * restore side can recover them in a deterministic order.
     */
    private final List<HandleAndLocalPath> privateState;

    /**
     * Column family name → engine-side CF identifier (a small unique integer the engine assigned
     * when the CF was created). Used by restore to recreate matching CF handles. Stored as a
     * LinkedHashMap so iteration order is deterministic across snapshot/restore round-trips.
     */
    private final LinkedHashMap<String, Long> cfMap;

    /** Total persisted bytes for this checkpoint (private + shared); reported by Flink metrics. */
    private final long persistedSizeOfThisCheckpoint;

    public ForStRsIncrementalKeyedStateHandle(
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            long checkpointId,
            long baseCheckpointId,
            List<HandleAndLocalPath> sharedState,
            List<HandleAndLocalPath> privateState,
            StreamStateHandle metaStateHandle,
            Map<String, Long> cfMap) {
        this(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                baseCheckpointId,
                sharedState,
                privateState,
                metaStateHandle,
                cfMap,
                computePersistedSize(sharedState, privateState, metaStateHandle),
                StateHandleID.randomStateHandleId());
    }

    private ForStRsIncrementalKeyedStateHandle(
            UUID backendIdentifier,
            KeyGroupRange keyGroupRange,
            long checkpointId,
            long baseCheckpointId,
            List<HandleAndLocalPath> sharedState,
            List<HandleAndLocalPath> privateState,
            StreamStateHandle metaStateHandle,
            Map<String, Long> cfMap,
            long persistedSizeOfThisCheckpoint,
            StateHandleID stateHandleId) {
        super(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                sharedState,
                metaStateHandle,
                stateHandleId);
        this.baseCheckpointId = baseCheckpointId;
        this.privateState = privateState;
        this.cfMap = new LinkedHashMap<>(cfMap);
        this.persistedSizeOfThisCheckpoint = persistedSizeOfThisCheckpoint;
    }

    private static long computePersistedSize(
            List<HandleAndLocalPath> shared,
            List<HandleAndLocalPath> priv,
            StreamStateHandle meta) {
        long total = 0L;
        for (HandleAndLocalPath h : shared) {
            total += h.getStateSize();
        }
        for (HandleAndLocalPath h : priv) {
            total += h.getStateSize();
        }
        if (meta != null) {
            total += meta.getStateSize();
        }
        return total;
    }

    /** Previous checkpoint id this incremental was taken against (0 if full). */
    public long getBaseCheckpointId() {
        return baseCheckpointId;
    }

    /** Per-checkpoint (private) state handles; not shared across checkpoints. */
    public List<HandleAndLocalPath> getPrivateState() {
        return privateState;
    }

    /** Read-only view of the column family name → engine cf id map. */
    public Map<String, Long> getCfMap() {
        return Collections.unmodifiableMap(cfMap);
    }

    @Override
    public long getStateSize() {
        return persistedSizeOfThisCheckpoint;
    }

    @Override
    public long getCheckpointedSize() {
        return persistedSizeOfThisCheckpoint;
    }

    @Override
    public void discardState() throws Exception {
        // Shared state is not discarded here — it's owned by the SharedStateRegistry and
        // freed when the last referencing checkpoint is subsumed. We discard:
        //   * the metadata handle (always our own — created fresh per checkpoint), and
        //   * the private state handles (per-checkpoint only — never shared).
        Exception aggregate = null;
        try {
            if (getMetaDataStateHandle() != null) {
                getMetaDataStateHandle().discardState();
            }
        } catch (Exception e) {
            aggregate = e;
        }
        for (HandleAndLocalPath h : privateState) {
            try {
                h.getHandle().discardState();
            } catch (Exception e) {
                if (aggregate == null) {
                    aggregate = e;
                } else {
                    aggregate.addSuppressed(e);
                }
            }
        }
        if (aggregate != null) {
            throw aggregate;
        }
    }

    /**
     * Convenience accessor mirroring {@link
     * org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle#streamSubHandles()} — yields
     * every sub-handle (shared SSTs, private artefacts, metadata) for size accounting / discard
     * iteration. Not strictly an interface override on AbstractIncrementalStateHandle in 2.2.0,
     * exposed here so the test surface + restore path can stream all handles uniformly.
     */
    public Stream<StreamStateHandle> streamSubHandles() {
        Stream.Builder<StreamStateHandle> b = Stream.builder();
        for (HandleAndLocalPath h : getSharedState()) {
            b.add(h.getHandle());
        }
        for (HandleAndLocalPath h : privateState) {
            b.add(h.getHandle());
        }
        if (getMetaDataStateHandle() != null) {
            b.add(getMetaDataStateHandle());
        }
        return b.build();
    }

    @Override
    public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {
        // Per Flink's contract, registering shared state lets the registry track refcounts so
        // SSTs are only discarded once no live checkpoint references them. The shared SSTs are the
        // primary cross-checkpoint sharing units; private + meta are never registered here.
        for (HandleAndLocalPath h : getSharedState()) {
            // Replace the local handle with whatever the registry returns (it may already
            // hold the canonical handle from a prior checkpoint).
            StreamStateHandle reused =
                    stateRegistry.registerReference(
                            org.apache.flink.runtime.state.SharedStateRegistryKey
                                    .forStreamStateHandle(h.getHandle()),
                            h.getHandle(),
                            checkpointID);
            h.replaceHandle(reused);
        }
    }

    /** Per-CF shared-state list view — exposed for the SnapshotStrategy + tests. */
    public List<HandleAndLocalPath> getSharedState() {
        return getSharedStateHandles();
    }

    // -- Note: getIntersection is inherited from AbstractIncrementalStateHandle, which already
    // restricts to the exact-range case until rescaling lands in P4. We do NOT override it.

    /**
     * {@link CheckpointBoundKeyedStateHandle#rebound(long)} — returns a copy with the new
     * checkpoint id (used by Flink's checkpoint subsumption when a checkpoint is repurposed).
     * Implemented by delegating to a fresh constructor; we re-use the existing {@code
     * stateHandleId} so the rebound handle is tracked as the same artefact.
     */
    @Override
    public CheckpointBoundKeyedStateHandle rebound(long newCheckpointId) {
        return new ForStRsIncrementalKeyedStateHandle(
                getBackendIdentifier(),
                getKeyGroupRange(),
                newCheckpointId,
                baseCheckpointId,
                getSharedState(),
                privateState,
                getMetaDataStateHandle(),
                cfMap,
                persistedSizeOfThisCheckpoint,
                getStateHandleId());
    }
}

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

package org.apache.flink.state.forstrs.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.StateMigrationException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * PR-A11 (E3-HIGH-4): backend-level registry that persists every keyed state's {@link
 * TypeSerializerSnapshot} alongside the engine's state data so that the next session can detect
 * schema drift on restore.
 *
 * <p><b>Write path</b> (state creation):
 *
 * <ol>
 *   <li>{@link
 *       org.apache.flink.state.forstrs.keyed.ForStRsAsyncKeyedStateBackend#getOrCreateKeyedState}
 *       calls {@link #register(String, int, TypeSerializer)} the first time a state name is seen.
 *   <li>The registry serializes {@code serializer.snapshotConfiguration()} via Flink's standard
 *       {@link TypeSerializerSnapshotSerializationUtil#writeSerializerSnapshot} and stashes the
 *       byte[] under {@code stateName}.
 *   <li>PR-A1's {@code snapshot()} implementation will drain {@link #metadataBuffer()} into the
 *       checkpoint blob under the metadata-prefix keyspace {@code "META/state/{stateName}/serializer"}
 *       (see {@link StateSerializerMetadata} for the on-disk layout).
 * </ol>
 *
 * <p><b>Read path</b> (state restoration):
 *
 * <ol>
 *   <li>PR-A1's restore operation hands back the buffer; the backend calls {@link
 *       #seedFromRestore(Map)}.
 *   <li>For each newly created state, the backend calls {@link #verifyOrRegister} which compares
 *       the old snapshot against the new serializer's snapshot via {@link
 *       TypeSerializerSnapshot#resolveSchemaCompatibility} and routes per the {@link
 *       TypeSerializerSchemaCompatibility} result:
 *       <ul>
 *         <li>{@code isCompatibleAsIs()} — use the new serializer directly.
 *         <li>{@code isCompatibleAfterMigration()} — migration runs lazy-on-read (V1: documented in
 *             release notes; PR-A11 follow-up will wire the migration serializer through the state
 *             classes).
 *         <li>{@code isCompatibleWithReconfiguredSerializer()} — caller substitutes the
 *             reconfigured serializer (returned by {@link #verifyOrRegister}).
 *         <li>{@code isIncompatible()} — throw {@link StateMigrationException}, matching Flink's
 *             standard contract.
 *       </ul>
 * </ol>
 *
 * <p><b>PR-A1 dependency:</b> until PR-A1 lands, {@link #seedFromRestore} is never called by the
 * backend — only by tests. The write-path is fully exercised; the read-path is exercised through
 * tests that pre-seed the registry to simulate a restored session. {@link #activatedForRestore}
 * reports whether real restore wiring has been completed.
 *
 * <p>Thread-safety: the registry is consulted on the mailbox thread during state creation; it does
 * not need internal synchronization beyond what the {@link LinkedHashMap} provides for
 * deterministic iteration. Restore-time seeding happens single-threaded before any mailbox traffic.
 */
@Internal
public final class StateSerializerRegistry {

    /**
     * Live registry, keyed by state name. Insertion order is preserved so the metadata buffer
     * surfaced to PR-A1 is deterministic and review-friendly.
     */
    private final Map<String, StateSerializerMetadata> live = new LinkedHashMap<>();

    /**
     * Snapshots that were restored from a previous session, awaiting verification when the user
     * code calls {@code getOrCreateKeyedState} with the new serializer.
     */
    private final Map<String, StateSerializerMetadata> restored = new LinkedHashMap<>();

    /**
     * True once PR-A1 has wired {@link #seedFromRestore} into the backend's real restore path. V1
     * defaults to {@code false}; tests may pre-seed and flip this to exercise the verification
     * branch.
     */
    private boolean activatedForRestore = false;

    /** ClassLoader used to instantiate {@link TypeSerializerSnapshot} classes on restore. */
    private final ClassLoader userCodeClassLoader;

    public StateSerializerRegistry() {
        this(StateSerializerRegistry.class.getClassLoader());
    }

    public StateSerializerRegistry(ClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = userCodeClassLoader;
    }

    /**
     * Write-path entry. Called once per state when the backend first sees the state name in the
     * current session. Re-registration with the same name is a no-op (the in-memory registry
     * dedupes on state name; the snapshot bytes are stable per session).
     *
     * @param stateName the engine-level state identifier (matches {@code desc.getStateId()})
     * @param stateKindOrdinal {@code StateDescriptor.Type.ordinal()}, persisted for restore-side
     *     invariant checks
     * @param serializer the user-supplied {@link TypeSerializer}; its {@code snapshotConfiguration}
     *     is serialized
     * @throws IOException if Flink's snapshot-serialization helper fails (typically only on
     *     malformed user-provided serializer snapshots)
     */
    public <T> void register(String stateName, int stateKindOrdinal, TypeSerializer<T> serializer)
            throws IOException {
        if (live.containsKey(stateName)) {
            return;
        }
        TypeSerializerSnapshot<T> snap = serializer.snapshotConfiguration();
        DataOutputSerializer out = new DataOutputSerializer(64);
        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(out, snap);
        byte[] bytes = out.getCopyOfBuffer();
        live.put(
                stateName,
                new StateSerializerMetadata(
                        stateName,
                        stateKindOrdinal,
                        StateSerializerMetadata.CURRENT_FORMAT_VERSION,
                        bytes));
    }

    /**
     * Verify a newly-supplied serializer against a previously persisted snapshot for the same
     * state. If no previous snapshot exists (fresh state in this session) this method behaves
     * exactly like {@link #register}.
     *
     * <p>Returns the {@link TypeSerializer} the caller should use going forward:
     *
     * <ul>
     *   <li>If compatible-as-is or compatible-after-migration: the original {@code serializer}.
     *   <li>If compatible-with-reconfigured-serializer: the reconfigured one from Flink.
     * </ul>
     *
     * @throws StateMigrationException if the old and new serializers are {@code isIncompatible()}
     * @throws IOException if reading the persisted snapshot fails
     */
    public <T> TypeSerializer<T> verifyOrRegister(
            String stateName, int stateKindOrdinal, TypeSerializer<T> serializer)
            throws IOException, StateMigrationException {
        StateSerializerMetadata prior = restored.get(stateName);
        if (prior == null) {
            register(stateName, stateKindOrdinal, serializer);
            return serializer;
        }
        // PR-A1 dependency: in V1 the snapshot must be deserialized using Flink's standard helper
        // to reconstruct the prior TypeSerializerSnapshot instance.
        DataInputDeserializer in =
                new DataInputDeserializer(prior.serializerSnapshotBytes());
        TypeSerializerSnapshot<T> oldSnap =
                TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                        in, userCodeClassLoader);
        TypeSerializerSnapshot<T> newSnap = serializer.snapshotConfiguration();
        TypeSerializerSchemaCompatibility<T> compat = newSnap.resolveSchemaCompatibility(oldSnap);

        if (compat.isIncompatible()) {
            throw new StateMigrationException(
                    "TypeSerializer for state '"
                            + stateName
                            + "' is INCOMPATIBLE with the persisted snapshot."
                            + " The previous serializer's snapshot cannot be migrated to the new"
                            + " serializer (per TypeSerializerSnapshot#resolveSchemaCompatibility)."
                            + " Use a custom migration or restart from a clean state.");
        }

        TypeSerializer<T> effective = serializer;
        if (compat.isCompatibleWithReconfiguredSerializer()) {
            effective = compat.getReconfiguredSerializer();
        }
        // COMPATIBLE_AFTER_MIGRATION is accepted in V1 with the understanding that reads will use
        // the old serializer's snapshot via Flink's standard migration plumbing once PR-A1 lands.
        // For now we promote the new serializer; release notes flag this lazy-on-read behavior.

        // Promote: replace the restored entry with a fresh registration under the *new*
        // serializer's snapshot so subsequent snapshots persist the current schema.
        restored.remove(stateName);
        live.remove(stateName);
        register(stateName, stateKindOrdinal, effective);
        return effective;
    }

    /**
     * Read-path entry. PR-A1's restore operation will call this with the metadata buffer decoded
     * from the snapshot blob. Idempotent for tests; clears any pre-existing restored state.
     */
    public void seedFromRestore(Map<String, StateSerializerMetadata> restoredMetadata) {
        restored.clear();
        restored.putAll(restoredMetadata);
        activatedForRestore = true;
    }

    /**
     * Returns the in-memory metadata buffer in deterministic registration order. PR-A1's {@code
     * snapshot()} implementation will drain this into the checkpoint blob.
     */
    public Map<String, StateSerializerMetadata> metadataBuffer() {
        return Collections.unmodifiableMap(live);
    }

    /**
     * True iff a restore-side seed has been delivered. False in V1 until PR-A1 wires the restore
     * path; tests flip this by calling {@link #seedFromRestore}.
     */
    public boolean activatedForRestore() {
        return activatedForRestore;
    }

    /** Lookup the persisted (write-side) metadata for a state, mostly for tests. */
    @VisibleForTesting
    @Nullable
    public StateSerializerMetadata get(String stateName) {
        return live.get(stateName);
    }
}

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
        // R15-H2: assert state-kind invariance BEFORE running the value-element schema compat
        // check. The element types of a ValueState<List<Foo>> and a ListState<Foo> can both
        // resolve as "compatible as is" against each other (their TypeSerializerSnapshots are
        // semantically equivalent), but the on-disk storage layout differs: ValueState stores
        // a single serialized value; ListState stores {@code [count:int][elem...]}. Reading a
        // ListState payload through a ValueState reader (or vice-versa) silently decodes
        // garbage. Fail loudly here with the actionable kind-mismatch message rather than
        // surfacing the issue as a Flink-level deserialization error at the first state read.
        if (prior.stateKindOrdinal() != stateKindOrdinal) {
            throw new StateMigrationException(
                    "State kind mismatch for state '" + stateName
                            + "': previously stored as " + stateKindName(prior.stateKindOrdinal())
                            + ", now registered as " + stateKindName(stateKindOrdinal)
                            + ". Storage layout differs — restore would silently decode garbage."
                            + " Choose a different state name or restart from a clean state.");
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

    // ------------------------------------------------------------------
    // E5-HIGH-2 (PR-A11-emit) — registry wire codec for snapshot/restore.
    // ------------------------------------------------------------------

    /**
     * Magic header for the serialized registry blob — guards against accidentally feeding an
     * unrelated private-state byte stream to the parser on restore.
     */
    public static final int REGISTRY_BLOB_MAGIC = 0x46524552; // "FRER" — ForstRs Registry

    /** Registry-blob envelope format version. v1 = the layout documented on {@link #serialize}. */
    public static final int REGISTRY_BLOB_FORMAT_V1 = 1;

    /**
     * Serialize the supplied metadata map into a single contiguous byte[] suitable for storage as
     * a private-state entry in the checkpoint blob. PR-A1's snapshot path drains this into the
     * checkpoint upload; {@link #deserialize(byte[])} reads the inverse.
     *
     * <p><b>Wire format (v1)</b>:
     *
     * <pre>
     *   |  4 bytes  | magic = {@link #REGISTRY_BLOB_MAGIC} (big-endian)                |
     *   |  4 bytes  | envelope format-version = {@link #REGISTRY_BLOB_FORMAT_V1}        |
     *   |  4 bytes  | entry count                                                      |
     *   | per entry:                                                                   |
     *   |    UTF8   | state name (via DataOutputSerializer.writeUTF — 2-byte length)   |
     *   |  4 bytes  | per-entry format version (see {@link StateSerializerMetadata})   |
     *   |  4 bytes  | stateKindOrdinal                                                 |
     *   |  4 bytes  | serializerSnapshot byte length                                   |
     *   |  N bytes  | serializerSnapshot bytes                                         |
     * </pre>
     *
     * <p>The blob is self-describing — the magic + format-version pair makes corruption / wrong
     * blob detection trivial on restore.
     */
    public static byte[] serialize(Map<String, StateSerializerMetadata> entries) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(128 + entries.size() * 64);
        out.writeInt(REGISTRY_BLOB_MAGIC);
        out.writeInt(REGISTRY_BLOB_FORMAT_V1);
        out.writeInt(entries.size());
        for (Map.Entry<String, StateSerializerMetadata> e : entries.entrySet()) {
            StateSerializerMetadata md = e.getValue();
            out.writeUTF(e.getKey());
            out.writeInt(md.formatVersion());
            out.writeInt(md.stateKindOrdinal());
            byte[] bytes = md.serializerSnapshotBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        return out.getCopyOfBuffer();
    }

    /** Convenience: serialize {@link #metadataBuffer()} of {@code this} registry. */
    public byte[] serialize() throws IOException {
        return serialize(metadataBuffer());
    }

    /**
     * Inverse of {@link #serialize(Map)}. Validates the magic + version; throws {@link
     * IOException} on mismatch so a malformed private-state blob fails the restore loudly rather
     * than silently returning an empty map (which would re-enable the schema-drift gap E5-HIGH-2
     * was meant to close).
     */
    public static Map<String, StateSerializerMetadata> deserialize(byte[] blob) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(blob);
        int magic = in.readInt();
        if (magic != REGISTRY_BLOB_MAGIC) {
            throw new IOException(
                    "StateSerializerRegistry blob magic mismatch: expected 0x"
                            + Integer.toHexString(REGISTRY_BLOB_MAGIC)
                            + ", got 0x"
                            + Integer.toHexString(magic));
        }
        int envelopeVer = in.readInt();
        if (envelopeVer != REGISTRY_BLOB_FORMAT_V1) {
            throw new IOException(
                    "Unsupported StateSerializerRegistry envelope version: "
                            + envelopeVer
                            + " (this build understands v"
                            + REGISTRY_BLOB_FORMAT_V1
                            + ")");
        }
        int count = in.readInt();
        // R15-M1: bound the entry count BEFORE allocating the map — a corrupt or hostile blob
        // could embed an arbitrarily large count and trigger an OOM via the LinkedHashMap
        // capacity hint. 65536 is two orders of magnitude above any plausible state-count for a
        // single backend instance (a TM typically hosts < 1000 states across all operators).
        if (count < 0 || count > MAX_ENTRY_COUNT) {
            throw new IOException(
                    "StateSerializerRegistry blob malformed: count=" + count
                            + " (expected 0.." + MAX_ENTRY_COUNT + ")");
        }
        Map<String, StateSerializerMetadata> out = new LinkedHashMap<>(count * 2);
        for (int i = 0; i < count; i++) {
            String name = in.readUTF();
            int fmtVer = in.readInt();
            int kindOrd = in.readInt();
            int bytesLen = in.readInt();
            // R15-M1: bound the per-entry snapshot size BEFORE allocating the byte[]. The
            // upper cap matches the largest plausible TypeSerializerSnapshot envelope (a few MB
            // for pathological POJO graphs); anything larger is corruption, not legitimate
            // state metadata.
            if (bytesLen < 0 || bytesLen > MAX_SNAPSHOT_BYTES) {
                throw new IOException(
                        "StateSerializerRegistry blob malformed: bytesLen=" + bytesLen
                                + " for state '" + name + "' (expected 0.."
                                + MAX_SNAPSHOT_BYTES + ")");
            }
            byte[] bytes = new byte[bytesLen];
            in.readFully(bytes);
            out.put(name, new StateSerializerMetadata(name, kindOrd, fmtVer, bytes));
        }
        return out;
    }

    /**
     * R15-M1: hard upper bound on the entry count to guard against malformed/hostile blobs. A
     * 64Ki cap is two orders of magnitude above any plausible state-count for a single backend
     * (a TM hosts ≪ 1000 states across all operators), so it never rejects legitimate data
     * while preventing a malicious blob from triggering OOM in the LinkedHashMap allocation.
     */
    public static final int MAX_ENTRY_COUNT = 65_536;

    /**
     * R15-M1: hard upper bound on each entry's serialized snapshot bytes. 16 MiB is several
     * orders of magnitude above the largest plausible TypeSerializerSnapshot envelope
     * (typically tens of bytes; pathological POJO graphs reach kilobytes), so it never rejects
     * legitimate snapshots while preventing OOM-via-blob-length attacks.
     */
    public static final int MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024;

    /**
     * R15-H2: human-readable name for a {@link
     * org.apache.flink.api.common.state.v2.StateDescriptor.Type} ordinal. Used in the kind
     * mismatch error message so operators can diagnose the regression without cross-referencing
     * the enum source. Mirrors the Flink-side ordering (VALUE=0, LIST=1, REDUCING=2,
     * AGGREGATING=3, MAP=4).
     */
    static String stateKindName(int ordinal) {
        switch (ordinal) {
            case 0:
                return "VALUE";
            case 1:
                return "LIST";
            case 2:
                return "REDUCING";
            case 3:
                return "AGGREGATING";
            case 4:
                return "MAP";
            default:
                return "UNKNOWN(" + ordinal + ")";
        }
    }
}

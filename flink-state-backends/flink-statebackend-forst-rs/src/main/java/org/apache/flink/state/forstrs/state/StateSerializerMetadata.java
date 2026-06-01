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

import java.util.Objects;

/**
 * PR-A11 (E3-HIGH-4): metadata record describing the {@code TypeSerializer} associated with a
 * registered keyed state, persisted alongside the engine's state data so that next-session restore
 * can detect schema drift.
 *
 * <p>Holds an opaque serialized {@code TypeSerializerSnapshot} byte[] (written via {@link
 * org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil#writeSerializerSnapshot}),
 * a small format version that lets us evolve this envelope in future PRs, and the state class kind
 * (VALUE / MAP / LIST / REDUCING / AGGREGATING) so each-state-class invariants can be re-asserted
 * on restore.
 *
 * <p><b>R24-H2 — TTL fields (format v2).</b> Format version 2 adds an explicit {@code ttlEnabled}
 * flag and an associated {@code ttlMillis} value. Pre-fix, the registry persisted only the user
 * serializer snapshot; TTL on/off was implicit. Because TTL writes {@code [long expiryTs][value]}
 * on disk (see {@link org.apache.flink.state.forstrs.state.ttl.TtlSerializer}), restoring a state
 * that was originally TTL-enabled with a fresh descriptor that has TTL disabled would
 * {@code resolveSchemaCompatibility} as COMPATIBLE_AS_IS — and the read path would decode the
 * 8-byte expiry header as the value head, silently corrupting every restored value. Persisting
 * the flag lets {@link StateSerializerRegistry#verifyOrRegister} surface a {@code
 * StateMigrationException} on toggle. Format v1 envelopes (no TTL fields) are accepted on read
 * with {@code ttlEnabled=false, ttlMillis=0} so existing snapshots continue to open; the registry
 * always WRITES v2 going forward.
 *
 * <p><b>Persistence path:</b> PR-A1 has not landed yet, so the engine does not yet have a real
 * snapshot+restore loop. Until then, the {@link StateSerializerRegistry} keeps this metadata in an
 * in-memory map and surfaces it through {@code metadataBuffer()} for PR-A1 to drain into the
 * snapshot blob it emits. On restore, PR-A1 will hand back a buffer that the registry can re-read.
 *
 * <p><b>Format spec (v2)</b> (what each entry will become inside the PR-A1 snapshot blob, under
 * a fixed metadata-prefix key {@code "META/state/{stateName}/serializer"}):
 *
 * <pre>
 *   |  4 bytes  | format-version (currently {@link #CURRENT_FORMAT_VERSION} = 2, big-endian)    |
 *   |  4 bytes  | state-kind ordinal (big-endian) — matches {@code StateDescriptor.Type.ordinal()}|
 *   |  1 byte   | ttlEnabled (v2+; absent in v1)                                                 |
 *   |  8 bytes  | ttlMillis (v2+; absent in v1; 0 when ttlEnabled=false)                         |
 *   |  4 bytes  | serializerSnapshot byte-length N (big-endian)                                  |
 *   |  N bytes  | TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot output         |
 * </pre>
 *
 * <p>The state name itself is the engine key, not encoded in the value.
 */
@Internal
public final class StateSerializerMetadata {

    /**
     * Per-entry envelope version written by the current build. v1 → v2 in R24-H2 to add the
     * TTL flag + millis. Readers tolerate v1 (defaults TTL fields to off/0); writers always emit
     * v2.
     */
    public static final int CURRENT_FORMAT_VERSION = 2;

    /** Legacy envelope version (no TTL fields). Accepted on read for backwards compatibility. */
    public static final int FORMAT_VERSION_V1 = 1;

    /** Current envelope version with TTL fields (R24-H2). */
    public static final int FORMAT_VERSION_V2 = 2;

    private final String stateName;
    private final int stateKindOrdinal;
    private final int formatVersion;
    private final byte[] serializerSnapshotBytes;
    /** R24-H2: persisted TTL flag — true iff the state was registered with a TTL configuration. */
    private final boolean ttlEnabled;
    /**
     * R24-H2: persisted TTL value in milliseconds at registration time. Zero when {@link
     * #ttlEnabled} is false. Persisting the value (not just the flag) lets future work surface
     * actionable diagnostics on TTL-window changes (a separate ticket; this round only enforces
     * the enabled/disabled toggle as a hard failure).
     */
    private final long ttlMillis;

    /**
     * Legacy 4-arg constructor preserved for callers that have not been migrated to specify TTL.
     * Defaults {@code ttlEnabled=false}, {@code ttlMillis=0}, which is the pre-R24-H2 behaviour.
     */
    public StateSerializerMetadata(
            String stateName,
            int stateKindOrdinal,
            int formatVersion,
            byte[] serializerSnapshotBytes) {
        this(stateName, stateKindOrdinal, formatVersion, serializerSnapshotBytes, false, 0L);
    }

    /** Full constructor — captures TTL fields alongside the serializer snapshot. */
    public StateSerializerMetadata(
            String stateName,
            int stateKindOrdinal,
            int formatVersion,
            byte[] serializerSnapshotBytes,
            boolean ttlEnabled,
            long ttlMillis) {
        this.stateName = Objects.requireNonNull(stateName, "stateName");
        this.stateKindOrdinal = stateKindOrdinal;
        this.formatVersion = formatVersion;
        this.serializerSnapshotBytes =
                Objects.requireNonNull(serializerSnapshotBytes, "serializerSnapshotBytes");
        this.ttlEnabled = ttlEnabled;
        this.ttlMillis = ttlMillis;
    }

    public String stateName() {
        return stateName;
    }

    public int stateKindOrdinal() {
        return stateKindOrdinal;
    }

    public int formatVersion() {
        return formatVersion;
    }

    /** R24-H2: whether the state was registered with TTL on. */
    public boolean ttlEnabled() {
        return ttlEnabled;
    }

    /** R24-H2: TTL value in milliseconds when {@link #ttlEnabled} is true; otherwise 0. */
    public long ttlMillis() {
        return ttlMillis;
    }

    /**
     * The serialized snapshot bytes. Internal use only — callers must not mutate. The byte[] is
     * returned by reference for zero-copy; this class is short-lived and never escapes the backend.
     */
    public byte[] serializerSnapshotBytes() {
        return serializerSnapshotBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StateSerializerMetadata)) {
            return false;
        }
        StateSerializerMetadata that = (StateSerializerMetadata) o;
        return stateKindOrdinal == that.stateKindOrdinal
                && formatVersion == that.formatVersion
                && ttlEnabled == that.ttlEnabled
                && ttlMillis == that.ttlMillis
                && stateName.equals(that.stateName)
                && java.util.Arrays.equals(serializerSnapshotBytes, that.serializerSnapshotBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(stateName, stateKindOrdinal, formatVersion, ttlEnabled, ttlMillis);
        result = 31 * result + java.util.Arrays.hashCode(serializerSnapshotBytes);
        return result;
    }

    @Override
    public String toString() {
        return "StateSerializerMetadata{name="
                + stateName
                + ", kind="
                + stateKindOrdinal
                + ", fmtVer="
                + formatVersion
                + ", ttl="
                + (ttlEnabled ? ("on/" + ttlMillis + "ms") : "off")
                + ", snapshotBytes="
                + serializerSnapshotBytes.length
                + '}';
    }
}

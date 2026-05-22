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
 * <p><b>Persistence path:</b> PR-A1 has not landed yet, so the engine does not yet have a real
 * snapshot+restore loop. Until then, the {@link StateSerializerRegistry} keeps this metadata in an
 * in-memory map and surfaces it through {@code metadataBuffer()} for PR-A1 to drain into the
 * snapshot blob it emits. On restore, PR-A1 will hand back a buffer that the registry can re-read.
 *
 * <p><b>Format spec (v1)</b> (what each entry will become inside the PR-A1 snapshot blob, under
 * a fixed metadata-prefix key {@code "META/state/{stateName}/serializer"}):
 *
 * <pre>
 *   |  4 bytes  | format-version (currently {@link #CURRENT_FORMAT_VERSION} = 1, big-endian)    |
 *   |  4 bytes  | state-kind ordinal (big-endian) — matches {@code StateDescriptor.Type.ordinal()}|
 *   |  4 bytes  | serializerSnapshot byte-length N (big-endian)                                  |
 *   |  N bytes  | TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot output         |
 * </pre>
 *
 * <p>The state name itself is the engine key, not encoded in the value.
 */
@Internal
public final class StateSerializerMetadata {

    /** Current format version for the metadata envelope written by PR-A11. */
    public static final int CURRENT_FORMAT_VERSION = 1;

    private final String stateName;
    private final int stateKindOrdinal;
    private final int formatVersion;
    private final byte[] serializerSnapshotBytes;

    public StateSerializerMetadata(
            String stateName,
            int stateKindOrdinal,
            int formatVersion,
            byte[] serializerSnapshotBytes) {
        this.stateName = Objects.requireNonNull(stateName, "stateName");
        this.stateKindOrdinal = stateKindOrdinal;
        this.formatVersion = formatVersion;
        this.serializerSnapshotBytes =
                Objects.requireNonNull(serializerSnapshotBytes, "serializerSnapshotBytes");
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

    /**
     * The serialized snapshot bytes. Internal use only — callers must not mutate. The byte[] is
     * returned by reference for zero-copy; this class is short-lived and never escapes the backend.
     */
    public byte[] serializerSnapshotBytes() {
        return serializerSnapshotBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StateSerializerMetadata)) return false;
        StateSerializerMetadata that = (StateSerializerMetadata) o;
        return stateKindOrdinal == that.stateKindOrdinal
                && formatVersion == that.formatVersion
                && stateName.equals(that.stateName)
                && java.util.Arrays.equals(serializerSnapshotBytes, that.serializerSnapshotBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(stateName, stateKindOrdinal, formatVersion);
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
                + ", snapshotBytes="
                + serializerSnapshotBytes.length
                + '}';
    }
}

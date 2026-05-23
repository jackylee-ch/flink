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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.state.forstrs.state.StateSerializerMetadata;
import org.apache.flink.state.forstrs.state.StateSerializerRegistry;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R36-M3 — V2 schema-drift acceptance suite. Complements
 * {@link V1SyncSchemaDriftTest}; focuses on V2-specific paths:
 *
 * <ul>
 *   <li>R36-M1: MapState user-key (UK) registry validation. Pre-R36-M1 the V2 MAP creation path
 *       passed {@code mapDesc.getUserKeySerializer()} through as-is, never registering it with
 *       the schema registry. A UK-schema-evolution restore would silently decode garbage on
 *       every lookup. The fix registers the UK under the synthetic registry key
 *       {@code "<stateName>$UK"} with the synthetic kind
 *       {@link StateSerializerRegistry#KIND_USER_KEY}. These tests drive the
 *       registry-level behavior directly (the V2 backend wires it identically).
 * </ul>
 *
 * <p>These tests exercise the registry surface directly rather than wiring a fully-constructed
 * async backend; the V2 MAP-case code in {@link ForStRsAsyncKeyedStateBackend} calls the same
 * {@code verifyOrRegister} entries this test asserts on. The advantage of testing at the
 * registry layer is determinism — no async dispatch, no engine FFI, no thread-pool tear-down
 * to worry about.
 */
class V2SchemaDriftTest {

    /**
     * R36-M1: register a fresh V2 MapState (no restored side) and assert that BOTH the value-side
     * MAP entry and the user-key-side {@code $UK}/{@code USER_KEY} entry are persisted in the
     * registry. Pre-R36-M1 only the MAP entry existed and a UK toggle slipped past restore-side
     * detection.
     */
    @Test
    void freshMapStateRegistrationPersistsTwoEntriesForValueAndUserKey() throws Exception {
        StateSerializerRegistry reg = new StateSerializerRegistry();
        // Mirror the V2 backend's call shape: first the MAP entry (value serializer), then the
        // synthetic USER_KEY entry. The async backend invokes verifyOrRegister twice per
        // MapState creation; verifyOrRegister on a fresh registry just delegates to register.
        int mapKind = org.apache.flink.api.common.state.v2.StateDescriptor.Type.MAP.ordinal();
        reg.verifyOrRegister("evolvedMap", mapKind, LongSerializer.INSTANCE);
        reg.verifyOrRegister(
                "evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX,
                StateSerializerRegistry.KIND_USER_KEY,
                StringSerializer.INSTANCE);

        StateSerializerMetadata valueEntry = reg.get("evolvedMap");
        StateSerializerMetadata ukEntry =
                reg.get("evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX);
        assertNotNull(valueEntry, "value-serializer entry must be registered");
        assertNotNull(ukEntry, "R36-M1: user-key entry must be registered under $UK suffix");
        assertEquals(mapKind, valueEntry.stateKindOrdinal());
        assertEquals(StateSerializerRegistry.KIND_USER_KEY, ukEntry.stateKindOrdinal());
        assertEquals(
                2,
                reg.metadataBuffer().size(),
                "R36-M1: a single MapState yields exactly two registry entries (value + UK)");
    }

    /**
     * R36-M1: verify the round-trip — serialize the live registry (with both entries) and
     * deserialize back. The deserialized map must contain BOTH entries with the same kind
     * ordinals so a restore-side {@code verifyOrRegister} can perform schema-drift detection
     * against the persisted UK snapshot.
     */
    @Test
    void mapStateUserKeyEntryRoundTripsThroughRegistryBlob() throws Exception {
        StateSerializerRegistry reg = new StateSerializerRegistry();
        int mapKind = org.apache.flink.api.common.state.v2.StateDescriptor.Type.MAP.ordinal();
        reg.verifyOrRegister("evolvedMap", mapKind, LongSerializer.INSTANCE);
        reg.verifyOrRegister(
                "evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX,
                StateSerializerRegistry.KIND_USER_KEY,
                StringSerializer.INSTANCE);

        byte[] blob = reg.serialize();
        Map<String, StateSerializerMetadata> deserialized = StateSerializerRegistry.deserialize(blob);
        assertEquals(
                2,
                deserialized.size(),
                "round-trip preserves both MAP and USER_KEY entries");
        assertEquals(mapKind, deserialized.get("evolvedMap").stateKindOrdinal());
        assertEquals(
                StateSerializerRegistry.KIND_USER_KEY,
                deserialized.get("evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX)
                        .stateKindOrdinal(),
                "R36-M1: USER_KEY kind ordinal (5) survives the wire-format round trip"
                        + " — the registry's bounds check now permits ordinals 0..5");
    }

    /**
     * R36-M1: a restored MapState whose UK was previously a {@link StringSerializer} cannot be
     * restored with an {@code IntSerializer} as the UK — the UK snapshot resolves as
     * {@link TypeSerializerSchemaCompatibility#isIncompatible() INCOMPATIBLE} and
     * {@code verifyOrRegister} on the {@code $UK} entry MUST throw
     * {@link StateMigrationException}. Pre-fix this UK toggle slipped through completely (the UK
     * was never registered), and every map.get() on the restored state would decode 4 bytes
     * (int) at a position where the old layout wrote a variable-length UTF8 string — silent
     * garbage.
     */
    @Test
    void restoredMapStateUserKeyDriftSurfaceAsStateMigrationException() throws Exception {
        // Build the pre-snapshot registry (the "old" job that was running before restart).
        StateSerializerRegistry oldReg = new StateSerializerRegistry();
        int mapKind = org.apache.flink.api.common.state.v2.StateDescriptor.Type.MAP.ordinal();
        oldReg.verifyOrRegister("evolvedMap", mapKind, LongSerializer.INSTANCE);
        oldReg.verifyOrRegister(
                "evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX,
                StateSerializerRegistry.KIND_USER_KEY,
                StringSerializer.INSTANCE);
        byte[] blob = oldReg.serialize();

        // Now the new job restarts and seeds from the snapshot.
        StateSerializerRegistry newReg = new StateSerializerRegistry();
        newReg.seedFromRestore(StateSerializerRegistry.deserialize(blob));

        // The new MapStateDescriptor keeps the same value type (Long) but changes the UK type
        // (String → Integer) — the value side resolves as compatible, the UK side resolves as
        // INCOMPATIBLE and MUST surface as StateMigrationException at the UK verifyOrRegister.
        TypeSerializer<Long> sameValue = LongSerializer.INSTANCE;
        TypeSerializer<Integer> changedUk = IntSerializer.INSTANCE;

        // Value side passes (sanity check; pre-R36-M1 the UK was silently dropped here too).
        TypeSerializer<Long> resolvedValue =
                newReg.verifyOrRegister("evolvedMap", mapKind, sameValue);
        assertSame(
                sameValue,
                resolvedValue,
                "value serializer compatible-as-is → registry returns the same instance");

        // UK side throws under R36-M1.
        StateMigrationException ex =
                assertThrows(
                        StateMigrationException.class,
                        () ->
                                newReg.verifyOrRegister(
                                        "evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX,
                                        StateSerializerRegistry.KIND_USER_KEY,
                                        changedUk),
                        "R36-M1: a UK schema change MUST surface as StateMigrationException at"
                                + " the synthetic $UK registry entry");
        assertTrue(
                ex.getMessage().contains("evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX),
                "exception message must name the offending registry entry: " + ex.getMessage());
    }

    /**
     * R36-M1: a UK that was previously registered with kind=USER_KEY must NOT be confused with a
     * primary state of the same suffixed name. The kind-mismatch guard in {@code verifyOrRegister}
     * already surfaces this: registering the suffixed name with a non-USER_KEY ordinal collides
     * on the kind invariant. This protects against an accidental future refactor that drops the
     * suffix convention.
     */
    @Test
    void registeringSuffixedNameWithWrongKindIsRejected() throws Exception {
        StateSerializerRegistry reg = new StateSerializerRegistry();
        // Register a USER_KEY-kind entry under the suffixed name.
        reg.register(
                "myMap" + StateSerializerRegistry.USER_KEY_SUFFIX,
                StateSerializerRegistry.KIND_USER_KEY,
                StringSerializer.INSTANCE);
        // Re-registering the SAME suffixed name as a different kind must fail with
        // IllegalStateException (kind mismatch within a single session) — proves the kind
        // invariant covers the new USER_KEY ordinal.
        IllegalStateException ex =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                reg.register(
                                        "myMap" + StateSerializerRegistry.USER_KEY_SUFFIX,
                                        org.apache.flink.api.common.state.v2.StateDescriptor.Type
                                                .VALUE.ordinal(),
                                        StringSerializer.INSTANCE),
                        "R36-M1: registering a $UK-suffixed name with a non-USER_KEY kind must"
                                + " be rejected by the duplicate-kind guard");
        assertTrue(
                ex.getMessage().contains("Duplicate state registration with different kind"),
                "kind-mismatch message must surface: " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("USER_KEY"),
                "error message must name the USER_KEY kind: " + ex.getMessage());
    }

    /**
     * R36-M3: the V2 ValueState write-path covers schema-drift validation. Reuse the registry
     * directly to confirm a ValueState V2 round-trip catches an INCOMPATIBLE serializer change.
     * The async backend's createStateInternal VALUE case calls verifyOrRegister with kind=VALUE;
     * we drive the same registry surface.
     */
    @Test
    void v2ValueStateIncompatibleSchemaThrowsStateMigrationException() throws Exception {
        StateSerializerRegistry oldReg = new StateSerializerRegistry();
        int valueKind = org.apache.flink.api.common.state.v2.StateDescriptor.Type.VALUE.ordinal();
        oldReg.register("counter", valueKind, LongSerializer.INSTANCE);
        byte[] blob = oldReg.serialize();

        StateSerializerRegistry newReg = new StateSerializerRegistry();
        newReg.seedFromRestore(StateSerializerRegistry.deserialize(blob));

        // String → Long for the same name is INCOMPATIBLE.
        assertThrows(
                StateMigrationException.class,
                () -> newReg.verifyOrRegister("counter", valueKind, StringSerializer.INSTANCE),
                "R36-M3: V2 ValueState schema-incompat must surface as StateMigrationException");
    }

    /**
     * R36-M3: a kind-mismatch on the primary entry (registering "evolved" as VALUE when it was
     * persisted as LIST) MUST surface as StateMigrationException. Mirrors the V1-sync coverage
     * at the registry layer — both V1 and V2 read this same primitive.
     */
    @Test
    void kindMismatchOnPrimaryEntryThrowsStateMigrationException() throws Exception {
        StateSerializerRegistry oldReg = new StateSerializerRegistry();
        int listKind = org.apache.flink.api.common.state.v2.StateDescriptor.Type.LIST.ordinal();
        int valueKind = org.apache.flink.api.common.state.v2.StateDescriptor.Type.VALUE.ordinal();
        oldReg.register("ambiguous", listKind, StringSerializer.INSTANCE);
        byte[] blob = oldReg.serialize();

        StateSerializerRegistry newReg = new StateSerializerRegistry();
        newReg.seedFromRestore(StateSerializerRegistry.deserialize(blob));

        StateMigrationException ex =
                assertThrows(
                        StateMigrationException.class,
                        () ->
                                newReg.verifyOrRegister(
                                        "ambiguous", valueKind, StringSerializer.INSTANCE),
                        "R36-M3: kind drift (LIST → VALUE) must surface as StateMigrationException");
        assertTrue(
                ex.getMessage().contains("State kind mismatch"),
                "exception message must surface the kind-mismatch label: " + ex.getMessage());
    }

    /**
     * R36-M3: a freshly-registered USER_KEY entry that round-trips through serialize/deserialize
     * must verify as COMPATIBLE_AS_IS when the same serializer is re-supplied — proving the new
     * KIND_USER_KEY ordinal travels through the on-disk format without corruption.
     */
    @Test
    void userKeyEntryVerifyOrRegisterCompatibleAsIs() throws Exception {
        StateSerializerRegistry oldReg = new StateSerializerRegistry();
        oldReg.register(
                "evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX,
                StateSerializerRegistry.KIND_USER_KEY,
                StringSerializer.INSTANCE);
        byte[] blob = oldReg.serialize();

        StateSerializerRegistry newReg = new StateSerializerRegistry();
        newReg.seedFromRestore(StateSerializerRegistry.deserialize(blob));

        TypeSerializer<String> resolved =
                newReg.verifyOrRegister(
                        "evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX,
                        StateSerializerRegistry.KIND_USER_KEY,
                        StringSerializer.INSTANCE);
        // Same instance comes back — compatible-as-is path.
        assertSame(
                StringSerializer.INSTANCE,
                resolved,
                "R36-M3: UK serializer compatible-as-is → registry returns the supplied instance");

        // The entry has been promoted to live and dropped from restored — a second
        // verifyOrRegister with a DIFFERENT (but still compatible) serializer is treated as a
        // fresh in-session registration (no-op for same kind).
        // Confirm the live entry retains the USER_KEY kind ordinal.
        StateSerializerMetadata md =
                newReg.get("evolvedMap" + StateSerializerRegistry.USER_KEY_SUFFIX);
        assertNotNull(md);
        assertEquals(StateSerializerRegistry.KIND_USER_KEY, md.stateKindOrdinal());
    }
}

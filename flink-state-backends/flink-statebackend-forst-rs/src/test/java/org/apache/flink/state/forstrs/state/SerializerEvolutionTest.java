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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * PR-A11 (E3-HIGH-4) regression: when a state's TypeSerializer changes across sessions, the
 * backend must verify schema compatibility against the persisted {@link
 * org.apache.flink.api.common.typeutils.TypeSerializerSnapshot} and throw {@link
 * StateMigrationException} when the change is incompatible (rather than silently
 * deserializing garbage).
 *
 * <p>These tests exercise {@link StateSerializerRegistry} directly, simulating a "next session"
 * by:
 *
 * <ol>
 *   <li>Registering a state with the old serializer (write path).
 *   <li>Capturing the metadata buffer that PR-A1 will later persist into the snapshot blob.
 *   <li>Reseeding a fresh registry with that buffer ({@link
 *       StateSerializerRegistry#seedFromRestore}) to simulate restore.
 *   <li>Calling {@link StateSerializerRegistry#verifyOrRegister} with the new serializer and
 *       asserting the {@code resolveSchemaCompatibility} branch.
 * </ol>
 */
class SerializerEvolutionTest {

    private static final int VALUE_KIND = 0;

    /** Simulate a full session boundary: write registry → buffer → restored registry. */
    private static StateSerializerRegistry roundTrip(StateSerializerRegistry source)
            throws Exception {
        // Capture the metadata buffer (PR-A1 will drain this into the checkpoint blob).
        Map<String, StateSerializerMetadata> buffer =
                new HashMap<>(source.metadataBuffer());
        assertTrue(!buffer.isEmpty(), "source registry must have registered something");

        // Simulate the next session: fresh registry, seeded from the captured buffer.
        StateSerializerRegistry next = new StateSerializerRegistry();
        next.seedFromRestore(buffer);
        assertTrue(next.activatedForRestore(), "next session registry should be activated");
        return next;
    }

    // ----------------------------------------------------------------
    // INCOMPATIBLE: IntSerializer → LongSerializer
    // ----------------------------------------------------------------

    @Test
    void incompatibleIntToLongThrowsStateMigrationException() throws Exception {
        StateSerializerRegistry write = new StateSerializerRegistry();
        write.register("myValue", VALUE_KIND, IntSerializer.INSTANCE);
        assertNotNull(write.get("myValue"));

        StateSerializerRegistry read = roundTrip(write);

        // New session: same state name, but the user code now declares a LongSerializer.
        // resolveSchemaCompatibility(oldIntSnapshot) on a LongSerializerSnapshot must return
        // INCOMPATIBLE, which our registry surfaces as StateMigrationException.
        StateMigrationException ex =
                assertThrows(
                        StateMigrationException.class,
                        () ->
                                read.verifyOrRegister(
                                        "myValue", VALUE_KIND, LongSerializer.INSTANCE));
        assertTrue(
                ex.getMessage().contains("INCOMPATIBLE"),
                "Exception message should mention INCOMPATIBLE: " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("myValue"),
                "Exception message should mention the state name: " + ex.getMessage());
    }

    // ----------------------------------------------------------------
    // INCOMPATIBLE: StringSerializer → IntSerializer
    // ----------------------------------------------------------------

    @Test
    void incompatibleStringToIntThrowsStateMigrationException() throws Exception {
        StateSerializerRegistry write = new StateSerializerRegistry();
        write.register("myValue", VALUE_KIND, StringSerializer.INSTANCE);

        StateSerializerRegistry read = roundTrip(write);

        StateMigrationException ex =
                assertThrows(
                        StateMigrationException.class,
                        () ->
                                read.verifyOrRegister(
                                        "myValue", VALUE_KIND, IntSerializer.INSTANCE));
        assertTrue(
                ex.getMessage().contains("INCOMPATIBLE"),
                "Exception message should mention INCOMPATIBLE: " + ex.getMessage());
    }

    // ----------------------------------------------------------------
    // COMPATIBLE_AS_IS: IntSerializer → IntSerializer
    // ----------------------------------------------------------------

    @Test
    void compatibleIntToIntReturnsSameSerializerAndDoesNotThrow() throws Exception {
        StateSerializerRegistry write = new StateSerializerRegistry();
        write.register("myValue", VALUE_KIND, IntSerializer.INSTANCE);

        StateSerializerRegistry read = roundTrip(write);

        TypeSerializer<Integer> effective;
        try {
            effective = read.verifyOrRegister("myValue", VALUE_KIND, IntSerializer.INSTANCE);
        } catch (StateMigrationException ex) {
            fail("Same serializer must be COMPATIBLE_AS_IS, got: " + ex.getMessage());
            return;
        }
        // For COMPATIBLE_AS_IS, the registry must hand back the caller's own serializer.
        assertSame(IntSerializer.INSTANCE, effective);

        // And the registry must have promoted the entry to its live-buffer (so the next
        // snapshot persists the current schema).
        assertNotNull(read.get("myValue"));
        assertEquals(VALUE_KIND, read.get("myValue").stateKindOrdinal());
    }

    // ----------------------------------------------------------------
    // Fresh state (no prior snapshot) — verifyOrRegister behaves like register
    // ----------------------------------------------------------------

    @Test
    void freshStateRegistersWithoutVerification() throws Exception {
        StateSerializerRegistry reg = new StateSerializerRegistry();
        // No seedFromRestore: the registry is empty on the restored side. verifyOrRegister
        // must NOT throw; it should just register write-side metadata.
        TypeSerializer<Integer> effective =
                reg.verifyOrRegister("myValue", VALUE_KIND, IntSerializer.INSTANCE);
        assertSame(IntSerializer.INSTANCE, effective);
        assertNotNull(reg.get("myValue"));
    }

    // ----------------------------------------------------------------
    // Format invariant: metadata envelope carries the format version
    // ----------------------------------------------------------------

    @Test
    void metadataCarriesCurrentFormatVersion() throws Exception {
        StateSerializerRegistry reg = new StateSerializerRegistry();
        reg.register("v1", VALUE_KIND, IntSerializer.INSTANCE);

        StateSerializerMetadata md = reg.get("v1");
        assertNotNull(md);
        assertEquals("v1", md.stateName());
        assertEquals(VALUE_KIND, md.stateKindOrdinal());
        assertEquals(StateSerializerMetadata.CURRENT_FORMAT_VERSION, md.formatVersion());
        assertNotNull(md.serializerSnapshotBytes());
        assertTrue(
                md.serializerSnapshotBytes().length > 0,
                "Serialized snapshot must be non-empty for IntSerializer");
    }
}

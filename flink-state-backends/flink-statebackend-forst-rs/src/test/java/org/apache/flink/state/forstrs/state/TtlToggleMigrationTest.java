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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.util.StateMigrationException;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R24-H2 (HIGH): registered TTL flag must be persisted in the serializer metadata so that a TTL
 * toggle across sessions is surfaced as {@link StateMigrationException}, not silently corrupted.
 *
 * <p>Pre-fix, the registry persisted ONLY the user serializer snapshot; {@code ttlEnabled} was
 * implicit. Because the TTL serializer writes {@code [long expiry][value]} on disk, restarting
 * with the same descriptor but TTL disabled (config flip) would have {@code
 * resolveSchemaCompatibility} return COMPATIBLE_AS_IS, and the read path would decode the
 * 8-byte expiry header as the value head — silent corruption of every restored value.
 *
 * <p>Tests:
 *
 * <ol>
 *   <li>{@code ttlOnToOffThrowsStateMigrationException} — register with TTL=ON, simulate a
 *       snapshot+restore via {@link StateSerializerRegistry#serialize} / {@code deserialize},
 *       restore-side {@code verifyOrRegister} with TTL=OFF must throw.
 *   <li>{@code ttlOffToOnThrowsStateMigrationException} — symmetric: pre-snapshot TTL=OFF,
 *       restore-side TTL=ON must throw.
 *   <li>{@code ttlOnToOnPassesAndPreservesMillis} — matching flags resolve cleanly.
 *   <li>{@code v1BlobReadsWithTtlDisabledDefault} — legacy v1 envelopes (pre-R24-H2) deserialize
 *       with {@code ttlEnabled=false}, which is correct (no TTL was registered pre-fix).
 * </ol>
 */
class TtlToggleMigrationTest {

    private static final int VALUE_KIND = 0;

    @Test
    void ttlOnToOffThrowsStateMigrationException() throws Exception {
        // Session 1: register with TTL=ON.
        StateSerializerRegistry write = new StateSerializerRegistry();
        write.register("ttlState", VALUE_KIND, IntSerializer.INSTANCE,
                /* ttlEnabled= */ true,
                /* ttlMillis= */ 60_000L);

        // Verify the metadata persisted the TTL flag.
        StateSerializerMetadata md = write.get("ttlState");
        assertNotNull(md);
        assertTrue(md.ttlEnabled(), "ttlEnabled must persist in metadata after register");
        assertEquals(60_000L, md.ttlMillis());
        assertEquals(StateSerializerMetadata.FORMAT_VERSION_V2, md.formatVersion());

        // Session boundary: blob → re-parse → seed.
        byte[] blob = write.serialize();
        Map<String, StateSerializerMetadata> parsed = StateSerializerRegistry.deserialize(blob);
        StateSerializerMetadata restoredMd = parsed.get("ttlState");
        assertNotNull(restoredMd, "round-tripped map contains ttlState");
        assertTrue(restoredMd.ttlEnabled(), "ttlEnabled must round-trip through serialize/deserialize");

        StateSerializerRegistry read = new StateSerializerRegistry();
        read.seedFromRestore(parsed);

        // Session 2: descriptor flipped to TTL=OFF — must throw.
        StateMigrationException ex =
                assertThrows(
                        StateMigrationException.class,
                        () ->
                                read.verifyOrRegister(
                                        "ttlState",
                                        VALUE_KIND,
                                        IntSerializer.INSTANCE,
                                        /* ttlEnabled= */ false,
                                        /* ttlMillis= */ 0L));
        assertTrue(
                ex.getMessage().contains("TTL toggle"),
                "exception should mention TTL toggle: " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("prior=TTL_ENABLED"),
                "exception should mention prior=TTL_ENABLED: " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("now=TTL_DISABLED"),
                "exception should mention now=TTL_DISABLED: " + ex.getMessage());
    }

    @Test
    void ttlOffToOnThrowsStateMigrationException() throws Exception {
        // Symmetric scenario: pre-snapshot TTL=OFF, restore-side TTL=ON.
        StateSerializerRegistry write = new StateSerializerRegistry();
        write.register("ttlState", VALUE_KIND, IntSerializer.INSTANCE,
                /* ttlEnabled= */ false,
                /* ttlMillis= */ 0L);

        byte[] blob = write.serialize();
        Map<String, StateSerializerMetadata> parsed = StateSerializerRegistry.deserialize(blob);
        StateSerializerRegistry read = new StateSerializerRegistry();
        read.seedFromRestore(parsed);

        StateMigrationException ex =
                assertThrows(
                        StateMigrationException.class,
                        () ->
                                read.verifyOrRegister(
                                        "ttlState",
                                        VALUE_KIND,
                                        IntSerializer.INSTANCE,
                                        /* ttlEnabled= */ true,
                                        /* ttlMillis= */ 30_000L));
        assertTrue(
                ex.getMessage().contains("prior=TTL_DISABLED"),
                "exception should mention prior=TTL_DISABLED: " + ex.getMessage());
        assertTrue(
                ex.getMessage().contains("now=TTL_ENABLED"),
                "exception should mention now=TTL_ENABLED: " + ex.getMessage());
    }

    @Test
    void ttlOnToOnPassesAndPreservesMillis() throws Exception {
        StateSerializerRegistry write = new StateSerializerRegistry();
        write.register("ttlState", VALUE_KIND, IntSerializer.INSTANCE,
                /* ttlEnabled= */ true,
                /* ttlMillis= */ 60_000L);

        byte[] blob = write.serialize();
        Map<String, StateSerializerMetadata> parsed = StateSerializerRegistry.deserialize(blob);
        StateSerializerRegistry read = new StateSerializerRegistry();
        read.seedFromRestore(parsed);

        // Same flag, possibly different millis — must NOT throw. The verifyOrRegister returns
        // the caller's own serializer (COMPATIBLE_AS_IS).
        assertSame(
                IntSerializer.INSTANCE,
                read.verifyOrRegister(
                        "ttlState",
                        VALUE_KIND,
                        IntSerializer.INSTANCE,
                        /* ttlEnabled= */ true,
                        /* ttlMillis= */ 90_000L));

        // The newly-promoted live entry should reflect the NEW millis (the registry replaces
        // the restored entry with a fresh one under the new descriptor).
        StateSerializerMetadata live = read.get("ttlState");
        assertNotNull(live, "live entry exists after promote");
        assertTrue(live.ttlEnabled());
        assertEquals(90_000L, live.ttlMillis(), "live entry reflects the new TTL window");
    }

    @Test
    void v1BlobReadsWithTtlDisabledDefault() throws Exception {
        // Construct a v1-shaped metadata in memory and round-trip through serialize/deserialize.
        // Because the writer emits whatever fmtVer the metadata carries, this exercises the
        // v1 read path (no TTL fields on the wire) and asserts the parsed entry defaults to
        // ttlEnabled=false, ttlMillis=0.
        StateSerializerMetadata v1Entry =
                new StateSerializerMetadata(
                        "legacyState",
                        VALUE_KIND,
                        StateSerializerMetadata.FORMAT_VERSION_V1,
                        new byte[] {1, 2, 3, 4} /* opaque snapshot bytes */);
        assertFalse(v1Entry.ttlEnabled(), "v1 entries default to ttlEnabled=false");
        assertEquals(0L, v1Entry.ttlMillis());

        java.util.LinkedHashMap<String, StateSerializerMetadata> map = new java.util.LinkedHashMap<>();
        map.put("legacyState", v1Entry);
        byte[] blob = StateSerializerRegistry.serialize(map);

        Map<String, StateSerializerMetadata> parsed = StateSerializerRegistry.deserialize(blob);
        StateSerializerMetadata back = parsed.get("legacyState");
        assertNotNull(back, "v1 entry round-trips");
        assertEquals(StateSerializerMetadata.FORMAT_VERSION_V1, back.formatVersion());
        assertFalse(back.ttlEnabled(), "v1 entries deserialize with ttlEnabled=false");
        assertEquals(0L, back.ttlMillis());
    }
}

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

import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link ForStRsIncrementalKeyedStateHandle} (B-Prod-P3 Task 3.3). */
class ForStRsIncrementalKeyedStateHandleTest {

    private static StreamStateHandle bytes(String name, int n) {
        byte[] payload = new byte[n];
        for (int i = 0; i < n; i++) {
            payload[i] = (byte) i;
        }
        return new ByteStreamStateHandle(name, payload);
    }

    @Test
    void carriesAllConstructorFields() {
        UUID backend = UUID.randomUUID();
        KeyGroupRange kgr = new KeyGroupRange(0, 127);
        StreamStateHandle s1 = bytes("sst-1", 100);
        StreamStateHandle s2 = bytes("sst-2", 200);
        StreamStateHandle priv = bytes("MANIFEST", 50);
        StreamStateHandle meta = bytes("_metadata", 25);
        List<HandleAndLocalPath> shared =
                List.of(
                        HandleAndLocalPath.of(s1, "000123.sst"),
                        HandleAndLocalPath.of(s2, "000124.sst"));
        List<HandleAndLocalPath> privateState = List.of(HandleAndLocalPath.of(priv, "MANIFEST"));
        Map<String, Long> cfMap = new LinkedHashMap<>();
        cfMap.put("default", 0L);
        cfMap.put("user-state-counter", 1L);

        ForStRsIncrementalKeyedStateHandle h =
                new ForStRsIncrementalKeyedStateHandle(
                        backend, kgr, 7L, 5L, shared, privateState, meta, cfMap);

        assertEquals(backend, h.getBackendIdentifier());
        assertEquals(kgr, h.getKeyGroupRange());
        assertEquals(7L, h.getCheckpointId());
        assertEquals(5L, h.getBaseCheckpointId());
        assertEquals(shared, h.getSharedState());
        assertEquals(privateState, h.getPrivateState());
        assertSame(meta, h.getMetaDataStateHandle());
        assertEquals(cfMap, h.getCfMap());
        // size = 100 + 200 + 50 + 25 = 375
        assertEquals(375L, h.getStateSize());
        assertEquals(375L, h.getCheckpointedSize());
    }

    @Test
    void implementsIncrementalKeyedStateHandle() {
        ForStRsIncrementalKeyedStateHandle h =
                new ForStRsIncrementalKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 0),
                        1L,
                        0L,
                        List.of(),
                        List.of(),
                        bytes("meta", 10),
                        Map.of());
        assertInstanceOf(IncrementalKeyedStateHandle.class, h);
    }

    @Test
    void streamSubHandlesIncludesAllArtefacts() {
        StreamStateHandle s1 = bytes("sst-1", 1);
        StreamStateHandle p1 = bytes("priv-1", 1);
        StreamStateHandle meta = bytes("meta", 1);
        ForStRsIncrementalKeyedStateHandle h =
                new ForStRsIncrementalKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 0),
                        1L,
                        0L,
                        List.of(HandleAndLocalPath.of(s1, "1.sst")),
                        List.of(HandleAndLocalPath.of(p1, "MANIFEST")),
                        meta,
                        Map.of());
        long count = h.streamSubHandles().count();
        assertEquals(3L, count, "1 shared + 1 private + 1 meta = 3 sub-handles");
    }

    @Test
    void getIntersectionReturnsThisOnExactRange() {
        KeyGroupRange kgr = new KeyGroupRange(10, 20);
        ForStRsIncrementalKeyedStateHandle h =
                new ForStRsIncrementalKeyedStateHandle(
                        UUID.randomUUID(),
                        kgr,
                        1L,
                        0L,
                        List.of(),
                        List.of(),
                        bytes("m", 1),
                        Map.of());
        assertSame(h, h.getIntersection(kgr));
    }

    @Test
    void reboundCarriesSameStateHandleIdAndNewCheckpointId() {
        ForStRsIncrementalKeyedStateHandle h =
                new ForStRsIncrementalKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 0),
                        7L,
                        0L,
                        List.of(),
                        List.of(),
                        bytes("m", 1),
                        Map.of());
        ForStRsIncrementalKeyedStateHandle rebound =
                (ForStRsIncrementalKeyedStateHandle) h.rebound(99L);
        assertEquals(99L, rebound.getCheckpointId());
        assertEquals(h.getStateHandleId(), rebound.getStateHandleId());
    }

    @Test
    void discardStateOfPrivateAndMetaDoesNotThrow() throws Exception {
        // ByteStreamStateHandle.discardState() is a no-op; just confirm no exception escapes.
        ForStRsIncrementalKeyedStateHandle h =
                new ForStRsIncrementalKeyedStateHandle(
                        UUID.randomUUID(),
                        new KeyGroupRange(0, 0),
                        1L,
                        0L,
                        List.of(HandleAndLocalPath.of(bytes("sst", 5), "1.sst")),
                        List.of(HandleAndLocalPath.of(bytes("p", 5), "MANIFEST")),
                        bytes("meta", 5),
                        Map.of());
        h.discardState();
        assertTrue(true, "discard completed without throwing");
    }
}

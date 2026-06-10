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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class V1SyncMapStateNamespaceIsolationTest {

    @Test
    void internalMapStateIsIsolatedByCurrentNamespace() throws Exception {
        try (V1Pair pair = openV1Pair()) {
            pair.backend.setCurrentKey(7);

            MapStateDescriptor<Long, Long> descriptor =
                    new MapStateDescriptor<>(
                            "hopWindowMap",
                            LongSerializer.INSTANCE,
                            LongSerializer.INSTANCE);
            descriptor.initializeSerializerUnlessSet(new ExecutionConfig());

            @SuppressWarnings({"unchecked", "rawtypes"})
            InternalMapState<Integer, Integer, Long, Long> state =
                    (InternalMapState<Integer, Integer, Long, Long>)
                            (State)
                                    pair.backend.createOrUpdateInternalState(
                                            IntSerializer.INSTANCE,
                                            (StateDescriptor) descriptor,
                                            StateSnapshotTransformer.StateSnapshotTransformFactory
                                                    .noTransform());

            state.setCurrentNamespace(100);
            state.put(1L, 10L);

            state.setCurrentNamespace(200);
            state.put(1L, 20L);
            state.put(2L, 200L);

            state.setCurrentNamespace(100);
            assertEquals(10L, state.get(1L));
            assertNull(state.get(2L));

            state.setCurrentNamespace(200);
            assertEquals(20L, state.get(1L));
            assertEquals(200L, state.get(2L));

            state.clear();

            state.setCurrentNamespace(100);
            assertEquals(10L, state.get(1L));
            assertFalse(state.isEmpty());

            state.setCurrentNamespace(200);
            assertNull(state.get(1L));
            assertNull(state.get(2L));
            assertFalse(state.iterator().hasNext());
        }
    }

    private static V1Pair openV1Pair() {
        Arena arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        FrsDb db = linker.dbOpenMemory(arena);
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
        ForStRsKeyedStateBackend<Integer> delegate =
                new ForStRsKeyedStateBackend<>(
                        arena,
                        linker,
                        db,
                        cf,
                        IntSerializer.INSTANCE,
                        /* ownsResources= */ true,
                        keyGroupRange,
                        /* numberOfKeyGroups= */ 1);
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        ForStRsAbstractKeyedStateBackend<Integer> backend =
                new ForStRsAbstractKeyedStateBackend<>(
                        IntSerializer.INSTANCE,
                        Thread.currentThread().getContextClassLoader(),
                        new ExecutionConfig(),
                        cancelStreamRegistry,
                        delegate,
                        keyGroupRange,
                        /* numberOfKeyGroups= */ 1);
        ForStRsSstRegistry sstRegistry = new ForStRsSstRegistry();
        ForStRsSnapshotStrategy strategy =
                new ForStRsSnapshotStrategy(
                        linker,
                        db,
                        UUID.randomUUID(),
                        keyGroupRange,
                        sstRegistry,
                        new ForStRsSstUploader(),
                        arena,
                        Map.of("default", 0L));
        backend.setSnapshotStrategy(strategy, sstRegistry);
        return new V1Pair(backend, cancelStreamRegistry, cf, db, arena);
    }

    private record V1Pair(
            ForStRsAbstractKeyedStateBackend<Integer> backend,
            CloseableRegistry cancelStreamRegistry,
            FrsCfHandle cf,
            FrsDb db,
            Arena arena)
            implements AutoCloseable {
        @Override
        public void close() throws Exception {
            try {
                backend.close();
            } finally {
                cancelStreamRegistry.close();
            }
        }
    }
}

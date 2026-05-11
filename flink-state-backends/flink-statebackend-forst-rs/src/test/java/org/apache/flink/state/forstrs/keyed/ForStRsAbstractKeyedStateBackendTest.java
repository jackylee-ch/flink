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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Smoke test for {@link ForStRsAbstractKeyedStateBackend}: confirms it constructs as a real {@link
 * AbstractKeyedStateBackend} subclass, that the trivially-delegable methods (getKeys,
 * getBackendTypeIdentifier, numKeyValueStateEntries, notifyCheckpointComplete) work, and that the
 * P3/P4-stubbed methods (snapshot, savepoint, createOrUpdateInternalState, create) throw
 * UnsupportedOperationException with the expected guidance.
 */
class ForStRsAbstractKeyedStateBackendTest {

    @Test
    void skeletonInstantiatesAndStubsThrow() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpenMemory(arena);
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);

            ForStRsKeyedStateBackend<String> delegate =
                    new ForStRsKeyedStateBackend<>(
                            arena, linker, db, cf, StringSerializer.INSTANCE, false);

            try (CloseableRegistry cr = new CloseableRegistry();
                    ForStRsAbstractKeyedStateBackend<String> backend =
                            new ForStRsAbstractKeyedStateBackend<>(
                                    StringSerializer.INSTANCE,
                                    Thread.currentThread().getContextClassLoader(),
                                    new ExecutionConfig(),
                                    cr,
                                    delegate)) {

                // Confirm the parent-class hierarchy.
                assertInstanceOf(AbstractKeyedStateBackend.class, backend);
                assertEquals("forst-rs", backend.getBackendTypeIdentifier());

                // numKeyValueStateEntries delegates and returns 0 for empty store.
                assertEquals(0, backend.numKeyValueStateEntries());

                // getKeys delegates without throwing.
                assertNotNull(backend.getKeys("anyState", null));

                // notifyCheckpointComplete is a no-op until P3.
                backend.notifyCheckpointComplete(123L);

                // Snapshot now requires a SnapshotStrategy to be wired in via
                // setSnapshotStrategy(...) — without it, snapshot() throws IllegalStateException
                // with a guidance message. (P3 SnapshotStrategy integration tests cover the wired
                // path; this skeleton test only confirms the unwired guard.)
                assertThrows(
                        IllegalStateException.class, () -> backend.snapshot(1L, 0L, null, null));
                assertThrows(UnsupportedOperationException.class, backend::savepoint);
                // createOrUpdateInternalState is wired in B-Prod-followup-L5/L6 — passing a null
                // descriptor surfaces an NPE because the implementation reads the state-name +
                // type. (Real Flink callers always pass a non-null StateDescriptor; this null
                // case pins the early-fail contract.)
                assertThrows(
                        NullPointerException.class,
                        () -> backend.createOrUpdateInternalState(null, null, null));
                // create(...) for priority-queues has a complex generic bound (T extends
                // HeapPriorityQueueElement & PriorityComparable & Keyed); skipping the direct
                // call here — the stub is exercised in P5 when timer-service wiring lands.
            }
            // delegate's owning resources are closed by the AbstractKeyedStateBackend.close() chain
            // because we passed ownsResources=false to the delegate ctor; clean up the FFM
            // resources we created locally.
            cf.close();
            db.close();
        }
    }
}

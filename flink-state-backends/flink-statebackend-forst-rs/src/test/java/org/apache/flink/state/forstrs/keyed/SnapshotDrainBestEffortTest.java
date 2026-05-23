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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.state.ForStRsAggregatingStateV2;
import org.apache.flink.state.forstrs.state.ForStRsReducingStateV2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R28-H2 regression test: per-state try/catch protection in the async snapshot drain loops
 * (PHASE 1.b/c/d/e in {@link ForStRsAsyncKeyedStateBackend#snapshot}).
 *
 * <p>Pre-fix: a throw from any state's drain method (e.g. {@code flushOnBarrier()}) propagated
 * out of the per-list loop, stranding every later state's pending buffer in the arena. The
 * resulting checkpoint silently truncated user data and the failure surface only showed the
 * first throw, hiding subsequent failures.
 *
 * <p>Post-fix: each loop wraps the per-state call in {@code try/catch Throwable}, warn-logs the
 * failure, captures the first failure as the root cause, and addSuppressed for the rest. After
 * every state has been attempted, the aggregated failure is rethrown so the snapshot strategy
 * still observes a failed pre-flush.
 *
 * <p>Test strategy: inject a throwing state subclass into the {@code registeredReducingStates}
 * and {@code registeredAggregatingStates} lists (the only register-public lists). Verify that:
 *
 * <ol>
 *   <li>Both states' {@code flushOnBarrier()} is invoked despite the first throwing.
 *   <li>{@code snapshot()} surfaces a RuntimeException whose cause chain references the drain
 *       failure (R28-H2 wraps as {@code RuntimeException("Snapshot pre-drain failed", first)}).
 * </ol>
 */
class SnapshotDrainBestEffortTest {

    private ForStRsAsyncKeyedStateBackend<Integer> openBackend(Path dbDir) {
        Arena arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        FrsDb db = linker.dbOpen(arena, dbDir.toString());
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);
        return new ForStRsAsyncKeyedStateBackend<>(
                arena,
                linker,
                db,
                cf,
                IntSerializer.INSTANCE,
                new KeyGroupRange(0, 0),
                /* totalKeyGroups= */ 1,
                /* ownsResources= */ true);
    }

    /**
     * R28-H2: both registered reducing-states must have their {@code flushOnBarrier()} invoked
     * EVEN WHEN the first one throws. Pre-fix, the second state's drain would be skipped.
     */
    @Test
    void drainContinuesAcrossThrowingStateAndSurfacesAggregatedFailure(@TempDir Path tmp)
            throws Exception {
        ForStRsAsyncKeyedStateBackend<Integer> backend = openBackend(tmp.resolve("db"));
        try {
            // Counters in array slots so the lambda can mutate them.
            AtomicInteger throwerCalls = new AtomicInteger();
            AtomicInteger laterCalls = new AtomicInteger();

            // Thrower goes first in the list so its throw exercises the per-state catch and
            // the loop continues to the second state. Implementation order is
            // insertion-order (ArrayList).
            ThrowingReducing thrower = new ThrowingReducing(throwerCalls);
            ThrowingReducing later = new ThrowingReducing(laterCalls, false /* dont throw */);
            injectReducingStates(backend, List.of(thrower, later));

            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            // R28-H2: snapshot must throw because pre-drain hit an error, but BOTH states must
            // have been visited (insertion-order drain with per-state try/catch).
            Throwable thrown =
                    assertThrows(
                            Throwable.class,
                            () -> {
                                RunnableFuture<SnapshotResult<?>> fut =
                                        (RunnableFuture) backend.snapshot(1L, 0L, factory, opts);
                                if (fut != null && !fut.isDone()) {
                                    fut.run();
                                    fut.get();
                                }
                            });
            assertNotNull(thrown, "snapshot must surface the drain failure");
            assertEquals(
                    1,
                    throwerCalls.get(),
                    "PHASE 1.d drain loop invokes the throwing state's flushOnBarrier exactly once");
            assertEquals(
                    1,
                    laterCalls.get(),
                    "PHASE 1.d drain loop continues to the second state after the first throws"
                            + " (R28-H2: per-state try/catch)");

            // R28-H2 wraps the aggregated drain failure in a RuntimeException whose cause chain
            // references the first throw. Walk the chain instead of asserting the exact wrapper
            // type — exception wrapping by the snapshot-strategy runner may add extra layers.
            boolean foundDrainCause = false;
            Throwable cur = thrown;
            for (int hop = 0; hop < 16 && cur != null; hop++) {
                if (cur.getMessage() != null
                        && cur.getMessage().contains("drain failed")) {
                    foundDrainCause = true;
                    break;
                }
                if (cur instanceof DrainTestException) {
                    foundDrainCause = true;
                    break;
                }
                cur = cur.getCause();
            }
            assertTrue(
                    foundDrainCause,
                    "snapshot failure cause chain must reference the drain failure (got: "
                            + thrown
                            + ")");
        } finally {
            try {
                backend.close();
            } catch (Throwable ignored) {
                // Best-effort: the test's reflective injection bypasses the registry's
                // construction invariants; close may surface secondary failures we don't care
                // about here.
            }
        }
    }

    /** Replaces the {@code registeredReducingStates} list contents via reflection. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void injectReducingStates(
            ForStRsAsyncKeyedStateBackend<?> backend,
            List<? extends ForStRsReducingStateV2<?>> states)
            throws Exception {
        Field f =
                ForStRsAsyncKeyedStateBackend.class.getDeclaredField("registeredReducingStates");
        f.setAccessible(true);
        List rawList = (List) f.get(backend);
        rawList.clear();
        rawList.addAll(states);
    }

    /** Sentinel exception type — exposed so the test can detect it through unwrapping. */
    private static final class DrainTestException extends RuntimeException {
        DrainTestException(String msg) {
            super(msg);
        }
    }

    /**
     * Minimal stand-in for {@link ForStRsReducingStateV2} that records {@code flushOnBarrier}
     * calls and optionally throws. Extends the real class so the registered* list's type
     * parameter is satisfied; only the methods invoked by the drain loop are overridden.
     */
    private static final class ThrowingReducing extends ForStRsReducingStateV2<Integer> {
        private final AtomicInteger counter;
        private final boolean shouldThrow;

        ThrowingReducing(AtomicInteger counter) {
            this(counter, true);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        ThrowingReducing(AtomicInteger counter, boolean shouldThrow) {
            // 5-arg ctor: (stateName, valueSerializer, reduceFn, classifier, slotScope).
            // The drain test only invokes flushOnBarrier (overridden below), so the nulls
            // never reach the real implementation paths.
            super(
                    /* stateName */ "drain-test",
                    /* valueSerializer */ null,
                    /* reduceFn */ null,
                    /* classifier */ null,
                    /* slotScope */ null);
            this.counter = counter;
            this.shouldThrow = shouldThrow;
        }

        @Override
        public void flushOnBarrier() {
            counter.incrementAndGet();
            if (shouldThrow) {
                throw new DrainTestException("drain failed for state 'drain-test' (synthetic)");
            }
        }
    }
}

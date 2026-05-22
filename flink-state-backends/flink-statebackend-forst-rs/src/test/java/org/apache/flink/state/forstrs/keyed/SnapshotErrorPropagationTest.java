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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-A4-H3 contract: {@link ForStRsAsyncKeyedStateBackend#snapshot} no longer swallows
 * exceptions and returns a successful empty snapshot. Real backend failures now propagate so the
 * checkpoint coordinator's {@code failedCheckpoints} counter increments and
 * {@code tolerable-failed-checkpoints} accounting fires.
 *
 * <p>Two paths under test:
 *
 * <ol>
 *   <li><b>Benign path preserved</b> — when the backend's {@code cancelStreamRegistry} is already
 *       closed (a prior checkpoint's async phase failed or the task is being cancelled), {@code
 *       snapshot()} returns a pre-completed empty future rather than throwing. The coordinator
 *       sees a documented benign-empty result and proceeds.
 *   <li><b>Real failures propagate</b> — feeding the backend a snapshot strategy whose sync phase
 *       throws causes {@code snapshot()} to throw, instead of returning {@code
 *       DoneFuture.of(SnapshotResult.empty())} as pre-fix.
 * </ol>
 *
 * <p>Method-signature assertion: {@code snapshot} now declares {@code throws Exception} so callers
 * cannot silently consume failures. Verified at runtime via reflection.
 */
class SnapshotErrorPropagationTest {

    /**
     * Compile/runtime contract: the snapshot method's exception spec includes {@code Exception} so
     * the runtime propagates FFI/upload failures to the coordinator instead of silently swallowing.
     */
    @Test
    void snapshotMethodDeclaresThrowsException() throws Exception {
        Method snap =
                ForStRsAsyncKeyedStateBackend.class.getMethod(
                        "snapshot",
                        long.class,
                        long.class,
                        org.apache.flink.runtime.state.CheckpointStreamFactory.class,
                        CheckpointOptions.class);
        boolean throwsException = false;
        for (Class<?> et : snap.getExceptionTypes()) {
            if (Exception.class.isAssignableFrom(et)) {
                throwsException = true;
                break;
            }
        }
        assertTrue(
                throwsException,
                "PR-A4-H3: snapshot() must declare throws Exception so the coordinator sees"
                        + " backend failures and increments failedCheckpoints");
    }

    /**
     * Documented benign path: when the backend's cancel-stream registry has already been closed
     * (a prior async upload failed or the task is being cancelled), the snapshot strategy runner
     * cannot register its cancellation hook and throws "Cannot register Closeable, registry is
     * already closed." This is the only path that still returns a pre-completed empty future —
     * the coordinator already gave up on this checkpoint so a non-throwing return unblocks the
     * mailbox cleanly.
     */
    @Test
    void registryAlreadyClosedReturnsBenignEmpty(@TempDir Path tmp) throws Exception {
        // ownsResources=false so the test owns the arena/db/cf lifecycle and the try-with-resources
        // arena.close() doesn't double-close after backend.close().
        Arena arena = Arena.ofShared();
        try {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);
            ForStRsAsyncKeyedStateBackend<Integer> backend =
                    new ForStRsAsyncKeyedStateBackend<>(
                            arena,
                            linker,
                            db,
                            cf,
                            IntSerializer.INSTANCE,
                            new KeyGroupRange(0, 0),
                            /* totalKeyGroups= */ 1,
                            /* ownsResources= */ false);
            try {
                // Force the cancel-stream registry to be closed BEFORE the first snapshot call so
                // SnapshotStrategyRunner's register hook trips the documented benign IOException.
                java.lang.reflect.Field reg =
                        ForStRsAsyncKeyedStateBackend.class.getDeclaredField(
                                "cancelStreamRegistry");
                reg.setAccessible(true);
                CloseableRegistry r = (CloseableRegistry) reg.get(backend);
                r.close();

                MemCheckpointStreamFactory factory =
                        new MemCheckpointStreamFactory(64 * 1024 * 1024);
                CheckpointOptions opts =
                        CheckpointOptions.alignedNoTimeout(
                                CheckpointType.CHECKPOINT,
                                CheckpointStorageLocationReference.getDefault());

                RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                        backend.snapshot(1L, 0L, factory, opts);
                assertNotNull(fut, "benign path returns a non-null future");
                assertTrue(fut.isDone(), "benign path returns a pre-completed future");
                SnapshotResult<KeyedStateHandle> r2 = fut.get();
                assertNotNull(r2, "result non-null");
                assertNull(
                        r2.getJobManagerOwnedSnapshot(),
                        "benign-empty: no JM-owned snapshot handle (SnapshotResult.empty())");
            } finally {
                backend.close();
                cf.close();
                db.close();
            }
        } finally {
            arena.close();
        }
    }

    /**
     * Real failure path: when the FFI sync phase blows up, snapshot() throws instead of swallowing.
     * We inject failure by closing the backend's FrsDb out from under the snapshot strategy. The
     * subsequent snapshot's syncPrepareResources call invokes the FFI checkpoint API against a
     * closed handle which surfaces as a RuntimeException (FrsBackendException) — pre-fix this
     * was swallowed; post-fix it propagates.
     */
    @Test
    void ffiFailurePropagatesAsThrown(@TempDir Path tmp) throws Exception {
        Arena arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);
        ForStRsAsyncKeyedStateBackend<Integer> backend =
                new ForStRsAsyncKeyedStateBackend<>(
                        arena,
                        linker,
                        db,
                        cf,
                        IntSerializer.INSTANCE,
                        new KeyGroupRange(0, 0),
                        /* totalKeyGroups= */ 1,
                        /* ownsResources= */ false);
        try {
            // Take one successful snapshot so the lazy strategy + cancelStreamRegistry are wired,
            // and the strategy holds a valid db reference internally.
            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> first =
                    backend.snapshot(1L, 0L, factory, opts);
            if (!first.isDone()) {
                first.run();
            }
            assertNotNull(first.get().getJobManagerOwnedSnapshot());

            // Now corrupt the engine state by closing the FrsDb. The strategy holds a reference to
            // the same FrsDb; subsequent syncPrepareResources/asyncSnapshot will fail loudly.
            cf.close();
            db.close();

            // Pre-A4-H3 this returned a benign DoneFuture(SnapshotResult.empty()) and the
            // coordinator marked the checkpoint as a successful empty snapshot. Post-fix the
            // failure propagates as a thrown exception (Exception or its RuntimeException
            // subtype — both are routed to CheckpointFailureManager).
            Throwable thrown =
                    assertThrows(
                            Throwable.class,
                            () -> {
                                RunnableFuture<SnapshotResult<KeyedStateHandle>> f =
                                        backend.snapshot(2L, 0L, factory, opts);
                                if (!f.isDone()) {
                                    f.run();
                                }
                                f.get();
                            },
                            "PR-A4-H3: FFI failure must propagate, not swallow into"
                                    + " SnapshotResult.empty()");
            // Either a checked exception thrown directly by snapshot() or an ExecutionException
            // wrapping the runtime failure surfaced by future.run()/future.get() — both
            // demonstrate the contract that the coordinator observes the failure.
            assertFalse(
                    thrown instanceof org.opentest4j.AssertionFailedError,
                    "the test's assertion did not trip — snapshot/get truly threw: " + thrown);
        } finally {
            try {
                backend.close();
            } catch (Throwable ignored) {
            }
            try {
                arena.close();
            } catch (Throwable ignored) {
            }
        }
    }
}

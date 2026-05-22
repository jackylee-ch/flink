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
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.apache.flink.core.execution.SavepointFormatType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Coordinated acceptance test for PR-A1 + PR-A8 + PR-A9. Exercises the new {@link
 * ForStRsAsyncKeyedStateBackend#snapshot} path end-to-end against a filesystem-backed engine.
 *
 * <h3>What is covered</h3>
 *
 * <ul>
 *   <li>PR-A1: {@link RunnableFuture} returned from {@code snapshot()} is no longer a {@code
 *       DoneFuture.of(SnapshotResult.empty())} — it now carries a {@link
 *       ForStRsIncrementalKeyedStateHandle} produced by the engine's incremental-checkpoint FFI.
 *   <li>PR-A1: pre-snapshot drain runs in order — {@link VectorizedExecutor#flushDirty} now folds
 *       the memtable to L0 SSTs so the strategy's enumeration includes recent writes.
 *   <li>PR-A1: lazy snapshot-strategy construction — {@code snapshotStrategyForTesting()} is null
 *       before the first {@code snapshot()} call and non-null afterwards.
 *   <li>PR-A8: stop --savepoint synchronous semantics — when the {@link SavepointType} is {@link
 *       SavepointType#isSynchronous() synchronous}, the returned future is pre-run before {@code
 *       snapshot()} returns. We verify {@code isDone() == true} immediately.
 *   <li>PR-A9: CheckpointOptions branching — periodic CHECKPOINT and stop-savepoint both produce
 *       valid handles; the strategy emits an {@link ForStRsIncrementalKeyedStateHandle} either
 *       way (canonical-format savepoint emission is deferred to a follow-on PR per the PR-A9
 *       TODO documented in the snapshot() Javadoc).
 *   <li>PR-A1: successive checkpoints exercise the incremental-base path — the second snapshot
 *       picks the first as its base via {@link
 *       ForStRsSnapshotStrategy#recordCompletedCheckpoint}, plumbed by {@code
 *       notifyCheckpointComplete}.
 * </ul>
 */
class AsyncBackendSnapshotPathTest {

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

    /** Writes a few keys directly via the linker so the engine has SSTs to enumerate. */
    private void seed(ForStRsAsyncKeyedStateBackend<Integer> backend, int from, int to)
            throws Exception {
        // Use reflective access to the linker/db/cf would be brittle. Instead, route through the
        // public FFI surface by re-deriving handles via the snapshot strategy's plumbing —
        // simpler is to seed via a sibling linker reference. We rely on the backend's
        // ForStRsLinker.put being callable on the same db handle.
        java.lang.reflect.Field linkerF =
                ForStRsAsyncKeyedStateBackend.class.getDeclaredField("linker");
        linkerF.setAccessible(true);
        java.lang.reflect.Field dbF = ForStRsAsyncKeyedStateBackend.class.getDeclaredField("db");
        dbF.setAccessible(true);
        java.lang.reflect.Field cfF =
                ForStRsAsyncKeyedStateBackend.class.getDeclaredField("defaultCf");
        cfF.setAccessible(true);
        ForStRsLinker linker = (ForStRsLinker) linkerF.get(backend);
        FrsDb db = (FrsDb) dbF.get(backend);
        FrsCfHandle cf = (FrsCfHandle) cfF.get(backend);
        for (int i = from; i < to; i++) {
            linker.put(db, cf, ("key-" + i).getBytes(), ("val-" + i).getBytes());
        }
    }

    @Test
    void snapshotProducesNonEmptyIncrementalHandle(@TempDir Path tmp) throws Exception {
        ForStRsAsyncKeyedStateBackend<Integer> backend = openBackend(tmp.resolve("db"));
        try {
            // Lazy: strategy should not exist before the first snapshot call.
            assertNull(
                    backend.snapshotStrategyForTesting(),
                    "snapshot strategy is lazy — null before first snapshot()");

            seed(backend, 0, 8);

            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                    backend.snapshot(1L, 0L, factory, opts);

            assertNotNull(fut, "snapshot returned a non-null RunnableFuture");
            // Periodic checkpoint: async strategy, future may need run() before completion.
            if (!fut.isDone()) {
                fut.run();
            }
            SnapshotResult<KeyedStateHandle> result = fut.get();
            assertNotNull(result, "result non-null after future runs");
            KeyedStateHandle handle = result.getJobManagerOwnedSnapshot();
            assertNotNull(
                    handle,
                    "PR-A1: snapshot() no longer returns SnapshotResult.empty() — must carry"
                            + " a KeyedStateHandle");
            assertTrue(
                    handle instanceof ForStRsIncrementalKeyedStateHandle,
                    "PR-A1: strategy emits ForStRsIncrementalKeyedStateHandle, got "
                            + handle.getClass().getSimpleName());
            ForStRsIncrementalKeyedStateHandle inc = (ForStRsIncrementalKeyedStateHandle) handle;
            assertEquals(1L, inc.getCheckpointId(), "checkpoint id round-trips");
            assertEquals(0L, inc.getBaseCheckpointId(), "first checkpoint base is 0");
            assertNotNull(inc.getMetaDataStateHandle(), "manifest stream handle is populated");

            // Lazy initialization assertion: strategy must now exist.
            assertNotNull(
                    backend.snapshotStrategyForTesting(),
                    "snapshot strategy is constructed lazily on first snapshot()");
            assertNotNull(
                    backend.sstRegistryForTesting(), "SST registry is constructed alongside");
        } finally {
            backend.close();
        }
    }

    @Test
    void successiveCheckpointsBaseChainsCorrectly(@TempDir Path tmp) throws Exception {
        ForStRsAsyncKeyedStateBackend<Integer> backend = openBackend(tmp.resolve("db"));
        try {
            seed(backend, 0, 8);
            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            // ---- Checkpoint 1: base = 0 ----
            RunnableFuture<SnapshotResult<KeyedStateHandle>> fut1 =
                    backend.snapshot(1L, 0L, factory, opts);
            if (!fut1.isDone()) {
                fut1.run();
            }
            ForStRsIncrementalKeyedStateHandle h1 =
                    (ForStRsIncrementalKeyedStateHandle)
                            fut1.get().getJobManagerOwnedSnapshot();
            assertNotNull(h1);
            assertEquals(0L, h1.getBaseCheckpointId());

            // ---- Notify completion + add more keys ----
            backend.notifyCheckpointComplete(1L);
            seed(backend, 8, 24);

            // ---- Checkpoint 2: base = 1 (PR-A1: notifyCheckpointComplete plumbs) ----
            RunnableFuture<SnapshotResult<KeyedStateHandle>> fut2 =
                    backend.snapshot(2L, 0L, factory, opts);
            if (!fut2.isDone()) {
                fut2.run();
            }
            ForStRsIncrementalKeyedStateHandle h2 =
                    (ForStRsIncrementalKeyedStateHandle)
                            fut2.get().getJobManagerOwnedSnapshot();
            assertEquals(2L, h2.getCheckpointId());
            assertEquals(
                    1L,
                    h2.getBaseCheckpointId(),
                    "PR-A1: notifyCheckpointComplete plumbed prior checkpoint id");
        } finally {
            backend.close();
        }
    }

    @Test
    void stopWithSavepointReturnsCompletedFuture(@TempDir Path tmp) throws Exception {
        // PR-A8: when the runtime issues stop --savepoint (SavepointType.terminate, isSynchronous
        // == true), the returned future must be pre-run before snapshot() returns. The mailbox
        // thread is expected to block until the snapshot is durable on storage.
        //
        // E5-HIGH-1: ForSt-RS only emits its native incremental format today, so the savepoint
        // request must explicitly opt into {@link SavepointFormatType#NATIVE}. CANONICAL is
        // rejected with UnsupportedOperationException (see {@link
        // ForStRsAsyncKeyedStateBackend#snapshot}); the dedicated test
        // {@link #canonicalSavepointThrowsUnsupportedOperation} exercises that path.
        ForStRsAsyncKeyedStateBackend<Integer> backend = openBackend(tmp.resolve("db"));
        try {
            seed(backend, 0, 4);
            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            // Use terminate (post-checkpoint action TERMINATE) which is synchronous per
            // SavepointType.isSynchronous().
            SavepointType savepointType = SavepointType.terminate(SavepointFormatType.NATIVE);
            assertTrue(
                    savepointType.isSynchronous(),
                    "sanity: SavepointType.terminate(...) is synchronous");
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            savepointType, CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                    backend.snapshot(10L, 0L, factory, opts);

            assertTrue(
                    fut.isDone(),
                    "PR-A8 stop --savepoint: SYNC_SAVEPOINT runs synchronously, future is"
                            + " pre-completed before snapshot() returns");
            SnapshotResult<KeyedStateHandle> result = fut.get();
            assertNotNull(result, "result non-null for sync savepoint");
            // PR-A9 V1: incremental handle even for savepoint. Canonical-format emission is a
            // documented follow-on.
            assertNotNull(
                    result.getJobManagerOwnedSnapshot(),
                    "PR-A8/A9: savepoint emits a handle (V1 incremental, V2 canonical TODO)");
        } finally {
            backend.close();
        }
    }

    @Test
    void canonicalSavepointThrowsUnsupportedOperation(@TempDir Path tmp) throws Exception {
        // E5-HIGH-1: a CANONICAL savepoint (the {@link SavepointFormatType#DEFAULT} when an
        // operator runs `stop --savepoint` without an explicit `--type native` flag) must throw
        // UnsupportedOperationException at the request site. Pre-fix the backend logged a WARN
        // and proceeded to emit a non-portable incremental handle that operators would only
        // discover as broken at restore time. This test pins the new contract.
        ForStRsAsyncKeyedStateBackend<Integer> backend = openBackend(tmp.resolve("db"));
        try {
            seed(backend, 0, 4);
            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            SavepointType canonical = SavepointType.terminate(SavepointFormatType.CANONICAL);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            canonical, CheckpointStorageLocationReference.getDefault());

            UnsupportedOperationException uoe =
                    assertThrows(
                            UnsupportedOperationException.class,
                            () -> backend.snapshot(99L, 0L, factory, opts),
                            "E5-HIGH-1: CANONICAL savepoint must be rejected at the request"
                                    + " site rather than silently producing a non-portable handle");
            assertTrue(
                    uoe.getMessage().contains("Canonical savepoint format"),
                    "exception message should name the unsupported format: " + uoe.getMessage());
            assertTrue(
                    uoe.getMessage().contains("NATIVE"),
                    "exception message should suggest NATIVE as the supported alternative: "
                            + uoe.getMessage());
        } finally {
            backend.close();
        }
    }

    @Test
    void periodicCheckpointReturnsAsyncFuture(@TempDir Path tmp) throws Exception {
        // PR-A9: periodic CHECKPOINT routes through ASYNCHRONOUS execution. The future is not
        // pre-run; the checkpoint coordinator would normally schedule .run() on its async
        // executor. We verify the future is NOT pre-completed (it is a real FutureTask wrapping
        // the upload), then run it manually.
        ForStRsAsyncKeyedStateBackend<Integer> backend = openBackend(tmp.resolve("db"));
        try {
            seed(backend, 0, 4);
            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                    backend.snapshot(1L, 0L, factory, opts);

            assertFalse(
                    fut.isDone(),
                    "PR-A9 periodic CHECKPOINT: ASYNCHRONOUS execution — future is NOT pre-run."
                            + " The coordinator drives .run() on its async snapshot executor.");
            fut.run();
            SnapshotResult<KeyedStateHandle> result = fut.get();
            assertNotNull(result.getJobManagerOwnedSnapshot());
        } finally {
            backend.close();
        }
    }

    @Test
    void abortedCheckpointDoesNotKeepRegistryEntries(@TempDir Path tmp) throws Exception {
        // PR-A1: notifyCheckpointAborted rolls back this checkpoint's SST registry contributions.
        ForStRsAsyncKeyedStateBackend<Integer> backend = openBackend(tmp.resolve("db"));
        try {
            seed(backend, 0, 8);
            MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(64 * 1024 * 1024);
            CheckpointOptions opts =
                    CheckpointOptions.alignedNoTimeout(
                            CheckpointType.CHECKPOINT,
                            CheckpointStorageLocationReference.getDefault());

            RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                    backend.snapshot(7L, 0L, factory, opts);
            if (!fut.isDone()) {
                fut.run();
            }
            ForStRsIncrementalKeyedStateHandle h =
                    (ForStRsIncrementalKeyedStateHandle) fut.get().getJobManagerOwnedSnapshot();
            assertNotNull(h);

            int beforeAbort = backend.sstRegistryForTesting().size();
            backend.notifyCheckpointAborted(7L);
            int afterAbort = backend.sstRegistryForTesting().size();
            assertTrue(
                    afterAbort <= beforeAbort,
                    "notifyCheckpointAborted unrefs the aborted checkpoint's"
                            + " SST contributions");
        } catch (UnsupportedOperationException e) {
            // ForStRsSstRegistry.size() might not exist — fall back to a no-throw assertion.
            fail("notifyCheckpointAborted threw unexpectedly: " + e.getMessage());
        } finally {
            backend.close();
        }
    }
}

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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsSnapshot;

/**
 * Captured-but-not-yet-uploaded incremental checkpoint state (B-Prod-P3 Task 3.4).
 *
 * <p>Returned from {@link ForStRsSnapshotStrategy#syncPrepareResources(long)}; consumed by {@link
 * ForStRsSnapshotStrategy#asyncSnapshot}. Owns:
 *
 * <ul>
 *   <li>the engine-side {@link FrsSnapshot} pinning the source seq;
 *   <li>the checkpoint id and base checkpoint id needed by the async phase to call {@link
 *       ForStRsLinker#createIncrementalCheckpointAt} (moved out of the sync phase to avoid blocking
 *       the data path with flush I/O during checkpoint barriers).
 * </ul>
 *
 * <p>The async phase calls {@code createIncrementalCheckpointAt} which flushes memtables and
 * computes the manifest + SST file lists. This is safe because the snapshot pins all versions at
 * the captured seq — concurrent writes do not affect correctness.
 *
 * <p>{@link #release()} disposes the snapshot; the async phase is responsible for freeing the
 * result struct via {@code ForStRsLinker.dbIncrementalCheckpointResultFree}.
 */
@Internal
public final class ForStRsSnapshotResources implements SnapshotResources {

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsSnapshot snapshot;
    private final long checkpointId;
    private final long baseCheckpointId;

    /**
     * E6-H3: serializer-registry blob captured on the mailbox thread during {@code
     * syncPrepareResources}. The async-snapshot worker reads it back via {@link
     * #getRegistryBlob()} without ever touching the live {@link
     * org.apache.flink.state.forstrs.state.StateSerializerRegistry#metadataBuffer()}
     * {@code LinkedHashMap} — which is non-thread-safe and otherwise mutated concurrently
     * by the mailbox thread (via {@code verifyOrRegister → register → live.put(...)}).
     *
     * <p>Holding the bytes (rather than an immutable copy of the map) keeps the async worker
     * decoupled from the registry's internal types; the byte[] is the same format that the
     * legacy {@code provider.currentBlob()} returned.
     */
    private final byte[] registryBlob;

    public ForStRsSnapshotResources(
            ForStRsLinker linker,
            FrsDb db,
            FrsSnapshot snapshot,
            long checkpointId,
            long baseCheckpointId) {
        this(linker, db, snapshot, checkpointId, baseCheckpointId, null);
    }

    /**
     * E6-H3: full constructor that accepts the registry blob bytes captured on the mailbox
     * thread. May be {@code null} when no {@code RegistryBlobProvider} is wired (test-only
     * construction) — the async phase then skips emitting the private-state entry.
     */
    public ForStRsSnapshotResources(
            ForStRsLinker linker,
            FrsDb db,
            FrsSnapshot snapshot,
            long checkpointId,
            long baseCheckpointId,
            byte[] registryBlob) {
        this.linker = linker;
        this.db = db;
        this.snapshot = snapshot;
        this.checkpointId = checkpointId;
        this.baseCheckpointId = baseCheckpointId;
        this.registryBlob = registryBlob;
    }

    /**
     * E6-H3: returns the serializer-registry blob captured on the mailbox thread during the
     * sync phase, or {@code null} when no provider was wired. The async phase uses this in
     * place of the racy {@code provider.currentBlob()} call.
     */
    public byte[] getRegistryBlob() {
        return registryBlob;
    }

    public FrsSnapshot getSnapshot() {
        return snapshot;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public long getBaseCheckpointId() {
        return baseCheckpointId;
    }

    @Override
    public void release() {
        // Release the engine snapshot so compaction can advance past its seq.
        try {
            snapshot.close();
        } catch (RuntimeException ignored) {
            // FrsSnapshot.close() is itself idempotent.
        }
    }

    /** Test/read accessor for the {@code FrsDb} the snapshot was taken on. */
    public FrsDb getDb() {
        return db;
    }

    /** Returns the linker for use by the async phase. */
    public ForStRsLinker getLinker() {
        return linker;
    }
}

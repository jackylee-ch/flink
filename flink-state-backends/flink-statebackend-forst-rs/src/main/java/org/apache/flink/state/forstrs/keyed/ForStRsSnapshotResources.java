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

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.List;

/**
 * Captured-but-not-yet-uploaded incremental checkpoint state (B-Prod-P3 Task 3.4).
 *
 * <p>Returned from {@link ForStRsSnapshotStrategy#syncPrepareResources(long)}; consumed by {@link
 * ForStRsSnapshotStrategy#asyncSnapshot}. Owns:
 *
 * <ul>
 *   <li>the engine-side {@link FrsSnapshot} pinning the source seq;
 *   <li>the per-checkpoint result struct (Arena-allocated 32-byte buffer) that must be freed via
 *       {@link ForStRsLinker#dbIncrementalCheckpointResultFree(MemorySegment)} once async upload
 *       completes;
 *   <li>the manifest path (private state — uploaded once, owned by this checkpoint);
 *   <li>the lists of new + shared SST file paths the async phase must upload + register.
 * </ul>
 *
 * <p>{@link #release()} disposes the snapshot + frees the result struct; the Arena that backs the
 * struct is provided by the strategy and outlives this resource.
 */
@Internal
public final class ForStRsSnapshotResources implements SnapshotResources {

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsSnapshot snapshot;
    private final MemorySegment resultStruct;
    private final Path manifestPath;
    private final List<Path> newSstFiles;
    private final List<Path> sharedSstFiles;
    private final long checkpointId;
    private final long baseCheckpointId;

    public ForStRsSnapshotResources(
            ForStRsLinker linker,
            FrsDb db,
            FrsSnapshot snapshot,
            MemorySegment resultStruct,
            Path manifestPath,
            List<Path> newSstFiles,
            List<Path> sharedSstFiles,
            long checkpointId,
            long baseCheckpointId) {
        this.linker = linker;
        this.db = db;
        this.snapshot = snapshot;
        this.resultStruct = resultStruct;
        this.manifestPath = manifestPath;
        this.newSstFiles = newSstFiles;
        this.sharedSstFiles = sharedSstFiles;
        this.checkpointId = checkpointId;
        this.baseCheckpointId = baseCheckpointId;
    }

    public FrsSnapshot getSnapshot() {
        return snapshot;
    }

    public Path getManifestPath() {
        return manifestPath;
    }

    public List<Path> getNewSstFiles() {
        return newSstFiles;
    }

    public List<Path> getSharedSstFiles() {
        return sharedSstFiles;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public long getBaseCheckpointId() {
        return baseCheckpointId;
    }

    @Override
    public void release() {
        // Free the engine-side checkpoint-result allocations first (before releasing the snapshot —
        // the manifest C string lives in the result struct).
        try {
            linker.dbIncrementalCheckpointResultFree(resultStruct);
        } catch (RuntimeException ignored) {
            // Idempotent on the native side; swallow to honor SnapshotResources.release()'s
            // best-effort contract.
        }
        // Then release the engine snapshot so compaction can advance past its seq.
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

    /** Test/read accessor for the captured 32-byte result struct (Arena-allocated). */
    public MemorySegment getResultStruct() {
        return resultStruct;
    }
}

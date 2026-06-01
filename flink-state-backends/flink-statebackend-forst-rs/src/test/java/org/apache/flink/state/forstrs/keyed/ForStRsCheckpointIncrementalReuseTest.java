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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FRS-CKPT-INCREMENTAL verification (the FileMappingManager-equivalent claim): proves that the
 * incremental checkpoint path reference-links unchanged SST files across checkpoints instead of
 * re-uploading them, and reports the per-checkpoint upload-byte split.
 *
 * <p>Mechanism under test (see {@link ForStRsSnapshotStrategy#doAsyncSnapshot}): the engine's
 * {@code createIncrementalCheckpointAt} splits the live file set into NEW vs SHARED relative to the
 * base checkpoint; only NEW SSTs are uploaded, while SHARED SSTs are resolved from
 * {@link ForStRsSstRegistry} (the prior checkpoint's already-uploaded handle) and re-registered.
 *
 * <p>Empirical proof of "no re-upload": for every SST that appears in BOTH checkpoint 1 and
 * checkpoint 2 (same local path), checkpoint 2 must carry the IDENTICAL
 * {@code StreamStateHandle} — a fresh upload would mint a new handle id. Same id ⟺ referenced.
 */
class ForStRsCheckpointIncrementalReuseTest {

    @Test
    void unchangedSstsAreReferencedNotReuploadedAcrossCheckpoints(@TempDir Path tmp)
            throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                // Seed a non-trivial batch so the checkpoint flush produces real SST files.
                for (int i = 0; i < 2000; i++) {
                    linker.put(db, cf, ("k-" + i).getBytes(), ("v-" + i).getBytes());
                }

                ForStRsSstRegistry registry = new ForStRsSstRegistry();
                ForStRsSstUploader uploader = new ForStRsSstUploader();
                ForStRsSnapshotStrategy strategy =
                        new ForStRsSnapshotStrategy(
                                linker,
                                db,
                                UUID.randomUUID(),
                                new KeyGroupRange(0, 0),
                                registry,
                                uploader,
                                arena,
                                Map.of("default", 0L));
                MemCheckpointStreamFactory factory = new MemCheckpointStreamFactory(256 * 1024 * 1024);

                // ---- Checkpoint 1 (full; base = 0). ----
                ForStRsSnapshotResources res1 = strategy.syncPrepareResources(1L);
                SnapshotResult<?> r1 = strategy.asyncSnapshot(res1, 1L, 0L, factory, null)
                        .get(new CloseableRegistry());
                IncrementalRemoteKeyedStateHandle h1 =
                        (IncrementalRemoteKeyedStateHandle) r1.getJobManagerOwnedSnapshot();
                strategy.recordCompletedCheckpoint(1L);
                strategy.takePendingRegistrations(1L);

                // Map every ckpt-1 SST: localPath -> (handle id, bytes).
                Map<String, String> ckpt1IdByPath = new HashMap<>();
                Map<String, Long> ckpt1BytesByPath = new HashMap<>();
                long ckpt1TotalBytes = 0L;
                for (HandleAndLocalPath hlp : h1.getSharedState()) {
                    StreamStateHandle h = hlp.getHandle();
                    ckpt1IdByPath.put(hlp.getLocalPath(), h.getStreamStateHandleID().toString());
                    ckpt1BytesByPath.put(hlp.getLocalPath(), hlp.getStateSize());
                    ckpt1TotalBytes += hlp.getStateSize();
                }
                assertFalse(ckpt1IdByPath.isEmpty(), "ckpt 1 must produce at least one SST");

                // A small mutation, then checkpoint 2 based on ckpt 1.
                for (int i = 2000; i < 2016; i++) {
                    linker.put(db, cf, ("k-" + i).getBytes(), ("v-" + i).getBytes());
                }
                ForStRsSnapshotResources res2 = strategy.syncPrepareResources(2L);
                SnapshotResult<?> r2 = strategy.asyncSnapshot(res2, 2L, 0L, factory, null)
                        .get(new CloseableRegistry());
                IncrementalRemoteKeyedStateHandle h2 =
                        (IncrementalRemoteKeyedStateHandle) r2.getJobManagerOwnedSnapshot();

                // Classify ckpt-2 shared SSTs into referenced-from-ckpt1 vs newly-uploaded, and
                // assert every path shared with ckpt 1 carries the IDENTICAL handle (no re-upload).
                long referencedBytes = 0L;
                long newBytes = 0L;
                int referencedCount = 0;
                for (HandleAndLocalPath hlp : h2.getSharedState()) {
                    String prevId = ckpt1IdByPath.get(hlp.getLocalPath());
                    if (prevId != null) {
                        assertTrue(
                                prevId.equals(hlp.getHandle().getStreamStateHandleID().toString()),
                                "FRS-CKPT-INCREMENTAL: SST '"
                                        + hlp.getLocalPath()
                                        + "' present in both checkpoints must be REFERENCED (same"
                                        + " StreamStateHandle id), not re-uploaded. ckpt1="
                                        + prevId
                                        + " ckpt2="
                                        + hlp.getHandle().getStreamStateHandleID());
                        referencedBytes += hlp.getStateSize();
                        referencedCount++;
                    } else {
                        newBytes += hlp.getStateSize();
                    }
                }

                System.out.println(
                        "[FRS-CKPT-INCREMENTAL] ckpt1 total SST bytes="
                                + ckpt1TotalBytes
                                + " | ckpt2 referenced (NOT re-uploaded) bytes="
                                + referencedBytes
                                + " across "
                                + referencedCount
                                + " SSTs | ckpt2 new-upload bytes="
                                + newBytes);

                // The incremental property: ckpt 2 reuses ckpt 1's bytes by reference, and the
                // new upload is only the delta (well under what a full re-copy would be).
                assertTrue(referencedCount > 0,
                        "ckpt 2 must reference at least one unchanged SST from ckpt 1");
                assertTrue(referencedBytes > 0, "referenced (reused) bytes must be > 0");
                assertTrue(
                        newBytes < ckpt1TotalBytes,
                        "FRS-CKPT-INCREMENTAL: ckpt 2 new-upload bytes ("
                                + newBytes
                                + ") must be less than a full re-copy of ckpt 1 ("
                                + ckpt1TotalBytes
                                + ") — proves incremental, not full snapshot");
            }
        }
    }
}

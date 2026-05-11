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

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsSnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MVCC isolation IT (B-Prod-P3 Task 3.8).
 *
 * <p>Asserts that a snapshot taken at sequence S sees only writes whose seq &lt;= S, regardless of
 * concurrent writes happening on a different thread. This is the property the snapshot strategy
 * relies on for incremental checkpoint correctness — the captured manifest must describe a
 * consistent state, not a torn one.
 *
 * <p>Test plan:
 *
 * <ol>
 *   <li>Pre-load 1000 keys ({@code pre-N} → {@code pre-val-N}) and capture a snapshot {@code S}.
 *   <li>From a writer pool of 8 threads, write 100k keys ({@code post-N} → {@code post-val-N}) in
 *       parallel — each thread writes its slice of the keyspace.
 *   <li>Once writers complete, sample {@code getAt(S, ...)} for every {@code pre-N} key — they must
 *       all return their original values, no nulls and no surprise post-snapshot values.
 *   <li>Sample {@code getAt(S, ...)} for a handful of {@code post-N} keys — they must all return
 *       null (snapshot did not see the post-snapshot writes).
 * </ol>
 *
 * <p>Test uses 100k post-snapshot writes per spec; reduce to 10k under {@code -Dquick=true} to keep
 * CI under a minute.
 */
class ForStRsMVCCIsolationIT {

    @Test
    void snapshotIsolatedFromConcurrentWrites(@TempDir Path tmp) throws Exception {
        int preCount = 1_000;
        int postCount =
                Boolean.parseBoolean(System.getProperty("quick", "false")) ? 10_000 : 100_000;
        int writerThreads = 8;

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                // ---- Pre-load. ----
                for (int i = 0; i < preCount; i++) {
                    linker.put(db, cf, ("pre-" + i).getBytes(), ("pre-val-" + i).getBytes());
                }

                // ---- Capture the snapshot. ----
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                try {
                    // ---- Concurrent writers. ----
                    ExecutorService es = Executors.newFixedThreadPool(writerThreads);
                    CountDownLatch ready = new CountDownLatch(writerThreads);
                    CountDownLatch start = new CountDownLatch(1);
                    CountDownLatch done = new CountDownLatch(writerThreads);
                    AtomicInteger errs = new AtomicInteger();
                    int sliceSize = postCount / writerThreads;
                    for (int t = 0; t < writerThreads; t++) {
                        final int from = t * sliceSize;
                        final int to = (t + 1 == writerThreads) ? postCount : from + sliceSize;
                        es.submit(
                                () -> {
                                    try {
                                        ready.countDown();
                                        start.await();
                                        for (int i = from; i < to; i++) {
                                            linker.put(
                                                    db,
                                                    cf,
                                                    ("post-" + i).getBytes(),
                                                    ("post-val-" + i).getBytes());
                                        }
                                    } catch (Throwable t1) {
                                        errs.incrementAndGet();
                                    } finally {
                                        done.countDown();
                                    }
                                });
                    }
                    ready.await(10, TimeUnit.SECONDS);
                    start.countDown();
                    assertTrue(done.await(120, TimeUnit.SECONDS), "writers should finish in 120s");
                    es.shutdown();
                    assertEquals(0, errs.get(), "no writer should throw");

                    // ---- Verify snapshot isolation: every pre-* key visible at original value.
                    // ----
                    for (int i = 0; i < preCount; i++) {
                        byte[] got = linker.getAt(db, cf, snap, ("pre-" + i).getBytes());
                        assertTrue(got != null, "pre-" + i + " must be visible at the snapshot");
                        assertEquals("pre-val-" + i, new String(got));
                    }

                    // ---- Sample post-* keys: snapshot must NOT see them. ----
                    int[] sample = {0, 17, postCount / 4, postCount / 2, postCount - 1};
                    for (int i : sample) {
                        byte[] got = linker.getAt(db, cf, snap, ("post-" + i).getBytes());
                        assertNull(
                                got,
                                "post-" + i + " was written AFTER the snapshot, must be invisible");
                    }
                } finally {
                    snap.close();
                }
            }
        }
    }
}

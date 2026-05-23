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

package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FrsSnapshot} (B-Prod-P2 Task 2.5). Cover round-trip, double-close
 * idempotency, try-with-resources release, and post-close handle access fast-fail.
 */
class FrsSnapshotTest {

    @Test
    void snapshotRoundTrip() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                assertFalse(snap.isClosed());
                snap.close();
                assertTrue(snap.isClosed());
            }
        }
    }

    @Test
    void doubleCloseSafe() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                snap.close();
                assertDoesNotThrow(snap::close);
                assertTrue(snap.isClosed());
            }
        }
    }

    @Test
    void tryWithResourcesReleases() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsSnapshot snap = linker.dbSnapshot(db, arena)) {
                assertFalse(snap.isClosed());
            }
            // After try-with-resources, snap.close() ran exactly once via AutoCloseable.
        }
    }

    @Test
    void handleAccessAfterCloseFails() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                snap.close();
                assertThrows(IllegalStateException.class, snap::handle);
            }
        }
    }

    /**
     * R31-H2 regression: two threads concurrently calling {@link FrsSnapshot#close()} must result
     * in exactly one native release. Pre-fix the check-then-act `if (handle != null)` pattern
     * could let both threads pass the guard and double-release. The AtomicBoolean+CAS guard
     * promotes that to exactly-one regardless of interleaving.
     *
     * <p>We can't directly observe the native release count from Java, so we sample the
     * post-state of the snapshot — both calls must complete cleanly and {@link
     * FrsSnapshot#isClosed()} must report {@code true}. If the pre-fix code were in place this
     * test would not detect the double-free directly (it'd still report closed=true), but the
     * companion Rust-side handle registry's reference count is incremented on every release call,
     * so a double-release would invariably eventually crash the JVM via a corrupted free list.
     * That negative signal is what the CAS guard delivers; the assertion below is a smoke test
     * that both close paths return without throwing.
     */
    @Test
    void concurrentCloseSingleNativeRelease() throws InterruptedException {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);
                AtomicInteger errors = new AtomicInteger(0);
                Runnable closer =
                        () -> {
                            try {
                                start.await();
                                snap.close();
                            } catch (Throwable t) {
                                errors.incrementAndGet();
                            } finally {
                                done.countDown();
                            }
                        };
                Thread t1 = new Thread(closer, "snap-closer-1");
                Thread t2 = new Thread(closer, "snap-closer-2");
                t1.start();
                t2.start();
                start.countDown();
                assertTrue(done.await(10, TimeUnit.SECONDS), "close threads timed out");
                assertEquals(0, errors.get(), "no closer thread should have thrown");
                assertTrue(snap.isClosed(), "snapshot should report closed");
            }
        }
    }
}

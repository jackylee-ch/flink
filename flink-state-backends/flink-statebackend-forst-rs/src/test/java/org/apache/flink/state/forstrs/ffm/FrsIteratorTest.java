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
 * Unit tests for {@link FrsIterator} CAS double-close guard (R32-H3). Mirrors {@link
 * FrsSnapshotTest} — the same protection pattern must apply to every Java handle that wraps a
 * native pointer with a non-idempotent native close symbol.
 */
class FrsIteratorTest {

    @Test
    void iteratorRoundTrip() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                FrsIterator it = linker.iteratorOpen(db, cf, arena);
                assertFalse(it.isClosed());
                it.close();
                assertTrue(it.isClosed());
            }
        }
    }

    @Test
    void doubleCloseSafe() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                FrsIterator it = linker.iteratorOpen(db, cf, arena);
                it.close();
                assertDoesNotThrow(it::close);
                assertTrue(it.isClosed());
            }
        }
    }

    @Test
    void tryWithResourcesReleases() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                    FrsIterator it = linker.iteratorOpen(db, cf, arena)) {
                assertFalse(it.isClosed());
            }
            // After try-with-resources, it.close() ran exactly once via AutoCloseable.
        }
    }

    @Test
    void handleAccessAfterCloseFails() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                FrsIterator it = linker.iteratorOpen(db, cf, arena);
                it.close();
                assertThrows(IllegalStateException.class, it::handle);
            }
        }
    }

    /**
     * R32-H3 regression: two threads concurrently calling {@link FrsIterator#close()} must result
     * in exactly one native release. Pre-fix the check-then-act `if (!closed)` pattern could let
     * both threads pass the guard and double-free the native iterator. The AtomicBoolean+CAS
     * guard promotes that to exactly-one regardless of interleaving.
     *
     * <p>Mirrors {@code FrsSnapshotTest#concurrentCloseSingleNativeRelease}. Direct observation of
     * the native release count is unavailable from Java; the smoke signal is that both threads
     * complete cleanly and {@link FrsIterator#isClosed()} reports {@code true}. The CAS guard
     * delivers the deeper guarantee against double-free, which would otherwise eventually crash
     * the JVM via a corrupted free list.
     */
    @Test
    void concurrentCloseSingleNativeRelease() throws InterruptedException {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                FrsIterator it = linker.iteratorOpen(db, cf, arena);
                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch done = new CountDownLatch(2);
                AtomicInteger errors = new AtomicInteger(0);
                Runnable closer =
                        () -> {
                            try {
                                start.await();
                                it.close();
                            } catch (Throwable t) {
                                errors.incrementAndGet();
                            } finally {
                                done.countDown();
                            }
                        };
                Thread t1 = new Thread(closer, "iter-closer-1");
                Thread t2 = new Thread(closer, "iter-closer-2");
                t1.start();
                t2.start();
                start.countDown();
                assertTrue(done.await(10, TimeUnit.SECONDS), "close threads timed out");
                assertEquals(0, errors.get(), "no closer thread should have thrown");
                assertTrue(it.isClosed(), "iterator should report closed");
            }
        }
    }
}

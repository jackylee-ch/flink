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

package org.apache.flink.state.forstrs.keyed.sst;

import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link ForStRsSstRegistry} (B-Prod-P3 Task 3.1). 8 tests total. */
class ForStRsSstRegistryTest {

    private static StreamStateHandle handle(String name) {
        return new ByteStreamStateHandle(name, new byte[] {1, 2, 3});
    }

    /** Test 1: register-new — new id gets ref-count 1 and is in the registry. */
    @Test
    void registerNewCreatesEntry() {
        ForStRsSstRegistry r = new ForStRsSstRegistry();
        StateHandleID id = new StateHandleID("sst-1");
        StreamStateHandle h = handle("sst-1");
        assertTrue(r.register(id, h), "first register should create a new entry");
        assertTrue(r.contains(id));
        assertEquals(1, r.refCount(id));
        assertEquals(1, r.size());
    }

    /** Test 2: register-twice-increments-ref — second register bumps ref to 2 without replace. */
    @Test
    void registerTwiceIncrementsRefCount() {
        ForStRsSstRegistry r = new ForStRsSstRegistry();
        StateHandleID id = new StateHandleID("sst-2");
        StreamStateHandle h1 = handle("h1");
        StreamStateHandle h2 = handle("h2");
        r.register(id, h1);
        assertFalse(r.register(id, h2), "second register should NOT create a new entry");
        assertEquals(2, r.refCount(id));
        // The first handle wins (SST bytes are immutable; later handles describe the same bytes).
        Optional<StreamStateHandle> got = r.get(id);
        assertTrue(got.isPresent());
        assertSame(h1, got.get(), "first handle is preserved across re-registrations");
    }

    /** Test 3: unregister-decrements — single unregister on ref=2 drops to 1 and keeps entry. */
    @Test
    void unregisterDecrementsRefCount() {
        ForStRsSstRegistry r = new ForStRsSstRegistry();
        StateHandleID id = new StateHandleID("sst-3");
        r.register(id, handle("h"));
        r.register(id, handle("h"));
        assertEquals(2, r.refCount(id));
        assertFalse(r.unregister(id), "unregister with refs remaining should NOT evict");
        assertEquals(1, r.refCount(id));
        assertTrue(r.contains(id));
    }

    /** Test 4: unregister-to-zero-removes — last unregister evicts the entry. */
    @Test
    void unregisterToZeroEvicts() {
        ForStRsSstRegistry r = new ForStRsSstRegistry();
        StateHandleID id = new StateHandleID("sst-4");
        r.register(id, handle("h"));
        assertTrue(r.unregister(id), "final unregister should evict");
        assertFalse(r.contains(id));
        assertEquals(0, r.refCount(id));
        assertEquals(0, r.size());
    }

    /** Test 5: get-on-missing-returns-empty — Optional.empty() for unknown ids. */
    @Test
    void getOnMissingReturnsEmpty() {
        ForStRsSstRegistry r = new ForStRsSstRegistry();
        Optional<StreamStateHandle> got = r.get(new StateHandleID("missing"));
        assertFalse(got.isPresent());
    }

    /** Test 6: size-tracking — size reflects unique registered ids, not total ref-counts. */
    @Test
    void sizeTracksUniqueIds() {
        ForStRsSstRegistry r = new ForStRsSstRegistry();
        StateHandleID id1 = new StateHandleID("a");
        StateHandleID id2 = new StateHandleID("b");
        r.register(id1, handle("a"));
        r.register(id1, handle("a")); // ref=2, still 1 distinct id
        r.register(id2, handle("b"));
        assertEquals(2, r.size());
        r.unregister(id1); // ref=1, still in
        assertEquals(2, r.size());
        r.unregister(id1); // ref=0, evicted
        assertEquals(1, r.size());
    }

    /** Test 7: unregister-on-missing returns false and does not throw. */
    @Test
    void unregisterOnMissingIsNoOp() {
        ForStRsSstRegistry r = new ForStRsSstRegistry();
        assertFalse(r.unregister(new StateHandleID("nope")));
        assertEquals(0, r.size());
    }

    /**
     * Test 8: concurrent register/unregister — 16 threads each register the same id N times then
     * unregister N times. Final size + ref-count must both be zero (every register exactly paired
     * with one unregister).
     */
    @Test
    void concurrentRegisterUnregister() throws Exception {
        ForStRsSstRegistry r = new ForStRsSstRegistry();
        StateHandleID id = new StateHandleID("contended");
        StreamStateHandle h = handle("contended");
        int threads = 16;
        int iterationsPerThread = 500;
        ExecutorService es = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errs = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            es.submit(
                    () -> {
                        try {
                            start.await();
                            for (int i = 0; i < iterationsPerThread; i++) {
                                r.register(id, h);
                            }
                            for (int i = 0; i < iterationsPerThread; i++) {
                                r.unregister(id);
                            }
                        } catch (Throwable t1) {
                            errs.incrementAndGet();
                        } finally {
                            done.countDown();
                        }
                    });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "all threads should finish in 30s");
        es.shutdown();
        assertEquals(0, errs.get(), "no exceptions should escape");
        assertEquals(0, r.size(), "every register paired with one unregister");
        assertEquals(0, r.refCount(id));
    }
}

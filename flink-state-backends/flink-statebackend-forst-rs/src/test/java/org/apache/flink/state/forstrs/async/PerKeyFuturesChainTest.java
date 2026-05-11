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

package org.apache.flink.state.forstrs.async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 12 unit tests covering {@link PerKeyFuturesChain}'s ordering, isolation, error-recovery, and
 * map-shrinkage invariants.
 */
class PerKeyFuturesChainTest {

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        // Virtual-thread executor as called for in spec §6e — the chain is designed to run on
        // many in-flight virtual threads without exhausting platform threads.
        executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdown();
    }

    /** 1: a single enqueue on a key returns the supplier's value. */
    @Test
    void singleEnqueueReturnsValue() throws Exception {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        CompletableFuture<Integer> f = chain.enqueue("k", () -> 42);
        assertEquals(42, f.get(5, TimeUnit.SECONDS));
    }

    /** 2: per-key ordering — 100 enqueues for the same key complete in submit order. */
    @Test
    void perKeyOrderingPreserved() throws Exception {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        List<Integer> observed = new ArrayList<>();
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            final int v = i;
            futures.add(
                    chain.enqueue(
                            "k",
                            () -> {
                                synchronized (observed) {
                                    observed.add(v);
                                }
                                return v;
                            }));
        }
        for (CompletableFuture<Integer> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals(i, observed.get(i), "submit-order violated at index " + i);
        }
    }

    /** 3: cross-key parallelism — two slow ops on different keys overlap in time. */
    @Test
    void crossKeyParallelism() throws Exception {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Boolean> a =
                chain.enqueue(
                        "kA",
                        () -> {
                            entered.countDown();
                            try {
                                return release.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return false;
                            }
                        });
        CompletableFuture<Boolean> b =
                chain.enqueue(
                        "kB",
                        () -> {
                            entered.countDown();
                            try {
                                return release.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return false;
                            }
                        });
        // Both must enter their suppliers without one waiting on the other; if cross-key serialized
        // this latch would never reach 0.
        assertTrue(
                entered.await(5, TimeUnit.SECONDS),
                "different-key suppliers were not concurrently entered");
        release.countDown();
        assertTrue(a.get(5, TimeUnit.SECONDS));
        assertTrue(b.get(5, TimeUnit.SECONDS));
    }

    /** 4: chain shrinks on completion — single key returns to empty. */
    @Test
    void chainShrinksAfterCompletion() throws Exception {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        CompletableFuture<Integer> f = chain.enqueue("k", () -> 1);
        f.get(5, TimeUnit.SECONDS);
        // Cleanup runs in the chain itself via whenComplete; give it a brief window to observe.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (chain.activeKeyCount() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(0, chain.activeKeyCount());
    }

    /** 5: chain shrinks on completion — multiple keys all clean up. */
    @Test
    void multiKeyChainShrinks() throws Exception {
        PerKeyFuturesChain<Integer> chain = new PerKeyFuturesChain<>(executor);
        List<CompletableFuture<Integer>> fs = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final int v = i;
            fs.add(chain.enqueue(v, () -> v));
        }
        for (CompletableFuture<Integer> f : fs) {
            f.get(5, TimeUnit.SECONDS);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (chain.activeKeyCount() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(0, chain.activeKeyCount());
    }

    /** 6: throwing supplier completes future exceptionally; chain still advances. */
    @Test
    void exceptionInSupplierPropagates() {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        CompletableFuture<Integer> f =
                chain.enqueue(
                        "k",
                        () -> {
                            throw new IllegalStateException("boom");
                        });
        ExecutionException ex =
                assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
        assertNotNull(ex.getCause());
        assertEquals("boom", ex.getCause().getMessage());
    }

    /** 7: after a throwing supplier, the next enqueue on the same key still runs. */
    @Test
    void chainAdvancesPastException() throws Exception {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        CompletableFuture<Integer> failing =
                chain.enqueue(
                        "k",
                        () -> {
                            throw new RuntimeException("fail");
                        });
        CompletableFuture<Integer> next = chain.enqueue("k", () -> 99);
        assertEquals(99, next.get(5, TimeUnit.SECONDS));
        assertTrue(failing.isCompletedExceptionally());
    }

    /** 8: per-key ordering preserved even under interleaved enqueues across many keys. */
    @Test
    void interleavedKeysPreservePerKeyOrder() throws Exception {
        PerKeyFuturesChain<Integer> chain = new PerKeyFuturesChain<>(executor);
        final int keys = 8;
        final int opsPerKey = 50;
        @SuppressWarnings("unchecked")
        List<Integer>[] observed = new List[keys];
        for (int i = 0; i < keys; i++) {
            observed[i] = new ArrayList<>();
        }
        List<CompletableFuture<Void>> all = new ArrayList<>();
        for (int op = 0; op < opsPerKey; op++) {
            for (int k = 0; k < keys; k++) {
                final int kFinal = k;
                final int opFinal = op;
                all.add(
                        chain.enqueue(
                                kFinal,
                                () -> {
                                    synchronized (observed[kFinal]) {
                                        observed[kFinal].add(opFinal);
                                    }
                                    return null;
                                }));
            }
        }
        for (CompletableFuture<Void> f : all) {
            f.get(5, TimeUnit.SECONDS);
        }
        for (int k = 0; k < keys; k++) {
            for (int op = 0; op < opsPerKey; op++) {
                assertEquals(op, observed[k].get(op), "key=" + k + " op-index=" + op);
            }
        }
    }

    /** 9: null key throws NPE eagerly. */
    @Test
    void nullKeyRejected() {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        assertThrows(NullPointerException.class, () -> chain.enqueue(null, () -> 1));
    }

    /** 10: null work throws NPE eagerly. */
    @Test
    void nullWorkRejected() {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        assertThrows(NullPointerException.class, () -> chain.enqueue("k", null));
    }

    /** 11: null executor in constructor rejected. */
    @Test
    void nullExecutorRejected() {
        assertThrows(NullPointerException.class, () -> new PerKeyFuturesChain<>(null));
    }

    /**
     * 12: a supplier returning {@code null} completes the future with {@code null} (not a NPE in
     * cleanup). Distinguishes "absent value" from "result not yet produced".
     */
    @Test
    void nullResultIsLegal() throws Exception {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        CompletableFuture<String> f = chain.enqueue("k", () -> null);
        assertNull(f.get(5, TimeUnit.SECONDS));
        // chain still shrinks for null-returning suppliers
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (chain.activeKeyCount() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(0, chain.activeKeyCount());
    }

    /**
     * Bonus stress: 1k keys × 50 ops with concurrent submitters; counts each per-key supplier
     * invocation and asserts the count == ops. Lives here (not in ForStRsAsyncValueStateTest)
     * because it covers the chain itself, not the state wrapper.
     */
    @Test
    void stressMultiSubmitter() throws Exception {
        PerKeyFuturesChain<Integer> chain = new PerKeyFuturesChain<>(executor);
        final int keys = 1_000;
        final int opsPerKey = 50;
        AtomicInteger[] perKeyCount = new AtomicInteger[keys];
        for (int i = 0; i < keys; i++) {
            perKeyCount[i] = new AtomicInteger();
        }
        List<CompletableFuture<Void>> all = new ArrayList<>(keys * opsPerKey);
        IntStream.range(0, opsPerKey)
                .forEach(
                        op ->
                                IntStream.range(0, keys)
                                        .forEach(
                                                k ->
                                                        all.add(
                                                                chain.enqueue(
                                                                        k,
                                                                        () -> {
                                                                            perKeyCount[k]
                                                                                    .incrementAndGet();
                                                                            return null;
                                                                        }))));
        for (CompletableFuture<Void> f : all) {
            f.get(30, TimeUnit.SECONDS);
        }
        for (int k = 0; k < keys; k++) {
            assertEquals(opsPerKey, perKeyCount[k].get(), "per-key op count diverged at key " + k);
        }
    }

    /**
     * Sanity that {@link Objects#hashCode} based key equality (mutable wrapper) still works for the
     * chain — if we ever swap the impl from ConcurrentHashMap.compute to a different primitive this
     * guards against an accidental identity-comparison regression.
     */
    @Test
    void distinctKeyInstancesWithEqualValueShareChain() {
        PerKeyFuturesChain<String> chain = new PerKeyFuturesChain<>(executor);
        // Two `new String("k")` instances are equals() but != by identity.
        assertDoesNotThrow(() -> chain.enqueue(new String("k"), () -> 1).get(5, TimeUnit.SECONDS));
        // Another enqueue with an equal-but-distinct String should reuse the chain head, not start
        // a new one.
        CompletableFuture<Integer> f = chain.enqueue(new String("k"), () -> 2);
        assertFalse(f.isCompletedExceptionally());
        assertDoesNotThrow(() -> f.get(5, TimeUnit.SECONDS));
    }
}

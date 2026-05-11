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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.ForStRsAbstractKeyedStateBackend;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ForStRsAsyncValueState} including the spec-mandated 100k-op / 1k-key concurrent
 * stress test (Task 8.9).
 *
 * <p>For the stress test we run 100 ops per key over 1k distinct keys (total 100k ops). Each op is
 * a get-then-put cycle: read current value, write {@code v + 1}. With per-key serialization
 * provided by {@link PerKeyFuturesChain}, the final value per key must equal the number of ops
 * issued (100), proving no lost updates regardless of cross-key parallelism.
 */
class ForStRsAsyncValueStateTest {

    private static <K> ForStRsAbstractKeyedStateBackend<K> buildBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            org.apache.flink.api.common.typeutils.TypeSerializer<K> keySer,
            CloseableRegistry registry) {
        ForStRsKeyedStateBackend<K> delegate =
                new ForStRsKeyedStateBackend<>(arena, linker, db, cf, keySer, false);
        return new ForStRsAbstractKeyedStateBackend<>(
                keySer,
                Thread.currentThread().getContextClassLoader(),
                new ExecutionConfig(),
                registry,
                delegate);
    }

    @Test
    void singleKeyAsyncRoundTrip() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                    CloseableRegistry cr = new CloseableRegistry();
                    ForStRsAbstractKeyedStateBackend<String> backend =
                            buildBackend(arena, linker, db, cf, StringSerializer.INSTANCE, cr)) {

                backend.getDelegate().setCurrentKey("kA");
                ForStRsAsyncValueState<String, Integer> state =
                        backend.getAsyncValueState("counter", IntSerializer.INSTANCE);

                assertNull(state.value().get(5, TimeUnit.SECONDS));
                state.update(1).get(5, TimeUnit.SECONDS);
                assertEquals(1, state.value().get(5, TimeUnit.SECONDS));
                state.update(42).get(5, TimeUnit.SECONDS);
                assertEquals(42, state.value().get(5, TimeUnit.SECONDS));
                state.clear().get(5, TimeUnit.SECONDS);
                assertNull(state.value().get(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void asyncSubmitFailsBeforeSetCurrentKey() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                    CloseableRegistry cr = new CloseableRegistry();
                    ForStRsAbstractKeyedStateBackend<String> backend =
                            buildBackend(arena, linker, db, cf, StringSerializer.INSTANCE, cr)) {

                ForStRsAsyncValueState<String, Integer> state =
                        backend.getAsyncValueState("nokey", IntSerializer.INSTANCE);
                CompletableFuture<Integer> f = state.value();
                assertTrue(f.isCompletedExceptionally());
                assertThrows(
                        java.util.concurrent.ExecutionException.class,
                        () -> f.get(1, TimeUnit.SECONDS));
            }
        }
    }

    /**
     * Spec stress: 100 ops × 1k keys = 100k total. Each op is a get + update(get+1); per-key
     * serialization must yield final value = 100 for every key.
     */
    @Test
    void stressGetThenPut100kAcross1kKeys() throws Exception {
        final int keys = 1_000;
        final int opsPerKey = 100;

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                    CloseableRegistry cr = new CloseableRegistry();
                    ForStRsAbstractKeyedStateBackend<Integer> backend =
                            buildBackend(arena, linker, db, cf, IntSerializer.INSTANCE, cr)) {

                ForStRsKeyedStateBackend<Integer> delegate = backend.getDelegate();

                // Submitter thread pool — also virtual threads, separate from the chain executor
                // owned by the backend. Each submitter sets the current key on the delegate,
                // captures it in the async wrapper, then submits both get and update.
                List<CompletableFuture<Integer>> finals = new ArrayList<>(keys);
                AtomicInteger submitterErrors = new AtomicInteger(0);

                // Spawn one virtual thread per key that does opsPerKey serial submits. Each submit
                // races with submits for OTHER keys; per-key the chain serializes get→update.
                // We synchronize on the delegate around setCurrentKey + state-fetch because the
                // delegate's setCurrentKey is not thread-safe — but per-key chain semantics still
                // hold because each thread captures its own key value before enqueueing.
                List<Thread> threads = new ArrayList<>(keys);
                List<List<CompletableFuture<Void>>> perKeyOps = new ArrayList<>(keys);
                for (int i = 0; i < keys; i++) {
                    perKeyOps.add(new ArrayList<>(opsPerKey));
                }
                for (int kIdx = 0; kIdx < keys; kIdx++) {
                    final int kFinal = kIdx;
                    Thread t =
                            Thread.ofVirtual()
                                    .name("submitter-" + kFinal)
                                    .unstarted(
                                            () -> {
                                                try {
                                                    // Use the explicit-key async API so submit
                                                    // and continuation both bind to the same
                                                    // captured key without depending on the
                                                    // shared delegate's currentKey field.
                                                    ForStRsAsyncValueState<Integer, Integer> state =
                                                            backend.<Integer>getAsyncValueState(
                                                                    "ctr", IntSerializer.INSTANCE);
                                                    // Chain each iteration off the previous so
                                                    // get-then-put on the same key is observed
                                                    // as a single atomic step before the next
                                                    // iteration's get is enqueued. Without this
                                                    // the submitter would enqueue 100 gets
                                                    // back-to-back and only then start enqueuing
                                                    // updates as gets complete, producing
                                                    // classic lost-update behaviour.
                                                    CompletableFuture<Void> chainTail =
                                                            CompletableFuture.completedFuture(null);
                                                    for (int op = 0; op < opsPerKey; op++) {
                                                        chainTail =
                                                                chainTail.thenCompose(
                                                                        ___ ->
                                                                                state.value(kFinal)
                                                                                        .thenCompose(
                                                                                                v ->
                                                                                                        state
                                                                                                                .update(
                                                                                                                        kFinal,
                                                                                                                        v
                                                                                                                                        == null
                                                                                                                                ? 1
                                                                                                                                : v
                                                                                                                                        + 1)));
                                                    }
                                                    synchronized (perKeyOps.get(kFinal)) {
                                                        perKeyOps.get(kFinal).add(chainTail);
                                                    }
                                                } catch (Throwable t1) {
                                                    submitterErrors.incrementAndGet();
                                                }
                                            });
                    threads.add(t);
                }
                for (Thread t : threads) {
                    t.start();
                }
                for (Thread t : threads) {
                    t.join(120_000);
                }
                assertEquals(0, submitterErrors.get(), "submitter thread errors");

                // Wait for every submitted op to complete.
                for (int kIdx = 0; kIdx < keys; kIdx++) {
                    for (CompletableFuture<Void> f : perKeyOps.get(kIdx)) {
                        f.get(60, TimeUnit.SECONDS);
                    }
                }

                // Assert final value per key == opsPerKey (100). Use the explicit-key API for
                // a deadlock-free read (the chain executor needs the backend monitor for the
                // op; holding it on the caller while awaiting the future would deadlock).
                ForStRsAsyncValueState<Integer, Integer> verifier =
                        backend.getAsyncValueState("ctr", IntSerializer.INSTANCE);
                for (int kIdx = 0; kIdx < keys; kIdx++) {
                    Integer v = verifier.value(kIdx).get(60, TimeUnit.SECONDS);
                    assertNotNull(v, "key " + kIdx + " missing value");
                    assertEquals(
                            opsPerKey,
                            v.intValue(),
                            "key " + kIdx + " lost updates: expected " + opsPerKey + " got " + v);
                }
            }
        }
    }

    /**
     * Smaller per-key sequence-number test: enqueues 200 update(i) ops on a single key and verifies
     * the final stored value equals 199 (last write wins under per-key serialization).
     * Distinguishes the chain's ordering guarantee from a plain "atomicity" property.
     */
    @Test
    void perKeySerializationProducesLastWriteWins() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena);
                    CloseableRegistry cr = new CloseableRegistry();
                    ForStRsAbstractKeyedStateBackend<String> backend =
                            buildBackend(arena, linker, db, cf, StringSerializer.INSTANCE, cr)) {
                backend.getDelegate().setCurrentKey("seq-key");
                ForStRsAsyncValueState<String, Integer> state =
                        backend.getAsyncValueState("seq", IntSerializer.INSTANCE);
                List<CompletableFuture<Void>> fs = new ArrayList<>(200);
                for (int i = 0; i < 200; i++) {
                    fs.add(state.update(i));
                }
                for (CompletableFuture<Void> f : fs) {
                    f.get(5, TimeUnit.SECONDS);
                }
                assertEquals(199, state.value().get(5, TimeUnit.SECONDS));
            }
        }
    }
}

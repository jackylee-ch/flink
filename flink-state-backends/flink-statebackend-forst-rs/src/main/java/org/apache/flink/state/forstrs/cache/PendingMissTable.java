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

package org.apache.flink.state.forstrs.cache;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Per-(stateName, key) in-flight miss table for the RMW path (umbrella spec §3 Trace C MISS path +
 * §15 component). Coalesces concurrent add() calls on the same key into one GET + one combiner pass
 * + one PUT.
 *
 * <p>Invariant: at most one outstanding GET per (stateName, key) at any time. Subsequent add()s
 * while a GET is in flight join the convoy as additional pendingInputs.
 *
 * <p>Backed by ConcurrentHashMap because the operator thread issues add() concurrently with the GET
 * continuation that resolves the miss (the resolution may happen on a Flink async-state continuation
 * thread).
 *
 * @param <IN>  the input type supplied by add() callers (e.g. the value being reduced)
 * @param <ACC> the accumulator type produced by the combiner
 */
public class PendingMissTable<IN, ACC> {

    private final ConcurrentHashMap<MissKey, PendingMiss<IN, ACC>> pendingMisses =
            new ConcurrentHashMap<>();

    /**
     * Register a new miss or join the existing convoy for {@code (stateName, key)}.
     *
     * <p>If this is the first miss on this pair, {@code issueGet} is called exactly once to fire
     * off the GET request. Subsequent calls for the same pair while the GET is in flight add their
     * {@code input} to the convoy queue silently — {@code issueGet} is NOT called again.
     *
     * @param stateName logical state name
     * @param key       raw key (byte[] or any Object; byte[] keys are compared by content)
     * @param input     the input value being added by the caller
     * @param issueGet  called exactly once on the first miss to enqueue the GET
     */
    public void beginOrJoin(String stateName, Object key, IN input, Supplier<Void> issueGet) {
        MissKey mk = MissKey.of(stateName, key);
        boolean[] created = {false};
        PendingMiss<IN, ACC> pm = pendingMisses.computeIfAbsent(mk, k -> {
            created[0] = true;
            return new PendingMiss<>();
        });
        // Add input AFTER the entry is visible in the map, before issueGet.
        // computeIfAbsent returns the same object for concurrent callers once it's inserted,
        // so pendingInputs.add() is safe here: the convoy object itself is not replaced,
        // only its contents are appended. For V1 single-operator-thread semantics this is fine;
        // a ConcurrentLinkedDeque swap-in is the multi-thread upgrade path.
        pm.pendingInputs.add(input);
        if (created[0]) {
            issueGet.get();
        }
    }

    /**
     * Resolve a pending miss with the engine's returned prior value. Folds all queued inputs via
     * the combiner in arrival order, calls {@code ack} once per queued caller (with the final
     * accumulator), then removes the entry from the table.
     *
     * @param stateName logical state name
     * @param key       raw key
     * @param priorAcc  accumulator value returned by the engine (null if key was absent)
     * @param combiner  {@code (acc, in) -> acc'} — must not throw
     * @param ack       called once per queued input with the final folded accumulator
     * @return the final folded accumulator, or {@code priorAcc} if no entry existed
     */
    public ACC resolve(
            String stateName,
            Object key,
            ACC priorAcc,
            BiFunction<ACC, IN, ACC> combiner,
            Consumer<ACC> ack) {
        MissKey mk = MissKey.of(stateName, key);
        PendingMiss<IN, ACC> pm = pendingMisses.remove(mk);
        if (pm == null) {
            return priorAcc;
        }
        ACC acc = priorAcc;
        for (IN inp : pm.pendingInputs) {
            acc = combiner.apply(acc, inp);
        }
        // Ack each caller once with the final accumulator
        int n = pm.pendingInputs.size();
        for (int i = 0; i < n; i++) {
            ack.accept(acc);
        }
        return acc;
    }

    /**
     * Resolve with explicit failure handler. If {@code combiner} throws at any input, the entire
     * convoy fails: all chained callers receive the throwable via {@code errorHandler}; the pending
     * entry is removed; the cache is NOT written.
     *
     * @param stateName    logical state name
     * @param key          raw key
     * @param priorAcc     accumulator from engine (null if key was absent)
     * @param combiner     {@code (acc, in) -> acc'} — may throw
     * @param ack          called once per queued input on success (receives final acc)
     * @param errorHandler called once per queued input on failure (receives the Throwable)
     * @return the final folded accumulator on success, or {@code null} on failure
     */
    public ACC resolveWithFailureHandler(
            String stateName,
            Object key,
            ACC priorAcc,
            BiFunction<ACC, IN, ACC> combiner,
            Consumer<ACC> ack,
            Consumer<Throwable> errorHandler) {
        MissKey mk = MissKey.of(stateName, key);
        PendingMiss<IN, ACC> pm = pendingMisses.remove(mk);
        if (pm == null) {
            return priorAcc;
        }
        int n = pm.pendingInputs.size();
        ACC acc = priorAcc;
        try {
            for (IN inp : pm.pendingInputs) {
                acc = combiner.apply(acc, inp);
            }
            for (int i = 0; i < n; i++) {
                ack.accept(acc);
            }
            return acc;
        } catch (Throwable t) {
            for (int i = 0; i < n; i++) {
                errorHandler.accept(t);
            }
            return null;
        }
    }

    /** Returns the number of (stateName, key) pairs with an active pending miss. */
    public int activeMissCount() {
        return pendingMisses.size();
    }

    /**
     * Returns the number of pending inputs in the convoy for {@code (stateName, key)}, or 0 if no
     * miss is active for that pair.
     */
    public int pendingInputsCount(String stateName, Object key) {
        PendingMiss<IN, ACC> pm = pendingMisses.get(MissKey.of(stateName, key));
        return pm == null ? 0 : pm.pendingInputs.size();
    }

    // -----------------------------------------------------------------
    // Internal key type — content-equal for byte[] keys
    // -----------------------------------------------------------------

    private record MissKey(String stateName, byte[] keyBytes) {

        static MissKey of(String stateName, Object key) {
            byte[] bytes;
            if (key instanceof byte[] arr) {
                bytes = arr;
            } else {
                bytes = key.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            return new MissKey(stateName, bytes);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MissKey m
                    && stateName.equals(m.stateName)
                    && Arrays.equals(keyBytes, m.keyBytes);
        }

        @Override
        public int hashCode() {
            return stateName.hashCode() * 31 + Arrays.hashCode(keyBytes);
        }
    }
}

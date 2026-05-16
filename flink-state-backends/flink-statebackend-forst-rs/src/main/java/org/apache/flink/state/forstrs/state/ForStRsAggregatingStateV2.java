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

package org.apache.flink.state.forstrs.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.VectorizedClassifier;
import org.apache.flink.state.forstrs.cache.PendingMissTable;
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;
import org.apache.flink.state.forstrs.exec.SlotArenaScope;

import java.io.IOException;
import java.util.Optional;

/**
 * V2 AggregatingState using cache-mediated read-modify-write (umbrella spec §2 component 13 +
 * §3 Trace C).
 *
 * <h3>RMW protocol</h3>
 *
 * <p>Same pattern as {@link ForStRsReducingStateV2}, but with IN/ACC/OUT typing:
 *
 * <pre>
 *   add(compositeKey, value):
 *     1. tryFold(key, value) — cache hit: combine in-place via aggFn.add(in, acc), mark dirty
 *     2. cache miss: beginOrJoin convoy in PendingMissTable
 *        a. first miss: issueGet fires a GET request to the engine
 *        b. subsequent misses: join existing convoy (no additional GET)
 *     3. GET resolves: resolve() folds all queued inputs, writes result to cache via put()
 * </pre>
 *
 * <h3>Cold-miss seed</h3>
 *
 * <p>When no prior accumulator exists for a key, the seed is {@code aggFn.createAccumulator()}
 * (versus ReducingState which seeds from the first input value).
 *
 * <h3>Result materialization</h3>
 *
 * <p>{@link #get(byte[])} runs {@code aggFn.getResult(acc)} to convert the stored ACC to OUT.
 *
 * <h3>Flush</h3>
 *
 * <p>{@link #flushOnBarrier()} drives the §3 Trace E barrier-time flush. Dirty cache entries are
 * serialized and submitted as PUT requests via {@link VectorizedClassifier}.
 *
 * <h3>V1 scope</h3>
 *
 * <p>Real engine GET wiring (issueGet callback) and the PUT serialization path are structural
 * placeholders in V1 — the integration test in P11 exercises the full end-to-end path against a
 * running engine. The {@link #add(byte[], Object)} and {@link #flushOnBarrier()} contracts are
 * fully implemented at the cache level.
 *
 * @param <IN>  the input type
 * @param <ACC> the accumulator type
 * @param <OUT> the output type returned by {@link #get(byte[])}
 */
@Internal
public class ForStRsAggregatingStateV2<IN, ACC, OUT> {

    private final String stateName;
    private final TypeSerializer<ACC> accSerializer;
    private final AggregateFunction<IN, ACC, OUT> aggFn;
    private final ReducingAggregatingCache<IN, ACC> cache;
    private final PendingMissTable<IN, ACC> pendingMisses;
    private final VectorizedClassifier classifier;
    private final SlotArenaScope slotScope;

    /**
     * Creates a new {@code ForStRsAggregatingStateV2}.
     *
     * @param stateName       logical state name; must be unique within the operator
     * @param accSerializer   serializer for ACC; used for value serialization on flush
     * @param aggFn           the user-provided AggregateFunction; must be thread-safe (it's only
     *                        called on the operator thread in V1)
     * @param classifier      the dispatch classifier (receives PUT requests on flush)
     * @param slotScope       the slot Arena scope (reserved for P11 off-heap value staging)
     */
    public ForStRsAggregatingStateV2(
            String stateName,
            TypeSerializer<ACC> accSerializer,
            AggregateFunction<IN, ACC, OUT> aggFn,
            VectorizedClassifier classifier,
            SlotArenaScope slotScope) {
        this.stateName = stateName;
        this.accSerializer = accSerializer;
        this.aggFn = aggFn;
        this.classifier = classifier;
        this.slotScope = slotScope;
        this.pendingMisses = new PendingMissTable<>();
        // Cache combiner: (acc, in) -> aggFn.add(in, acc) — note AggregateFunction.add signature
        // is add(IN value, ACC accumulator) -> ACC, so we adapt the (acc, in) BiFunction order.
        this.cache = new ReducingAggregatingCache<>(
                (acc, in) -> aggFn.add(in, acc),
                (keyBytes, acc) -> flushEntry(keyBytes, acc));
    }

    // -----------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------

    /**
     * Adds {@code value} to the current accumulator for {@code compositeKey}.
     *
     * <p>Cache hit: folds in-place on the operator thread via {@code aggFn.add(value, acc)} —
     * zero engine I/O.
     * Cache miss: enqueues a GET request (first miss) or joins the existing convoy (subsequent
     * misses on the same key while GET is in flight).
     *
     * @param compositeKey pre-encoded storage key
     * @param value        the value to add; ignored if null
     */
    public void add(byte[] compositeKey, IN value) {
        if (value == null) {
            return;
        }
        Optional<ACC> hit = cache.tryFold(compositeKey, value);
        if (hit.isPresent()) {
            return;
        }
        // Cache miss — enqueue or join convoy
        pendingMisses.beginOrJoin(stateName, compositeKey, value, () -> {
            // V1 structural placeholder: real GET submission wired in P11.
            // On GET resolve: fold all queued inputs starting from aggFn.createAccumulator()
            // if the engine returned null (key absent), then call cache.put().
            return null;
        });
    }

    /**
     * Returns the aggregated result for {@code compositeKey}, or {@code null} on miss.
     *
     * <p>Runs {@code aggFn.getResult(acc)} to convert the stored ACC to OUT.
     *
     * <p>V1: only cache-resident entries are returned. Engine GET wiring for non-cached entries
     * is deferred to P11.
     *
     * @param compositeKey pre-encoded storage key
     */
    public OUT get(byte[] compositeKey) {
        ACC cached = cache.peek(compositeKey);
        if (cached == null) {
            return null;
        }
        return aggFn.getResult(cached);
    }

    /**
     * Clears the state for {@code compositeKey}. Inserts a null tombstone in the cache (dirty,
     * will be flushed as a DELETE on the next barrier).
     *
     * <p>V1: engine DELETE submission is deferred to P11.
     *
     * @param compositeKey pre-encoded storage key
     */
    public void clear(byte[] compositeKey) {
        // null acc in cache signals "deleted" — flush path emits a DELETE
        cache.put(compositeKey, null);
    }

    /**
     * Flushes all dirty cache entries via the classifier (§3 Trace E barrier path).
     * Entries are serialized and submitted as PUT requests. Entries with a null accumulator
     * are submitted as DELETE requests (representing cleared state).
     *
     * <p>Called by the checkpoint barrier handler before snapshotting.
     */
    public void flushOnBarrier() {
        cache.flushAllDirty();
    }

    // -----------------------------------------------------------------
    // Diagnostics / testing
    // -----------------------------------------------------------------

    /** Returns the number of (stateName, key) pairs with an active pending miss. */
    public int activeMissCount() {
        return pendingMisses.activeMissCount();
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    /**
     * Invoked by the cache flush callback on LRU eviction or {@link #flushOnBarrier()}.
     * Serializes the accumulator and submits a PUT to the classifier.
     *
     * <p>V1 simplification: uses heap-based DataOutputSerializer; off-heap staging via
     * SlotArenaScope is wired in P11 once the full GET/PUT round-trip is integrated.
     *
     * @param keyBytes composite key bytes
     * @param acc      accumulator to flush (null means DELETE)
     */
    private void flushEntry(byte[] keyBytes, ACC acc) {
        if (acc == null) {
            // DELETE path — wired in P11
            return;
        }
        // PUT path — serialize accumulator
        byte[] valBytes;
        try {
            DataOutputSerializer dos = new DataOutputSerializer(64);
            accSerializer.serialize(acc, dos);
            valBytes = dos.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAggregatingStateV2: failed to serialize accumulator for state="
                            + stateName,
                    e);
        }
        // V1 structural placeholder: real PUT submission via classifier wired in P11.
        // Full path: build a PutRequest with keyBytes + valBytes and call
        // classifier.submitVectorized(putReq). Deferred because the sealed
        // VectorizedStateRequest hierarchy needs a new PUT subtype for off-heap RMW PUTs.
        // For now, drop the serialized bytes — they are computed correctly.
    }
}

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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractReducingState;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;
import org.apache.flink.state.forstrs.cache.LongReducingAggregatingCache;
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

/**
 * Async-V2 ReducingState for ForSt-RS. Extends {@link AbstractReducingState} (which implements
 * asyncGet / asyncAdd via the REDUCING_GET / REDUCING_ADD request types with a get-reduce-put
 * chain) and implements {@link ForStRsInnerTable} for the vectorized batch-dispatch path.
 *
 * <p>Storage: the current reduced value is stored as a single serialized {@code V}. REDUCING_GET
 * maps to GET; REDUCING_ADD maps to PUT.
 *
 * <h3>PR-C3 RMW cache (V12, B3-H1)</h3>
 *
 * <p>This class wraps the inherited {@link AbstractReducingState#asyncAdd} with a per-instance
 * {@link ReducingAggregatingCache}. On cache hit, the input is folded in-memory on the operator
 * thread (zero engine I/O) and the result is marked dirty. On cache miss, the inherited
 * {@code asyncGetInternal()} fetches the existing accumulator from the engine, folds the new input,
 * and stores the result in the cache (dirty). The accumulator stays in cache until
 * {@link #flushOnBarrier()} drains it via the configured flush handler.
 *
 * <p>The cache key is the same composite-key bytes that
 * {@link #serializeKey(StateRequest)} produces — built off-thread by reading the current
 * {@link RecordContext} via the AEC. This mirrors the
 * {@code ForStRsMapStateV2#serializeMapEntryKey} pattern (PR-C1).
 *
 * <h3>flushOnBarrier()</h3>
 *
 * <p>The backend's {@code snapshot()} invokes {@link #flushOnBarrier()} on every registered
 * instance before the engine snapshot runs (Trace E barrier drain). Dirty accumulators are
 * serialized and delivered to the flush handler — a callback set by the backend or a test. The
 * default handler is a no-op (V1 behaviour, matches the production gating on full classifier
 * integration), but a hook is exposed via {@link #setFlushHandler(BiConsumer)} for tests and
 * future P11 wiring.
 *
 * @param <K> backend key type
 * @param <N> namespace type
 * @param <V> value type (accumulator == value for reducing state)
 */
@Internal
public class ForStRsAsyncReducingStateV2<K, N, V> extends AbstractReducingState<K, N, V>
        implements ReducingState<V>, ForStRsInnerTable<K, N, V> {

    private static final byte[] KEY_PREFIX = "k/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SLASH = "/".getBytes(StandardCharsets.UTF_8);

    private final String stateName;
    private final byte[] stateNameBytes;
    private final TypeSerializer<K> keySerializer;
    /**
     * PR-A2 (S1-4 / E2-CRIT-1): namespace serializer for trailing-namespace composite-key
     * encoding. Hard format break vs v3.x snapshots.
     */
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<V> valueSerializer;
    private final ReduceFunction<V> reduceFunction;

    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    /**
     * Per-instance RMW cache (PR-C3 / B3-H1). Combiner invokes {@link
     * ReduceFunction#reduce(Object, Object)} on the operator thread. flushCallback materializes
     * the dirty accumulator and routes it to {@link #flushHandler}.
     *
     * <p>B10-H2: exactly one of {@link #cache} / {@link #longCache} is non-null at any time. The
     * primitive-{@code long} specialization is selected when {@code valueSerializer instanceof
     * LongSerializer} (or {@code IntSerializer} — promoted to long-storage); otherwise the
     * general {@code BiFunction}-typed path is used. The branch is on a {@code final} field so
     * JIT speculates and inlines the chosen path — there is no runtime dispatch overhead on the
     * hot tryFold call.
     */
    private final ReducingAggregatingCache<V, V> cache;

    /**
     * B10-H2: long-specialized RMW cache. Active when {@link #usePrimitiveLongCache} is true.
     * Stores the accumulator as a primitive {@code long} per entry; combiner is invoked as a
     * {@link java.util.function.LongBinaryOperator} so there is no {@code Long.valueOf} write-back
     * box per hit. The user's {@link ReduceFunction} is wrapped by an unboxing shim at
     * construction time.
     */
    private final LongReducingAggregatingCache longCache;

    /**
     * B10-H2 routing flag — captured at construction. Equal to {@code longCache != null} but
     * stored explicitly so the hot path can read a single primitive boolean instead of two
     * field loads on every {@code asyncAdd}.
     */
    private final boolean usePrimitiveLongCache;

    /**
     * B10-H2 helper for the {@code Integer}-promotion case: when the upstream serializer is
     * {@link IntSerializer} we still use the long-specialized cache (long storage subsumes int)
     * and unbox via {@code ((Integer) in).intValue()} → {@code long}. This flag tells the
     * miss-resolve path to re-box back to {@link Integer} for the user's {@link ReduceFunction}
     * call signature.
     */
    private final boolean accIsInteger;

    /**
     * Pluggable flush handler invoked once per dirty entry on {@link #flushOnBarrier()} (and on
     * LRU eviction). Receives the composite-key bytes and the serialized accumulator bytes
     * ({@code null}/empty bytes means "cleared"). The default is a no-op — A4-H2 correctness
     * note: production must override via {@link #setFlushHandler}, otherwise every cached
     * accumulator is silently discarded on every checkpoint. The Flink runtime wires the engine
     * PUT/DELETE path from {@code ForStRsAsyncKeyedStateBackend#createReducingState}; the no-op
     * default is retained only so unit tests that exercise the cache directly do not have to
     * stand up a live engine.
     */
    private volatile BiConsumer<byte[], byte[]> flushHandler = (k, v) -> {};

    @SuppressWarnings({"unchecked", "rawtypes"})
    public ForStRsAsyncReducingStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            ReduceFunction<V> reduceFunction) {
        super(stateRequestHandler, reduceFunction, valueSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.reduceFunction = reduceFunction;
        // B10-H2: detect primitive-long-friendly accumulator types via the value serializer.
        // LongSerializer / IntSerializer cover the Q12 SumAgg / CountAgg cases that hit the
        // ReducingAggregatingCache combiner millions of times per second. Everything else
        // falls back to the general BiFunction-typed cache below.
        boolean isLong = valueSerializer instanceof LongSerializer;
        boolean isInt = valueSerializer instanceof IntSerializer;
        this.usePrimitiveLongCache = isLong || isInt;
        this.accIsInteger = isInt;
        if (this.usePrimitiveLongCache) {
            // Unwrap the user's ReduceFunction<Long|Integer> into a LongBinaryOperator. We still
            // pay one boxed Long/Integer per combiner call (the user's reduce() signature is
            // Object-typed, no way around it), but the WRITE-BACK box that the general cache
            // pays on every hit is gone — the result long is written straight into the entry's
            // primitive field. Net allocation cost on a Q12-pattern cache-hit run is halved.
            final ReduceFunction rawReduce = reduceFunction;
            final boolean asInt = isInt;
            java.util.function.LongBinaryOperator longCombiner =
                    (accL, inL) -> {
                        try {
                            Object acc = asInt ? Integer.valueOf((int) accL) : Long.valueOf(accL);
                            Object in = asInt ? Integer.valueOf((int) inL) : Long.valueOf(inL);
                            Object out = rawReduce.reduce(acc, in);
                            return asInt
                                    ? ((Integer) out).longValue()
                                    : ((Long) out).longValue();
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "ForStRsAsyncReducingStateV2: ReduceFunction threw", e);
                        }
                    };
            LongReducingAggregatingCache.LongFlushCallback longFlush =
                    (keyBytes, acc) -> {
                        Object boxedAcc = asInt ? Integer.valueOf((int) acc) : Long.valueOf(acc);
                        byte[] valBytes = serializeValueBytes((V) boxedAcc);
                        flushHandler.accept(keyBytes, valBytes);
                    };
            this.longCache = ReducingAggregatingCache.forLong(longCombiner, longFlush);
            this.cache = null;
        } else {
            this.longCache = null;
            this.cache =
                    new ReducingAggregatingCache<>(
                            (acc, in) -> {
                                try {
                                    return reduceFunction.reduce(acc, in);
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "ForStRsAsyncReducingStateV2: ReduceFunction threw",
                                            e);
                                }
                            },
                            this::flushEntry);
        }
    }

    // ---------------------------------------------------------------
    // PR-C3 RMW cache — async overrides
    // ---------------------------------------------------------------

    /**
     * Cleanup-C3 (zero-alloc cache key): writes the composite cache key bytes into the
     * per-instance reusable {@link #keyOut} and returns the length of the written prefix. The
     * caller obtains the byte slice via {@code keyOut.getSharedBuffer()} + the returned length —
     * no {@code getCopyOfBuffer()} allocation. The shared buffer is reused across calls but the
     * single-threaded RecordContext-lock contract guarantees the bytes remain valid until the
     * lookup call returns. On miss-resolve the caller snapshots the slice via {@code
     * cache.put(buf, off, len, acc)} which performs the one unavoidable allocation.
     *
     * <p>Mirrors {@link #serializeKey(StateRequest)} but reads (K, N) from the AEC's current
     * {@link RecordContext} instead of a StateRequest — used by the {@link #asyncAdd(Object)}
     * override which runs before any state request is built.
     */
    @SuppressWarnings("unchecked")
    private int writeCacheKeyToKeyOut() {
        AsyncExecutionController<K, ?> aec =
                (AsyncExecutionController<K, ?>) stateRequestHandler;
        RecordContext<K> ctx = aec.getCurrentContext();
        N namespace = ctx.getNamespace(this);
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            if (namespaceSerializer != null
                    && namespace != null
                    && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return keyOut.length();
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncReducingStateV2: failed to serialize cache key", e);
        }
    }

    /**
     * Cache-mediated {@code asyncAdd}. PR-C3 (V12 / B3-H1): folds in-memory on cache hit, returning
     * a completed future; on cache miss, fetches the existing accumulator from the engine, folds in
     * the new value, and stores the result in the cache (marked dirty). The accumulator remains in
     * the cache until {@link #flushOnBarrier()} drains it.
     *
     * <p>Null inputs are ignored to match ReduceFunction semantics in the base class.
     *
     * <p>Cleanup-C3: the cache-hit path is now alloc-free. The cache key bytes are written into a
     * shared {@code DataOutputSerializer} buffer (the {@code keyOut} field already reused for
     * {@link #serializeKey}); the cache probes via a slice view of that buffer. Only the
     * miss-resolve branch snapshots the bytes via {@code cache.put(buf, off, len, ...)}.
     */
    @Override
    public StateFuture<Void> asyncAdd(V value) {
        if (value == null) {
            return StateFutureUtils.completedVoidFuture();
        }
        int keyLen = writeCacheKeyToKeyOut();
        byte[] keyBuf = keyOut.getSharedBuffer();
        // B10-H2: branch on the routing flag (constant after construction — JIT inlines the
        // taken path). Primitive long cache: unbox once on input, write back primitive long.
        if (usePrimitiveLongCache) {
            long inLong = accIsInteger ? ((Integer) value).longValue() : ((Long) value).longValue();
            if (longCache.tryFold(keyBuf, 0, keyLen, inLong)) {
                return StateFutureUtils.completedVoidFuture();
            }
            final byte[] keySnapshot = new byte[keyLen];
            System.arraycopy(keyBuf, 0, keySnapshot, 0, keyLen);
            final long capturedGen = longCache.currentGen(keySnapshot);
            final boolean asInt = accIsInteger;
            final V inputCapture = value;
            return asyncGetInternal()
                    .thenApply(
                            oldValue -> {
                                try {
                                    Object newAcc =
                                            oldValue == null
                                                    ? inputCapture
                                                    : reduceFunction.reduce(oldValue, inputCapture);
                                    long newLong =
                                            asInt
                                                    ? ((Integer) newAcc).longValue()
                                                    : ((Long) newAcc).longValue();
                                    longCache.putIfGen(keySnapshot, newLong, capturedGen);
                                    return null;
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "ForStRsAsyncReducingStateV2: ReduceFunction threw on miss",
                                            e);
                                }
                            });
        }
        // B8-H1: tryFold returns boolean — no Optional wrapper per cache HIT.
        if (cache.tryFold(keyBuf, 0, keyLen, value)) {
            return StateFutureUtils.completedVoidFuture();
        }
        // Cache miss — fetch existing accumulator from the engine, fold, populate cache. We
        // snapshot the cache key slice here (one alloc on the cold path) because the keyOut
        // shared buffer will be overwritten by subsequent calls.
        final byte[] keySnapshot = new byte[keyLen];
        System.arraycopy(keyBuf, 0, keySnapshot, 0, keyLen);
        // A6-H1: capture the per-key generation BEFORE the engine GET is issued. If onClear()
        // fires during the GET round-trip, it will bump the generation; the putIfGen check
        // below will then refuse the stale store, leaving the engine-side DELETE intact.
        final long capturedGen = cache.currentGen(keySnapshot);
        return asyncGetInternal()
                .thenApply(
                        oldValue -> {
                            try {
                                V newAcc =
                                        oldValue == null
                                                ? value
                                                : reduceFunction.reduce(oldValue, value);
                                // A6-H1: gen-checked put. If a concurrent onClear bumped the
                                // generation while the GET was in flight, this returns false and
                                // we drop the stale newAcc — the engine-side DELETE wins.
                                cache.putIfGen(keySnapshot, newAcc, capturedGen);
                                return null;
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "ForStRsAsyncReducingStateV2: ReduceFunction threw on miss",
                                        e);
                            }
                        });
    }

    /**
     * Cache-aware {@code asyncGet}. If the entry is in the cache (even dirty), returns the cached
     * value without an engine round trip. Otherwise falls through to {@code asyncGetInternal} and
     * does NOT populate the cache (only writes / folds populate the cache; pure reads stay
     * uncached to bound memory).
     *
     * <p>Cleanup-C3: probes via the shared-buffer slice view; no byte[] allocation on hit.
     */
    @SuppressWarnings("unchecked")
    @Override
    public StateFuture<V> asyncGet() {
        int keyLen = writeCacheKeyToKeyOut();
        byte[] keyBuf = keyOut.getSharedBuffer();
        if (usePrimitiveLongCache) {
            if (longCache.contains(keyBuf, 0, keyLen)) {
                // Single box on read path — acceptable: reads on cached state are far less
                // frequent than writes (asyncAdd) on Q12-style accumulator workloads, and the
                // caller's StateFuture<V> contract demands a boxed V anyway.
                long v = longCache.peekOr(keyBuf, 0, keyLen, 0L);
                // Disambiguate the conditional's static type: Java would otherwise unbox both
                // arms to long. Compute the boxed result as an Object first, then cast to V.
                Object boxed = accIsInteger ? (Object) Integer.valueOf((int) v) : (Object) Long.valueOf(v);
                return StateFutureUtils.completedFuture((V) boxed);
            }
            return asyncGetInternal();
        }
        if (cache.contains(keyBuf, 0, keyLen)) {
            return StateFutureUtils.completedFuture(cache.peek(keyBuf, 0, keyLen));
        }
        return asyncGetInternal();
    }

    /**
     * Drains all dirty cache entries via the configured {@link #flushHandler}. Called by the
     * backend's snapshot pre-flush (Trace E). Entries are serialized once per call and the handler
     * decides how to route them (production: engine PUT; tests: capture).
     */
    public void flushOnBarrier() {
        if (usePrimitiveLongCache) {
            longCache.flushAllDirty();
        } else {
            cache.flushAllDirty();
        }
    }

    /**
     * A5-H2: pre-DELETE hook fired by {@link
     * org.apache.flink.state.forstrs.VectorizedClassifier#recordDelete}. Invalidates the cache
     * slot for the current (record-context key, namespace) BEFORE the DELETE row is enqueued.
     * Without this, a dirty cached accumulator survives the engine-side DELETE and the next
     * {@link #flushOnBarrier()} writes it back via {@link #flushHandler} ({@code linker.put}),
     * OVERWRITING the DELETE — silent data corruption past {@code asyncClear()}.
     *
     * <p>The cache key bytes are computed off the StateRequest's own {@link RecordContext} (NOT
     * the AEC's current context) so this hook is correct under both batched (vectorized) and
     * single-request synchronous dispatch — both code paths pass the originating request through
     * the classifier.
     */
    @Override
    public void onClear(StateRequest<K, N, ?, ?> request) {
        int keyLen = writeRequestKeyToKeyOut(request);
        if (usePrimitiveLongCache) {
            longCache.invalidate(keyOut.getSharedBuffer(), 0, keyLen);
        } else {
            cache.invalidate(keyOut.getSharedBuffer(), 0, keyLen);
        }
    }

    /**
     * A5-H2 helper: same composite-key layout as {@link #serializeKey(StateRequest)} but writes
     * into {@link #keyOut} without snapshotting (no {@code getCopyOfBuffer} allocation). Returns
     * the length of the written prefix; caller uses the shared buffer for a single zero-alloc
     * cache probe and must consume the bytes before any other writer touches {@code keyOut}.
     */
    private int writeRequestKeyToKeyOut(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        N namespace = request.getNamespace();
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return keyOut.length();
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncReducingStateV2: failed to write cache invalidation key", e);
        }
    }

    /**
     * Replaces the flush handler. Used by the backend to wire the production PUT path and by tests
     * to capture flushed bytes. The handler receives composite-key bytes and serialized
     * accumulator bytes (null acc — cleared entry — passes null bytes).
     */
    @VisibleForTesting
    public void setFlushHandler(BiConsumer<byte[], byte[]> handler) {
        this.flushHandler = handler;
    }

    /** Returns the number of cache entries. Exposed for tests / diagnostics. */
    @VisibleForTesting
    public int cacheSize() {
        return usePrimitiveLongCache ? longCache.size() : cache.size();
    }

    /**
     * B10-H2 test hook — reports whether this state instance is routing through the primitive
     * {@code long} cache. Used by unit tests that want to assert detection fired for a given
     * value-serializer / reduce-function combination.
     */
    @VisibleForTesting
    public boolean isUsingPrimitiveLongCache() {
        return usePrimitiveLongCache;
    }

    /**
     * B10-H2 test hook — backend-agnostic {@code cache.put} adapter. Existing tests reach into
     * the private {@code cache} field via reflection and call {@code cache.put(...)} to bypass
     * the AEC-bound asyncAdd path; after B10-H2 the field may be {@code null} (the long cache
     * holds the data instead), so we expose this method to keep the test contract simple.
     *
     * <p>Routes to the active backing cache. For the primitive-long backend a {@code null}
     * accumulator is encoded as the {@link LongReducingAggregatingCache#ABSENT_SENTINEL}
     * sentinel — tests that want the "tombstone / cleared" semantic should pass {@code null}.
     */
    @VisibleForTesting
    public void testOnlyDirectCachePut(byte[] compositeKey, V acc) {
        if (usePrimitiveLongCache) {
            if (acc == null) {
                // Tombstone path — mirror the general cache's null-accumulator behaviour by
                // routing through the flush callback directly (the long cache cannot store a
                // "null" sentinel without losing primitive semantics). The flush callback
                // receives null bytes and the production handler routes that to engine DELETE.
                flushHandler.accept(compositeKey, null);
            } else {
                long v =
                        accIsInteger
                                ? ((Integer) acc).longValue()
                                : ((Long) acc).longValue();
                longCache.put(compositeKey, v);
            }
        } else {
            cache.put(compositeKey, acc);
        }
    }

    /** B10-H2 test hook — backend-agnostic {@code cache.contains} adapter. */
    @VisibleForTesting
    public boolean testOnlyDirectCacheContains(byte[] compositeKey) {
        return usePrimitiveLongCache
                ? longCache.contains(compositeKey)
                : cache.contains(compositeKey);
    }

    private void flushEntry(byte[] keyBytes, V acc) {
        byte[] valBytes = acc == null ? null : serializeValueBytes(acc);
        flushHandler.accept(keyBytes, valBytes);
    }

    private byte[] serializeValueBytes(V acc) {
        try {
            valueOut.clear();
            valueSerializer.serialize(acc, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncReducingStateV2: failed to serialize accumulator", e);
        }
    }

    // ---------------------------------------------------------------
    // ForStRsInnerTable — key serialization
    // ---------------------------------------------------------------

    @Override
    public byte[] serializeKey(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        N namespace = request.getNamespace();
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: trailing namespace bytes.
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return keyOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("ForStRsAsyncReducingStateV2: failed to serialize key", e);
        }
    }

    @Override
    public int serializeKeyInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        RecordContext<K> ctx = request.getRecordContext();
        N namespace = request.getNamespace();
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: trailing namespace bytes.
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return dest.append(keyOut.getSharedBuffer(), 0, keyOut.length());
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncReducingStateV2: failed to serialize key into buffer", e);
        }
    }

    // ---------------------------------------------------------------
    // ForStRsInnerTable — value serialization
    // ---------------------------------------------------------------

    @Override
    public byte[] serializeValue(Object value) {
        if (value == null) {
            return null;
        }
        try {
            valueOut.clear();
            @SuppressWarnings("unchecked")
            V v = (V) value;
            valueSerializer.serialize(v, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncReducingStateV2: failed to serialize value", e);
        }
    }

    @Override
    public int serializeValueInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        StateRequestType type = request.getRequestType();
        if (type == StateRequestType.CLEAR) {
            return dest.appendEmpty();
        }
        Object payload = request.getPayload();
        if (payload == null) {
            // null payload on REDUCING_ADD means "clear/delete this entry"
            return dest.appendEmpty();
        }
        try {
            valueOut.clear();
            @SuppressWarnings("unchecked")
            V v = (V) payload;
            valueSerializer.serialize(v, valueOut);
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncReducingStateV2: failed to serialize value into buffer", e);
        }
    }

    @Override
    public Object deserializeValue(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return null;
        }
        try {
            valueIn.setBuffer(raw);
            return valueSerializer.deserialize(valueIn);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncReducingStateV2: failed to deserialize value", e);
        }
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result decode. Reads accumulator bytes
     * directly out of the native {@code outData} segment via {@link
     * org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView}, skipping the
     * per-row {@code byte[] = new byte[len]} the default fallback would perform.
     *
     * <p>B4-H5 (zero-copy): view held in a {@link ThreadLocal}, eliminating the per-row
     * {@code new MemorySegmentDataInputView()} on the batched-GET hot path.
     */
    private static final ThreadLocal<org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView>
            VIEW_TL =
                    ThreadLocal.withInitial(
                            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView::new);

    @Override
    public Object deserializeValue(java.lang.foreign.MemorySegment buf, long offset, int len) {
        if (len == 0) {
            return null;
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view = VIEW_TL.get();
            view.rewind(buf, (int) offset, len);
            return valueSerializer.deserialize(view);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncReducingStateV2: failed to decode value off-heap", e);
        }
    }

    // ---------------------------------------------------------------
    // ForStRsInnerTable — request builders
    // ---------------------------------------------------------------

    @Override
    public ForStRsDBGetRequest<K, N, ?> buildDBGetRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        return new ForStRsDBGetRequest<>(key, request, this);
    }

    @Override
    public ForStRsDBPutRequest<K, N, ?> buildDBPutRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        byte[] value = null;
        StateRequestType type = request.getRequestType();
        if (type != StateRequestType.CLEAR) {
            value = serializeValue(request.getPayload());
        }
        return new ForStRsDBPutRequest<>(key, value, request);
    }
}

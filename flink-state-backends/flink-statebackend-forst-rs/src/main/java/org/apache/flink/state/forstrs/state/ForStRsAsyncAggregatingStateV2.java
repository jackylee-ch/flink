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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractAggregatingState;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

/**
 * Async-V2 AggregatingState for ForSt-RS. Extends {@link AbstractAggregatingState} (which
 * implements asyncGet / asyncAdd via AGGREGATING_GET / AGGREGATING_ADD request types with a
 * get-accumulate-put chain) and implements {@link ForStRsInnerTable} for the vectorized
 * batch-dispatch path.
 *
 * <p>Storage: the accumulator {@code ACC} is stored as a single serialized value.
 * AGGREGATING_GET maps to GET (returns raw ACC bytes → deserialized ACC).
 * AGGREGATING_ADD maps to PUT (payload is the new ACC).
 *
 * <h3>PR-C3 RMW cache (V12, B3-H2)</h3>
 *
 * <p>This class wraps the inherited {@link AbstractAggregatingState#asyncAdd} with a per-instance
 * {@link ReducingAggregatingCache}. On cache hit, {@link AggregateFunction#add(Object, Object)} is
 * applied in-memory on the operator thread (zero engine I/O) and the result is marked dirty. On
 * cache miss, the inherited {@code asyncGetInternal()} fetches the existing accumulator from the
 * engine (or seeds a new one via {@link AggregateFunction#createAccumulator()}), folds in the new
 * input, and stores the result in the cache (dirty). The accumulator stays in cache until
 * {@link #flushOnBarrier()} drains it via the configured flush handler.
 *
 * <p>The cache key is the same composite-key bytes that {@link #serializeKey(StateRequest)}
 * produces — built off-thread by reading the current {@link RecordContext} via the AEC. This
 * mirrors the {@code ForStRsMapStateV2#serializeMapEntryKey} pattern (PR-C1).
 *
 * @param <K> backend key type
 * @param <N> namespace type
 * @param <IN> input element type
 * @param <ACC> accumulator type (stored in the engine)
 * @param <OUT> output type returned by asyncGet
 */
@Internal
public class ForStRsAsyncAggregatingStateV2<K, N, IN, ACC, OUT>
        extends AbstractAggregatingState<K, N, IN, ACC, OUT>
        implements AggregatingState<IN, OUT>, ForStRsInnerTable<K, N, ACC> {

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
    private final TypeSerializer<ACC> accSerializer;
    private final AggregateFunction<IN, ACC, OUT> aggregateFn;

    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    /**
     * Per-instance RMW cache (PR-C3 / B3-H2). Combiner invokes {@link
     * AggregateFunction#add(Object, Object)} on the operator thread. flushCallback materializes
     * the dirty accumulator and routes it to {@link #flushHandler}.
     */
    private final ReducingAggregatingCache<IN, ACC> cache;

    /**
     * Pluggable flush handler invoked once per dirty entry on {@link #flushOnBarrier()} (and on
     * LRU eviction). Receives the composite-key bytes and the serialized accumulator bytes
     * ({@code null}/empty bytes means "cleared"). The default is a no-op — A4-H2 correctness
     * note: production must override via {@link #setFlushHandler}, otherwise every cached
     * accumulator is silently discarded on every checkpoint. The Flink runtime wires the engine
     * PUT/DELETE path from {@code ForStRsAsyncKeyedStateBackend#createAggregatingState}; the
     * no-op default is retained only so unit tests that exercise the cache directly do not have
     * to stand up a live engine.
     */
    private volatile BiConsumer<byte[], byte[]> flushHandler = (k, v) -> {};

    public ForStRsAsyncAggregatingStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<ACC> accSerializer,
            AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        super(stateRequestHandler, aggregateFunction, accSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.accSerializer = accSerializer;
        this.aggregateFn = aggregateFunction;
        this.cache =
                new ReducingAggregatingCache<>(
                        // AggregateFunction.add signature is add(IN, ACC) → ACC, so reorder.
                        (acc, in) -> aggregateFunction.add(in, acc),
                        this::flushEntry);
    }

    // ---------------------------------------------------------------
    // PR-C3 RMW cache — async overrides
    // ---------------------------------------------------------------

    /**
     * Cleanup-C3 (zero-alloc cache key): writes the composite cache key bytes into the
     * per-instance reusable {@link #keyOut} and returns the length. Callers probe the cache via a
     * slice view over {@code keyOut.getSharedBuffer()} — no {@code getCopyOfBuffer()} allocation.
     * The miss-resolve branch snapshots the slice once before deferring to {@code asyncGetInternal}.
     *
     * <p>Mirrors {@link #serializeKey(StateRequest)} but reads the (K, N) from the AEC's current
     * {@link RecordContext} instead of a StateRequest.
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
                    "ForStRsAsyncAggregatingStateV2: failed to serialize cache key", e);
        }
    }

    /**
     * Cache-mediated {@code asyncAdd}. PR-C3 (V12 / B3-H2): folds in-memory on cache hit, returning
     * a completed future; on cache miss, fetches the existing accumulator from the engine (or
     * seeds a fresh one via {@link AggregateFunction#createAccumulator()}), folds in the new
     * value, and stores the result in the cache (marked dirty).
     *
     * <p>Null inputs are ignored to match base-class semantics.
     *
     * <p>Cleanup-C3: cache-hit path is alloc-free; the key bytes are probed as a slice view of
     * the per-instance {@code keyOut} shared buffer. Only the miss-resolve branch snapshots the
     * bytes (one allocation on the cold path).
     */
    @Override
    public StateFuture<Void> asyncAdd(IN value) {
        if (value == null) {
            return StateFutureUtils.completedVoidFuture();
        }
        int keyLen = writeCacheKeyToKeyOut();
        byte[] keyBuf = keyOut.getSharedBuffer();
        // B8-H1: tryFold returns boolean — no Optional wrapper per cache HIT.
        if (cache.tryFold(keyBuf, 0, keyLen, value)) {
            return StateFutureUtils.completedVoidFuture();
        }
        // Cache miss — snapshot the key bytes (the only allocation on the miss path) so that
        // subsequent serialize calls don't clobber the shared keyOut buffer before the cache
        // put completes.
        final byte[] keySnapshot = new byte[keyLen];
        System.arraycopy(keyBuf, 0, keySnapshot, 0, keyLen);
        // A6-H1: capture the per-key generation BEFORE the engine GET is issued. If onClear()
        // fires during the GET round-trip, it will bump the generation; the putIfGen check
        // below will then refuse the stale store, leaving the engine-side DELETE intact.
        final long capturedGen = cache.currentGen(keySnapshot);
        return asyncGetInternal()
                .thenApply(
                        oldAcc -> {
                            try {
                                ACC seed =
                                        (oldAcc == null)
                                                ? aggregateFn.createAccumulator()
                                                : oldAcc;
                                ACC newAcc = aggregateFn.add(value, seed);
                                // A6-H1: gen-checked put. If a concurrent onClear bumped the
                                // generation while the GET was in flight, this returns false and
                                // we drop the stale newAcc — the engine-side DELETE wins.
                                cache.putIfGen(keySnapshot, newAcc, capturedGen);
                                return null;
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "ForStRsAsyncAggregatingStateV2: AggregateFunction threw on miss",
                                        e);
                            }
                        });
    }

    /**
     * Cache-aware {@code asyncGet}. If the entry is in the cache (even dirty), returns the cached
     * value (after {@link AggregateFunction#getResult(Object)}) without an engine round trip.
     * Otherwise falls through to the base implementation.
     *
     * <p>Cleanup-C3: probes via the shared-buffer slice view; no byte[] allocation on hit.
     */
    @Override
    public StateFuture<OUT> asyncGet() {
        int keyLen = writeCacheKeyToKeyOut();
        byte[] keyBuf = keyOut.getSharedBuffer();
        if (cache.contains(keyBuf, 0, keyLen)) {
            ACC cached = cache.peek(keyBuf, 0, keyLen);
            OUT out = cached == null ? null : aggregateFn.getResult(cached);
            return StateFutureUtils.completedFuture(out);
        }
        return super.asyncGet();
    }

    /**
     * Drains all dirty cache entries via the configured {@link #flushHandler}. Called by the
     * backend's snapshot pre-flush (Trace E).
     */
    public void flushOnBarrier() {
        cache.flushAllDirty();
    }

    /**
     * A5-H2: pre-DELETE hook fired by {@link
     * org.apache.flink.state.forstrs.VectorizedClassifier#recordDelete}. Invalidates the cache
     * slot for the current (record-context key, namespace) BEFORE the DELETE row is enqueued.
     * Without this, a dirty cached accumulator survives the engine-side DELETE and the next
     * {@link #flushOnBarrier()} writes it back via {@link #flushHandler} ({@code linker.put}),
     * OVERWRITING the DELETE — silent data corruption past {@code asyncClear()}.
     *
     * <p>Mirrors {@code ForStRsAsyncReducingStateV2.onClear}; see that class for the rationale on
     * using the StateRequest's own {@link RecordContext} instead of the AEC's current context.
     */
    @Override
    public void onClear(StateRequest<K, N, ?, ?> request) {
        int keyLen = writeRequestKeyToKeyOut(request);
        cache.invalidate(keyOut.getSharedBuffer(), 0, keyLen);
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
                    "ForStRsAsyncAggregatingStateV2: failed to write cache invalidation key", e);
        }
    }

    /**
     * Replaces the flush handler. Used by the backend to wire the production PUT path and by tests
     * to capture flushed bytes. The handler receives composite-key bytes and serialized
     * accumulator bytes (null acc → null bytes).
     */
    @VisibleForTesting
    public void setFlushHandler(BiConsumer<byte[], byte[]> handler) {
        this.flushHandler = handler;
    }

    /** Returns the number of cache entries. Exposed for tests / diagnostics. */
    @VisibleForTesting
    public int cacheSize() {
        return cache.size();
    }

    private void flushEntry(byte[] keyBytes, ACC acc) {
        byte[] valBytes = acc == null ? null : serializeAccBytes(acc);
        flushHandler.accept(keyBytes, valBytes);
    }

    private byte[] serializeAccBytes(ACC acc) {
        try {
            valueOut.clear();
            accSerializer.serialize(acc, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncAggregatingStateV2: failed to serialize accumulator", e);
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
            throw new RuntimeException(
                    "ForStRsAsyncAggregatingStateV2: failed to serialize key", e);
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
                    "ForStRsAsyncAggregatingStateV2: failed to serialize key into buffer", e);
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
            ACC acc = (ACC) value;
            accSerializer.serialize(acc, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncAggregatingStateV2: failed to serialize accumulator", e);
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
            // null payload on AGGREGATING_ADD means "clear/delete this entry"
            return dest.appendEmpty();
        }
        try {
            valueOut.clear();
            @SuppressWarnings("unchecked")
            ACC acc = (ACC) payload;
            accSerializer.serialize(acc, valueOut);
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncAggregatingStateV2: failed to serialize accumulator into buffer",
                    e);
        }
    }

    @Override
    public Object deserializeValue(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return null;
        }
        try {
            valueIn.setBuffer(raw);
            return accSerializer.deserialize(valueIn);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncAggregatingStateV2: failed to deserialize accumulator", e);
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
            return accSerializer.deserialize(view);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncAggregatingStateV2: failed to decode accumulator off-heap", e);
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

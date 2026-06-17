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
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
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
import org.apache.flink.runtime.state.v2.AbstractValueState;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;
import org.apache.flink.state.forstrs.cache.ReducingAggregatingCache;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * V2 async ValueState for ForSt-RS. Extends Flink's {@link AbstractValueState} for the async state
 * API and implements {@link ForStRsInnerTable} for batch execution.
 */
@Internal
public class ForStRsValueStateV2<K, N, V> extends AbstractValueState<K, N, V>
        implements ValueState<V>, ForStRsInnerTable<K, N, V> {

    private static final byte[] KEY_PREFIX = "k/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SLASH = "/".getBytes(StandardCharsets.UTF_8);

    /**
     * Cleanup-A5 (zero-copy): registration-time ordinal counter. Each ValueStateV2 instance gets a
     * dense, monotonically-increasing integer at construction so the per-{@link RecordContext}
     * cache slot can be a {@code Slot[]} indexed by ordinal — no {@code Map.put}, no
     * {@code String} hashing/concat per call. Counter is process-static; only ValueStateV2
     * consumes {@code ctx.getExtra()} today, so its ordinal space is private to this class.
     */
    private static final AtomicInteger NEXT_STATE_ORDINAL = new AtomicInteger(0);

    /** Initial growth size for the per-ctx slot array. */
    private static final int INITIAL_SLOT_CAPACITY = 4;

    private final String stateName;
    private final byte[] stateNameBytes;

    /**
     * Cleanup-A5: dense ordinal assigned at construction. Used as the index into the {@code Slot[]}
     * the per-record {@link RecordContext} holds via {@link RecordContext#getExtra()}. Stable for
     * this instance's lifetime — the operator constructs each ValueStateV2 once at registration.
     */
    private final int stateOrdinal;

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<V> valueSerializer;
    /**
     * PR-A2 (S1-4 / E2-CRIT-1): namespace serializer used to encode the request namespace as the
     * trailing component of the storage composite key. Without this, all namespaces for the same
     * (key, stateName) pair collide on the same storage cell — silent cross-window state
     * corruption. May be {@code null} only for unit tests using void/no-op namespace; in
     * production the keyed backend supplies a real serializer.
     */
    private final TypeSerializer<N> namespaceSerializer;
    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    /**
     * Cleanup-A5: per-(stateOrdinal, namespace) cache slot. The {@code byte[]} is the composite
     * key; {@code nsIdentity} is the {@link System#identityHashCode} of the namespace whose bytes
     * produced it — invalidated by writing a new identity, no per-call String alloc.
     */
    static final class Slot {
        int nsIdentity;
        byte[] composite;
    }

    /**
     * R-2 (2026-06-17): per-instance write-back RMW cache for the value. This is the ValueState
     * analog of the {@link ReducingAggregatingCache} that
     * {@link ForStRsAsyncAggregatingStateV2}/{@code ForStRsAsyncReducingStateV2} already use, and of
     * the {@code MapStateCache} that {@link ForStRsMapStateV2} uses. NexMark q11 (session-window
     * count) and q17 (unbounded group-agg) both store their accumulator Row in a
     * {@code ValueState<RowData>} and do a per-record SYNC get→fold→put (the fold runs in the
     * generated SQL operator). Without this cache every record pays a real engine VALUE_GET +
     * VALUE_UPDATE; with it, a hot key's repeated touches collapse to in-cache reads/writes and
     * one barrier-time flush.
     *
     * <p>The cache "combiner" is last-write-wins replace: {@code (oldValue, newValue) -> newValue}.
     * That is how an {@code asyncUpdate(v)} maps onto {@link ReducingAggregatingCache#tryFold}
     * (fold the new value over whatever is cached, marking the slot dirty). Reads
     * ({@link #asyncValue}) return the cached (possibly dirty) value — read-your-writes — so the
     * UPDATE_BEFORE/UPDATE_AFTER changelog the operator computes is byte-identical to the
     * engine-backed path.
     */
    private final ReducingAggregatingCache<V, V> cache;

    /**
     * Pluggable flush handler invoked once per dirty entry on {@link #flushOnBarrier()} (and on LRU
     * eviction). Receives composite-key bytes and serialized value bytes ({@code null} value bytes
     * means "cleared" → DELETE). Default no-op so unit tests can exercise the cache without a live
     * engine; production overrides via {@link #setFlushHandler} (wired by the backend).
     */
    private volatile BiConsumer<byte[], byte[]> flushHandler = (k, v) -> {};

    /**
     * R-2: the write-back cache is only safe when a real (non-no-op) flush handler is wired AND the
     * instance is registered for the barrier drain — otherwise dirty cached values would be
     * silently discarded. The backend sets this via {@link #setFlushHandler}. The TTL-wrapped inner
     * ValueStateV2 (constructed but neither registered nor flush-wired) keeps the cache OFF and
     * flows through the existing engine path, so TTL semantics are unaffected.
     */
    private volatile boolean flushHandlerWired = false;

    /** STAGE-1 Task 7 / B-SPIKE: injected by the backend under two-regime; null otherwise. */
    @javax.annotation.Nullable
    private org.apache.flink.state.forstrs.exec.RegimeSwitch regimeSwitch;

    public void setRegimeSwitch(org.apache.flink.state.forstrs.exec.RegimeSwitch rs) {
        this.regimeSwitch = rs;
    }

    /**
     * RMW cache usable ⇔ NOT legacy-pipelined/parallel AND (no regime switch OR regime LIGHT). This
     * is the SAME predicate {@link ForStRsAsyncAggregatingStateV2#rmwCacheUsable} uses. Under the
     * uniform NexMark config ({@code FRS_RS_EXECUTOR} unset → inline executor →
     * {@code regimeSwitch == null}, single-threaded mailbox) the cache is ON — the regime q11/q17
     * run in. Under the opt-in parallel / pipelined executors the cache is bypassed and the value
     * flows through the existing batched VALUE_GET/VALUE_UPDATE path (the proven-correct parallel
     * config), so no query is robbed.
     */
    private boolean rmwCacheUsable() {
        if (!flushHandlerWired) {
            return false;
        }
        if (ForStRsMapStateV2.legacyPipelinedActive()
                || ForStRsMapStateV2.pipelinedExecutorActive()) {
            return false;
        }
        if (PARALLEL_EXECUTOR_ACTIVE) {
            return false;
        }
        return regimeSwitch == null || regimeSwitch.isLight();
    }

    /**
     * Snapshot of whether a parallel executor is selected. The per-worker key-group affinity that
     * makes {@code MapStateCache} correct under parallel is not (yet) plumbed for this cache, so we
     * bypass it whenever a parallel/pipelined executor is active — the single-threaded mailbox
     * (inline / default) is the only regime where this cache runs, exactly like the default
     * MapStateCache. Read once at construction (the executor mode is fixed for the backend's life).
     */
    private static final boolean PARALLEL_EXECUTOR_ACTIVE = parallelExecutorActiveEnv();

    private static boolean parallelExecutorActiveEnv() {
        String m = System.getenv("FRS_RS_EXECUTOR");
        if (m != null) {
            String t = m.trim();
            if (t.equals("coordinated")
                    || t.equals("routing")
                    || t.equals("routing-async")
                    || t.equals("two-regime")
                    || t.equals("adaptive")
                    || t.equals("routing-adaptive")) {
                return true;
            }
        }
        String legacy = System.getenv("FRS_RS_PARALLEL_EXECUTOR");
        return legacy != null && legacy.trim().equals("1");
    }

    public ForStRsValueStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer) {
        super(stateRequestHandler, valueSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.stateOrdinal = NEXT_STATE_ORDINAL.getAndIncrement();
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        // Last-write-wins combiner: asyncUpdate(v) folds v over the cached value by replacing it.
        this.cache = new ReducingAggregatingCache<>((old, in) -> in, this::flushEntry);
    }

    /** Test/diagnostic accessor for the per-instance ordinal. */
    public int stateOrdinal() {
        return stateOrdinal;
    }

    // -----------------------------------------------------------------
    // R-2 write-back RMW cache — async overrides
    // -----------------------------------------------------------------

    /**
     * Cache-mediated {@code asyncValue}. On a cache hit returns the cached (possibly dirty) value
     * immediately (zero engine I/O — read-your-writes). On a miss, falls through to the inherited
     * batched VALUE_GET and seeds the cache (via {@code putIfAbsent}-style gen check) so a
     * concurrent {@code asyncUpdate} wins.
     */
    @Override
    public StateFuture<V> asyncValue() {
        if (!rmwCacheUsable()) {
            return super.asyncValue();
        }
        int keyLen = writeCacheKeyToKeyOut();
        byte[] keyBuf = keyOut.getSharedBuffer();
        if (cache.contains(keyBuf, 0, keyLen)) {
            return StateFutureUtils.completedFuture(cache.peek(keyBuf, 0, keyLen));
        }
        final byte[] keySnapshot = new byte[keyLen];
        System.arraycopy(keyBuf, 0, keySnapshot, 0, keyLen);
        final long capturedGen = cache.currentGen(keySnapshot);
        return super.asyncValue()
                .thenApply(
                        engineValue -> {
                            // If a concurrent asyncUpdate/asyncClear already populated (or
                            // tombstoned) the slot while this GET was in flight, that value is
                            // fresher — return it, do NOT clobber it. Otherwise seed the cache
                            // from the engine value, gen-checked so a clear that fired during the
                            // GET still wins (putIfGen refuses the seed on a generation mismatch).
                            if (cache.contains(keySnapshot)) {
                                return cache.peek(keySnapshot);
                            }
                            cache.putIfGen(keySnapshot, engineValue, capturedGen);
                            return engineValue;
                        });
    }

    /**
     * Cache-mediated {@code asyncUpdate}. Folds the new value into the cache (last-write-wins
     * replace) and marks the slot dirty, returning a completed future — no per-record engine PUT.
     * The value is flushed to the engine on the next checkpoint barrier ({@link #flushOnBarrier()})
     * or when the LRU evicts the entry.
     */
    @Override
    public StateFuture<Void> asyncUpdate(V value) {
        if (!rmwCacheUsable()) {
            return super.asyncUpdate(value);
        }
        if (value == null) {
            // Match base-class contract: update(null) is not "clear". The off-heap V2 path
            // requireNonNulls a null VALUE_UPDATE payload; mirror that strictness here so a
            // null update surfaces the same way regardless of the cache being on.
            return super.asyncUpdate(value);
        }
        int keyLen = writeCacheKeyToKeyOut();
        byte[] keyBuf = keyOut.getSharedBuffer();
        // tryFold replaces the cached value (combiner is (old,in)->in) and marks dirty on hit.
        if (cache.tryFold(keyBuf, 0, keyLen, value)) {
            return StateFutureUtils.completedVoidFuture();
        }
        // Miss — seed the slot with the new value (dirty). put() snapshots the key bytes.
        cache.put(keyBuf, 0, keyLen, value);
        return StateFutureUtils.completedVoidFuture();
    }

    /**
     * Invalidates the cache slot for the cleared key (generation-bumped so an in-flight miss-resolve
     * cannot resurrect it) BEFORE the engine-side DELETE is enqueued by the inherited CLEAR path.
     * Without this a dirty cached value would survive the DELETE and the next barrier flush would
     * write it back, silently undoing the clear.
     */
    @Override
    public void onClear(StateRequest<K, N, ?, ?> request) {
        if (rmwCacheUsable()) {
            int keyLen = writeRequestKeyToKeyOut(request);
            cache.invalidate(keyOut.getSharedBuffer(), 0, keyLen);
        }
    }

    /**
     * Drains all dirty cache entries via the configured {@link #flushHandler}. Called by the backend
     * snapshot pre-flush (PHASE 1.d), alongside the Reducing/Aggregating RMW-cache drains.
     */
    public void flushOnBarrier() {
        cache.flushAllDirty();
    }

    /** Replaces the flush handler. Used by the backend to wire the production PUT/DELETE path. */
    @VisibleForTesting
    public void setFlushHandler(BiConsumer<byte[], byte[]> handler) {
        this.flushHandler = handler;
        this.flushHandlerWired = true;
    }

    /** Returns the number of cache entries. Exposed for tests / diagnostics. */
    @VisibleForTesting
    public int cacheSize() {
        return cache.size();
    }

    private void flushEntry(byte[] keyBytes, V value) {
        byte[] valBytes = value == null ? null : serializeValueBytes(value);
        flushHandler.accept(keyBytes, valBytes);
    }

    private byte[] serializeValueBytes(V value) {
        try {
            valueOut.clear();
            valueSerializer.serialize(value, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsValueStateV2: failed to serialize value for flush", e);
        }
    }

    /**
     * Writes the composite cache key into {@link #keyOut} from the AEC's current
     * {@link RecordContext} and returns its length. Mirrors {@link #serializeKey(StateRequest)} but
     * reads (K, N) from the current context, and leaves the bytes in the shared buffer for a
     * zero-alloc cache probe. Mirrors {@code ForStRsAsyncAggregatingStateV2.writeCacheKeyToKeyOut}.
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
                    "ForStRsValueStateV2: failed to serialize cache key", e);
        }
    }

    /** Same composite-key layout, but reads (K, N) from a StateRequest's own RecordContext. */
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
                    "ForStRsValueStateV2: failed to write cache invalidation key", e);
        }
    }

    // -- ForStRsInnerTable implementation --

    @Override
    @SuppressWarnings("unchecked")
    public byte[] serializeKey(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        // Cleanup-A2/A5 (zero-copy): the previous implementation allocated three heap byte[]s
        // per call (namespaceOut.getCopyOfBuffer, keyOut.getCopyOfBuffer, the final composite
        // new byte[len]) plus four System.arraycopy memcpys, plus a Map.put with a String key
        // concatenated from stateName + "::" + namespace-identityHashCode every call. All of
        // that violated the zero-copy invariant. The new path is:
        //   * single-sweep into keyOut (writes KEY_PREFIX, key, SLASH, stateNameBytes, SLASH,
        //     namespace bytes — no intermediate byte[]s, no arraycopy fan-out),
        //   * cache slot is a Slot[] indexed by per-instance stateOrdinal (set at construction),
        //     so the lookup is a single array load with no String hashing,
        //   * Slot.nsIdentity is the System.identityHashCode of the current namespace;
        //     identityHashCode is stable for a JVM-resident object and changes mean a different
        //     namespace, which forces a cache miss + re-serialize. identityHashCode collisions
        //     are theoretically possible but Flink's single-threaded operator hands us one
        //     namespace at a time from a small pool, and a false-positive hit would silently
        //     serve the wrong composite bytes — same risk as the previous implementation, which
        //     keyed off the same identityHashCode inside its String cacheKey.
        N namespace = request.getNamespace();
        int nsIdentity = System.identityHashCode(namespace);
        Object extra = ctx.getExtra();
        Slot[] slots;
        if (extra instanceof Slot[]) {
            slots = (Slot[]) extra;
            if (stateOrdinal < slots.length) {
                Slot s = slots[stateOrdinal];
                if (s != null && s.composite != null && s.nsIdentity == nsIdentity) {
                    return s.composite; // HIT: zero allocations on this path.
                }
            } else {
                // Grow: state ordinal climbed past the current slot array. Reuse the old slots.
                Slot[] grown = new Slot[Math.max(stateOrdinal + 1, slots.length * 2)];
                System.arraycopy(slots, 0, grown, 0, slots.length);
                slots = grown;
                ctx.setExtra(slots);
            }
        } else {
            int cap = Math.max(INITIAL_SLOT_CAPACITY, stateOrdinal + 1);
            slots = new Slot[cap];
            ctx.setExtra(slots);
        }
        // MISS: serialize composite key in a single sweep into keyOut, then snapshot to byte[].
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: trailing namespace bytes go directly into keyOut — no intermediate
            // namespaceOut buffer + getCopyOfBuffer round-trip. Pre-A2 v3.x snapshots are NOT
            // compatible (format break is independent of this cleanup).
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            byte[] composite = keyOut.getCopyOfBuffer(); // ONE alloc on miss.
            Slot s = slots[stateOrdinal];
            if (s == null) {
                s = new Slot();
                slots[stateOrdinal] = s;
            }
            s.nsIdentity = nsIdentity;
            s.composite = composite;
            return composite;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public byte[] serializeValue(Object value) {
        if (value == null) {
            return null;
        }
        try {
            valueOut.clear();
            valueSerializer.serialize((V) value, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize value", e);
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
            throw new RuntimeException("Failed to deserialize value", e);
        }
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result decode. Skips the per-row {@code
     * byte[] = new byte[len]} that the default {@link ForStRsInnerTable#deserializeValue(
     * java.lang.foreign.MemorySegment, long, int)} fallback would perform; instead reads
     * the value bytes directly from the native {@code outData} segment via {@link
     * org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView}.
     *
     * <p>B4-H5 (zero-copy): the view itself is held in a {@link ThreadLocal}, eliminating
     * the per-row {@code new MemorySegmentDataInputView()} that was happening inside the
     * batched-GET completion loop. The view is mutable (its backing segment + position
     * are reset by {@link MemorySegmentDataInputView#rewind}), so per-thread reuse is safe.
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
                    "ForStRsValueStateV2: failed to decode value off-heap", e);
        }
    }

    @Override
    public ForStRsDBGetRequest<K, N, ?> buildDBGetRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        return new ForStRsDBGetRequest<>(key, request, this);
    }

    @Override
    public ForStRsDBPutRequest<K, N, ?> buildDBPutRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        byte[] value = null;
        if (request.getRequestType() == StateRequestType.VALUE_UPDATE) {
            value = serializeValue(request.getPayload());
        }
        return new ForStRsDBPutRequest<>(key, value, request);
    }

    // -- Vectorized serialization (writes directly into off-heap buffer) --

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
            // PR-A2: append namespace bytes to match the byte-array serializeKey() path.
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return dest.append(keyOut.getSharedBuffer(), 0, keyOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int serializeValueInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        if (request.getRequestType() != StateRequestType.VALUE_UPDATE) {
            return dest.appendEmpty();
        }
        Object payload = request.getPayload();
        // R16-L3: symmetry guard with the V1 sync update() path (ForStRsValueState.update()
        // calls Objects.requireNonNull on its newValue argument). Pre-fix, the V2 vectorized
        // path silently treated a null update payload as a delete-via-empty-bytes encoding
        // (appendEmpty), which is a different semantic than the V1 path's clear NullPointer-
        // Exception. Callers using {@link ValueState#asyncUpdate(Object)} with null should
        // route through {@link ValueState#asyncClear()} instead — making this explicit at the
        // backend boundary prevents accidental "update(null) ≡ clear()" code patterns from
        // silently working on V2 while throwing on V1.
        Objects.requireNonNull(payload, "VALUE_UPDATE payload must not be null; use clear() instead");
        try {
            valueOut.clear();
            valueSerializer.serialize((V) payload, valueOut);
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize value", e);
        }
    }
}

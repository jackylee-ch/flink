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
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractMapState;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;
import org.apache.flink.state.forstrs.ForStRsIterableState;
import org.apache.flink.state.forstrs.IteratorEntryView;
import org.apache.flink.state.forstrs.cache.MapStateCache;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * V2 async MapState for ForSt-RS. Key encoding appends the serialized user key to the composite
 * prefix: {@code "k/" + serialize(K) + "/" + stateName + "/" + serialize(UK)}.
 */
@Internal
public class ForStRsMapStateV2<K, N, UK, UV> extends AbstractMapState<K, N, UK, UV>
        implements MapState<UK, UV>,
                ForStRsInnerTable<K, N, UV>,
                ForStRsIterableState<K, N, UK, UV> {

    private static final byte[] KEY_PREFIX = "k/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SLASH = "/".getBytes(StandardCharsets.UTF_8);

    private final String stateName;
    private final byte[] stateNameBytes;
    private final TypeSerializer<K> keySerializer;
    /**
     * PR-A2 (S1-4 / E2-CRIT-1): namespace serializer used to encode the request namespace into
     * the composite key between the stateName SLASH and the user key. Layout:
     * {@code [KEY_PREFIX][serialize(K)][/][stateName][/][serialize(N)][serialize(UK)]}.
     * Without this, MapState entries for distinct namespaces collide on the same storage row.
     * NOTE: this is a hard format break vs v3.x snapshots — restoring a pre-A2 snapshot will
     * surface as missing entries, not silent corruption.
     */
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;
    private final DataOutputSerializer keyOut = new DataOutputSerializer(128);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    /**
     * Per-state-instance LRU cache for (operatorKey, userKey) → value lookups. Eliminates engine
     * round-trips for repeated reads of the same map entry within a window. See {@link
     * MapStateCache} for semantics (write-through, LRU 256K cap, single-threaded).
     */
    private final MapStateCache<UV> cache = new MapStateCache<>();

    /**
     * PR-C1 (V2-8 / Z3-6 / C-H5): per-state off-heap staging buffer mirroring the V1-sync
     * {@code statebuf} path. Non-null only when the backend constructed this state with the
     * off-heap-aware constructor (i.e. when {@code linker/db/cf} are available — which is always
     * true in production; the legacy constructor stays as null-buffer for unit tests that don't
     * have a live engine handle).
     *
     * <p>Writes stage here and skip the V2 columnar dispatch entirely until {@link #flushOffHeapBuffer}
     * is called by the backend snapshot pre-hook or the buffer hits its auto-flush watermark.
     */
    @Nullable private final MapStateArrowBuffer offHeapBuf;

    @Nullable private final ForStRsLinker linker;
    @Nullable private final FrsDb db;
    @Nullable private final FrsCfHandle cf;

    @SuppressWarnings("unchecked")
    public ForStRsMapStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer) {
        this(
                stateRequestHandler,
                stateName,
                keySerializer,
                namespaceSerializer,
                userKeySerializer,
                userValueSerializer,
                /* linker */ null,
                /* db */ null,
                /* cf */ null);
    }

    /**
     * PR-C1 off-heap-aware constructor. When {@code linker/db/cf} are non-null this state will
     * stage writes into an off-heap {@link MapStateArrowBuffer} that drains via
     * {@code linker.batchPut} on threshold or snapshot. When any of the three is null we fall
     * back to the V2 columnar-dispatch path (legacy behaviour, used by unit tests).
     */
    @SuppressWarnings("unchecked")
    public ForStRsMapStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer,
            @Nullable ForStRsLinker linker,
            @Nullable FrsDb db,
            @Nullable FrsCfHandle cf) {
        super(stateRequestHandler, (TypeSerializer<UV>) userValueSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.userKeySerializer = userKeySerializer;
        this.userValueSerializer = userValueSerializer;
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.offHeapBuf = (linker != null && db != null && cf != null) ? new MapStateArrowBuffer() : null;
    }

    // -----------------------------------------------------------------
    // Cache-mediated async overrides — bypass engine on cache hit.
    //
    // The base AbstractMapState methods (asyncGet/Put/Remove/Contains) are NOT final; we override
    // them and consult the per-state LRU cache before/instead-of dispatching to the framework.
    // Write-through semantics: PUT/REMOVE always update both the cache and the engine, so the two
    // are in sync after any successful write. There is no dirty state to flush at barriers.
    //
    // asyncClear() is final in AbstractKeyedState and cannot be overridden directly. PR-A6
    // (S1-11 / E2-HIGH-2) addresses this by hooking the CLEAR request inside
    // buildDBPutRequest() — both the LRU cache and the off-heap arrow buffer are invalidated for
    // the (operatorKey + namespace) prefix before the engine receives the prefix-delete, so a
    // subsequent asyncGet under the cleared namespace cannot return a stale cached value.
    // -----------------------------------------------------------------

    /**
     * Builds the cache key bytes for the given userKey, using the current operator key from the
     * AEC's current RecordContext. Mirrors {@link #serializeKey(StateRequest)} but for the override
     * path where we don't have a StateRequest yet.
     *
     * <p>PR-A2: includes serialized namespace bytes between stateName and userKey so the
     * MapStateCache distinguishes entries that share (operatorKey, stateName, userKey) but differ
     * by namespace. Otherwise window-keyed MapState reads/writes silently collide across windows.
     */
    private byte[] serializeMapEntryKey(UK userKey) {
        @SuppressWarnings("unchecked")
        AsyncExecutionController<K, ?> aec = (AsyncExecutionController<K, ?>) stateRequestHandler;
        RecordContext<K> ctx = aec.getCurrentContext();
        @SuppressWarnings("unchecked")
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
            userKeySerializer.serialize(userKey, keyOut);
            return keyOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize map entry key for cache", e);
        }
    }

    @Override
    public StateFuture<UV> asyncGet(UK userKey) {
        byte[] keyBytes = serializeMapEntryKey(userKey);
        MapStateCache.Lookup<UV> hit = cache.lookup(keyBytes);
        if (hit != null && hit.cached()) {
            // Cache hit (or known-missing tombstone) — return completed future immediately.
            return StateFutureUtils.completedFuture(hit.value());
        }
        // PR-C1: probe the off-heap buffer before falling through to the engine. A buffer hit
        // resolves locally; a buffer tombstone short-circuits to null without an engine probe.
        if (offHeapBuf != null) {
            MapStateArrowBuffer.Lookup bufHit = offHeapBuf.lookup(keyBytes);
            if (bufHit.cached) {
                UV resolved = bufHit.tombstone ? null : deserializeFromBuffer(bufHit.row);
                cache.put(keyBytes, resolved);
                return StateFutureUtils.completedFuture(resolved);
            }
        }
        // Miss — fall through, then populate cache on result.
        return super.asyncGet(userKey)
                .thenApply(
                        value -> {
                            cache.put(keyBytes, value);
                            return value;
                        });
    }

    @Override
    public StateFuture<Void> asyncPut(UK userKey, UV value) {
        byte[] keyBytes = serializeMapEntryKey(userKey);
        cache.put(keyBytes, value);
        // PR-C1: stage the PUT off-heap. Bypasses the V2 columnar dispatch until the buffer's
        // auto-flush watermark or the snapshot pre-hook drains it via linker.batchPut.
        if (offHeapBuf != null) {
            // PR-M3: avoid getCopyOfBuffer() — MapStateArrowBuffer copies the value bytes into
            // its own off-heap staging segment, so we can pass valueOut's shared buffer + length
            // and let it consume them synchronously. Eliminates one byte[] alloc per asyncPut.
            if (value == null) {
                offHeapBuf.putShared(keyBytes, 0, keyBytes.length, null, 0, 0, linker, db, cf);
            } else {
                try {
                    valueOut.clear();
                    @SuppressWarnings("unchecked")
                    UV uv = (UV) value;
                    userValueSerializer.serialize(uv, valueOut);
                    offHeapBuf.putShared(
                            keyBytes,
                            0,
                            keyBytes.length,
                            valueOut.getSharedBuffer(),
                            0,
                            valueOut.length(),
                            linker,
                            db,
                            cf);
                } catch (java.io.IOException e) {
                    throw new RuntimeException(
                            "Failed to serialize map value for off-heap staging", e);
                }
            }
            return StateFutureUtils.completedFuture(null);
        }
        return super.asyncPut(userKey, value);
    }

    @Override
    public StateFuture<Void> asyncRemove(UK userKey) {
        byte[] keyBytes = serializeMapEntryKey(userKey);
        cache.remove(keyBytes);
        if (offHeapBuf != null) {
            // Stage a buffer tombstone + drop any prior buffered PUT for this key. The engine
            // delete fires when the buffer is drained.
            offHeapBuf.remove(keyBytes, linker, db, cf);
            return StateFutureUtils.completedFuture(null);
        }
        return super.asyncRemove(userKey);
    }

    @Override
    public StateFuture<Boolean> asyncContains(UK userKey) {
        byte[] keyBytes = serializeMapEntryKey(userKey);
        MapStateCache.Lookup<UV> hit = cache.lookup(keyBytes);
        if (hit != null && hit.cached()) {
            return StateFutureUtils.completedFuture(hit.value() != null);
        }
        if (offHeapBuf != null) {
            MapStateArrowBuffer.Lookup bufHit = offHeapBuf.lookup(keyBytes);
            if (bufHit.cached) {
                return StateFutureUtils.completedFuture(!bufHit.tombstone);
            }
        }
        return super.asyncContains(userKey);
    }

    /**
     * Serializes a user value to a heap byte[] for off-heap staging. Reuses the per-instance
     * {@code valueOut} buffer the V2 framework already uses for {@link #serializeValue}, so this
     * adds no per-call allocation beyond the unavoidable {@code getCopyOfBuffer} (which the
     * MapStateArrowBuffer needs to memcpy into its off-heap data region).
     */
    @SuppressWarnings("unchecked")
    private byte[] serializeUserValue(UV value) {
        if (value == null) {
            return null;
        }
        try {
            valueOut.clear();
            userValueSerializer.serialize(value, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize map value for off-heap staging", e);
        }
    }

    /**
     * Deserializes a user value out of the off-heap buffer's value-data segment for the given
     * row. Uses the zero-copy {@code MemorySegmentDataInputView} path (PR-B1).
     */
    private UV deserializeFromBuffer(int row) {
        MemorySegment vd = offHeapBuf.valueDataSegment();
        int vOff = offHeapBuf.valueOffsetOf(row);
        int vLen = offHeapBuf.valueLengthOf(row);
        if (vLen == 0) {
            return null;
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view =
                    new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView();
            view.rewind(vd, vOff, vLen);
            return userValueSerializer.deserialize(view);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to deserialize map value from off-heap buffer", e);
        }
    }

    /**
     * PR-C1 pre-snapshot flush hook. Called by the backend's {@code snapshot()} Trace E barrier
     * drain to push all staged writes + tombstones to the engine BEFORE the snapshot reads from
     * the engine. No-op when {@link #offHeapBuf} is null (legacy/test path).
     */
    public void flushOffHeapBuffer() {
        if (offHeapBuf != null) {
            offHeapBuf.flushTo(linker, db, cf);
        }
    }

    /** Visible for tests: returns the underlying buffer, or null if running in legacy mode. */
    @Nullable
    public MapStateArrowBuffer offHeapBufferForTests() {
        return offHeapBuf;
    }

    /**
     * D5-H2: releases the per-state {@link MapStateCache} arena. Invoked by the async backend
     * during {@code dispose()} so every {@code Arena.ofShared()} allocated by the cache (5
     * off-heap segments: keyOffsets, keyLengths, keyData, hashIndex, accessTime) is reclaimed
     * promptly instead of surviving to JVM exit.
     *
     * <p>Idempotent — safe to call multiple times. Mirrors the cache's idempotent {@code close}
     * contract.
     */
    public void close() {
        cache.close();
    }

    /** Visible for tests: exposes the internal cache so tests can assert lifecycle behaviour. */
    public MapStateCache<UV> cacheForTests() {
        return cache;
    }

    @Override
    public byte[] serializeKey(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        Object payload = request.getPayload();
        StateRequestType type = request.getRequestType();
        N namespace = request.getNamespace();
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: namespace BEFORE user key so per-namespace prefix scans remain
            // contiguous (getIterPrefix builds the same prefix up to-and-including namespace).
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            if (type == StateRequestType.MAP_GET
                    || type == StateRequestType.MAP_PUT
                    || type == StateRequestType.MAP_CONTAINS
                    || type == StateRequestType.MAP_REMOVE) {
                @SuppressWarnings("unchecked")
                UK userKey =
                        (UK)
                                (type == StateRequestType.MAP_PUT
                                        ? ((Tuple2<?, ?>) payload).f0
                                        : payload);
                userKeySerializer.serialize(userKey, keyOut);
            }
            return keyOut.getCopyOfBuffer();
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
            userValueSerializer.serialize((UV) value, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize map value", e);
        }
    }

    @Override
    public Object deserializeValue(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return null;
        }
        try {
            valueIn.setBuffer(raw);
            return userValueSerializer.deserialize(valueIn);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize map value", e);
        }
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result decode. Reads the user-value bytes
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
    public Object deserializeValue(MemorySegment buf, long offset, int len) {
        if (len == 0) {
            return null;
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view = VIEW_TL.get();
            view.rewind(buf, (int) offset, len);
            return userValueSerializer.deserialize(view);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsMapStateV2: failed to decode user value off-heap", e);
        }
    }

    @Override
    public ForStRsDBGetRequest<K, N, ?> buildDBGetRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        return new ForStRsDBGetRequest<>(key, request, this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ForStRsDBPutRequest<K, N, ?> buildDBPutRequest(StateRequest<K, N, ?, ?> request) {
        StateRequestType type = request.getRequestType();
        // PR-A6 (S1-11 / E2-HIGH-2): when the framework dispatches asyncClear(), invalidate the
        // per-instance caches BEFORE the engine sees the prefix-delete. Without this, the cache
        // and the off-heap arrow buffer continue to hold the pre-clear entries for the cleared
        // (operatorKey, namespace) and a subsequent asyncGet returns a stale value even though
        // the engine has correctly deleted the row. asyncClear() itself is final on
        // AbstractKeyedState (can't override), so we hook the request-build step which the AEC
        // invokes inside the same RecordContext lock that serializes all per-record state ops.
        if (type == StateRequestType.CLEAR) {
            byte[] prefix = getIterPrefix(request);
            cache.clearForPrefix(prefix);
            if (offHeapBuf != null) {
                offHeapBuf.clearForPrefix(prefix, linker, db, cf);
            }
        }
        byte[] key = serializeKey(request);
        byte[] value = null;
        if (type == StateRequestType.MAP_PUT) {
            Tuple2<?, ?> tuple = (Tuple2<?, ?>) request.getPayload();
            value = serializeValue(tuple.f1);
        }
        return new ForStRsDBPutRequest<>(key, value, request);
    }

    // -- Vectorized serialization (writes directly into off-heap buffer) --

    @Override
    public int serializeKeyInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        RecordContext<K> ctx = request.getRecordContext();
        Object payload = request.getPayload();
        StateRequestType type = request.getRequestType();
        N namespace = request.getNamespace();
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: namespace bytes before user key.
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            if (type == StateRequestType.MAP_GET
                    || type == StateRequestType.MAP_PUT
                    || type == StateRequestType.MAP_CONTAINS
                    || type == StateRequestType.MAP_REMOVE) {
                @SuppressWarnings("unchecked")
                UK userKey =
                        (UK)
                                (type == StateRequestType.MAP_PUT
                                        ? ((Tuple2<?, ?>) payload).f0
                                        : payload);
                userKeySerializer.serialize(userKey, keyOut);
            }
            return dest.append(keyOut.getSharedBuffer(), 0, keyOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int serializeValueInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        if (request.getRequestType() != StateRequestType.MAP_PUT) {
            return dest.appendEmpty();
        }
        Tuple2<?, ?> tuple = (Tuple2<?, ?>) request.getPayload();
        if (tuple == null || tuple.f1 == null) {
            return dest.appendEmpty();
        }
        try {
            valueOut.clear();
            userValueSerializer.serialize((UV) tuple.f1, valueOut);
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize map value", e);
        }
    }

    // --- ForStRsIterableState implementation ---

    @Override
    public byte[] getIterPrefix(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        N namespace = request.getNamespace();
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: include namespace in iter prefix so an entries() scan only returns
            // entries from the current namespace. Without this, the scan would leak entries
            // across all namespaces sharing the same (operatorKey, stateName) prefix.
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return keyOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize iter prefix", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public UK deserializeUserKey(byte[] rawKey, int userKeyOffset) {
        try {
            DataInputDeserializer in =
                    new DataInputDeserializer(rawKey, userKeyOffset, rawKey.length - userKeyOffset);
            return userKeySerializer.deserialize(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize user key", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public UV deserializeUserValue(byte[] rawValue) {
        if (rawValue == null || rawValue.length == 0) {
            return null;
        }
        return (UV) deserializeValue(rawValue);
    }

    // --- Slice-based decoders (Commit B): skip the outer per-entry byte[] copy by reading the
    // key/value range directly out of the chunk MemorySegment. The inner DataInputDeserializer
    // still consumes a byte[] (Flink's serializer contract); eliminating that final alloc requires
    // a MemorySegment-backed DataInputView and is deferred to V1.2. ---

    @Override
    public UK deserializeUserKey(IteratorEntryView view, int userKeyPrefixOffset) {
        try {
            int rangeLen = view.keyLength() - userKeyPrefixOffset;
            byte[] buf = new byte[rangeLen];
            MemorySegment.copy(
                    view.chunkBuf(),
                    ValueLayout.JAVA_BYTE,
                    view.keyOffset() + userKeyPrefixOffset,
                    buf,
                    0,
                    rangeLen);
            DataInputDeserializer in = new DataInputDeserializer(buf, 0, rangeLen);
            return userKeySerializer.deserialize(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize user key from view", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public UV deserializeUserValue(IteratorEntryView view) {
        if (view.isValueEmpty()) {
            return null;
        }
        try {
            int len = view.valueLength();
            byte[] buf = new byte[len];
            MemorySegment.copy(
                    view.chunkBuf(), ValueLayout.JAVA_BYTE, view.valueOffset(), buf, 0, len);
            DataInputDeserializer in = new DataInputDeserializer(buf, 0, len);
            return (UV) userValueSerializer.deserialize(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize user value from view", e);
        }
    }

    @Override
    public StateRequestHandler getStateRequestHandler() {
        return stateRequestHandler;
    }

    @Override
    public State asState() {
        return this;
    }
}

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
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * V2 async ValueState for ForSt-RS. Extends Flink's {@link AbstractValueState} for the async state
 * API and implements {@link ForStRsInnerTable} for batch execution.
 */
@Internal
public class ForStRsValueStateV2<K, N, V> extends AbstractValueState<K, N, V>
        implements ValueState<V>, ForStRsInnerTable<K, N, V> {

    private static final byte[] KEY_PREFIX = "k/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SLASH = "/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final String stateName;
    private final byte[] stateNameBytes;
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
    private final DataOutputSerializer namespaceOut = new DataOutputSerializer(32);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    public ForStRsValueStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer) {
        super(stateRequestHandler, valueSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
    }

    // -- ForStRsInnerTable implementation --

    @Override
    @SuppressWarnings("unchecked")
    public byte[] serializeKey(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        // PR-A5 (S1-10 / A1-H6 fix): RecordContext.extra is shared across ValueStates within
        // the same operator. Previously stored a single byte[] composite key, which meant a
        // second ValueState would read the FIRST state's composite (wrong stateName encoded)
        // — silent cross-state corruption. Now keyed by stateName via a Map<String, byte[]>.
        //
        // PR-A2 (S1-4 / E2-CRIT-1): cache slot must also be invalidated on namespace switch,
        // otherwise the same (key, stateName) entry serves stale composite bytes that point at
        // the previously-encoded namespace. We key the slot by
        // `stateName + "::" + System.identityHashCode(namespace)`. identityHashCode is constant
        // for the lifetime of a namespace instance (single-threaded operator thread sees stable
        // refs from Flink's window/timer scheduler), and a different namespace object yields a
        // different cache miss, forcing fresh composite-key serialization.
        N namespace = request.getNamespace();
        String cacheKey = stateName + "::" + System.identityHashCode(namespace);
        Object extra = ctx.getExtra();
        java.util.Map<String, byte[]> slot;
        if (extra instanceof java.util.Map<?, ?>) {
            slot = (java.util.Map<String, byte[]>) extra;
            byte[] cached = slot.get(cacheKey);
            if (cached != null) {
                return cached;
            }
        } else {
            slot = new java.util.HashMap<>(4);
            ctx.setExtra(slot);
        }
        try {
            // PR-A2: encode namespace bytes as the trailing component of the composite key.
            // Pre-A2 v3.x snapshots are NOT compatible with this format — the key format
            // changed from `[KEY_PREFIX][key][/][stateName][/]` to
            // `[KEY_PREFIX][key][/][stateName][/][namespaceBytes]`. Restoring an old snapshot
            // surfaces as missing keys (loud "value() == null" reads), not silent corruption.
            // See RELEASE-NOTES: "v4.0 keyed-state binary format is incompatible with v3.x".
            byte[] namespaceBytes;
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceOut.clear();
                namespaceSerializer.serialize(namespace, namespaceOut);
                namespaceBytes = namespaceOut.getCopyOfBuffer();
            } else {
                namespaceBytes = EMPTY_BYTES;
            }
            keyOut.clear();
            keySerializer.serialize(ctx.getKey(), keyOut);
            byte[] keyBytes = keyOut.getCopyOfBuffer();
            int len =
                    KEY_PREFIX.length
                            + keyBytes.length
                            + SLASH.length
                            + stateNameBytes.length
                            + SLASH.length
                            + namespaceBytes.length;
            byte[] composite = new byte[len];
            int off = 0;
            System.arraycopy(KEY_PREFIX, 0, composite, off, KEY_PREFIX.length);
            off += KEY_PREFIX.length;
            System.arraycopy(keyBytes, 0, composite, off, keyBytes.length);
            off += keyBytes.length;
            System.arraycopy(SLASH, 0, composite, off, SLASH.length);
            off += SLASH.length;
            System.arraycopy(stateNameBytes, 0, composite, off, stateNameBytes.length);
            off += stateNameBytes.length;
            System.arraycopy(SLASH, 0, composite, off, SLASH.length);
            off += SLASH.length;
            System.arraycopy(namespaceBytes, 0, composite, off, namespaceBytes.length);
            slot.put(cacheKey, composite);
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
     */
    @Override
    public Object deserializeValue(java.lang.foreign.MemorySegment buf, long offset, int len) {
        if (len == 0) {
            return null;
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view =
                    new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView();
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
        if (payload == null) {
            return dest.appendEmpty();
        }
        try {
            valueOut.clear();
            valueSerializer.serialize((V) payload, valueOut);
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize value", e);
        }
    }
}

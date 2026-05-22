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
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractListState;
import org.apache.flink.runtime.state.v2.adaptor.CompleteStateIterator;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Async-V2 ListState for ForSt-RS. Extends {@link AbstractListState} (which wires asyncGet /
 * asyncAdd / asyncUpdate / asyncAddAll via the StateRequestHandler) and implements {@link
 * ForStRsInnerTable} so it participates in the vectorized batch-dispatch path.
 *
 * <p>Storage encoding: the list is serialised as {@code [count:int][elem0][elem1]...}, written by a
 * single PUT. LIST_ADD (single element) encodes as count=1 so it is also a full PUT of the
 * one-element list (simple but correct; append-merge optimisation is a follow-up).
 *
 * <p>The VectorizedClassifier already routes LIST_GET → GET, LIST_UPDATE / LIST_ADD / LIST_ADD_ALL
 * → PUT (non-null), CLEAR → DELETE. serializeValueInto handles all payload shapes.
 *
 * @param <K> backend key type
 * @param <N> namespace type
 * @param <V> list element type
 */
@Internal
public class ForStRsAsyncListStateV2<K, N, V> extends AbstractListState<K, N, V>
        implements ListState<V>, ForStRsInnerTable<K, N, List<V>> {

    private static final byte[] KEY_PREFIX = "k/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SLASH = "/".getBytes(StandardCharsets.UTF_8);

    private final String stateName;
    private final byte[] stateNameBytes;
    private final TypeSerializer<K> keySerializer;
    /**
     * PR-A2 (S1-4 / E2-CRIT-1): namespace serializer used to append serialized namespace bytes
     * as the trailing component of the composite key. Without this, ListState entries for
     * different windows collide on the same storage cell. Hard format break vs v3.x snapshots.
     */
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<V> elementSerializer;

    // Per-instance (single-threaded operator thread) serializers
    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(128);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    public ForStRsAsyncListStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> elementSerializer) {
        super(stateRequestHandler, elementSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.elementSerializer = elementSerializer;
    }

    // ---------------------------------------------------------------
    // ForStRsInnerTable — key serialization + state-name lookup (V20.2)
    // ---------------------------------------------------------------

    @Override
    public String getStateName() {
        return stateName;
    }

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
            throw new RuntimeException("ForStRsAsyncListStateV2: failed to serialize key", e);
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
            throw new RuntimeException("ForStRsAsyncListStateV2: failed to serialize key", e);
        }
    }

    // ---------------------------------------------------------------
    // ForStRsInnerTable — value serialization
    // ---------------------------------------------------------------

    /**
     * Serialises a {@code List<V>} as {@code [count:int][elem0][elem1]...}.
     * Called for LIST_UPDATE, LIST_ADD_ALL payloads. For LIST_ADD, payload is a single V
     * — handled by serializeValueInto / serializeValue(Object).
     */
    @Override
    public byte[] serializeValue(Object value) {
        if (value == null) {
            return null;
        }
        try {
            valueOut.clear();
            if (value instanceof List) {
                @SuppressWarnings("unchecked")
                List<V> list = (List<V>) value;
                valueOut.writeInt(list.size());
                for (V elem : list) {
                    elementSerializer.serialize(elem, valueOut);
                }
            } else {
                // Single element (LIST_ADD payload is bare V)
                @SuppressWarnings("unchecked")
                V elem = (V) value;
                valueOut.writeInt(1);
                elementSerializer.serialize(elem, valueOut);
            }
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("ForStRsAsyncListStateV2: failed to serialize value", e);
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
            return dest.appendEmpty();
        }
        try {
            valueOut.clear();
            if (type == StateRequestType.LIST_ADD) {
                // Single element
                @SuppressWarnings("unchecked")
                V elem = (V) payload;
                valueOut.writeInt(1);
                elementSerializer.serialize(elem, valueOut);
            } else {
                // LIST_UPDATE or LIST_ADD_ALL — payload is List<V>
                @SuppressWarnings("unchecked")
                List<V> list = (List<V>) payload;
                valueOut.writeInt(list.size());
                for (V elem : list) {
                    elementSerializer.serialize(elem, valueOut);
                }
            }
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncListStateV2: failed to serialize value into buffer", e);
        }
    }

    /**
     * Deserialises raw bytes into a {@link StateIterator}&lt;V&gt; wrapped in a {@link
     * CompleteStateIterator}. The VectorizedExecutor calls this for LIST_GET results and completes
     * the InternalAsyncFuture with the returned iterator.
     */
    @Override
    public Object deserializeValue(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return new CompleteStateIterator<V>(Collections.emptyList());
        }
        try {
            valueIn.setBuffer(raw);
            // V20.1 / Format B: loop [count][elems*] chunks until EOF. Backward-compatible
            // with legacy v3.8 single-chunk [count=1][elem] payloads (reads one chunk,
            // hits EOF, returns the singleton). Required for V3 (LIST_ADD → APPEND_MERGE)
            // because the engine merge-operator concatenates operand bytes verbatim,
            // producing a multi-chunk payload after K appends.
            List<V> list = new ArrayList<>();
            while (valueIn.available() > 0) {
                int count = valueIn.readInt();
                if (count < 0) {
                    throw new IOException(
                            "ForStRsAsyncListStateV2: negative count in merged payload: "
                                    + count);
                }
                for (int i = 0; i < count; i++) {
                    list.add(elementSerializer.deserialize(valueIn));
                }
            }
            return new CompleteStateIterator<>(list);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncListStateV2: failed to deserialize value", e);
        }
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result decode. Reads the multi-chunk payload
     * directly out of the native {@code outData} segment via {@link
     * org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView}, mirroring the
     * legacy {@link #deserializeValue(byte[])} logic but without the per-row
     * {@code byte[] = new byte[len]} the default fallback would perform.
     */
    @Override
    public Object deserializeValue(java.lang.foreign.MemorySegment buf, long offset, int len) {
        if (len == 0) {
            return new CompleteStateIterator<V>(Collections.emptyList());
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view =
                    new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView();
            view.rewind(buf, (int) offset, len);
            List<V> list = new ArrayList<>();
            while (view.remaining() > 0) {
                int count = view.readInt();
                if (count < 0) {
                    throw new IOException(
                            "ForStRsAsyncListStateV2: negative count in merged payload: "
                                    + count);
                }
                for (int i = 0; i < count; i++) {
                    list.add(elementSerializer.deserialize(view));
                }
            }
            return new CompleteStateIterator<>(list);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncListStateV2: failed to decode value off-heap", e);
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

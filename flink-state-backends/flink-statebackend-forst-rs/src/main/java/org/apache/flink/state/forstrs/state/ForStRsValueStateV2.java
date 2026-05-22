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

    private final String stateName;
    private final byte[] stateNameBytes;
    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<V> valueSerializer;
    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    public ForStRsValueStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<V> valueSerializer) {
        super(stateRequestHandler, valueSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.keySerializer = keySerializer;
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
        Object extra = ctx.getExtra();
        java.util.Map<String, byte[]> slot;
        if (extra instanceof java.util.Map<?, ?>) {
            slot = (java.util.Map<String, byte[]>) extra;
            byte[] cached = slot.get(stateName);
            if (cached != null) {
                return cached;
            }
        } else {
            slot = new java.util.HashMap<>(4);
            ctx.setExtra(slot);
        }
        try {
            keyOut.clear();
            keySerializer.serialize(ctx.getKey(), keyOut);
            byte[] keyBytes = keyOut.getCopyOfBuffer();
            int len =
                    KEY_PREFIX.length
                            + keyBytes.length
                            + SLASH.length
                            + stateNameBytes.length
                            + SLASH.length;
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
            slot.put(stateName, composite);
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
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
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

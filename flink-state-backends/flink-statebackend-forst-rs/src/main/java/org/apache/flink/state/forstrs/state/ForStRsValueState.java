/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.state.forstrs.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.bridge.ForStRsColumnFamily;
import org.apache.flink.state.forstrs.bridge.ForStRsDb;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * ValueState backed by ForSt-RS. The currently-keyed entry is resolved via
 * the classic {@code [KeyGroupPrefix][Key][Namespace]} encoding so the
 * on-disk layout stays compatible with the Flink ForSt backend.
 *
 * <p>This is a working prototype — production PR#3 wires it up through the
 * Flink keyed state backend lifecycle (register serializers, namespace
 * filters, resource tracking).
 */
public class ForStRsValueState<K, N, V> implements ValueState<V> {

    private final ForStRsDb db;
    private final ForStRsColumnFamily cf;
    private final TypeSerializer<V> valueSerializer;
    private final byte[] keyPrefix;
    private final DataOutputSerializer writeBuffer = new DataOutputSerializer(64);

    private byte[] currentKey;

    public ForStRsValueState(
            ForStRsDb db,
            ForStRsColumnFamily cf,
            TypeSerializer<V> valueSerializer,
            byte[] keyPrefix) {
        this.db = db;
        this.cf = cf;
        this.valueSerializer = valueSerializer;
        this.keyPrefix = keyPrefix;
    }

    /**
     * Updates the contextual key. Called by the runtime immediately before
     * any state operation for a new record.
     */
    public void setCurrentKey(byte[] serializedKey) {
        this.currentKey = concat(keyPrefix, serializedKey);
    }

    @Override
    @Nullable
    public V value() throws IOException {
        ensureCurrentKey();
        byte[] bytes = db.get(cf, currentKey);
        if (bytes == null) {
            return null;
        }
        DataInputDeserializer in = new DataInputDeserializer(bytes);
        return valueSerializer.deserialize(in);
    }

    @Override
    public void update(@Nullable V value) throws IOException {
        ensureCurrentKey();
        if (value == null) {
            db.delete(cf, currentKey);
            return;
        }
        writeBuffer.clear();
        valueSerializer.serialize(value, writeBuffer);
        byte[] bytes = writeBuffer.getCopyOfBuffer();
        db.put(cf, currentKey, bytes);
    }

    @Override
    public void clear() {
        ensureCurrentKey();
        try {
            db.delete(cf, currentKey);
        } catch (Exception ignore) {
            // clear() is best-effort per the Flink contract.
        }
    }

    private void ensureCurrentKey() {
        if (currentKey == null) {
            throw new IllegalStateException(
                    "ForStRsValueState: currentKey has not been set for this record");
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}

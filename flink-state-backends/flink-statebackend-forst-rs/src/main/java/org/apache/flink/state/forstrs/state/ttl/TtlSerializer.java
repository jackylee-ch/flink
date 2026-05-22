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

package org.apache.flink.state.forstrs.state.ttl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * Wraps a user value serializer so each value cell is stored as {@code [long expiryTs][value
 * bytes]}.
 *
 * <p>PR-A7 (S1-12). The 8-byte big-endian expiry timestamp comes first; the inner serializer
 * handles everything after. {@code copy(DataInputView, DataOutputView)} forwards the prefix
 * verbatim and then delegates the body copy to the inner serializer.
 *
 * <p><b>Storage format break:</b> see {@link TtlValue}.
 */
@Internal
public class TtlSerializer<V> extends TypeSerializer<TtlValue<V>> {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<V> valueSerializer;

    public TtlSerializer(TypeSerializer<V> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return valueSerializer.isImmutableType();
    }

    @Override
    public TypeSerializer<TtlValue<V>> duplicate() {
        TypeSerializer<V> innerDup = valueSerializer.duplicate();
        return innerDup == valueSerializer ? this : new TtlSerializer<>(innerDup);
    }

    @Override
    public TtlValue<V> createInstance() {
        return new TtlValue<>(Long.MAX_VALUE, valueSerializer.createInstance());
    }

    @Override
    public TtlValue<V> copy(TtlValue<V> from) {
        if (from == null) {
            return null;
        }
        V innerCopy = from.getValue() == null ? null : valueSerializer.copy(from.getValue());
        return new TtlValue<>(from.getExpiryTimestamp(), innerCopy);
    }

    @Override
    public TtlValue<V> copy(TtlValue<V> from, TtlValue<V> reuse) {
        // TtlValue is immutable; ignore the reuse hint.
        return copy(from);
    }

    @Override
    public int getLength() {
        int inner = valueSerializer.getLength();
        return inner < 0 ? -1 : (Long.BYTES + inner);
    }

    @Override
    public void serialize(TtlValue<V> record, DataOutputView target) throws IOException {
        target.writeLong(record.getExpiryTimestamp());
        V inner = record.getValue();
        if (inner == null) {
            // Inner is null only when caller explicitly wrote a TtlValue with null payload —
            // delegate to the inner serializer; some serializers may not tolerate null, so
            // callers should ensure the underlying state's null-handling is exercised by the
            // composite, not the serializer.
            throw new IOException(
                    "TtlSerializer cannot serialize a null inner value; "
                            + "tombstones should be encoded by state deletion, not TtlValue(null).");
        }
        valueSerializer.serialize(inner, target);
    }

    @Override
    public TtlValue<V> deserialize(DataInputView source) throws IOException {
        long expiry = source.readLong();
        V inner = valueSerializer.deserialize(source);
        return new TtlValue<>(expiry, inner);
    }

    @Override
    public TtlValue<V> deserialize(TtlValue<V> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        long expiry = source.readLong();
        target.writeLong(expiry);
        valueSerializer.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TtlSerializer)) {
            return false;
        }
        TtlSerializer<?> other = (TtlSerializer<?>) obj;
        return Objects.equals(valueSerializer, other.valueSerializer);
    }

    @Override
    public int hashCode() {
        return 0xC1A55ED + valueSerializer.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<TtlValue<V>> snapshotConfiguration() {
        throw new UnsupportedOperationException(
                "PR-A7: TtlSerializer snapshot configuration deferred to PR-A11 (state migration). "
                        + "Enabling TTL on existing state currently requires fresh state.");
    }
}

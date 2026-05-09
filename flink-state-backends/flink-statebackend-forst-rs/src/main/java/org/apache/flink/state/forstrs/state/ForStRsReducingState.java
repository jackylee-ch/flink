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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.io.IOException;

/**
 * Minimal {@link ReducingState} implementation backed by ForSt-RS via the {@link ForStRsLinker} FFM
 * bridge.
 *
 * <p>This is a Phase-D L4 stepping stone: it demonstrates the keyed-state bridging pattern for a
 * reducing-typed value end-to-end (serialize → put → get → deserialize → reduce → put) without yet
 * plugging into Flink's {@code AbstractKeyedStateBackend} composition. A real keyed-state binding
 * would derive the per-record key from the operator's {@code KeyContext} (key + namespace) and
 * concat with the state-id prefix; here we accept a fixed prefix at construction time and treat it
 * as the full ForSt key. That is sufficient for proof-of-concept round-trips and unit testing the
 * FFM contract, but not for production-quality keyed-state.
 *
 * <p><b>Storage encoding</b>: the single reduced value is serialized via the configured {@link
 * TypeSerializer} and stored under the construction-time {@code keyPrefix} — semantically the same
 * single-value layout used by {@link ForStRsValueState}.
 *
 * <p><b>Concurrency</b>: {@link #add(Object)} is implemented as read-modify-write across a separate
 * get + put, so concurrent appenders against the same key may lose updates. This is intentional for
 * the stepping-stone — RocksDB's ReducingState uses the merge-operator path to avoid this; that
 * optimization is deferred until the keyed-state binding lands and the payload-bound merge contract
 * is wired.
 *
 * @param <T> element / accumulator type
 */
@Internal
public class ForStRsReducingState<T> implements ReducingState<T> {

    /** Initial buffer size for value serialization (grows on demand). */
    private static final int DEFAULT_OUTPUT_BUFFER = 64;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final byte[] keyPrefix;
    private final TypeSerializer<T> serializer;
    private final ReduceFunction<T> reduceFunction;

    private final DataOutputSerializer outputBuffer;
    private final DataInputDeserializer inputBuffer;

    public ForStRsReducingState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            byte[] keyPrefix,
            TypeSerializer<T> serializer,
            ReduceFunction<T> reduceFunction) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = keyPrefix.clone();
        this.serializer = serializer;
        this.reduceFunction = reduceFunction;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    @Override
    public T get() throws IOException {
        return readValue();
    }

    @Override
    public void add(T value) throws Exception {
        if (value == null) {
            // Per Flink's AppendingState javadoc the behavior on null is undefined; we treat it as
            // a no-op to avoid invoking the reducer with a null operand and corrupting the state.
            return;
        }
        T current = readValue();
        T next = (current == null) ? value : reduceFunction.reduce(current, value);
        writeValue(next);
    }

    @Override
    public void clear() {
        linker.delete(db, cf, computeKey());
    }

    /** Reads the encoded value under {@link #computeKey()} or returns {@code null} if absent. */
    private T readValue() throws IOException {
        byte[] raw = linker.lookupKv(db, cf, computeKey());
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        return serializer.deserialize(inputBuffer);
    }

    /** Serializes {@code value} (assumed non-null) and stores it. */
    private void writeValue(T value) throws IOException {
        outputBuffer.clear();
        serializer.serialize(value, outputBuffer);
        byte[] payload = outputBuffer.getCopyOfBuffer();
        linker.put(db, cf, computeKey(), payload);
    }

    /**
     * Returns the ForSt-RS key for the current logical entry. In this stepping-stone, we use the
     * constructor-supplied prefix verbatim — a full keyed-state binding will append the per-record
     * key + namespace here.
     */
    private byte[] computeKey() {
        return keyPrefix;
    }
}

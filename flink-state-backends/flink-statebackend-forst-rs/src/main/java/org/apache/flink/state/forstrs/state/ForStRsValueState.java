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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.io.IOException;

/**
 * Minimal {@link ValueState} implementation backed by ForSt-RS via the {@link ForStRsLinker} FFM
 * bridge.
 *
 * <p>This is a Phase-D L4 stepping stone: it demonstrates the keyed-state bridging pattern
 * end-to-end (serialize → put → get → deserialize) without yet plugging into Flink's {@code
 * AbstractKeyedStateBackend} composition. A real keyed-state binding would derive the per-record
 * key from the operator's {@code KeyContext} (key + namespace) and concat with the state-id prefix;
 * here we accept a fixed prefix at construction time and treat it as the full ForSt key. That is
 * sufficient for proof-of-concept round-trips and unit testing the FFM contract, but not for
 * production-quality keyed-state.
 *
 * @param <T> element type
 */
@Internal
public class ForStRsValueState<T> implements ValueState<T> {

    /** Initial buffer size for value serialization (grows on demand). */
    private static final int DEFAULT_OUTPUT_BUFFER = 64;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final byte[] keyPrefix;
    private final TypeSerializer<T> serializer;

    private final DataOutputSerializer outputBuffer;
    private final DataInputDeserializer inputBuffer;

    public ForStRsValueState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            byte[] keyPrefix,
            TypeSerializer<T> serializer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = keyPrefix.clone();
        this.serializer = serializer;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    @Override
    public T value() throws IOException {
        byte[] raw = linker.get(db, cf, computeKey());
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        return serializer.deserialize(inputBuffer);
    }

    @Override
    public void update(T value) throws IOException {
        if (value == null) {
            // Per Flink's contract: null update behaves as clear.
            clear();
            return;
        }
        outputBuffer.clear();
        serializer.serialize(value, outputBuffer);
        byte[] payload = outputBuffer.getCopyOfBuffer();
        linker.put(db, cf, computeKey(), payload);
    }

    @Override
    public void clear() {
        linker.delete(db, cf, computeKey());
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

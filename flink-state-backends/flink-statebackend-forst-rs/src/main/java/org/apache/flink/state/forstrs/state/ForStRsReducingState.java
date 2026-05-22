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
import java.util.function.Supplier;

/**
 * Minimal {@link ReducingState} implementation backed by ForSt-RS via the {@link ForStRsLinker} FFM
 * bridge. See {@link ForStRsValueState} for a description of the two construction modes (static
 * byte[] prefix vs. kg-prefixed lazy compute per spec §6).
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
    private final Supplier<byte[]> keyComputer;
    private final TypeSerializer<T> serializer;
    private final ReduceFunction<T> reduceFunction;

    private final DataOutputSerializer outputBuffer;
    private final DataInputDeserializer inputBuffer;

    /** Legacy byte[]-prefix constructor. */
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
        this.keyComputer = null;
        this.serializer = serializer;
        this.reduceFunction = reduceFunction;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    /** Spec §6 kg-prefixed constructor. */
    public ForStRsReducingState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            TypeSerializer<T> serializer,
            ReduceFunction<T> reduceFunction,
            Supplier<byte[]> keyComputer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = null;
        this.keyComputer = keyComputer;
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

    private T readValue() throws IOException {
        byte[] raw = linker.lookupKv(db, cf, computeKey());
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        return serializer.deserialize(inputBuffer);
    }

    private void writeValue(T value) throws IOException {
        outputBuffer.clear();
        serializer.serialize(value, outputBuffer);
        // PR-E4: reuse the serializer's internal buffer; the engine consumes the value
        // bytes synchronously inside the critical-mode FFM call, so no defensive copy is
        // required. Eliminates one byte[] allocation per add().
        linker.put(
                db,
                cf,
                computeKey(),
                outputBuffer.getSharedBuffer(),
                0,
                outputBuffer.length());
    }

    private byte[] computeKey() {
        if (keyComputer != null) {
            return keyComputer.get();
        }
        return keyPrefix;
    }
}

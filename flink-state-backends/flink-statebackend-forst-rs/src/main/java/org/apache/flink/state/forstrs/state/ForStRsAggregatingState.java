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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Minimal {@link AggregatingState} implementation backed by ForSt-RS via the {@link ForStRsLinker}
 * FFM bridge. See {@link ForStRsValueState} for a description of the two construction modes (static
 * byte[] prefix vs. kg-prefixed lazy compute per spec §6).
 *
 * @param <IN> input value type
 * @param <ACC> accumulator type (the type that is serialized and persisted)
 * @param <OUT> output / result type
 */
@Internal
public class ForStRsAggregatingState<IN, ACC, OUT> implements AggregatingState<IN, OUT> {

    /** Initial buffer size for accumulator serialization (grows on demand). */
    private static final int DEFAULT_OUTPUT_BUFFER = 64;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final byte[] keyPrefix;
    private final Supplier<byte[]> keyComputer;
    private final TypeSerializer<ACC> accSerializer;
    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    private final DataOutputSerializer outputBuffer;
    private final DataInputDeserializer inputBuffer;

    /** Legacy byte[]-prefix constructor. */
    public ForStRsAggregatingState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            byte[] keyPrefix,
            TypeSerializer<ACC> accSerializer,
            AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = keyPrefix.clone();
        this.keyComputer = null;
        this.accSerializer = accSerializer;
        this.aggregateFunction = aggregateFunction;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    /** Spec §6 kg-prefixed constructor. */
    public ForStRsAggregatingState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            TypeSerializer<ACC> accSerializer,
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            Supplier<byte[]> keyComputer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = null;
        this.keyComputer = keyComputer;
        this.accSerializer = accSerializer;
        this.aggregateFunction = aggregateFunction;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    @Override
    public OUT get() throws IOException {
        ACC acc = readAccumulator();
        if (acc == null) {
            return null;
        }
        return aggregateFunction.getResult(acc);
    }

    @Override
    public void add(IN value) throws IOException {
        ACC current = readAccumulator();
        if (current == null) {
            current = aggregateFunction.createAccumulator();
        }
        ACC next = aggregateFunction.add(value, current);
        if (next == null) {
            clear();
            return;
        }
        writeAccumulator(next);
    }

    @Override
    public void clear() {
        linker.delete(db, cf, computeKey());
    }

    private ACC readAccumulator() throws IOException {
        byte[] raw = linker.lookupKv(db, cf, computeKey());
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        return accSerializer.deserialize(inputBuffer);
    }

    private void writeAccumulator(ACC acc) throws IOException {
        outputBuffer.clear();
        accSerializer.serialize(acc, outputBuffer);
        byte[] payload = outputBuffer.getCopyOfBuffer();
        linker.put(db, cf, computeKey(), payload);
    }

    private byte[] computeKey() {
        if (keyComputer != null) {
            return keyComputer.get();
        }
        return keyPrefix;
    }
}

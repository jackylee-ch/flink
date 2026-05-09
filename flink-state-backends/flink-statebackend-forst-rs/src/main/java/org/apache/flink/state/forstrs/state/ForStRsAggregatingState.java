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

/**
 * Minimal {@link AggregatingState} implementation backed by ForSt-RS via the {@link ForStRsLinker}
 * FFM bridge.
 *
 * <p>This is a Phase-D L4 stepping stone: it demonstrates the keyed-state bridging pattern for an
 * aggregating-typed value end-to-end (serialize accumulator → put → get → deserialize → add → put)
 * without yet plugging into Flink's {@code AbstractKeyedStateBackend} composition. A real
 * keyed-state binding would derive the per-record key from the operator's {@code KeyContext} (key +
 * namespace) and concat with the state-id prefix; here we accept a fixed prefix at construction
 * time and treat it as the full ForSt key. That is sufficient for proof-of-concept round-trips and
 * unit testing the FFM contract, but not for production-quality keyed-state.
 *
 * <p><b>Type-parameter discipline.</b> The {@link AggregateFunction} carries three type parameters
 * &mdash; {@code IN} (input), {@code ACC} (accumulator), and {@code OUT} (result). Only the
 * accumulator is persisted, so the configured {@link TypeSerializer} must be the one for {@code
 * ACC}. Do <em>not</em> pass an {@code IN} or {@code OUT} serializer here &mdash; many subtle bugs
 * arise from confusing the three. {@link #get()} reads the {@code ACC}, then projects through
 * {@link AggregateFunction#getResult(Object)} to produce {@code OUT}; {@link #add(Object)} reads
 * the {@code ACC}, calls {@link AggregateFunction#add(Object, Object)}, and writes the resulting
 * {@code ACC} back.
 *
 * <p><b>Storage encoding</b>: the single accumulator is serialized via the configured
 * {@code TypeSerializer<ACC>} and stored under the construction-time {@code keyPrefix} &mdash;
 * semantically the same single-value layout used by {@link ForStRsValueState} and {@link
 * ForStRsReducingState}.
 *
 * <p><b>Concurrency</b>: {@link #add(Object)} is implemented as read-modify-write across a separate
 * get + put, so concurrent appenders against the same key may lose updates. This is intentional for
 * the stepping-stone &mdash; RocksDB's AggregatingState has the same property; merge-operator
 * acceleration is deferred until the keyed-state binding lands and the payload-bound merge contract
 * is wired.
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
    private final TypeSerializer<ACC> accSerializer;
    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    private final DataOutputSerializer outputBuffer;
    private final DataInputDeserializer inputBuffer;

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
            // Defensive: if the user-supplied function returns a null accumulator, treat that as
            // "no state to persist" rather than serializing a null reference.
            clear();
            return;
        }
        writeAccumulator(next);
    }

    @Override
    public void clear() {
        linker.delete(db, cf, computeKey());
    }

    /**
     * Reads the encoded accumulator under {@link #computeKey()} or returns {@code null} if absent.
     */
    private ACC readAccumulator() throws IOException {
        byte[] raw = linker.lookupKv(db, cf, computeKey());
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        return accSerializer.deserialize(inputBuffer);
    }

    /** Serializes {@code acc} (assumed non-null) and stores it. */
    private void writeAccumulator(ACC acc) throws IOException {
        outputBuffer.clear();
        accSerializer.serialize(acc, outputBuffer);
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

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
import java.util.function.Supplier;

/**
 * Minimal {@link ValueState} implementation backed by ForSt-RS via the {@link ForStRsLinker} FFM
 * bridge.
 *
 * <p>Two construction modes are supported:
 *
 * <ol>
 *   <li><b>Static byte[] prefix (legacy / direct-test mode)</b>: the entire ForSt key is the
 *       construction-time {@code keyPrefix}. Used by the Phase-D L4 stepping-stone tests.
 *   <li><b>Key-group prefixed mode (spec §6, P1+)</b>: the ForSt key is computed lazily per call
 *       via a caller-supplied {@link Supplier Supplier&lt;byte[]&gt;}, which is expected to invoke
 *       {@link org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer#encodeForState}
 *       with the current key-group, current user-key, and stateName.
 * </ol>
 *
 * @param <T> stored value type
 */
@Internal
public class ForStRsValueState<T> implements ValueState<T> {

    /** Initial buffer size for value serialization (grows on demand). */
    private static final int DEFAULT_OUTPUT_BUFFER = 64;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final TypeSerializer<T> serializer;

    /** Mode 1: static byte[] prefix. */
    private final byte[] keyPrefix;

    /** Mode 2: kg-prefixed lazy compute (returns the full composite ForSt key per call). */
    private final Supplier<byte[]> keyComputer;

    private final DataOutputSerializer outputBuffer;
    private final DataInputDeserializer inputBuffer;

    /**
     * Legacy / stepping-stone constructor: caller supplies the ForSt key directly as a fixed
     * prefix.
     */
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
        this.keyComputer = null;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    /**
     * Spec §6 constructor: composite ForSt key is recomputed per call from the supplied keyComputer
     * (which the keyed-state backend wires to {@code ForStRsKeyGroupedSerializer
     * .encodeForState(currentKg, currentKey, stateName)}).
     */
    public ForStRsValueState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            TypeSerializer<T> serializer,
            Supplier<byte[]> keyComputer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = null;
        this.serializer = serializer;
        this.keyComputer = keyComputer;
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
     * Returns the ForSt-RS key for the current logical entry. In legacy/byte[] mode this is the
     * construction-time prefix verbatim; in kg-prefixed mode it is freshly built by invoking the
     * key-computer (typically wired to {@code ForStRsKeyGroupedSerializer.encodeForState}).
     */
    private byte[] computeKey() {
        if (keyComputer != null) {
            return keyComputer.get();
        }
        return keyPrefix;
    }
}

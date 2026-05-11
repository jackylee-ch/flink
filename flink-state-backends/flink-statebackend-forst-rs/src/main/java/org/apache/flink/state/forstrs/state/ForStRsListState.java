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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Minimal {@link ListState} implementation backed by ForSt-RS via the {@link ForStRsLinker} FFM
 * bridge. See {@link ForStRsValueState} for a description of the two construction modes (static
 * byte[] prefix vs. kg-prefixed lazy compute per spec §6).
 *
 * <p><b>Storage encoding</b>: the entire list is serialized into a single value as {@code [count:
 * i32 BE][elem_0_serialized][elem_1_serialized]...[elem_{count-1}_serialized]} via the configured
 * {@link TypeSerializer}.
 *
 * @param <T> element type
 */
@Internal
public class ForStRsListState<T> implements ListState<T> {

    private static final long serialVersionUID = 1L;

    /** Initial buffer size for value serialization (grows on demand). */
    private static final int DEFAULT_OUTPUT_BUFFER = 64;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final byte[] keyPrefix;
    private final Supplier<byte[]> keyComputer;
    private final TypeSerializer<T> serializer;

    private final DataOutputSerializer outputBuffer;
    private final DataInputDeserializer inputBuffer;

    /** Legacy byte[]-prefix constructor. */
    public ForStRsListState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            byte[] keyPrefix,
            TypeSerializer<T> serializer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = keyPrefix.clone();
        this.keyComputer = null;
        this.serializer = serializer;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    /** Spec §6 kg-prefixed constructor. */
    public ForStRsListState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            TypeSerializer<T> serializer,
            Supplier<byte[]> keyComputer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = null;
        this.keyComputer = keyComputer;
        this.serializer = serializer;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    @Override
    public Iterable<T> get() throws IOException {
        List<T> stored = readList();
        return stored == null ? Collections.emptyList() : stored;
    }

    @Override
    public void add(T value) throws IOException {
        if (value == null) {
            throw new NullPointerException("ForStRsListState.add does not accept null values");
        }
        List<T> current = readList();
        if (current == null) {
            current = new ArrayList<>(1);
        }
        current.add(value);
        writeList(current);
    }

    @Override
    public void update(List<T> values) throws IOException {
        if (values == null || values.isEmpty()) {
            clear();
            return;
        }
        for (T v : values) {
            if (v == null) {
                throw new NullPointerException(
                        "ForStRsListState.update does not accept null elements");
            }
        }
        writeList(values);
    }

    @Override
    public void addAll(List<T> values) throws IOException {
        if (values == null || values.isEmpty()) {
            return;
        }
        for (T v : values) {
            if (v == null) {
                throw new NullPointerException(
                        "ForStRsListState.addAll does not accept null elements");
            }
        }
        List<T> current = readList();
        if (current == null) {
            current = new ArrayList<>(values.size());
        }
        current.addAll(values);
        writeList(current);
    }

    @Override
    public void clear() {
        linker.delete(db, cf, computeKey());
    }

    private List<T> readList() throws IOException {
        byte[] raw = linker.lookupKv(db, cf, computeKey());
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        int count = inputBuffer.readInt();
        if (count < 0) {
            throw new IOException("Negative element count in encoded ListState payload: " + count);
        }
        List<T> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(serializer.deserialize(inputBuffer));
        }
        return out;
    }

    private void writeList(List<T> values) throws IOException {
        outputBuffer.clear();
        outputBuffer.writeInt(values.size());
        for (T v : values) {
            serializer.serialize(v, outputBuffer);
        }
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

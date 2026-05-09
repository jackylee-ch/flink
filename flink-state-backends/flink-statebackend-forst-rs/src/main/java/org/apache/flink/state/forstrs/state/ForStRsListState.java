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

/**
 * Minimal {@link ListState} implementation backed by ForSt-RS via the {@link ForStRsLinker} FFM
 * bridge.
 *
 * <p>This is a Phase-D L4 stepping stone: it demonstrates the keyed-state bridging pattern for a
 * list-typed value end-to-end (serialize list → put → get → deserialize list) without yet plugging
 * into Flink's {@code AbstractKeyedStateBackend} composition. A real keyed-state binding would
 * derive the per-record key from the operator's {@code KeyContext} (key + namespace) and concat
 * with the state-id prefix; here we accept a fixed prefix at construction time and treat it as the
 * full ForSt key. That is sufficient for proof-of-concept round-trips and unit testing the FFM
 * contract, but not for production-quality keyed-state.
 *
 * <p><b>Storage encoding</b>: the entire list is serialized into a single value as
 * {@code [count: i32 BE][elem_0_serialized][elem_1_serialized]...[elem_{count-1}_serialized]} via
 * the configured {@link TypeSerializer}. The value is stored under the construction-time
 * {@code keyPrefix}.
 *
 * <p><b>Concurrency</b>: {@code add} / {@code addAll} are implemented as read-modify-write across
 * a separate get + put, so concurrent appenders against the same key may lose updates. This is
 * intentional for the stepping-stone — RocksDB's ListState uses the merge-operator backed
 * append-only encoding to avoid this; that optimization is deferred until the keyed-state binding
 * lands and the payload-bound merge contract is wired.
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
    private final TypeSerializer<T> serializer;

    private final DataOutputSerializer outputBuffer;
    private final DataInputDeserializer inputBuffer;

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
        this.serializer = serializer;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    @Override
    public Iterable<T> get() throws IOException {
        List<T> stored = readList();
        // Per Flink contract: empty list view rather than null when present-but-empty.
        return stored == null ? Collections.emptyList() : stored;
    }

    @Override
    public void add(T value) throws IOException {
        if (value == null) {
            // Flink's contract states the behavior is undefined for null; mirror RocksDB's choice
            // by rejecting it explicitly to avoid silently corrupting the encoded list.
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
            // Per Flink contract: an empty list (or null in our stepping-stone tolerance) clears.
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
            // No-op — Flink's contract says empty addAll leaves state unchanged.
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

    /**
     * Reads the encoded list under {@link #computeKey()} or returns {@code null} if no entry
     * exists. The caller may safely mutate the returned list — it is freshly allocated.
     */
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

    /** Serializes {@code values} (assumed non-empty, non-null elements) and stores it. */
    private void writeList(List<T> values) throws IOException {
        outputBuffer.clear();
        outputBuffer.writeInt(values.size());
        for (T v : values) {
            serializer.serialize(v, outputBuffer);
        }
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

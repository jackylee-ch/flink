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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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
        // FRS-V1-VEC (2026-06-01): APPEND-MERGE instead of read-modify-write. The prior
        // path did `readList()` (sync engine GET → byte[]) + append + `writeList()` (sync
        // PUT of the WHOLE list) on EVERY add — O(list) bytes per element, O(list^2) over
        // the list, plus a per-record sync GET+PUT. The default CF is configured with the
        // engine's RawConcatMergeOperator (ffi lib.rs:131), so an add is now a single
        // merge-append of one `[count=1][elem]` operand — NO read, NO whole-list rewrite —
        // exactly like the V2 ListState APPEND_MERGE path. `get` resolves the concatenated
        // operands (RawConcat) and decodeMerged below sums the per-chunk counts.
        outputBuffer.clear();
        outputBuffer.writeInt(1);
        serializer.serialize(value, outputBuffer);
        appendMergeOperand(outputBuffer.getSharedBuffer(), outputBuffer.length());
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
        // FRS-V1-VEC: APPEND-MERGE one `[count=N][elems]` operand (no read-modify-write).
        outputBuffer.clear();
        outputBuffer.writeInt(values.size());
        for (T v : values) {
            serializer.serialize(v, outputBuffer);
        }
        appendMergeOperand(outputBuffer.getSharedBuffer(), outputBuffer.length());
    }

    /**
     * FRS-V1-VEC (2026-06-01): append a single raw merge operand to this list's key via
     * {@code frs_vec_merge_append}. The default CF's RawConcatMergeOperator concatenates
     * operand bytes, so each operand is a self-describing `[count:i32 BE][elems]` chunk and
     * {@link #readList} decodes the concatenation. No read of the existing list is needed.
     */
    private void appendMergeOperand(byte[] operand, int operandLen) throws IOException {
        byte[] ck = computeKey();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keySeg = arena.allocate(Math.max(1, ck.length));
            MemorySegment.copy(ck, 0, keySeg, ValueLayout.JAVA_BYTE, 0, ck.length);
            MemorySegment opData = arena.allocate(Math.max(1, operandLen));
            MemorySegment.copy(operand, 0, opData, ValueLayout.JAVA_BYTE, 0, operandLen);
            MemorySegment ptrs = arena.allocate(ValueLayout.ADDRESS, 1);
            MemorySegment lens = arena.allocate(ValueLayout.JAVA_INT, 1);
            ptrs.setAtIndex(ValueLayout.ADDRESS, 0, opData);
            lens.setAtIndex(ValueLayout.JAVA_INT, 0, operandLen);
            int rc =
                    linker.frsVecMergeAppend(
                            db.handle(), cf.handle(), keySeg, ck.length, ptrs, lens, 1);
            if (rc != 0) {
                throw new IOException("frs_vec_merge_append rc=" + rc);
            }
        }
    }

    @Override
    public void clear() {
        // R0C-NEW-H1 Tier-2: segment FFI surface.
        byte[] ck = computeKey();
        linker.deleteSegment(db, cf, MemorySegment.ofArray(ck), 0L, ck.length);
    }

    private List<T> readList() throws IOException {
        byte[] raw = linker.lookupKv(db, cf, computeKey());
        if (raw == null) {
            return null;
        }
        // FRS-V1-VEC (2026-06-01): the stored value is now a CONCATENATION of merge
        // operands (RawConcatMergeOperator), each a self-describing `[count:i32 BE][elems]`
        // chunk — plus, if `update()` did a Put base, that base as the first chunk. Decode
        // every chunk until the buffer is exhausted (mirrors ForStRsListStateV2.get). A
        // legacy single-Put value (pre-change) decodes as exactly one chunk — backward
        // compatible.
        inputBuffer.setBuffer(raw);
        List<T> out = new ArrayList<>();
        while (inputBuffer.available() > 0) {
            int count = inputBuffer.readInt();
            if (count < 0) {
                throw new IOException(
                        "Negative element count in encoded ListState payload: " + count);
            }
            for (int i = 0; i < count; i++) {
                out.add(serializer.deserialize(inputBuffer));
            }
        }
        return out;
    }

    private void writeList(List<T> values) throws IOException {
        outputBuffer.clear();
        outputBuffer.writeInt(values.size());
        for (T v : values) {
            serializer.serialize(v, outputBuffer);
        }
        // PR-E4: reuse the serializer's internal buffer; the engine consumes the value
        // bytes synchronously inside the critical-mode FFM call, so no defensive copy is
        // required. Eliminates one byte[] allocation per writeList.
        // R0C-NEW-H1 Tier-2: segment FFI surface.
        byte[] ck = computeKey();
        byte[] vs = outputBuffer.getSharedBuffer();
        linker.putSegment(
                db,
                cf,
                MemorySegment.ofArray(ck),
                0L,
                ck.length,
                MemorySegment.ofArray(vs),
                0L,
                outputBuffer.length());
    }

    /** FRS-NAMESPACE (2026-05-30): optional per-op namespace suffix (default null = unchanged). */
    private byte[] namespaceSuffix = null;

    public void setNamespaceSuffix(byte[] ns) {
        this.namespaceSuffix = ns;
    }

    private byte[] computeKey() {
        byte[] base = (keyComputer != null) ? keyComputer.get() : keyPrefix;
        if (namespaceSuffix == null || namespaceSuffix.length == 0) {
            return base;
        }
        byte[] out = java.util.Arrays.copyOf(base, base.length + namespaceSuffix.length);
        System.arraycopy(namespaceSuffix, 0, out, base.length, namespaceSuffix.length);
        return out;
    }
}

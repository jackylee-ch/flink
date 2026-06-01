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

import org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView;
import org.apache.flink.state.forstrs.v1sync.MemorySegmentDataOutputView;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

    // FRS-V1-VEC (2026-06-01): off-heap Arrow batch-execution mode (mirrors
    // ForStRsValueState's statebuf). Non-null only via the off-heap constructor. add()
    // becomes read-(buffer-or-zerocopy-pinned)-reduce-insert; writes accumulate in the
    // off-heap Arrow buffer and drain to the engine in ONE batch via flushStateBuffer()
    // (wired into the backend's checkpoint drain). The key is the namespace-aware
    // computeKey() copied into the off-heap scratch, so window/session correctness is
    // preserved while value storage + reads are off-heap + zero-copy.
    private final Supplier<MemorySegment> scratchSupplier;
    private final ArrowBinaryBuffer statebuf;
    private final ArrowBinaryBufferAutoTuner tuner;
    private final MemorySegmentDataInputView offheapInputView;
    private final MemorySegmentDataOutputView offheapOutputView;

    /**
     * FRS-NAMESPACE (2026-05-30): optional per-op namespace suffix appended to the composite
     * key so Flink's (key, namespace) addressing is honoured. DEFAULT null → key bytes are
     * byte-identical to the pre-fix behaviour, so direct (non-adapter) callers and the
     * single-namespace path are completely unaffected. ONLY the InternalKvState adapter
     * (window/merging-window state) sets this, per operation, from the serialized current
     * namespace. This is the fix for SESSION-window correctness (q11/q15): without it all of a
     * key's windows collide on one slot → MergingWindowSet divergence + wrong results.
     */
    private byte[] namespaceSuffix = null;

    /** Set the namespace suffix for subsequent ops (null/empty = no suffix). */
    public void setNamespaceSuffix(byte[] ns) {
        this.namespaceSuffix = ns;
    }

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
        this.scratchSupplier = null;
        this.statebuf = null;
        this.tuner = null;
        this.offheapInputView = null;
        this.offheapOutputView = null;
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
        this.scratchSupplier = null;
        this.statebuf = null;
        this.tuner = null;
        this.offheapInputView = null;
        this.offheapOutputView = null;
    }

    /**
     * FRS-V1-VEC off-heap batch-execution constructor: writes accumulate in an off-heap
     * Arrow {@code statebuf} and drain to the engine in one batch on checkpoint; reads are
     * zero-copy (statebuf hit or {@code getPinnedSegment}). The reduce is applied in Java
     * on add (the engine cannot run the user ReduceFunction).
     */
    public ForStRsReducingState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            byte[] keyPrefix,
            TypeSerializer<T> serializer,
            ReduceFunction<T> reduceFunction,
            Supplier<MemorySegment> scratchSupplier,
            ArrowBinaryBuffer statebuf,
            ArrowBinaryBufferAutoTuner tuner) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = keyPrefix.clone();
        this.keyComputer = null;
        this.serializer = serializer;
        this.reduceFunction = reduceFunction;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
        this.scratchSupplier = scratchSupplier;
        this.statebuf = statebuf;
        this.tuner = tuner;
        this.offheapInputView = new MemorySegmentDataInputView();
        this.offheapOutputView = new MemorySegmentDataOutputView();
    }

    /**
     * Reads the current accumulator for the key already written into {@code scratch[0,keyLen)}:
     * off-heap statebuf hit (zero-copy) or a {@code getPinnedSegment} miss-read into
     * {@code scratch[keyLen..]}. Returns null if absent. Leaves {@code scratch[0,keyLen)}
     * (the key) intact for a subsequent insert.
     */
    private T readAccOffheap(MemorySegment scratch, int keyLen) throws IOException {
        int row = statebuf.find(scratch, 0, keyLen);
        tuner.observeRead(row >= 0, statebuf.size(), statebuf.capacity());
        if (row >= 0) {
            offheapInputView.rewind(
                    statebuf.valueDataSegment(),
                    statebuf.valueOffsetOf(row),
                    statebuf.valueLengthOf(row));
            return serializer.deserialize(offheapInputView);
        }
        int resOff = keyLen;
        int resMax = (int) (scratch.byteSize() - resOff);
        int resLen = linker.getPinnedSegment(db, cf, scratch, 0, keyLen, scratch, resOff, resMax);
        if (resLen < 0) {
            return null;
        }
        offheapInputView.rewind(scratch, resOff, resLen);
        return serializer.deserialize(offheapInputView);
    }

    /** FRS-V1-VEC: batch-drain the off-heap buffer to the engine. No-op in legacy mode. */
    public void flushStateBuffer() {
        if (statebuf != null) {
            statebuf.flushTo(linker, db, cf);
        }
    }

    @Override
    public T get() throws IOException {
        if (statebuf != null) {
            byte[] ck = computeKey();
            MemorySegment scratch = scratchSupplier.get();
            MemorySegment.copy(ck, 0, scratch, ValueLayout.JAVA_BYTE, 0, ck.length);
            return readAccOffheap(scratch, ck.length);
        }
        return readValue();
    }

    @Override
    public void add(T value) throws Exception {
        if (value == null) {
            return;
        }
        if (statebuf != null) {
            // FRS-V1-VEC: read-(off-heap)-reduce-insert. No per-add engine PUT — the
            // reduced accumulator stays in the off-heap Arrow buffer and drains in batch.
            byte[] ck = computeKey();
            MemorySegment scratch = scratchSupplier.get();
            MemorySegment.copy(ck, 0, scratch, ValueLayout.JAVA_BYTE, 0, ck.length);
            int keyLen = ck.length;
            T current = readAccOffheap(scratch, keyLen);
            T next = (current == null) ? value : reduceFunction.reduce(current, value);
            int valStart = keyLen;
            offheapOutputView.reset(scratch, valStart);
            serializer.serialize(next, offheapOutputView);
            int valLen = offheapOutputView.position() - valStart;
            if (statebuf.needsFlush() || statebuf.shouldAutoFlush()) {
                statebuf.flushTo(linker, db, cf);
            }
            int row = statebuf.insert(scratch, 0, keyLen, scratch, valStart, valLen);
            if (row == ArrowBinaryBuffer.INSERT_NEEDS_FLUSH) {
                statebuf.flushTo(linker, db, cf);
                statebuf.insert(scratch, 0, keyLen, scratch, valStart, valLen);
            }
            return;
        }
        T current = readValue();
        T next = (current == null) ? value : reduceFunction.reduce(current, value);
        writeValue(next);
    }

    @Override
    public void clear() {
        if (statebuf != null) {
            byte[] ck = computeKey();
            MemorySegment scratch = scratchSupplier.get();
            MemorySegment.copy(ck, 0, scratch, ValueLayout.JAVA_BYTE, 0, ck.length);
            statebuf.remove(scratch, 0, ck.length);
            linker.deleteSegment(db, cf, scratch, 0, ck.length);
            return;
        }
        // R0C-NEW-H1 Tier-2: segment FFI surface.
        byte[] ck = computeKey();
        linker.deleteSegment(db, cf, MemorySegment.ofArray(ck), 0L, ck.length);
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

    private byte[] computeKey() {
        byte[] base = (keyComputer != null) ? keyComputer.get() : keyPrefix;
        // FRS-NAMESPACE: append the namespace suffix when set (adapter/window path). When
        // null/empty the result is byte-identical to the pre-fix key (back-compat).
        if (namespaceSuffix == null || namespaceSuffix.length == 0) {
            return base;
        }
        byte[] out = java.util.Arrays.copyOf(base, base.length + namespaceSuffix.length);
        System.arraycopy(namespaceSuffix, 0, out, base.length, namespaceSuffix.length);
        return out;
    }
}

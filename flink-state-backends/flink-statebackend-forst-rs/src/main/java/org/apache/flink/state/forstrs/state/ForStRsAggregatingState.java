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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

    // FRS-V1-VEC (2026-06-01): off-heap Arrow batch-execution mode (mirrors ValueState/
    // ReducingState). When non-null, readAccumulator/writeAccumulator route through the
    // off-heap statebuf (batched writes drained at checkpoint, zero-copy reads); the Java
    // AggregateFunction is applied in-memory on add (engine can't run it). Key = the
    // namespace-aware computeKey() copied into off-heap scratch (window/session-correct).
    private final java.util.function.Supplier<MemorySegment> scratchSupplier;
    private final ArrowBinaryBuffer statebuf;
    private final ArrowBinaryBufferAutoTuner tuner;
    private final org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView offheapInputView;
    private final org.apache.flink.state.forstrs.v1sync.MemorySegmentDataOutputView offheapOutputView;

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
        this.scratchSupplier = null;
        this.statebuf = null;
        this.tuner = null;
        this.offheapInputView = null;
        this.offheapOutputView = null;
    }

    /** FRS-V1-VEC off-heap batch-execution constructor. */
    public ForStRsAggregatingState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            byte[] keyPrefix,
            TypeSerializer<ACC> accSerializer,
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            java.util.function.Supplier<MemorySegment> scratchSupplier,
            ArrowBinaryBuffer statebuf,
            ArrowBinaryBufferAutoTuner tuner) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = keyPrefix.clone();
        this.keyComputer = null;
        this.accSerializer = accSerializer;
        this.aggregateFunction = aggregateFunction;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
        this.scratchSupplier = scratchSupplier;
        this.statebuf = statebuf;
        this.tuner = tuner;
        this.offheapInputView =
                new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView();
        this.offheapOutputView =
                new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataOutputView();
    }

    /** FRS-V1-VEC: batch-drain the off-heap buffer to the engine. No-op in legacy mode. */
    public void flushStateBuffer() {
        if (statebuf != null) {
            statebuf.flushTo(linker, db, cf);
        }
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
        this.scratchSupplier = null;
        this.statebuf = null;
        this.tuner = null;
        this.offheapInputView = null;
        this.offheapOutputView = null;
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

    private ACC readAccumulator() throws IOException {
        if (statebuf != null) {
            // FRS-V1-VEC: off-heap statebuf hit (zero-copy) or zero-copy getPinnedSegment.
            byte[] ck = computeKey();
            MemorySegment scratch = scratchSupplier.get();
            MemorySegment.copy(ck, 0, scratch, ValueLayout.JAVA_BYTE, 0, ck.length);
            int keyLen = ck.length;
            int row = statebuf.find(scratch, 0, keyLen);
            tuner.observeRead(row >= 0, statebuf.size(), statebuf.capacity());
            if (row >= 0) {
                offheapInputView.rewind(
                        statebuf.valueDataSegment(),
                        statebuf.valueOffsetOf(row),
                        statebuf.valueLengthOf(row));
                return accSerializer.deserialize(offheapInputView);
            }
            int resOff = keyLen;
            int resMax = (int) (scratch.byteSize() - resOff);
            int resLen =
                    linker.getPinnedSegment(db, cf, scratch, 0, keyLen, scratch, resOff, resMax);
            if (resLen < 0) {
                return null;
            }
            offheapInputView.rewind(scratch, resOff, resLen);
            return accSerializer.deserialize(offheapInputView);
        }
        byte[] raw = linker.lookupKv(db, cf, computeKey());
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        return accSerializer.deserialize(inputBuffer);
    }

    private void writeAccumulator(ACC acc) throws IOException {
        if (statebuf != null) {
            // FRS-V1-VEC: insert into the off-heap Arrow buffer (batched flush at ckpt).
            byte[] ck = computeKey();
            MemorySegment scratch = scratchSupplier.get();
            MemorySegment.copy(ck, 0, scratch, ValueLayout.JAVA_BYTE, 0, ck.length);
            int keyLen = ck.length;
            int valStart = keyLen;
            offheapOutputView.reset(scratch, valStart);
            accSerializer.serialize(acc, offheapOutputView);
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
        outputBuffer.clear();
        accSerializer.serialize(acc, outputBuffer);
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

    /** FRS-NAMESPACE (2026-05-30): optional per-op namespace suffix (default null = unchanged). */
    private byte[] namespaceSuffix = null;

    public void setNamespaceSuffix(byte[] ns) {
        this.namespaceSuffix = ns;
    }

    /** FRS-NAMESPACE: raw accumulator accessors for mergeNamespaces (session-window merge). */
    public ACC getAccumulator() throws java.io.IOException {
        return readAccumulator();
    }

    public void setAccumulator(ACC acc) throws java.io.IOException {
        if (acc == null) {
            clear();
        } else {
            writeAccumulator(acc);
        }
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

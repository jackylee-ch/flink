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
import java.util.function.Consumer;
import java.util.function.Function;
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

    // ------------------------------------------------------------------
    // Write-behind buffer hooks (optional — null when buffer is disabled)
    // ------------------------------------------------------------------

    /** Reads from the shared write buffer; returns null on miss. */
    private final Function<byte[], byte[]> writeBufferGet;

    /** Writes to the shared write buffer (deferred native put). */
    private final java.util.function.BiConsumer<byte[], byte[]> writeBufferPut;

    /** Deletes from the shared write buffer + issues native delete. */
    private final Consumer<byte[]> writeBufferDelete;

    /**
     * Legacy / stepping-stone constructor: caller supplies the ForSt key directly as a fixed
     * prefix. No write-buffer support.
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
        this.writeBufferGet = null;
        this.writeBufferPut = null;
        this.writeBufferDelete = null;
    }

    /**
     * Write-buffer-enabled constructor: caller supplies the ForSt key directly as a fixed prefix
     * plus write-buffer hooks from the owning backend.
     */
    public ForStRsValueState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            byte[] keyPrefix,
            TypeSerializer<T> serializer,
            Function<byte[], byte[]> writeBufferGet,
            java.util.function.BiConsumer<byte[], byte[]> writeBufferPut,
            Consumer<byte[]> writeBufferDelete) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = keyPrefix.clone();
        this.serializer = serializer;
        this.keyComputer = null;
        this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
        this.writeBufferGet = writeBufferGet;
        this.writeBufferPut = writeBufferPut;
        this.writeBufferDelete = writeBufferDelete;
    }

    /**
     * Spec section 6 constructor: composite ForSt key is recomputed per call from the supplied keyComputer
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
        this.writeBufferGet = null;
        this.writeBufferPut = null;
        this.writeBufferDelete = null;
    }

    // Deferred-put optimization: when value() is followed by update() for the
    // same key (the dominant read-modify-write pattern), we skip the separate
    // put() call and let update() use the already-computed key. This saves one
    // computeKey() call per read-modify-write cycle. The key is invalidated on
    // any call that changes the logical key (clear, or a new value() with a
    // different computed key).
    private byte[] lastValueKey;

    @Override
    public T value() throws IOException {
        lastValueKey = computeKey();
        // Check write buffer first (serves repeated reads for same key — 0 native calls)
        if (writeBufferGet != null) {
            byte[] buffered = writeBufferGet.apply(lastValueKey);
            if (buffered != null) {
                inputBuffer.setBuffer(buffered);
                return serializer.deserialize(inputBuffer);
            }
        }
        // Fast path: zero-copy from memtable inline storage (no Rust Vec alloc)
        byte[] raw = linker.getPinned(db, cf, lastValueKey);
        if (raw == null) {
            // Fallback: frs_get_fast — skips catch_unwind + Arc::clone for ~1.5µs savings
            raw = linker.getFast(db, cf, lastValueKey);
        }
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        return serializer.deserialize(inputBuffer);
    }

    @Override
    public void update(T value) throws IOException {
        if (value == null) {
            clear();
            return;
        }
        outputBuffer.clear();
        serializer.serialize(value, outputBuffer);
        byte[] payload = outputBuffer.getCopyOfBuffer();
        // Reuse the key from the preceding value() call if available
        byte[] key = (lastValueKey != null) ? lastValueKey : computeKey();
        // Buffer the write instead of calling native immediately
        if (writeBufferPut != null) {
            writeBufferPut.accept(key, payload);
        } else {
            linker.put(db, cf, key, payload);
        }
        lastValueKey = null; // consumed
    }

    @Override
    public void clear() {
        byte[] key = (lastValueKey != null) ? lastValueKey : computeKey();
        if (writeBufferDelete != null) {
            writeBufferDelete.accept(key);
        } else {
            linker.delete(db, cf, key);
        }
        lastValueKey = null;
    }

    /**
     * Combined get + put in one FFM call. Returns the old value (null if absent) and writes the new
     * value atomically. Saves one FFM boundary crossing vs separate {@link #value()} + {@link
     * #update(Object)} for the read-modify-write pattern.
     *
     * @param newValue the value to write (must not be null — use {@link #clear()} for deletion)
     * @return the previous value, or null if the key did not exist
     */
    public T getAndUpdate(T newValue) throws IOException {
        outputBuffer.clear();
        serializer.serialize(newValue, outputBuffer);
        byte[] payload = outputBuffer.getCopyOfBuffer();
        byte[] oldRaw = linker.getAndPut(db, cf, computeKey(), payload);
        if (oldRaw == null) {
            return null;
        }
        inputBuffer.setBuffer(oldRaw);
        return serializer.deserialize(inputBuffer);
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

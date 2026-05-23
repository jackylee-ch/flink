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
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractValueState;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * V2 async ValueState for ForSt-RS. Extends Flink's {@link AbstractValueState} for the async state
 * API and implements {@link ForStRsInnerTable} for batch execution.
 */
@Internal
public class ForStRsValueStateV2<K, N, V> extends AbstractValueState<K, N, V>
        implements ValueState<V>, ForStRsInnerTable<K, N, V> {

    private static final byte[] KEY_PREFIX = "k/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SLASH = "/".getBytes(StandardCharsets.UTF_8);

    /**
     * Cleanup-A5 (zero-copy): registration-time ordinal counter. Each ValueStateV2 instance gets a
     * dense, monotonically-increasing integer at construction so the per-{@link RecordContext}
     * cache slot can be a {@code Slot[]} indexed by ordinal — no {@code Map.put}, no
     * {@code String} hashing/concat per call. Counter is process-static; only ValueStateV2
     * consumes {@code ctx.getExtra()} today, so its ordinal space is private to this class.
     */
    private static final AtomicInteger NEXT_STATE_ORDINAL = new AtomicInteger(0);

    /** Initial growth size for the per-ctx slot array. */
    private static final int INITIAL_SLOT_CAPACITY = 4;

    private final String stateName;
    private final byte[] stateNameBytes;

    /**
     * Cleanup-A5: dense ordinal assigned at construction. Used as the index into the {@code Slot[]}
     * the per-record {@link RecordContext} holds via {@link RecordContext#getExtra()}. Stable for
     * this instance's lifetime — the operator constructs each ValueStateV2 once at registration.
     */
    private final int stateOrdinal;

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<V> valueSerializer;
    /**
     * PR-A2 (S1-4 / E2-CRIT-1): namespace serializer used to encode the request namespace as the
     * trailing component of the storage composite key. Without this, all namespaces for the same
     * (key, stateName) pair collide on the same storage cell — silent cross-window state
     * corruption. May be {@code null} only for unit tests using void/no-op namespace; in
     * production the keyed backend supplies a real serializer.
     */
    private final TypeSerializer<N> namespaceSerializer;
    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    /**
     * Cleanup-A5: per-(stateOrdinal, namespace) cache slot. The {@code byte[]} is the composite
     * key; {@code nsIdentity} is the {@link System#identityHashCode} of the namespace whose bytes
     * produced it — invalidated by writing a new identity, no per-call String alloc.
     */
    static final class Slot {
        int nsIdentity;
        byte[] composite;
    }

    public ForStRsValueStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer) {
        super(stateRequestHandler, valueSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.stateOrdinal = NEXT_STATE_ORDINAL.getAndIncrement();
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
    }

    /** Test/diagnostic accessor for the per-instance ordinal. */
    public int stateOrdinal() {
        return stateOrdinal;
    }

    // -- ForStRsInnerTable implementation --

    @Override
    @SuppressWarnings("unchecked")
    public byte[] serializeKey(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        // Cleanup-A2/A5 (zero-copy): the previous implementation allocated three heap byte[]s
        // per call (namespaceOut.getCopyOfBuffer, keyOut.getCopyOfBuffer, the final composite
        // new byte[len]) plus four System.arraycopy memcpys, plus a Map.put with a String key
        // concatenated from stateName + "::" + namespace-identityHashCode every call. All of
        // that violated the zero-copy invariant. The new path is:
        //   * single-sweep into keyOut (writes KEY_PREFIX, key, SLASH, stateNameBytes, SLASH,
        //     namespace bytes — no intermediate byte[]s, no arraycopy fan-out),
        //   * cache slot is a Slot[] indexed by per-instance stateOrdinal (set at construction),
        //     so the lookup is a single array load with no String hashing,
        //   * Slot.nsIdentity is the System.identityHashCode of the current namespace;
        //     identityHashCode is stable for a JVM-resident object and changes mean a different
        //     namespace, which forces a cache miss + re-serialize. identityHashCode collisions
        //     are theoretically possible but Flink's single-threaded operator hands us one
        //     namespace at a time from a small pool, and a false-positive hit would silently
        //     serve the wrong composite bytes — same risk as the previous implementation, which
        //     keyed off the same identityHashCode inside its String cacheKey.
        N namespace = request.getNamespace();
        int nsIdentity = System.identityHashCode(namespace);
        Object extra = ctx.getExtra();
        Slot[] slots;
        if (extra instanceof Slot[]) {
            slots = (Slot[]) extra;
            if (stateOrdinal < slots.length) {
                Slot s = slots[stateOrdinal];
                if (s != null && s.composite != null && s.nsIdentity == nsIdentity) {
                    return s.composite; // HIT: zero allocations on this path.
                }
            } else {
                // Grow: state ordinal climbed past the current slot array. Reuse the old slots.
                Slot[] grown = new Slot[Math.max(stateOrdinal + 1, slots.length * 2)];
                System.arraycopy(slots, 0, grown, 0, slots.length);
                slots = grown;
                ctx.setExtra(slots);
            }
        } else {
            int cap = Math.max(INITIAL_SLOT_CAPACITY, stateOrdinal + 1);
            slots = new Slot[cap];
            ctx.setExtra(slots);
        }
        // MISS: serialize composite key in a single sweep into keyOut, then snapshot to byte[].
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: trailing namespace bytes go directly into keyOut — no intermediate
            // namespaceOut buffer + getCopyOfBuffer round-trip. Pre-A2 v3.x snapshots are NOT
            // compatible (format break is independent of this cleanup).
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            byte[] composite = keyOut.getCopyOfBuffer(); // ONE alloc on miss.
            Slot s = slots[stateOrdinal];
            if (s == null) {
                s = new Slot();
                slots[stateOrdinal] = s;
            }
            s.nsIdentity = nsIdentity;
            s.composite = composite;
            return composite;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public byte[] serializeValue(Object value) {
        if (value == null) {
            return null;
        }
        try {
            valueOut.clear();
            valueSerializer.serialize((V) value, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize value", e);
        }
    }

    @Override
    public Object deserializeValue(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return null;
        }
        try {
            valueIn.setBuffer(raw);
            return valueSerializer.deserialize(valueIn);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize value", e);
        }
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result decode. Skips the per-row {@code
     * byte[] = new byte[len]} that the default {@link ForStRsInnerTable#deserializeValue(
     * java.lang.foreign.MemorySegment, long, int)} fallback would perform; instead reads
     * the value bytes directly from the native {@code outData} segment via {@link
     * org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView}.
     *
     * <p>B4-H5 (zero-copy): the view itself is held in a {@link ThreadLocal}, eliminating
     * the per-row {@code new MemorySegmentDataInputView()} that was happening inside the
     * batched-GET completion loop. The view is mutable (its backing segment + position
     * are reset by {@link MemorySegmentDataInputView#rewind}), so per-thread reuse is safe.
     */
    private static final ThreadLocal<org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView>
            VIEW_TL =
                    ThreadLocal.withInitial(
                            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView::new);

    @Override
    public Object deserializeValue(java.lang.foreign.MemorySegment buf, long offset, int len) {
        if (len == 0) {
            return null;
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view = VIEW_TL.get();
            view.rewind(buf, (int) offset, len);
            return valueSerializer.deserialize(view);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsValueStateV2: failed to decode value off-heap", e);
        }
    }

    @Override
    public ForStRsDBGetRequest<K, N, ?> buildDBGetRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        return new ForStRsDBGetRequest<>(key, request, this);
    }

    @Override
    public ForStRsDBPutRequest<K, N, ?> buildDBPutRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        byte[] value = null;
        if (request.getRequestType() == StateRequestType.VALUE_UPDATE) {
            value = serializeValue(request.getPayload());
        }
        return new ForStRsDBPutRequest<>(key, value, request);
    }

    // -- Vectorized serialization (writes directly into off-heap buffer) --

    @Override
    public int serializeKeyInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        RecordContext<K> ctx = request.getRecordContext();
        N namespace = request.getNamespace();
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: append namespace bytes to match the byte-array serializeKey() path.
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return dest.append(keyOut.getSharedBuffer(), 0, keyOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int serializeValueInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        if (request.getRequestType() != StateRequestType.VALUE_UPDATE) {
            return dest.appendEmpty();
        }
        Object payload = request.getPayload();
        // R16-L3: symmetry guard with the V1 sync update() path (ForStRsValueState.update()
        // calls Objects.requireNonNull on its newValue argument). Pre-fix, the V2 vectorized
        // path silently treated a null update payload as a delete-via-empty-bytes encoding
        // (appendEmpty), which is a different semantic than the V1 path's clear NullPointer-
        // Exception. Callers using {@link ValueState#asyncUpdate(Object)} with null should
        // route through {@link ValueState#asyncClear()} instead — making this explicit at the
        // backend boundary prevents accidental "update(null) ≡ clear()" code patterns from
        // silently working on V2 while throwing on V1.
        Objects.requireNonNull(payload, "VALUE_UPDATE payload must not be null; use clear() instead");
        try {
            valueOut.clear();
            valueSerializer.serialize((V) payload, valueOut);
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize value", e);
        }
    }
}

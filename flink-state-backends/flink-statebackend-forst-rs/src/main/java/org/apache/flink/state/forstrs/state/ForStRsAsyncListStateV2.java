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
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractListState;
import org.apache.flink.runtime.state.v2.adaptor.CompleteStateIterator;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Async-V2 ListState for ForSt-RS. Extends {@link AbstractListState} (which wires asyncGet /
 * asyncAdd / asyncUpdate / asyncAddAll via the StateRequestHandler) and implements {@link
 * ForStRsInnerTable} so it participates in the vectorized batch-dispatch path.
 *
 * <p>Storage encoding: the list is serialised as {@code [count:int][elem0][elem1]...}, written by a
 * single PUT. LIST_ADD (single element) encodes as count=1 so it is also a full PUT of the
 * one-element list (simple but correct; append-merge optimisation is a follow-up).
 *
 * <p>The VectorizedClassifier already routes LIST_GET → GET, LIST_UPDATE / LIST_ADD / LIST_ADD_ALL
 * → PUT (non-null), CLEAR → DELETE. serializeValueInto handles all payload shapes.
 *
 * @param <K> backend key type
 * @param <N> namespace type
 * @param <V> list element type
 */
@Internal
public class ForStRsAsyncListStateV2<K, N, V> extends AbstractListState<K, N, V>
        implements ListState<V>, ForStRsInnerTable<K, N, List<V>> {

    private static final byte[] KEY_PREFIX = "k/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SLASH = "/".getBytes(StandardCharsets.UTF_8);

    private final String stateName;
    private final byte[] stateNameBytes;
    private final TypeSerializer<K> keySerializer;
    /**
     * PR-A2 (S1-4 / E2-CRIT-1): namespace serializer used to append serialized namespace bytes
     * as the trailing component of the composite key. Without this, ListState entries for
     * different windows collide on the same storage cell. Hard format break vs v3.x snapshots.
     */
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<V> elementSerializer;

    // Per-instance (single-threaded operator thread) serializers
    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(128);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();
    /**
     * PR-C2: separate serializer used by the off-heap accumulator chunk-encode path so it doesn't
     * trample {@link #valueOut} (which is shared by the heap-byte[] {@link #serializeValueInto}
     * fallback). Encodes {@code [count][elem*]} chunks for LIST_ADD / LIST_ADD_ALL.
     */
    private final DataOutputSerializer chunkOut = new DataOutputSerializer(64);

    // PR-C2: per-state-instance off-heap accumulator + engine handles. When non-null, the
    // VectorizedClassifier's recordAppendMerge routes LIST_ADD / LIST_ADD_ALL chunks into
    // {@code buffer} via {@link #recordAppendMergeOffHeap}, bypassing the heap-AppendMergeBatchBuffer
    // path. Drained on auto-flush, on the backend's pre-snapshot hook, or before any
    // read-or-overwrite op (handled by the classifier ordering: GET / PUT / DELETE precede
    // APPEND_MERGE in {@code executeBatchRequests}, but only the AppendMerge buffer holds writes
    // that must be visible to subsequent batches' reads — read-after-write across batches is
    // covered by the buffer being drained before the next GET-issuing batch enters).
    private final ListStateArrowBuffer buffer;
    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;

    /**
     * Legacy 5-arg constructor — off-heap buffer disabled. Used by all pre-PR-C2 call sites and by
     * tests that don't exercise the off-heap fast path.
     */
    public ForStRsAsyncListStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> elementSerializer) {
        this(
                stateRequestHandler,
                stateName,
                keySerializer,
                namespaceSerializer,
                elementSerializer,
                null,
                null,
                null,
                null);
    }

    /**
     * PR-C2 constructor — off-heap accumulator enabled. The four trailing args are non-null
     * together (or all null, equivalent to the legacy constructor). When the buffer is configured,
     * {@code VectorizedClassifier.recordAppendMerge} routes LIST_ADD chunks here via
     * {@link #recordAppendMergeOffHeap}, and the backend calls {@link #flushPreSnapshot} before
     * checkpoint barrier drain.
     */
    public ForStRsAsyncListStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> elementSerializer,
            ListStateArrowBuffer buffer,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf) {
        super(stateRequestHandler, elementSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.elementSerializer = elementSerializer;
        this.buffer = buffer;
        this.linker = linker;
        this.db = db;
        this.cf = cf;
    }

    /** Test/backend accessor — may be null when the legacy constructor was used. */
    public ListStateArrowBuffer buffer() {
        return buffer;
    }

    /**
     * PR-C2: pre-snapshot drain hook. Called by {@code ForStRsAsyncKeyedStateBackend.snapshot()}
     * BEFORE the executor flush so accumulated single-element {@code asyncAdd} chunks become
     * durable on the engine side before Flink records the checkpoint barrier. No-op when the
     * buffer is not configured.
     */
    public void flushPreSnapshot() {
        if (buffer != null && !buffer.isEmpty() && linker != null && db != null && cf != null) {
            buffer.flushTo(linker, db, cf);
        }
    }

    /**
     * PR-C2: routes a LIST_ADD / LIST_ADD_ALL request into the per-state off-heap accumulator.
     * Called by {@code VectorizedClassifier.recordAppendMerge} when the state instance has a
     * configured buffer.
     *
     * <p><b>Wins vs the heap path:</b> the legacy
     * {@code recordAppendMerge → AppendMergeBatchBuffer} path materialises (a) a heap
     * {@code byte[]} for the composite key, (b) a heap {@code byte[]} for the value chunk, (c)
     * another heap-{@code byte[]} copy of the key inside {@link
     * org.apache.flink.state.forstrs.AppendMergeBatchBuffer#append} into the column buffer, and
     * (d) a per-row scratch {@link java.lang.foreign.Arena} for the per-call FFI. This routine
     * eliminates (c) and (d): chunk bytes are written into the per-state-instance off-heap arena
     * once via {@link ListStateArrowBuffer#append}, and the {@link #flushPreSnapshot} drain — or
     * an auto-flush threshold — issues a single {@code frs_vec_merge_append_batch} FFI for all
     * accumulated rows.
     *
     * <p><b>Ordering invariant:</b> append order = flush order. Since the classifier offers
     * requests in FIFO submit order and {@link ListStateArrowBuffer#append} is FIFO, the engine
     * receives operands in the exact in-call order asyncAdd was invoked. The engine's merge
     * operator concatenates operand bytes verbatim, so a subsequent asyncGet's
     * {@link #deserializeValue} sees the Format-B chunks in submit order — matching V20 §7.4.
     *
     * @return {@code null} when no buffer is configured (caller falls back to heap-path); else the
     *     per-row future that will be completed on the next buffer flush.
     */
    public CompletableFuture<Void> recordAppendMergeOffHeap(StateRequest<K, N, ?, ?> request) {
        if (buffer == null) {
            return null;
        }
        Object payload = request.getPayload();
        if (payload == null) {
            // Defensive — null payload routes to recordDelete in the classifier's outer switch
            // before reaching here. Returning null tells the caller to fall back to heap path.
            return null;
        }
        // Build composite key on heap (existing serializeKey API). The key bytes are copied into
        // the off-heap arena by the buffer's {@code append}; the heap byte[] is then garbage —
        // PR-C2 doesn't claim to eliminate the composite-key heap byte[], only the per-row
        // AppendMergeBatchBuffer key copy and the per-row scratch Arena.
        byte[] keyBytes = serializeKey(request);
        try {
            chunkOut.clear();
            StateRequestType type = request.getRequestType();
            if (type == StateRequestType.LIST_ADD) {
                @SuppressWarnings("unchecked")
                V elem = (V) payload;
                chunkOut.writeInt(1);
                elementSerializer.serialize(elem, chunkOut);
            } else if (type == StateRequestType.LIST_ADD_ALL) {
                @SuppressWarnings("unchecked")
                List<V> list = (List<V>) payload;
                chunkOut.writeInt(list.size());
                for (V e : list) {
                    elementSerializer.serialize(e, chunkOut);
                }
            } else {
                // Defensive — only LIST_ADD / LIST_ADD_ALL should ever reach this hook.
                return null;
            }
            // Pass MemorySegment views over the heap byte[] / shared buffer — buffer.append
            // copies into the off-heap arena and stores no reference to the source heap region.
            return buffer.append(
                    java.lang.foreign.MemorySegment.ofArray(keyBytes),
                    0L,
                    keyBytes.length,
                    java.lang.foreign.MemorySegment.ofArray(chunkOut.getSharedBuffer()),
                    0L,
                    chunkOut.length());
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncListStateV2.recordAppendMergeOffHeap: serialization failed", e);
        }
    }

    /**
     * PR-C2: post-append auto-flush check. Called by the classifier after
     * {@link #recordAppendMergeOffHeap}; if the buffer is at threshold the classifier flushes it
     * mid-batch via {@link #flushIfDirty} — same engine-side effect as a pre-snapshot drain but
     * bounded by buffer occupancy rather than checkpoint cadence.
     */
    public boolean shouldAutoFlush() {
        return buffer != null && buffer.shouldAutoFlush();
    }

    /** Drains the buffer if non-empty and the engine handles are configured. */
    public void flushIfDirty() {
        if (buffer != null && !buffer.isEmpty() && linker != null && db != null && cf != null) {
            buffer.flushTo(linker, db, cf);
        }
    }

    // ---------------------------------------------------------------
    // ForStRsInnerTable — key serialization + state-name lookup (V20.2)
    // ---------------------------------------------------------------

    @Override
    public String getStateName() {
        return stateName;
    }

    /**
     * B4-H4 (zero-copy): override so the classifier's APPEND_MERGE dispatch skips the
     * {@code listStateNames.contains(name)} {@link java.util.Set#contains(Object)} +
     * {@link String#hashCode()} per LIST_ADD record.
     */
    @Override
    public boolean isListState() {
        return true;
    }

    /**
     * A5-H1: pre-DELETE hook fired by {@link
     * org.apache.flink.state.forstrs.VectorizedClassifier#recordDelete}. Drains the off-heap
     * {@link ListStateArrowBuffer} BEFORE the DELETE row enters the executor's columnar batch
     * buffer. Without this, the V2 vectorized dispatch order (PUT → DELETE → ... → off-heap
     * drain) would let pending APPEND_MERGE bytes flush AFTER the engine-side DELETE, resurrecting
     * the cleared entry on the next read (state leak past asyncClear()).
     *
     * <p>The buffer is not prefix-indexed, so we drain it unconditionally on any clear — same
     * semantics as the legacy {@link #buildDBPutRequest} CLEAR branch.
     */
    @Override
    public void onClear(StateRequest<K, N, ?, ?> request) {
        flushIfDirty();
    }

    @Override
    public byte[] serializeKey(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        N namespace = request.getNamespace();
        try {
            keyOut.clear();
            keyOut.write(KEY_PREFIX);
            keySerializer.serialize(ctx.getKey(), keyOut);
            keyOut.write(SLASH);
            keyOut.write(stateNameBytes);
            keyOut.write(SLASH);
            // PR-A2: trailing namespace bytes.
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return keyOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("ForStRsAsyncListStateV2: failed to serialize key", e);
        }
    }

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
            // PR-A2: trailing namespace bytes.
            if (namespaceSerializer != null && !(namespace instanceof VoidNamespace)) {
                namespaceSerializer.serialize(namespace, keyOut);
            }
            return dest.append(keyOut.getSharedBuffer(), 0, keyOut.length());
        } catch (IOException e) {
            throw new RuntimeException("ForStRsAsyncListStateV2: failed to serialize key", e);
        }
    }

    // ---------------------------------------------------------------
    // ForStRsInnerTable — value serialization
    // ---------------------------------------------------------------

    /**
     * Serialises a {@code List<V>} as {@code [count:int][elem0][elem1]...}.
     * Called for LIST_UPDATE, LIST_ADD_ALL payloads. For LIST_ADD, payload is a single V
     * — handled by serializeValueInto / serializeValue(Object).
     */
    @Override
    public byte[] serializeValue(Object value) {
        if (value == null) {
            return null;
        }
        try {
            valueOut.clear();
            if (value instanceof List) {
                @SuppressWarnings("unchecked")
                List<V> list = (List<V>) value;
                valueOut.writeInt(list.size());
                for (V elem : list) {
                    elementSerializer.serialize(elem, valueOut);
                }
            } else {
                // Single element (LIST_ADD payload is bare V)
                @SuppressWarnings("unchecked")
                V elem = (V) value;
                valueOut.writeInt(1);
                elementSerializer.serialize(elem, valueOut);
            }
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("ForStRsAsyncListStateV2: failed to serialize value", e);
        }
    }

    @Override
    public int serializeValueInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        StateRequestType type = request.getRequestType();
        if (type == StateRequestType.CLEAR) {
            return dest.appendEmpty();
        }
        Object payload = request.getPayload();
        if (payload == null) {
            return dest.appendEmpty();
        }
        try {
            valueOut.clear();
            if (type == StateRequestType.LIST_ADD) {
                // Single element
                @SuppressWarnings("unchecked")
                V elem = (V) payload;
                valueOut.writeInt(1);
                elementSerializer.serialize(elem, valueOut);
            } else {
                // LIST_UPDATE or LIST_ADD_ALL — payload is List<V>
                @SuppressWarnings("unchecked")
                List<V> list = (List<V>) payload;
                valueOut.writeInt(list.size());
                for (V elem : list) {
                    elementSerializer.serialize(elem, valueOut);
                }
            }
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncListStateV2: failed to serialize value into buffer", e);
        }
    }

    /**
     * Deserialises raw bytes into a {@link StateIterator}&lt;V&gt; wrapped in a {@link
     * CompleteStateIterator}. The VectorizedExecutor calls this for LIST_GET results and completes
     * the InternalAsyncFuture with the returned iterator.
     */
    @Override
    public Object deserializeValue(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return new CompleteStateIterator<V>(Collections.emptyList());
        }
        try {
            valueIn.setBuffer(raw);
            // V20.1 / Format B: loop [count][elems*] chunks until EOF. Backward-compatible
            // with legacy v3.8 single-chunk [count=1][elem] payloads (reads one chunk,
            // hits EOF, returns the singleton). Required for V3 (LIST_ADD → APPEND_MERGE)
            // because the engine merge-operator concatenates operand bytes verbatim,
            // producing a multi-chunk payload after K appends.
            List<V> list = new ArrayList<>();
            while (valueIn.available() > 0) {
                int count = valueIn.readInt();
                if (count < 0) {
                    throw new IOException(
                            "ForStRsAsyncListStateV2: negative count in merged payload: "
                                    + count);
                }
                for (int i = 0; i < count; i++) {
                    list.add(elementSerializer.deserialize(valueIn));
                }
            }
            return new CompleteStateIterator<>(list);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncListStateV2: failed to deserialize value", e);
        }
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result decode. Reads the multi-chunk payload
     * directly out of the native {@code outData} segment via {@link
     * org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView}, mirroring the
     * legacy {@link #deserializeValue(byte[])} logic but without the per-row
     * {@code byte[] = new byte[len]} the default fallback would perform.
     *
     * <p>B4-H5 (zero-copy): view held in a {@link ThreadLocal}, eliminating the per-row
     * {@code new MemorySegmentDataInputView()} on the batched-GET hot path.
     */
    private static final ThreadLocal<org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView>
            VIEW_TL =
                    ThreadLocal.withInitial(
                            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView::new);

    @Override
    public Object deserializeValue(java.lang.foreign.MemorySegment buf, long offset, int len) {
        if (len == 0) {
            return new CompleteStateIterator<V>(Collections.emptyList());
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view = VIEW_TL.get();
            view.rewind(buf, (int) offset, len);
            List<V> list = new ArrayList<>();
            while (view.remaining() > 0) {
                int count = view.readInt();
                if (count < 0) {
                    throw new IOException(
                            "ForStRsAsyncListStateV2: negative count in merged payload: "
                                    + count);
                }
                for (int i = 0; i < count; i++) {
                    list.add(elementSerializer.deserialize(view));
                }
            }
            return new CompleteStateIterator<>(list);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsAsyncListStateV2: failed to decode value off-heap", e);
        }
    }

    // ---------------------------------------------------------------
    // ForStRsInnerTable — request builders
    // ---------------------------------------------------------------

    @Override
    public ForStRsDBGetRequest<K, N, ?> buildDBGetRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        return new ForStRsDBGetRequest<>(key, request, this);
    }

    @Override
    public ForStRsDBPutRequest<K, N, ?> buildDBPutRequest(StateRequest<K, N, ?, ?> request) {
        StateRequestType type = request.getRequestType();
        // A4-H1 / PR-A6 sibling: when asyncClear() is dispatched, drain the off-heap
        // ListStateArrowBuffer BEFORE the engine sees the DELETE. Without this, the per-state
        // accumulator still holds rows that will flush AFTER the DELETE row lands, and those
        // appends resurrect the entry — state leaks past asyncClear(). The ListStateArrowBuffer
        // (unlike MapStateV2's offHeapBuf) is not prefix-indexed, so we drain unconditionally;
        // subsequent appends for OTHER keys are unaffected because the buffer accumulator only
        // batches appends, not order-dependent reads. Mirrors ForStRsMapStateV2.buildDBPutRequest
        // (PR-A6, lines 464-470) which invalidates the cache + arrow buffer for the cleared
        // prefix before the DELETE request goes out.
        if (type == StateRequestType.CLEAR) {
            flushIfDirty();
        }
        byte[] key = serializeKey(request);
        byte[] value = null;
        if (type != StateRequestType.CLEAR) {
            value = serializeValue(request.getPayload());
        }
        return new ForStRsDBPutRequest<>(key, value, request);
    }
}

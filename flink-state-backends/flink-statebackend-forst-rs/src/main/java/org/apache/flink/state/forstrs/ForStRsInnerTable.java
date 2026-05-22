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

package org.apache.flink.state.forstrs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.asyncprocessing.StateRequest;

/**
 * Interface for ForSt-RS v2 state types to build typed DB requests for batch execution. Each state
 * type (Value, Map, List, etc.) implements this to serialize keys/values and create the appropriate
 * get/put request objects.
 *
 * <p>Two execution paths coexist:
 *
 * <ul>
 *   <li>Legacy: {@link #serializeKey} / {@link #serializeValue} return fresh {@code byte[]} per
 *       request, consumed by the {@code ForStRsStateExecutor}.
 *   <li>Vectorized: {@link #serializeKeyInto} / {@link #serializeValueInto} write directly into a
 *       caller-supplied {@link ColumnarBatchBuffer}, eliminating the per-request {@code byte[]}
 *       allocation. Used by the {@code VectorizedExecutor}.
 * </ul>
 *
 * <p>The default {@code *Into} impls delegate to the legacy methods + {@link
 * ColumnarBatchBuffer#append}, so existing state types keep working. State types on the hot path
 * (Value, Map) should override the {@code *Into} methods to serialize directly into the buffer's
 * underlying serializer.
 */
@Internal
public interface ForStRsInnerTable<K, N, V> {

    byte[] serializeKey(StateRequest<K, N, ?, ?> request);

    byte[] serializeValue(Object value);

    Object deserializeValue(byte[] raw);

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result decode overload. The vectorized GET path
     * holds the result bytes in a native {@link java.lang.foreign.MemorySegment} (the
     * {@code outData} buffer in {@code VectorizedExecutor}); copying each row into a fresh
     * {@code byte[]} just to feed the legacy {@link #deserializeValue(byte[])} entry point
     * burns one allocation per GET row on the hot path.
     *
     * <p>The default fallback copies into a temporary {@code byte[]} and delegates to the
     * legacy overload, so existing implementations keep working. State classes on the read
     * hot path (Value, Map, List, Reducing, Aggregating V2) override this directly with a
     * {@code MemorySegmentDataInputView} to read straight off-heap.
     *
     * @param buf the native segment holding the GET-result bytes
     * @param offset offset within {@code buf} where this row's value starts
     * @param len number of value bytes (zero means empty / "no value")
     * @return the deserialised value, or {@code null} if {@code len == 0}
     */
    default Object deserializeValue(
            java.lang.foreign.MemorySegment buf, long offset, int len) {
        // Fallback: copy into byte[] and call the legacy overload. State classes that want
        // zero-copy should override this directly.
        byte[] tmp = new byte[len];
        java.lang.foreign.MemorySegment.copy(
                buf, offset, java.lang.foreign.MemorySegment.ofArray(tmp), 0, len);
        return deserializeValue(tmp);
    }

    /**
     * Returns the state name. Used by the classifier's APPEND_MERGE routing (V3.1) to look up
     * the state in the {@code listStateNames} registry. Default impl returns {@code null} so
     * non-ListState implementations don't have to override.
     */
    default String getStateName() {
        return null;
    }

    /**
     * B4-H4 (zero-copy): per-state-instance boolean flag indicating whether this table is a
     * {@code ListState} (V2). The classifier's APPEND_MERGE dispatch path used to call {@code
     * listStateNames.contains(stateName)} per LIST_ADD record, which on the V1-sync / V2 hot path
     * was an extra {@code Set.contains(String)} + {@code String.hashCode()} every row. Routing
     * through this default-false flag (overridden to {@code true} by {@code ForStRsAsyncListStateV2}
     * / {@code ForStRsListStateV2}) eliminates that lookup. Non-ListState implementations don't
     * need to override.
     */
    default boolean isListState() {
        return false;
    }

    /**
     * A5-H1 / A5-H2: pre-DELETE hook fired on the vectorized DELETE path
     * ({@link org.apache.flink.state.forstrs.VectorizedClassifier#recordDelete}) BEFORE the
     * DELETE row is enqueued into the executor's columnar batch buffer. State classes use this
     * to drain per-instance write buffers and invalidate per-instance caches so that pending
     * writes / cached accumulators do NOT survive the engine-side DELETE.
     *
     * <p>Concrete overrides:
     *
     * <ul>
     *   <li>{@code ForStRsAsyncListStateV2.onClear} drains the per-state {@code
     *       ListStateArrowBuffer} (A5-H1 — without this, queued APPEND_MERGE rows would flush
     *       AFTER the DELETE and resurrect the cleared entry).
     *   <li>{@code ForStRsAsyncReducingStateV2.onClear} /
     *       {@code ForStRsAsyncAggregatingStateV2.onClear} invalidate the per-state
     *       {@code ReducingAggregatingCache} slot for the current record-context key (A5-H2 —
     *       without this, the dirty cached accumulator survives the DELETE and the next
     *       {@code flushOnBarrier()} would write it back, overwriting the DELETE).
     * </ul>
     *
     * <p>Default impl is a no-op; only state classes with per-instance buffers / caches need to
     * override.
     *
     * @param request the CLEAR / DELETE request being dispatched; carries the RecordContext used
     *     to compute the cache / buffer key
     */
    default void onClear(StateRequest<K, N, ?, ?> request) {
        // no-op default
    }

    ForStRsDBGetRequest<K, N, ?> buildDBGetRequest(StateRequest<K, N, ?, ?> request);

    ForStRsDBPutRequest<K, N, ?> buildDBPutRequest(StateRequest<K, N, ?, ?> request);

    /**
     * Serializes the request key directly into {@code dest}, returning the entry index that the
     * buffer assigned. The default implementation falls back to {@link #serializeKey} + {@link
     * ColumnarBatchBuffer#append(byte[])} so existing state types keep working without overrides.
     */
    default int serializeKeyInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        byte[] k = serializeKey(request);
        return dest.append(k, 0, k.length);
    }

    /**
     * Serializes the request value directly into {@code dest}, returning the entry index that the
     * buffer assigned. If the request has no value (e.g. CLEAR / VALUE_UPDATE with null), the entry
     * is appended as zero-length (caller should treat zero-length as "delete").
     */
    default int serializeValueInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        Object payload = request.getPayload();
        if (payload == null) {
            return dest.appendEmpty();
        }
        byte[] v = serializeValue(payload);
        if (v == null || v.length == 0) {
            return dest.appendEmpty();
        }
        return dest.append(v, 0, v.length);
    }
}

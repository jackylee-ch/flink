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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.AppendMergeRequest;
import org.apache.flink.state.forstrs.VectorizedClassifier;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * V2 ListState primitive backed by {@code APPEND_MERGE} for additive operations and {@code GET}
 * for reads. Per umbrella spec §1 §a, §2 component 11, §3 Trace B.
 *
 * <p>This is a <em>standalone</em> class (V1 scope-down) that demonstrates the APPEND_MERGE
 * dispatch proof-of-concept without wiring into the full Flink async-state execution framework.
 * Full AbstractListState / StateRequestHandler integration is a follow-up (P6-C).
 *
 * <h3>Storage encoding per operand</h3>
 *
 * <p>Each merge operand submitted to the engine has the form
 * {@code [count: i32 BE][elem_0_bytes]...[elem_{count-1}_bytes]}, mirroring the format used by
 * {@link ForStRsListState}. The engine's merge operator concatenates operand byte streams. The
 * {@link #get(byte[])} decoder re-parses the concatenated stream by reading successive
 * {@code (count, elem*)} tuples.
 *
 * <h3>APPEND_MERGE semantics (spec §1 §a)</h3>
 *
 * <ul>
 *   <li>APPEND_MERGE is ListState-only — callers must register the state name with the classifier
 *       before the first {@link #add}/{@link #addAll} call. The constructor does this automatically
 *       via {@link VectorizedClassifier#registerListState(String)}.
 *   <li>Reducing/Aggregating state MUST use the RMW cache path (P7), not this primitive.
 *   <li>{@link #addAll(byte[], List)} packs N elements into one request so the FFI call handles N
 *       operands in a single merge run.
 * </ul>
 *
 * @param <V> element type
 */
@Internal
public class ForStRsListStateV2<V> {

    private final String stateName;
    private final TypeSerializer<V> elementSerializer;
    private final VectorizedClassifier classifier;

    /**
     * Creates a new {@code ForStRsListStateV2}.
     *
     * <p>The {@code classifier} must have been initialised for new-kind buffers via
     * {@link VectorizedClassifier#initNewKindBuffers} before the first {@link #add} /
     * {@link #addAll} call. This constructor registers {@code stateName} in the classifier's
     * list-state registry so APPEND_MERGE for this name is accepted.
     *
     * @param stateName      logical state name; must be unique within the enclosing operator
     * @param elementSerializer serializer for individual list elements
     * @param classifier     the classifier that this state submits APPEND_MERGE requests to
     */
    public ForStRsListStateV2(
            String stateName,
            TypeSerializer<V> elementSerializer,
            VectorizedClassifier classifier) {
        this.stateName = stateName;
        this.elementSerializer = elementSerializer;
        this.classifier = classifier;
        // §1 §a guard: register so the classifier accepts APPEND_MERGE for this state name.
        classifier.registerListState(stateName);
    }

    // -----------------------------------------------------------------
    // APPEND_MERGE path (spec §3 Trace B)
    // -----------------------------------------------------------------

    /**
     * Appends a single element to the list for {@code compositeKey}.
     *
     * <p>Encodes the element as a one-element operand {@code [count=1][elem_bytes]} and submits
     * an {@link AppendMergeRequest} to the classifier. The future completes when the batch
     * containing this request is dispatched.
     *
     * @param compositeKey   pre-encoded storage key (e.g. {@code "k/" + K + "/" + stateName + "/"})
     * @param value          element to append; must not be null
     * @return future that completes when the merge has been applied
     * @throws NullPointerException if {@code value} is null
     */
    public CompletableFuture<Void> add(byte[] compositeKey, V value) {
        if (value == null) {
            throw new NullPointerException(
                    "ForStRsListStateV2.add() does not accept null elements");
        }
        MemorySegment keySlice = MemorySegment.ofArray(compositeKey);
        MemorySegment vSlice = encodeOneElement(value);
        AppendMergeRequest req = new AppendMergeRequest(
                stateName, keySlice, new MemorySegment[]{vSlice});
        classifier.submitVectorized(req);
        return req.future();
    }

    /**
     * Appends multiple elements to the list for {@code compositeKey} in a single merge run.
     *
     * <p>All N elements are packed into one {@link AppendMergeRequest} as N value slices (each
     * slice is one {@code [count=1][elem_bytes]} operand). This means the FFI call sees N
     * operands at once, which is more efficient than N separate {@link #add} calls.
     *
     * <p>An empty {@code values} list is a no-op (returns a completed future).
     *
     * @param compositeKey   pre-encoded storage key
     * @param values         elements to append; must not be null and must not contain null elements
     * @return future that completes when the merge has been applied
     * @throws NullPointerException if {@code values} or any element is null
     */
    public CompletableFuture<Void> addAll(byte[] compositeKey, List<V> values) {
        if (values == null) {
            throw new NullPointerException("ForStRsListStateV2.addAll() values list is null");
        }
        if (values.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        MemorySegment keySlice = MemorySegment.ofArray(compositeKey);
        MemorySegment[] slices = new MemorySegment[values.size()];
        for (int i = 0; i < values.size(); i++) {
            V v = values.get(i);
            if (v == null) {
                throw new NullPointerException(
                        "ForStRsListStateV2.addAll(): null element at index " + i);
            }
            slices[i] = encodeOneElement(v);
        }
        AppendMergeRequest req = new AppendMergeRequest(stateName, keySlice, slices);
        classifier.submitVectorized(req);
        return req.future();
    }

    // -----------------------------------------------------------------
    // Read / update / clear paths (incomplete in V1 — follow-up P6-C)
    // -----------------------------------------------------------------

    /**
     * Decodes a merged byte array (from a GET result) into a {@code List<V>}.
     *
     * <p>The merged value is a concatenation of one or more operands, each of the form
     * {@code [count: i32 BE][elem_0_bytes]...[elem_{count-1}_bytes]}. This decoder reads
     * successive {@code (count, elem*)} tuples until the buffer is exhausted.
     *
     * @param raw merged bytes returned by the engine; may be null or empty
     * @return decoded list; never null (returns an empty list for null/empty input)
     */
    public List<V> get(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return Collections.emptyList();
        }
        DataInputDeserializer in = new DataInputDeserializer(raw);
        List<V> result = new ArrayList<>();
        try {
            while (in.available() > 0) {
                int count = in.readInt();
                if (count < 0) {
                    throw new RuntimeException(
                            "Negative element count in merged ListState payload: " + count);
                }
                for (int i = 0; i < count; i++) {
                    result.add(elementSerializer.deserialize(in));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize merged list", e);
        }
        return result;
    }

    /**
     * Encodes a list for a full-replacement PUT (used by {@code update()} semantics: DELETE
     * old value, then PUT this encoded value, or issue as a PUT directly).
     *
     * <p>Encoded as {@code [count: i32 BE][elem_0_bytes]...[elem_{count-1}_bytes]} (a single
     * operand that replaces the existing merged value when stored via PUT rather than merge).
     *
     * @param values list to encode; must not be null; must not contain null elements
     * @return encoded byte array suitable for a PUT request
     */
    public byte[] encode(List<V> values) {
        if (values == null) {
            throw new NullPointerException("ForStRsListStateV2.encode() values is null");
        }
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            out.writeInt(values.size());
            for (V v : values) {
                if (v == null) {
                    throw new NullPointerException(
                            "ForStRsListStateV2.encode() null element in list");
                }
                elementSerializer.serialize(v, out);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode list", e);
        }
        return out.getCopyOfBuffer();
    }

    /**
     * Unregisters this state's name from the classifier's list-state registry.
     * Call when the state primitive is closed/destroyed.
     */
    public void close() {
        classifier.unregisterListState(stateName);
    }

    // -----------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------

    /**
     * Encodes a single element as a one-element operand:
     * {@code [count=1: i32 BE][elem_bytes]}.
     * Returns a heap {@link MemorySegment} whose bytes are copied into native scratch
     * memory during {@link org.apache.flink.state.forstrs.VectorizedExecutor#dispatchAppendMerge}.
     */
    private MemorySegment encodeOneElement(V value) {
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            out.writeInt(1); // count prefix: one element per operand
            elementSerializer.serialize(value, out);
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode list element", e);
        }
        return MemorySegment.ofArray(out.getCopyOfBuffer());
    }

    public String getStateName() {
        return stateName;
    }
}

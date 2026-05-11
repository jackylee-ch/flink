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

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.InternalKeyContextImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metrics.SizeTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

/**
 * Spec §4 skeleton: a {@link AbstractKeyedStateBackend} subclass that wires the ForSt-RS engine
 * into Flink's keyed-state SPI. The constructor satisfies Flink 2.2.0's {@code
 * AbstractKeyedStateBackend} ctor (10 args) and the abstract methods are implemented as
 * "implemented in P3/P4" stubs throwing {@link UnsupportedOperationException}; the inner
 * round-trip primitives still live on {@link ForStRsKeyedStateBackend} (which this skeleton
 * delegates to once full snapshot/restore wiring lands).
 *
 * <p><b>Why a separate class.</b> The existing {@link ForStRsKeyedStateBackend} (Phase-D L5) is a
 * standalone {@code Closeable} consumed by a wide test surface that would not survive switching
 * its parent class today (e.g., {@link ForStRsKeyedStateBackend#setCurrentKey} returns void with
 * the byte-prefix invalidation policy that cleanly works only when the class isn't already
 * inheriting key-context plumbing from {@code AbstractKeyedStateBackend}). Per the plan, this
 * skeleton lands now to "match what Flink's keyed-state SPI registries expect"; the L5 class will
 * be folded into this one in P3/P4 once snapshot/restore + key-group iteration are wired.
 *
 * @param <K> key type
 */
@Internal
public class ForStRsAbstractKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    /** A delegate L5 backend (existing simple Closeable) that owns the actual FFM handles. */
    private final ForStRsKeyedStateBackend<K> delegate;

    /**
     * Convenience constructor that wires the smallest-possible Flink runtime context (no metrics
     * tracking, no compression, no kvState registry, single key-group range [0, 0]) and delegates
     * the per-state CRUD work to a caller-supplied {@link ForStRsKeyedStateBackend}.
     */
    public ForStRsAbstractKeyedStateBackend(
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            CloseableRegistry cancelStreamRegistry,
            ForStRsKeyedStateBackend<K> delegate) {
        super(
                /* kvStateRegistry= */ null,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                TtlTimeProvider.DEFAULT,
                LatencyTrackingStateConfig.disabled(),
                SizeTrackingStateConfig.disabled(),
                cancelStreamRegistry,
                UncompressedStreamCompressionDecorator.INSTANCE,
                /* keyContext= */ defaultKeyContext());
        this.delegate = delegate;
    }

    private static <K> InternalKeyContext<K> defaultKeyContext() {
        return new InternalKeyContextImpl<>(new KeyGroupRange(0, 0), /* numberOfKeyGroups= */ 1);
    }

    /** Returns the delegate L5 backend; exposed for tests + future P3/P4 wiring. */
    public ForStRsKeyedStateBackend<K> getDelegate() {
        return delegate;
    }

    // ------------------------------------------------------------------
    // SPI surface — stubs lands in P3/P4 unless trivially delegable
    // ------------------------------------------------------------------

    @Override
    public String getBackendTypeIdentifier() {
        return "forst-rs";
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        // L5 delegate exposes a keys(stateName) iterator; namespace plumbing lands in P3.
        java.util.Iterator<K> it = delegate.keys(state);
        java.util.stream.Stream.Builder<K> b = Stream.builder();
        it.forEachRemaining(b::add);
        return b.build();
    }

    @Override
    public <N> Stream<K> getKeys(List<String> states, N namespace) {
        Stream<K> merged = Stream.empty();
        for (String s : states) {
            merged = Stream.concat(merged, getKeys(s, namespace));
        }
        return merged;
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        // Namespace dimension is not yet plumbed in L5 — every key is reported under
        // a null namespace. P3 wires real (key, namespace) pairs.
        return getKeys(state, /* namespace= */ null).map(k -> Tuple2.of(k, (N) null));
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions)
            throws Exception {
        throw new UnsupportedOperationException(
                "ForStRsAbstractKeyedStateBackend.snapshot is implemented in B-Prod-P3 (snapshot strategy)");
    }

    @Override
    public SavepointResources<K> savepoint() throws Exception {
        throw new UnsupportedOperationException(
                "ForStRsAbstractKeyedStateBackend.savepoint is implemented in B-Prod-P4 (savepoint resources)");
    }

    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        throw new UnsupportedOperationException(
                "ForStRsAbstractKeyedStateBackend.createOrUpdateInternalState is implemented in B-Prod-P3 (state-factory wiring)");
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        throw new UnsupportedOperationException(
                "ForStRsAbstractKeyedStateBackend.create (priority queue) is implemented in B-Prod-P5 (timer-service)");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // No-op until P3 wires checkpoint snapshot strategy.
    }

    @Override
    public int numKeyValueStateEntries() {
        // Best-effort: the L5 backend's count is accurate for the single-CF / single-keygroup
        // setup used until P3 wires multi-CF iteration. Cast guards against the long->int
        // conversion since Flink's interface returns int.
        long n = delegate.numKeyValueStateEntries();
        return (int) Math.min(Integer.MAX_VALUE, n);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            delegate.close();
        }
    }
}

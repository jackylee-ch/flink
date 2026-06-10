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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.state.forstrs.state.ForStRsAggregatingState;
import org.apache.flink.state.forstrs.state.ForStRsListState;
import org.apache.flink.state.forstrs.state.ForStRsMapState;
import org.apache.flink.state.forstrs.state.ForStRsReducingState;
import org.apache.flink.state.forstrs.state.ForStRsValueState;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Lightweight {@code InternalKvState}-shaped wrappers around the existing per-state ForStRs
 * classes. Used by {@link ForStRsAbstractKeyedStateBackend#createOrUpdateInternalState} so the
 * keyed-state backend exposes the {@code InternalKvState} surface Flink's higher-level machinery
 * requires while delegating all real work to {@link ForStRsKeyedStateBackend}'s existing
 * per-state-name factories.
 *
 * <p><b>Namespace model.</b> ForSt-RS currently supports a single implicit namespace per
 * state-name. Adapters track {@code currentNamespace} and forward it to the per-state cache key
 * prefix only via the existing state-name routing — i.e. the namespace is part of the state-name
 * identity but does not partition the keyspace further. This matches how the widely-used Flink
 * primitives (ValueState/ListState/MapState/ReducingState/AggregatingState) interact with the
 * runtime when no per-window namespacing is in play.
 *
 * <p><b>Queryable state &amp; TTL incremental cleanup.</b> The {@link
 * org.apache.flink.runtime.state.internal.InternalKvState#getSerializedValue} and {@code
 * getStateIncrementalVisitor} entry points throw {@link UnsupportedOperationException} — the former
 * is only used by Flink's queryable-state API (deprecated in Flink 2.x) and the latter is only
 * invoked when TTL-with-incremental-cleanup is enabled on the state descriptor.
 */
@Internal
final class ForStRsInternalKvStateAdapters {
    private ForStRsInternalKvStateAdapters() {}

    /** Base bookkeeping for InternalKvState semantics shared by all 5 adapter subclasses. */
    abstract static class AbstractAdapter<K, N, V>
            implements org.apache.flink.runtime.state.internal.InternalKvState<K, N, V> {
        final TypeSerializer<K> keySerializer;
        final TypeSerializer<N> namespaceSerializer;
        final TypeSerializer<V> valueSerializer;
        final String stateName;
        final ForStRsKeyedStateBackend<K> delegate;
        N currentNamespace;

        AbstractAdapter(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<V> valueSerializer,
                String stateName,
                ForStRsKeyedStateBackend<K> delegate) {
            this.keySerializer = keySerializer;
            this.namespaceSerializer = namespaceSerializer;
            this.valueSerializer = valueSerializer;
            this.stateName = stateName;
            this.delegate = delegate;
        }

        @Override
        public final TypeSerializer<K> getKeySerializer() {
            return keySerializer;
        }

        @Override
        public final TypeSerializer<N> getNamespaceSerializer() {
            return namespaceSerializer;
        }

        @Override
        public final TypeSerializer<V> getValueSerializer() {
            return valueSerializer;
        }

        @Override
        public final void setCurrentNamespace(N namespace) {
            this.currentNamespace = namespace;
        }

        /**
         * FRS-NAMESPACE (2026-05-30): serialize a namespace to bytes for the per-state key
         * suffix, so (key, namespace) addressing is honoured. Cached buffer; returns a copy
         * safe to retain. Used by the merging-window adapters (Reducing/Aggregating/List) to
         * partition window state per namespace and to implement mergeNamespaces — the fix for
         * session-window (q11/q15) correctness.
         */
        private final DataOutputSerializer nsBuffer = new DataOutputSerializer(16);

        final byte[] serializeNamespace(N ns) {
            try {
                nsBuffer.clear();
                namespaceSerializer.serialize(ns, nsBuffer);
                return nsBuffer.getCopyOfBuffer();
            } catch (java.io.IOException e) {
                throw new RuntimeException("ForStRs namespace serialize failed", e);
            }
        }

        @Override
        public final byte[] getSerializedValue(
                byte[] serializedKeyAndNamespace,
                TypeSerializer<K> safeKeySerializer,
                TypeSerializer<N> safeNamespaceSerializer,
                TypeSerializer<V> safeValueSerializer) {
            throw new UnsupportedOperationException(
                    "ForStRs backend does not support queryable-state getSerializedValue yet");
        }

        @Override
        public final org.apache.flink.runtime.state.internal.InternalKvState
                                .StateIncrementalVisitor<
                        K, N, V>
                getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
            throw new UnsupportedOperationException(
                    "ForStRs backend does not support state-incremental visitors (TTL"
                            + " incremental cleanup) yet");
        }
    }

    // ------------------------------------------------------------------
    // ValueState adapter
    // ------------------------------------------------------------------
    static final class ValueAdapter<K, N, V> extends AbstractAdapter<K, N, V>
            implements InternalValueState<K, N, V> {

        /**
         * Cached bound state object — avoids the HashMap lookup in {@code delegate.getValueState}
         * on every {@code value()}/{@code update()} call when the key hasn't changed (Phase B1+B3
         * optimization). Invalidated when {@code cachedGeneration != delegate.getKeyGeneration()}.
         */
        private ForStRsValueState<V> cachedState;

        private long cachedGeneration = -1;

        ValueAdapter(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<V> valueSerializer,
                String stateName,
                ForStRsKeyedStateBackend<K> delegate) {
            super(keySerializer, namespaceSerializer, valueSerializer, stateName, delegate);
        }

        private ForStRsValueState<V> bind() {
            long gen = delegate.getKeyGeneration();
            if (cachedState != null && gen == cachedGeneration) {
                return cachedState;
            }
            cachedState = delegate.getValueState(stateName, valueSerializer);
            cachedGeneration = gen;
            return cachedState;
        }

        // FRS-NAMESPACE: bind + set the key suffix to the CURRENT namespace before each op.
        private ForStRsValueState<V> bindNs() {
            ForStRsValueState<V> s = bind();
            s.setNamespaceSuffix(serializeNamespace(currentNamespace));
            return s;
        }

        @Override
        public V value() throws java.io.IOException {
            return bindNs().value();
        }

        @Override
        public void update(V value) throws java.io.IOException {
            bindNs().update(value);
        }

        @Override
        public void clear() {
            try {
                bindNs().clear();
            } catch (RuntimeException re) {
                throw re;
            }
        }
    }

    // ------------------------------------------------------------------
    // ListState adapter
    // ------------------------------------------------------------------
    static final class ListAdapter<K, N, T> extends AbstractAdapter<K, N, List<T>>
            implements InternalListState<K, N, T> {

        private final TypeSerializer<T> elementSerializer;

        /** Cached bound state (Phase B1+B3 optimization — same pattern as ValueAdapter). */
        private ForStRsListState<T> cachedState;

        private long cachedGeneration = -1;

        @SuppressWarnings({"unchecked", "rawtypes"})
        ListAdapter(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<T> elementSerializer,
                String stateName,
                ForStRsKeyedStateBackend<K> delegate) {
            super(
                    keySerializer,
                    namespaceSerializer,
                    /* list-value serializer not directly used; pass a raw cast */
                    (TypeSerializer) elementSerializer,
                    stateName,
                    delegate);
            this.elementSerializer = elementSerializer;
        }

        private ForStRsListState<T> bind() {
            long gen = delegate.getKeyGeneration();
            if (cachedState != null && gen == cachedGeneration) {
                return cachedState;
            }
            cachedState = delegate.getListState(stateName, elementSerializer);
            cachedGeneration = gen;
            return cachedState;
        }

        // FRS-NAMESPACE: bind + set the key suffix to the CURRENT namespace before each op.
        private ForStRsListState<T> bindNs() {
            ForStRsListState<T> s = bind();
            s.setNamespaceSuffix(serializeNamespace(currentNamespace));
            return s;
        }

        @Override
        public Iterable<T> get() throws Exception {
            return bindNs().get();
        }

        @Override
        public void add(T value) throws Exception {
            bindNs().add(value);
        }

        @Override
        public void update(List<T> values) throws Exception {
            bindNs().update(values);
        }

        @Override
        public void addAll(List<T> values) throws Exception {
            bindNs().addAll(values);
        }

        @Override
        public void clear() {
            bindNs().clear();
        }

        @Override
        public List<T> getInternal() throws Exception {
            Iterable<T> raw = bindNs().get();
            java.util.ArrayList<T> out = new java.util.ArrayList<>();
            if (raw != null) {
                for (T t : raw) {
                    out.add(t);
                }
            }
            return out;
        }

        @Override
        public void updateInternal(List<T> valueToStore) throws Exception {
            bindNs().update(valueToStore);
        }

        @Override
        public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
            // FRS-NAMESPACE: concat all source-namespace lists into the target namespace, then
            // clear the sources. Each get/add/clear is keyed by the per-namespace suffix.
            if (sources == null || sources.isEmpty()) {
                return;
            }
            ForStRsListState<T> s = bind();
            java.util.ArrayList<T> merged = new java.util.ArrayList<>();
            for (N src : sources) {
                s.setNamespaceSuffix(serializeNamespace(src));
                Iterable<T> raw = s.get();
                if (raw != null) {
                    for (T t : raw) {
                        merged.add(t);
                    }
                    s.clear();
                }
            }
            if (!merged.isEmpty()) {
                s.setNamespaceSuffix(serializeNamespace(target));
                s.addAll(merged);
            }
        }
    }

    // ------------------------------------------------------------------
    // MapState adapter
    // ------------------------------------------------------------------
    static final class MapAdapter<K, N, UK, UV> extends AbstractAdapter<K, N, Map<UK, UV>>
            implements InternalMapState<K, N, UK, UV> {

        private final TypeSerializer<UK> userKeySerializer;
        private final TypeSerializer<UV> userValueSerializer;

        /** Cached bound state (persists across key changes — kg-prefixed mode). */
        private ForStRsMapState<UK, UV> cachedState;

        @SuppressWarnings({"rawtypes", "unchecked"})
        MapAdapter(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<UK> userKeySerializer,
                TypeSerializer<UV> userValueSerializer,
                String stateName,
                ForStRsKeyedStateBackend<K> delegate) {
            super(
                    keySerializer,
                    namespaceSerializer,
                    /* map-value serializer not used by adapter */
                    (TypeSerializer) userValueSerializer,
                    stateName,
                    delegate);
            this.userKeySerializer = userKeySerializer;
            this.userValueSerializer = userValueSerializer;
        }

        private ForStRsMapState<UK, UV> bind() {
            if (cachedState != null) {
                return cachedState;
            }
            cachedState = delegate.getMapState(stateName, userKeySerializer, userValueSerializer);
            return cachedState;
        }

        private ForStRsMapState<UK, UV> bindNs() {
            ForStRsMapState<UK, UV> s = bind();
            s.setNamespaceSuffix(
                    currentNamespace == null ? null : serializeNamespace(currentNamespace));
            return s;
        }

        @Override
        public UV get(UK key) throws Exception {
            return bindNs().get(key);
        }

        @Override
        public void put(UK key, UV value) throws Exception {
            bindNs().put(key, value);
        }

        @Override
        public void putAll(Map<UK, UV> map) throws Exception {
            bindNs().putAll(map);
        }

        @Override
        public void remove(UK key) throws Exception {
            bindNs().remove(key);
        }

        @Override
        public boolean contains(UK key) throws Exception {
            return bindNs().contains(key);
        }

        @Override
        public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
            return bindNs().entries();
        }

        @Override
        public Iterable<UK> keys() throws Exception {
            return bindNs().keys();
        }

        @Override
        public Iterable<UV> values() throws Exception {
            return bindNs().values();
        }

        @Override
        public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
            return bindNs().iterator();
        }

        @Override
        public boolean isEmpty() throws Exception {
            return bindNs().isEmpty();
        }

        @Override
        public void clear() {
            bindNs().clear();
        }
    }

    // ------------------------------------------------------------------
    // ReducingState adapter (Internal* signature requires getInternal/updateInternal)
    // ------------------------------------------------------------------
    static final class ReducingAdapter<K, N, T> extends AbstractAdapter<K, N, T>
            implements InternalReducingState<K, N, T> {

        private final ReduceFunction<T> reduceFunction;

        /** Cached bound state (Phase B1+B3 optimization). */
        private ForStRsReducingState<T> cachedState;

        private long cachedGeneration = -1;

        ReducingAdapter(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<T> valueSerializer,
                String stateName,
                ReduceFunction<T> reduceFunction,
                ForStRsKeyedStateBackend<K> delegate) {
            super(keySerializer, namespaceSerializer, valueSerializer, stateName, delegate);
            this.reduceFunction = reduceFunction;
        }

        private ForStRsReducingState<T> bind() {
            long gen = delegate.getKeyGeneration();
            if (cachedState != null && gen == cachedGeneration) {
                return cachedState;
            }
            cachedState = delegate.getReducingState(stateName, valueSerializer, reduceFunction);
            cachedGeneration = gen;
            return cachedState;
        }

        // FRS-NAMESPACE: bind + set the key suffix to the CURRENT namespace before each op.
        private ForStRsReducingState<T> bindNs() {
            ForStRsReducingState<T> s = bind();
            s.setNamespaceSuffix(serializeNamespace(currentNamespace));
            return s;
        }

        @Override
        public T get() throws Exception {
            return bindNs().get();
        }

        @Override
        public void add(T value) throws Exception {
            bindNs().add(value);
        }

        @Override
        public void clear() {
            bindNs().clear();
        }

        @Override
        public T getInternal() throws Exception {
            return bindNs().get();
        }

        @Override
        public void updateInternal(T valueToStore) throws Exception {
            // Reducing state has no direct "set" semantics in the public API; we round-trip
            // via clear+add which matches what TTL state restoration does.
            ForStRsReducingState<T> s = bindNs();
            s.clear();
            if (valueToStore != null) {
                s.add(valueToStore);
            }
        }

        @Override
        public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
            // FRS-NAMESPACE: real merge. Combine every source namespace's reduced value into
            // `merged` (via reduceFunction) and clear the source; then fold `merged` into the
            // target namespace. Each get/clear/add is keyed by the per-namespace suffix.
            // Pre-fix this was a no-op → session windows lost merged-away state and the
            // MergingWindowSet diverged (q11/q15 crash). Mirrors RocksDB's merging semantics.
            if (sources == null || sources.isEmpty()) {
                return;
            }
            ForStRsReducingState<T> s = bind();
            T merged = null;
            for (N src : sources) {
                s.setNamespaceSuffix(serializeNamespace(src));
                T v = s.get();
                if (v != null) {
                    merged = (merged == null) ? v : reduceFunction.reduce(merged, v);
                    s.clear();
                }
            }
            if (merged != null) {
                s.setNamespaceSuffix(serializeNamespace(target));
                T cur = s.get();
                s.clear();
                s.add(cur == null ? merged : reduceFunction.reduce(cur, merged));
            }
        }
    }

    // ------------------------------------------------------------------
    // AggregatingState adapter
    // ------------------------------------------------------------------
    static final class AggregatingAdapter<K, N, IN, ACC, OUT> extends AbstractAdapter<K, N, ACC>
            implements InternalAggregatingState<K, N, IN, ACC, OUT> {

        private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

        /** Cached bound state (Phase B1+B3 optimization). */
        private ForStRsAggregatingState<IN, ACC, OUT> cachedState;

        private long cachedGeneration = -1;

        AggregatingAdapter(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<ACC> accSerializer,
                String stateName,
                AggregateFunction<IN, ACC, OUT> aggregateFunction,
                ForStRsKeyedStateBackend<K> delegate) {
            super(keySerializer, namespaceSerializer, accSerializer, stateName, delegate);
            this.aggregateFunction = aggregateFunction;
        }

        private ForStRsAggregatingState<IN, ACC, OUT> bind() {
            long gen = delegate.getKeyGeneration();
            if (cachedState != null && gen == cachedGeneration) {
                return cachedState;
            }
            cachedState =
                    delegate.getAggregatingState(stateName, valueSerializer, aggregateFunction);
            cachedGeneration = gen;
            return cachedState;
        }

        // FRS-NAMESPACE: bind + set the key suffix to the CURRENT namespace before each op.
        private ForStRsAggregatingState<IN, ACC, OUT> bindNs() {
            ForStRsAggregatingState<IN, ACC, OUT> s = bind();
            s.setNamespaceSuffix(serializeNamespace(currentNamespace));
            return s;
        }

        @Override
        public OUT get() throws Exception {
            return bindNs().get();
        }

        @Override
        public void add(IN value) throws Exception {
            bindNs().add(value);
        }

        @Override
        public void clear() {
            bindNs().clear();
        }

        @Override
        public ACC getInternal() throws Exception {
            // FRS-NAMESPACE: expose the raw accumulator (needed by mergeNamespaces + TTL).
            return bindNs().getAccumulator();
        }

        @Override
        public void updateInternal(ACC valueToStore) throws Exception {
            bindNs().setAccumulator(valueToStore);
        }

        @Override
        public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
            // FRS-NAMESPACE: merge source-namespace ACCUMULATORS into the target via the
            // AggregateFunction's merge(), then clear sources. Each get/set is keyed per
            // namespace. Pre-fix this was a no-op → session-window aggregates lost merged-away
            // state and the MergingWindowSet diverged (q11/q15 crash + wrong results).
            if (sources == null || sources.isEmpty()) {
                return;
            }
            ForStRsAggregatingState<IN, ACC, OUT> s = bind();
            ACC merged = null;
            for (N src : sources) {
                s.setNamespaceSuffix(serializeNamespace(src));
                ACC acc = s.getAccumulator();
                if (acc != null) {
                    merged = (merged == null) ? acc : aggregateFunction.merge(merged, acc);
                    s.clear();
                }
            }
            if (merged != null) {
                s.setNamespaceSuffix(serializeNamespace(target));
                ACC cur = s.getAccumulator();
                s.setAccumulator(cur == null ? merged : aggregateFunction.merge(cur, merged));
            }
        }
    }
}

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
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;

import javax.annotation.Nullable;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Encapsulates a MAP_ITER/MAP_ITER_KEY/MAP_ITER_VALUE request for prefix-scan execution. Uses
 * {@link ForStRsLinker#prefixLookupOpen} + {@link ForStRsLinker#iteratorNext} to iterate entries
 * matching a key prefix, batching up to {@link #CACHE_SIZE_LIMIT} entries.
 *
 * <p>Implements {@link VectorizedStateRequest} as Kind.ITER_PREFIX. The {@link #future()} method
 * returns {@code null} because completion is handled via Flink's {@code InternalAsyncFuture}.
 */
@Internal
public non-sealed class ForStRsDBIterRequest<K, N, UK, UV> implements VectorizedStateRequest {

    static final int CACHE_SIZE_LIMIT = 128;

    private final byte[] prefix;
    private final StateRequest<K, N, ?, ?> request;
    private final StateRequestType originalRequestType;
    private final ForStRsIterableState<K, N, UK, UV> iterableState;
    @Nullable private FrsIterator existingIterator;
    private String stateName = "unknown";

    // --- VectorizedStateRequest implementation ---

    @Override
    public Kind kind() {
        return Kind.ITER_PREFIX;
    }

    /**
     * V1 transition placeholder — returns {@code "unknown"} unless {@link #setStateName} called.
     */
    @Override
    public String stateName() {
        return stateName;
    }

    /** Sets the state name for classifier grouping and per-state metrics. */
    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    /**
     * Returns {@code null} — completion uses Flink's {@code InternalAsyncFuture} via {@link
     * #completeBatch} / {@link #process}.
     */
    @Override
    public CompletableFuture<?> future() {
        return null;
    }

    public ForStRsDBIterRequest(
            byte[] prefix,
            StateRequest<K, N, ?, ?> request,
            StateRequestType originalRequestType,
            ForStRsIterableState<K, N, UK, UV> iterableState,
            @Nullable FrsIterator existingIterator) {
        this.prefix = prefix;
        this.request = request;
        this.originalRequestType = originalRequestType;
        this.iterableState = iterableState;
        this.existingIterator = existingIterator;
    }

    public boolean hasExistingIterator() {
        return existingIterator != null;
    }

    public byte[] getPrefix() {
        return prefix;
    }

    @SuppressWarnings("unchecked")
    public void completeBatch(
            ForStRsLinker.IteratorEntry[] entries,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena) {
        if (originalRequestType == StateRequestType.MAP_IS_EMPTY) {
            boolean isEmpty = (entries.length == 0);
            ((InternalAsyncFuture<Boolean>) (InternalAsyncFuture<?>) request.getFuture())
                    .complete(isEmpty);
            return;
        }
        boolean encounterEnd = (entries.length < CACHE_SIZE_LIMIT);
        completeWithEntries(entries, encounterEnd, null);
    }

    public void process(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
        if (originalRequestType == StateRequestType.MAP_IS_EMPTY) {
            FrsIterator iter = linker.prefixLookupOpen(db, cf, prefix, arena);
            ForStRsLinker.IteratorEntry first = linker.iteratorNext(iter);
            iter.close();
            boolean isEmpty = (first == null);
            ((InternalAsyncFuture<Boolean>) (InternalAsyncFuture<?>) request.getFuture())
                    .complete(isEmpty);
            return;
        }

        FrsIterator iter = existingIterator;
        if (iter == null) {
            iter = linker.prefixLookupOpen(db, cf, prefix, arena);
        }

        List<ForStRsLinker.IteratorEntry> entryList = new ArrayList<>(CACHE_SIZE_LIMIT);
        boolean encounterEnd = false;
        while (entryList.size() < CACHE_SIZE_LIMIT) {
            ForStRsLinker.IteratorEntry entry = linker.iteratorNext(iter);
            if (entry == null) {
                encounterEnd = true;
                iter.close();
                iter = null;
                break;
            }
            entryList.add(entry);
        }

        ForStRsLinker.IteratorEntry[] entries =
                entryList.toArray(new ForStRsLinker.IteratorEntry[0]);
        FrsIterator continuationIter = encounterEnd ? null : iter;
        completeWithEntries(entries, encounterEnd, continuationIter);
    }

    @SuppressWarnings("unchecked")
    private void completeWithEntries(
            ForStRsLinker.IteratorEntry[] entries,
            boolean encounterEnd,
            @Nullable FrsIterator continuationIter) {
        int prefixLen = prefix.length;
        StateRequestHandler handler = iterableState.getStateRequestHandler();

        switch (originalRequestType) {
            case MAP_ITER:
                Collection<Map.Entry<UK, UV>> mapEntries = new ArrayList<>(entries.length);
                for (ForStRsLinker.IteratorEntry e : entries) {
                    UK uk = iterableState.deserializeUserKey(e.key(), prefixLen);
                    UV uv = iterableState.deserializeUserValue(e.value());
                    if (uv != null) {
                        mapEntries.add(new SimpleEntry<>(uk, uv));
                    }
                }
                ForStRsMapIterator<Map.Entry<UK, UV>> entryIter =
                        new ForStRsMapIterator<>(
                                iterableState.asState(),
                                StateRequestType.MAP_ITER,
                                handler,
                                mapEntries,
                                encounterEnd,
                                continuationIter);
                ((InternalAsyncFuture<StateIterator<Map.Entry<UK, UV>>>)
                                (InternalAsyncFuture<?>) request.getFuture())
                        .complete(entryIter);
                break;

            case MAP_ITER_KEY:
                Collection<UK> keys = new ArrayList<>(entries.length);
                for (ForStRsLinker.IteratorEntry e : entries) {
                    UK uk = iterableState.deserializeUserKey(e.key(), prefixLen);
                    keys.add(uk);
                }
                ForStRsMapIterator<UK> keyIter =
                        new ForStRsMapIterator<>(
                                iterableState.asState(),
                                StateRequestType.MAP_ITER_KEY,
                                handler,
                                keys,
                                encounterEnd,
                                continuationIter);
                ((InternalAsyncFuture<StateIterator<UK>>)
                                (InternalAsyncFuture<?>) request.getFuture())
                        .complete(keyIter);
                break;

            case MAP_ITER_VALUE:
                Collection<UV> values = new ArrayList<>(entries.length);
                for (ForStRsLinker.IteratorEntry e : entries) {
                    UV uv = iterableState.deserializeUserValue(e.value());
                    if (uv != null) {
                        values.add(uv);
                    }
                }
                ForStRsMapIterator<UV> valueIter =
                        new ForStRsMapIterator<>(
                                iterableState.asState(),
                                StateRequestType.MAP_ITER_VALUE,
                                handler,
                                values,
                                encounterEnd,
                                continuationIter);
                ((InternalAsyncFuture<StateIterator<UV>>)
                                (InternalAsyncFuture<?>) request.getFuture())
                        .complete(valueIter);
                break;

            default:
                throw new IllegalArgumentException(
                        "Unknown iter request type: " + originalRequestType);
        }
    }

    static class SimpleEntry<K, V> implements Map.Entry<K, V> {
        private final K key;
        private V value;

        SimpleEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V v) {
            V old = value;
            value = v;
            return old;
        }
    }
}

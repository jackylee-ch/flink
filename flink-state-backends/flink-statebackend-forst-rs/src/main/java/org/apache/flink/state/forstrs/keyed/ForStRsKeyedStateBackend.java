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
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.state.ForStRsAggregatingState;
import org.apache.flink.state.forstrs.state.ForStRsListState;
import org.apache.flink.state.forstrs.state.ForStRsMapState;
import org.apache.flink.state.forstrs.state.ForStRsReducingState;
import org.apache.flink.state.forstrs.state.ForStRsValueState;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Phase-D L5 stepping-stone keyed-state backend backed by ForSt-RS.
 *
 * <p><b>Scope.</b> This class implements the minimal "current key + state-id → state object"
 * pattern that Flink's {@code AbstractKeyedStateBackend} exposes, sufficient for end-to-end
 * round-tripping of {@link ForStRsValueState}, {@link ForStRsListState} and {@link ForStRsMapState}
 * (plus, when committed by the sibling agent, ForStRsReducingState / ForStRsAggregatingState). It
 * deliberately does <i>not</i> implement {@link
 * org.apache.flink.runtime.state.CheckpointableKeyedStateBackend} — that interface carries 25+
 * methods (snapshot strategies, key-group iteration, savepoint resources, priority-queue factory,
 * applyToAllKeys, …) that depend on substantial Flink-runtime plumbing not yet wired in this
 * Phase-D L5 stepping stone. The follow-up units that turn this into a fully Flink-integrated
 * backend are tracked as Phase-D L5 (sync v1) and Phase-D L6 (rescaling + checkpoints) per
 * {@code docs/superpowers/planning/v3.2/reports/B1_pr_split_plan.md}.
 *
 * <p><b>Key model.</b> Flink's keyed-state model is a function {@code (currentKey, stateId,
 * userKey?) → value}; the state-object hides the user-side {@code userKey} (e.g.
 * {@code MapState.put(uk, uv)}). This backend maps that to a single ForSt key namespace by
 * concatenating:
 * <pre>
 *   forstKey = "k/" || serialize(currentKey) || "/" || stateName.bytes(UTF-8) || "/" [|| serialize(uk)]
 * </pre>
 * The trailing user-key segment is handled inside {@link ForStRsMapState}; the per-state-name
 * prefix produced here is what the value/list/reducing/aggregating constructors receive as their
 * {@code keyPrefix}.
 *
 * <p><b>Lifetime.</b> The backend owns the {@link Arena}, {@link ForStRsLinker}, {@link FrsDb}
 * and default {@link FrsCfHandle}; {@link #close()} releases all of them in reverse order. State
 * objects returned by the {@code getXxxState} factories must not be used after {@link #close()}.
 *
 * <p><b>State caching.</b> State objects are cached by {@code stateName} so that successive
 * {@code getValueState("counter", …)} calls under the same current key return the same instance —
 * matching Flink's contract that state objects are stateful with respect to the current key. When
 * {@link #setCurrentKey(Object)} is invoked we recompute the per-state-name prefix lazily by
 * <i>recreating</i> the cached state objects for the new key, which is the simplest correct
 * behavior at this stepping-stone level. A future revision will replace the cache with a per-state
 * "rebind to current key" hook to avoid the per-key-switch object churn.
 *
 * @param <K> key type
 */
@Internal
public class ForStRsKeyedStateBackend<K> implements Closeable {

    /** Initial buffer size for currentKey serialization (grows on demand). */
    private static final int DEFAULT_KEY_BUFFER = 32;

    /**
     * Marker prefix for the keyed namespace, kept short to minimize per-record bytes. Keeping it
     * fixed across all keyed states owned by this backend means we can later introduce a separate
     * {@code "n/"} (namespace), {@code "p/"} (priority queue), etc. without colliding.
     */
    private static final byte[] KEYED_NS_MARKER = "k/".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SLASH = new byte[] {(byte) '/'};

    private final Arena arena;
    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle defaultCf;
    private final TypeSerializer<K> keySerializer;
    private final boolean ownsResources;

    private final DataOutputSerializer keyOutBuffer = new DataOutputSerializer(DEFAULT_KEY_BUFFER);

    /**
     * Cache of currently-bound state objects keyed by {@code stateName}. Cleared on every
     * {@link #setCurrentKey(Object)} call because the per-state {@code keyPrefix} embeds the
     * current key.
     */
    private final Map<String, Object> stateCache = new HashMap<>();

    private K currentKey;
    private byte[] currentKeyBytes;

    private boolean closed = false;

    /**
     * Constructs a backend that <i>owns</i> the supplied resources — {@link #close()} will close
     * each of them in reverse order. This is the constructor the {@code
     * ForStRsStateBackend.createKeyedStateBackend} factory uses.
     */
    public ForStRsKeyedStateBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle defaultCf,
            TypeSerializer<K> keySerializer) {
        this(arena, linker, db, defaultCf, keySerializer, /* ownsResources= */ true);
    }

    /**
     * Constructs a backend that may or may not own the supplied resources. When
     * {@code ownsResources} is {@code false}, {@link #close()} will only release the per-state
     * cache and leave the linker/db/cf/arena untouched — useful for tests that want to share an
     * Arena across multiple backends.
     */
    public ForStRsKeyedStateBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle defaultCf,
            TypeSerializer<K> keySerializer,
            boolean ownsResources) {
        this.arena = arena;
        this.linker = linker;
        this.db = db;
        this.defaultCf = defaultCf;
        this.keySerializer = keySerializer;
        this.ownsResources = ownsResources;
    }

    // ------------------------------------------------------------------
    // Current-key management
    // ------------------------------------------------------------------

    /** Sets the current key for subsequent state-object factory calls. */
    public void setCurrentKey(K newKey) {
        if (closed) {
            throw new IllegalStateException("ForStRsKeyedStateBackend already closed");
        }
        if (newKey == null) {
            throw new NullPointerException("setCurrentKey does not accept null");
        }
        // Serialize the new key once and stash the bytes; per-state prefixes are built lazily.
        byte[] serialized = serializeCurrentKey(newKey);
        // If the bytes are identical to the previous binding nothing changes; this is cheap
        // because Flink frequently calls setCurrentKey(sameKey) at operator boundaries.
        if (currentKeyBytes != null && bytesEqual(currentKeyBytes, serialized)) {
            this.currentKey = newKey;
            return;
        }
        this.currentKey = newKey;
        this.currentKeyBytes = serialized;
        // Invalidate the per-state cache because every entry's keyPrefix encodes the old key.
        this.stateCache.clear();
    }

    /** Returns the current key (last successfully {@code setCurrentKey}-ed value, or null). */
    public K getCurrentKey() {
        return currentKey;
    }

    // ------------------------------------------------------------------
    // State-object factories
    // ------------------------------------------------------------------

    /**
     * Returns a {@link ForStRsValueState} bound to the current key + supplied state-id. The same
     * instance is returned for repeated calls with the same {@code stateName} until
     * {@link #setCurrentKey(Object)} is invoked.
     *
     * @throws IllegalStateException if {@link #setCurrentKey(Object)} has not been called
     */
    public <T> ForStRsValueState<T> getValueState(
            String stateName, TypeSerializer<T> valueSerializer) {
        ensureCurrentKey();
        @SuppressWarnings("unchecked")
        ForStRsValueState<T> existing =
                (ForStRsValueState<T>) stateCache.get(valueStateCacheKey(stateName));
        if (existing != null) {
            return existing;
        }
        byte[] prefix = buildPrefix(stateName);
        ForStRsValueState<T> created =
                new ForStRsValueState<>(linker, db, defaultCf, prefix, valueSerializer);
        stateCache.put(valueStateCacheKey(stateName), created);
        return created;
    }

    /** Returns a {@link ForStRsListState} bound to the current key + state-id. */
    public <T> ForStRsListState<T> getListState(
            String stateName, TypeSerializer<T> elementSerializer) {
        ensureCurrentKey();
        @SuppressWarnings("unchecked")
        ForStRsListState<T> existing =
                (ForStRsListState<T>) stateCache.get(listStateCacheKey(stateName));
        if (existing != null) {
            return existing;
        }
        byte[] prefix = buildPrefix(stateName);
        ForStRsListState<T> created =
                new ForStRsListState<>(linker, db, defaultCf, prefix, elementSerializer);
        stateCache.put(listStateCacheKey(stateName), created);
        return created;
    }

    /** Returns a {@link ForStRsMapState} bound to the current key + state-id. */
    public <UK, UV> ForStRsMapState<UK, UV> getMapState(
            String stateName,
            TypeSerializer<UK> keySer,
            TypeSerializer<UV> valueSer) {
        ensureCurrentKey();
        @SuppressWarnings("unchecked")
        ForStRsMapState<UK, UV> existing =
                (ForStRsMapState<UK, UV>) stateCache.get(mapStateCacheKey(stateName));
        if (existing != null) {
            return existing;
        }
        byte[] prefix = buildPrefix(stateName);
        ForStRsMapState<UK, UV> created =
                new ForStRsMapState<>(linker, db, defaultCf, prefix, keySer, valueSer);
        stateCache.put(mapStateCacheKey(stateName), created);
        return created;
    }

    /**
     * Returns a {@link ForStRsReducingState} bound to the current key + state-id. The
     * {@code reduceFunction} is captured at first creation; subsequent calls under the same key
     * return the cached instance and the {@code reduceFunction} argument is ignored.
     */
    public <T> ForStRsReducingState<T> getReducingState(
            String stateName,
            TypeSerializer<T> elementSerializer,
            ReduceFunction<T> reduceFunction) {
        ensureCurrentKey();
        @SuppressWarnings("unchecked")
        ForStRsReducingState<T> existing =
                (ForStRsReducingState<T>) stateCache.get(reducingStateCacheKey(stateName));
        if (existing != null) {
            return existing;
        }
        byte[] prefix = buildPrefix(stateName);
        ForStRsReducingState<T> created =
                new ForStRsReducingState<>(
                        linker, db, defaultCf, prefix, elementSerializer, reduceFunction);
        stateCache.put(reducingStateCacheKey(stateName), created);
        return created;
    }

    /**
     * Returns a {@link ForStRsAggregatingState} bound to the current key + state-id. The
     * {@code aggregateFunction} is captured at first creation; subsequent calls under the same
     * key return the cached instance.
     */
    public <IN, ACC, OUT> ForStRsAggregatingState<IN, ACC, OUT> getAggregatingState(
            String stateName,
            TypeSerializer<ACC> accSerializer,
            AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        ensureCurrentKey();
        @SuppressWarnings("unchecked")
        ForStRsAggregatingState<IN, ACC, OUT> existing =
                (ForStRsAggregatingState<IN, ACC, OUT>)
                        stateCache.get(aggregatingStateCacheKey(stateName));
        if (existing != null) {
            return existing;
        }
        byte[] prefix = buildPrefix(stateName);
        ForStRsAggregatingState<IN, ACC, OUT> created =
                new ForStRsAggregatingState<>(
                        linker, db, defaultCf, prefix, accSerializer, aggregateFunction);
        stateCache.put(aggregatingStateCacheKey(stateName), created);
        return created;
    }

    // ------------------------------------------------------------------
    // Diagnostics / lifecycle
    // ------------------------------------------------------------------

    /**
     * Counts the number of distinct ForSt keys under the keyed-namespace marker. Provided for
     * tests; in production a more efficient per-CF cardinality estimator would replace this.
     */
    public long numKeyValueStateEntries() {
        long count = 0;
        try (Arena local = Arena.ofShared();
                org.apache.flink.state.forstrs.ffm.FrsIterator iter =
                        linker.prefixLookupOpen(db, defaultCf, KEYED_NS_MARKER, local)) {
            while (linker.iteratorNext(iter) != null) {
                count++;
            }
        }
        return count;
    }

    /** Returns the linker — exposed so tests can issue lower-level FFM calls if needed. */
    public ForStRsLinker getLinker() {
        return linker;
    }

    /** Returns the database handle. */
    public FrsDb getDb() {
        return db;
    }

    /** Returns the default column family handle. */
    public FrsCfHandle getDefaultCf() {
        return defaultCf;
    }

    /**
     * Releases all per-state-cache entries. When this backend was constructed with
     * {@code ownsResources=true}, also closes (in order) the default CF, the database, and the
     * Arena that owns the linker's symbol lookup.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        stateCache.clear();
        if (!ownsResources) {
            return;
        }
        // Close in reverse order of construction; each step swallows-and-rethrows the first
        // exception so that we always attempt the full chain.
        Throwable first = null;
        try {
            defaultCf.close();
        } catch (Throwable t) {
            first = t;
        }
        try {
            db.close();
        } catch (Throwable t) {
            if (first == null) {
                first = t;
            }
        }
        try {
            arena.close();
        } catch (Throwable t) {
            if (first == null) {
                first = t;
            }
        }
        if (first != null) {
            if (first instanceof RuntimeException re) {
                throw re;
            }
            if (first instanceof Error err) {
                throw err;
            }
            throw new IOException("ForStRsKeyedStateBackend close failed", first);
        }
    }

    /** Convenience for callers that prefer the Flink {@code Disposable} pattern. */
    public void dispose() {
        try {
            close();
        } catch (IOException e) {
            throw new RuntimeException("ForStRsKeyedStateBackend dispose failed", e);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void ensureCurrentKey() {
        if (closed) {
            throw new IllegalStateException("ForStRsKeyedStateBackend already closed");
        }
        if (currentKeyBytes == null) {
            throw new IllegalStateException(
                    "setCurrentKey(K) must be invoked before requesting a state object");
        }
    }

    /** Serializes {@code key} via the configured key serializer. */
    private byte[] serializeCurrentKey(K key) {
        keyOutBuffer.clear();
        try {
            keySerializer.serialize(key, keyOutBuffer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize current key", e);
        }
        return keyOutBuffer.getCopyOfBuffer();
    }

    /**
     * Builds the per-state-name keyPrefix passed to {@link ForStRsValueState} et al. Layout is
     * {@code "k/" || serialize(currentKey) || "/" || stateName.bytes || "/"} — the trailing slash
     * is what guarantees that {@code MapState}'s composite-key suffix can be peeled by stripping
     * exactly {@code prefix.length} bytes from the iterator-returned composite key.
     */
    private byte[] buildPrefix(String stateName) {
        byte[] nameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        int len =
                KEYED_NS_MARKER.length
                        + currentKeyBytes.length
                        + SLASH.length
                        + nameBytes.length
                        + SLASH.length;
        byte[] out = new byte[len];
        int off = 0;
        System.arraycopy(KEYED_NS_MARKER, 0, out, off, KEYED_NS_MARKER.length);
        off += KEYED_NS_MARKER.length;
        System.arraycopy(currentKeyBytes, 0, out, off, currentKeyBytes.length);
        off += currentKeyBytes.length;
        System.arraycopy(SLASH, 0, out, off, SLASH.length);
        off += SLASH.length;
        System.arraycopy(nameBytes, 0, out, off, nameBytes.length);
        off += nameBytes.length;
        System.arraycopy(SLASH, 0, out, off, SLASH.length);
        return out;
    }

    private static String valueStateCacheKey(String stateName) {
        return "v:" + stateName;
    }

    private static String listStateCacheKey(String stateName) {
        return "l:" + stateName;
    }

    private static String mapStateCacheKey(String stateName) {
        return "m:" + stateName;
    }

    private static String reducingStateCacheKey(String stateName) {
        return "r:" + stateName;
    }

    private static String aggregatingStateCacheKey(String stateName) {
        return "a:" + stateName;
    }

    private static boolean bytesEqual(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }
}

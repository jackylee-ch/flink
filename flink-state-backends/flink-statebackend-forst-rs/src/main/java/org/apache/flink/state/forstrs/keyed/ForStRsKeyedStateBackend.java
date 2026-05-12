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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;
import org.apache.flink.state.forstrs.state.ForStRsAggregatingState;
import org.apache.flink.state.forstrs.state.ForStRsListState;
import org.apache.flink.state.forstrs.state.ForStRsMapState;
import org.apache.flink.state.forstrs.state.ForStRsReducingState;
import org.apache.flink.state.forstrs.state.ForStRsValueState;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

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
 * backend are tracked as Phase-D L5 (sync v1) and Phase-D L6 (rescaling + checkpoints) per {@code
 * docs/superpowers/planning/v3.2/reports/B1_pr_split_plan.md}.
 *
 * <p><b>Key model.</b> Flink's keyed-state model is a function {@code (currentKey, stateId,
 * userKey?) → value}; the state-object hides the user-side {@code userKey} (e.g. {@code
 * MapState.put(uk, uv)}). This backend maps that to a single ForSt key namespace by concatenating:
 *
 * <pre>
 *   forstKey = "k/" || serialize(currentKey) || "/" || stateName.bytes(UTF-8) || "/" [|| serialize(uk)]
 * </pre>
 *
 * <p>The trailing user-key segment is handled inside {@link ForStRsMapState}; the per-state-name
 * prefix produced here is what the value/list/reducing/aggregating constructors receive as their
 * {@code keyPrefix}.
 *
 * <p><b>Lifetime.</b> The backend owns the {@link Arena}, {@link ForStRsLinker}, {@link FrsDb} and
 * default {@link FrsCfHandle}; {@link #close()} releases all of them in reverse order. State
 * objects returned by the {@code getXxxState} factories must not be used after {@link #close()}.
 *
 * <p><b>State caching.</b> State objects are cached by {@code stateName} so that successive {@code
 * getValueState("counter", …)} calls under the same current key return the same instance — matching
 * Flink's contract that state objects are stateful with respect to the current key. When {@link
 * #setCurrentKey(Object)} is invoked we recompute the per-state-name prefix lazily by
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
     * Cache of currently-bound state objects keyed by {@code stateName}. Cleared on every {@link
     * #setCurrentKey(Object)} call because the per-state {@code keyPrefix} embeds the current key.
     */
    private final Map<String, Object> stateCache = new HashMap<>();

    // ------------------------------------------------------------------
    // Write-behind buffer: defers native put calls and serves subsequent
    // reads from the buffer. Flushed in batch (batchPut) every
    // WRITE_BUFFER_FLUSH_THRESHOLD writes or on checkpoint/close.
    // ------------------------------------------------------------------

    /** Flush threshold: number of buffered writes before auto-flush. */
    private static final int WRITE_BUFFER_FLUSH_THRESHOLD = 1024;

    /**
     * Write-behind buffer: maps full composite ForSt keys to their latest value bytes. Shared
     * across all ValueState instances on this backend. Reads check this buffer first (0-cost hit);
     * writes go here instead of native. Flushed via {@link #flushWriteBuffer()} on threshold,
     * checkpoint, or close.
     */
    private final Map<ByteArrayWrapper, byte[]> writeBuffer = new HashMap<>();

    /** Running count of buffered writes since last flush. */
    private int writeBufferCount = 0;

    private K currentKey;
    private byte[] currentKeyBytes;

    /**
     * Monotonically increasing generation counter, bumped on every effective {@link
     * #setCurrentKey(Object)} call (i.e. when the key actually changes). Used by adapters to detect
     * stale cached state objects without a HashMap lookup on every state access (Phase B1+B3
     * optimization).
     */
    private long keyGeneration;

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
     * Constructs a backend that may or may not own the supplied resources. When {@code
     * ownsResources} is {@code false}, {@link #close()} will only release the per-state cache and
     * leave the linker/db/cf/arena untouched — useful for tests that want to share an Arena across
     * multiple backends.
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
        // Bump generation so adapters know their cached state is stale.
        this.keyGeneration++;
        // Invalidate the per-state cache because every entry's keyPrefix encodes the old key.
        this.stateCache.clear();
    }

    /** Returns the current key (last successfully {@code setCurrentKey}-ed value, or null). */
    public K getCurrentKey() {
        return currentKey;
    }

    /**
     * Returns the current key generation — a monotonically increasing counter bumped on every
     * effective key change. Adapters use this to detect stale cached state objects without a
     * HashMap lookup on every state access (Phase B1+B3 optimization).
     */
    public long getKeyGeneration() {
        return keyGeneration;
    }

    // ------------------------------------------------------------------
    // State-object factories
    // ------------------------------------------------------------------

    /**
     * Returns a {@link ForStRsValueState} bound to the current key + supplied state-id. The same
     * instance is returned for repeated calls with the same {@code stateName} until {@link
     * #setCurrentKey(Object)} is invoked.
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
                new ForStRsValueState<>(
                        linker,
                        db,
                        defaultCf,
                        prefix,
                        valueSerializer,
                        this::getFromWriteBuffer,
                        this::putToWriteBuffer,
                        this::deleteFromWriteBuffer);
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
            String stateName, TypeSerializer<UK> keySer, TypeSerializer<UV> valueSer) {
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
     * Returns a {@link ForStRsReducingState} bound to the current key + state-id. The {@code
     * reduceFunction} is captured at first creation; subsequent calls under the same key return the
     * cached instance and the {@code reduceFunction} argument is ignored.
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
     * Returns a {@link ForStRsAggregatingState} bound to the current key + state-id. The {@code
     * aggregateFunction} is captured at first creation; subsequent calls under the same key return
     * the cached instance.
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
        // Flush buffered writes so the count reflects all pending mutations.
        flushWriteBuffer();
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

    // ------------------------------------------------------------------
    // Phase-D L5 simplified snapshot / restore + key-iteration surface
    // ------------------------------------------------------------------

    /**
     * Phase-D L5 simplified snapshot API: writes a checkpoint of the current engine state into
     * {@code targetDir} (which must not yet exist) and returns the same path.
     *
     * <p>The full Flink {@code snapshot(checkpointId, timestamp, factory, options)} signature on
     * {@code CheckpointableKeyedStateBackend} returns a {@code RunnableFuture<SnapshotResult>};
     * that surface depends on {@code CheckpointStreamFactory} / {@code KeyedStateHandle} plumbing
     * not wired in this stepping stone. The simplified API here is enough for the snapshot+restore
     * round-trip tests and for documenting what the L6 hand-off needs to wrap.
     *
     * @throws IllegalStateException if this backend has already been {@link #close() closed}
     */
    public Path snapshot(Path targetDir) {
        if (closed) {
            throw new IllegalStateException("ForStRsKeyedStateBackend already closed");
        }
        // Flush buffered writes before checkpoint — correctness requirement.
        flushWriteBuffer();
        linker.createCheckpoint(db, targetDir.toString());
        return targetDir;
    }

    /**
     * Phase-D L5 restore counterpart to {@link #snapshot(Path)}. Opens a fresh {@link FrsDb} from a
     * checkpoint directory written by {@link #snapshot(Path)} (or any other call to {@link
     * ForStRsLinker#createCheckpoint(FrsDb, String)}) and returns a new backend instance pointing
     * at the restored state.
     *
     * <p>The supplied {@code linker} and {@code arena} are <i>borrowed</i> — they are not closed by
     * the returned backend (which is constructed with {@code ownsResources=false} for the arena +
     * linker but takes ownership of the database and default CF it creates internally). Callers can
     * keep using the same {@link Arena}/{@link ForStRsLinker} across snapshot+restore boundaries.
     *
     * <p>NOTE: the underlying engine opens the checkpoint directory <i>in place</i> — i.e. its
     * {@code db_path} is set to {@code snapshotDir}. Subsequent writes mutate the checkpoint files
     * directly. Copy the directory beforehand if you need to preserve the original snapshot.
     */
    public static <K> ForStRsKeyedStateBackend<K> restoreFromSnapshot(
            ForStRsLinker linker, Arena arena, Path snapshotDir, TypeSerializer<K> keySerializer) {
        FrsDb restored = linker.dbOpenFromCheckpoint(arena, snapshotDir.toString());
        FrsCfHandle cf;
        try {
            cf = linker.dbDefaultCf(restored, arena);
        } catch (RuntimeException e) {
            restored.close();
            throw e;
        }
        // ownsResources=false: arena+linker are caller-owned and must outlive the returned
        // backend. close() will still release the FrsDb and FrsCfHandle that we just opened.
        return new RestoredForStRsKeyedStateBackend<>(arena, linker, restored, cf, keySerializer);
    }

    /**
     * Returns an {@link Iterator} over every distinct key present under the given {@code
     * stateName}. Iteration order is the underlying ForSt-RS scan order (lexicographic over
     * serialized keys); callers should not rely on a stable ordering. Each key is materialized
     * lazily by deserializing the K-bytes embedded in the composite ForSt key.
     *
     * <p><b>Decoding.</b> Composite keys produced by this backend follow the layout {@code "k/" ||
     * serialize(K) || "/" || stateName.bytes || "/" [ || serialize(UK) ]}. To recover K we scan
     * with prefix {@code "k/"} and, for each composite key, locate the tail marker {@code "/" ||
     * stateName.bytes || "/"} after the {@code "k/"} prefix. Everything between {@code "k/"}
     * (offset 2) and that marker is treated as {@code serialize(K)} and fed back through {@link
     * TypeSerializer#deserialize}. Map-state entries (which add a user-key suffix) and
     * value/list/reducing/aggregating entries (which have no suffix) both yield the same K, and
     * duplicates are filtered via a {@link LinkedHashSet}.
     *
     * <p><b>Limits.</b> If {@code serialize(K)} can itself contain the byte sequence {@code "/" ||
     * stateName || "/"} the heuristic above could be ambiguous. We bias toward the <i>last</i>
     * occurrence of the marker so that map-state user-key suffixes never confuse the boundary. For
     * the typical Flink {@link org.apache.flink.api.common.typeutils.base.StringSerializer}
     * (length-prefixed UTF-8) and primitive serializers this is unambiguous.
     */
    public Iterator<K> keys(String stateName) {
        if (closed) {
            throw new IllegalStateException("ForStRsKeyedStateBackend already closed");
        }
        // Flush buffered writes so the scan reflects all pending mutations.
        flushWriteBuffer();
        byte[] nameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        byte[] tailMarker = new byte[1 + nameBytes.length + 1];
        tailMarker[0] = (byte) '/';
        System.arraycopy(nameBytes, 0, tailMarker, 1, nameBytes.length);
        tailMarker[tailMarker.length - 1] = (byte) '/';

        // Deduplicate keys (map-state entries can produce many composite keys per K).
        Set<K> seen = new LinkedHashSet<>();
        DataInputDeserializer in = new DataInputDeserializer();
        Arena local = Arena.ofShared();
        try {
            FrsIterator iter = linker.prefixLookupOpen(db, defaultCf, KEYED_NS_MARKER, local);
            try {
                while (true) {
                    ForStRsLinker.IteratorEntry entry = linker.iteratorNext(iter);
                    if (entry == null) {
                        break;
                    }
                    byte[] composite = entry.key();
                    int markerOffset =
                            findLastSubsequence(composite, KEYED_NS_MARKER.length, tailMarker);
                    if (markerOffset < 0) {
                        // Composite key does not encode this stateName — skip.
                        continue;
                    }
                    int kStart = KEYED_NS_MARKER.length;
                    int kLen = markerOffset - kStart;
                    if (kLen < 0) {
                        continue;
                    }
                    byte[] kBytes = new byte[kLen];
                    System.arraycopy(composite, kStart, kBytes, 0, kLen);
                    in.setBuffer(kBytes);
                    K decoded;
                    try {
                        decoded = keySerializer.deserialize(in);
                    } catch (IOException ioe) {
                        // Skip composite keys that don't deserialize cleanly under the configured
                        // key serializer rather than aborting the whole scan.
                        continue;
                    }
                    seen.add(decoded);
                }
            } finally {
                iter.close();
            }
        } finally {
            local.close();
        }
        return new ImmutableKeyIterator<>(seen.iterator());
    }

    /**
     * Convenience over {@link #keys(String)} that, for every key {@code k} present under {@code
     * stateName}, transiently {@link #setCurrentKey(Object) sets the current key} to {@code k} and
     * invokes {@code action}. The original current key is restored on completion (or cleared if
     * none was set). Useful for "scan + apply" use-cases like windowing eviction or timer-style
     * traversal.
     */
    public void applyToAllKeys(String stateName, Function<K, ?> action) {
        if (closed) {
            throw new IllegalStateException("ForStRsKeyedStateBackend already closed");
        }
        K previous = this.currentKey;
        try {
            Iterator<K> iter = keys(stateName);
            while (iter.hasNext()) {
                K k = iter.next();
                setCurrentKey(k);
                action.apply(k);
            }
        } finally {
            // Restore prior current-key binding so callers see no observable mutation.
            if (previous != null) {
                setCurrentKey(previous);
            }
        }
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
     * Returns the {@link Arena} that owns this backend's FFM resources. Exposed so co-resident
     * helpers (e.g. priority queues, B-Prod-P9) can attach short-lived FFM allocations to the same
     * lifetime instead of creating a parallel arena.
     */
    public Arena getArena() {
        return arena;
    }

    // ------------------------------------------------------------------
    // Write-behind buffer API — used by ForStRsValueState
    // ------------------------------------------------------------------

    /**
     * Returns the buffered value for the given composite ForSt key, or {@code null} if no buffered
     * write exists for that key. Called by {@link ForStRsValueState#value()} before falling through
     * to native.
     */
    public byte[] getFromWriteBuffer(byte[] key) {
        return writeBuffer.get(new ByteArrayWrapper(key));
    }

    /**
     * Buffers a write for the given composite ForSt key. The write is NOT sent to native
     * immediately — it will be flushed in batch when the threshold is reached, on checkpoint, or on
     * close. Called by {@link ForStRsValueState#update(Object)}.
     */
    public void putToWriteBuffer(byte[] key, byte[] value) {
        writeBuffer.put(new ByteArrayWrapper(key), value);
        writeBufferCount++;
        if (writeBufferCount >= WRITE_BUFFER_FLUSH_THRESHOLD) {
            flushWriteBuffer();
        }
    }

    /**
     * Removes a key from the write buffer and issues a native delete. Called by {@link
     * ForStRsValueState#clear()} to ensure the delete reaches the engine and the buffer doesn't
     * serve stale data.
     */
    public void deleteFromWriteBuffer(byte[] key) {
        writeBuffer.remove(new ByteArrayWrapper(key));
        linker.delete(db, defaultCf, key);
    }

    /**
     * Flushes all buffered writes to the engine in one batch call. Must be called before checkpoint
     * (correctness) and before close (durability). Safe to call when the buffer is empty (no-op).
     */
    public void flushWriteBuffer() {
        if (writeBuffer.isEmpty()) {
            return;
        }
        int count = writeBuffer.size();
        byte[][] keys = new byte[count][];
        byte[][] values = new byte[count][];
        int i = 0;
        for (Map.Entry<ByteArrayWrapper, byte[]> entry : writeBuffer.entrySet()) {
            keys[i] = entry.getKey().bytes;
            values[i] = entry.getValue();
            i++;
        }
        linker.batchPut(db, defaultCf, keys, values);
        writeBuffer.clear();
        writeBufferCount = 0;
    }

    /**
     * Wrapper for {@code byte[]} that provides value-based {@code hashCode}/{@code equals} so it
     * can serve as a HashMap key. The hash is computed once at construction.
     */
    static final class ByteArrayWrapper {
        final byte[] bytes;
        private final int hash;

        ByteArrayWrapper(byte[] bytes) {
            this.bytes = bytes;
            this.hash = java.util.Arrays.hashCode(bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ByteArrayWrapper w && java.util.Arrays.equals(bytes, w.bytes);
        }
    }

    /**
     * Releases all per-state-cache entries. When this backend was constructed with {@code
     * ownsResources=true}, also closes (in order) the default CF, the database, and the Arena that
     * owns the linker's symbol lookup.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // Flush any buffered writes before releasing resources.
        flushWriteBuffer();
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

    /**
     * Returns the highest index {@code i >= fromInclusive} such that {@code data[i ..
     * i+needle.length] == needle}, or {@code -1} when no such index exists. Picking the last
     * occurrence biases {@link #keys(String)} toward the value/list/map separator that sits at the
     * end of the K-segment — even if the K-segment itself happens to contain the same byte pattern.
     */
    private static int findLastSubsequence(byte[] data, int fromInclusive, byte[] needle) {
        if (needle.length == 0 || data.length < needle.length) {
            return -1;
        }
        int last = -1;
        outer:
        for (int i = fromInclusive; i + needle.length <= data.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (data[i + j] != needle[j]) {
                    continue outer;
                }
            }
            last = i;
        }
        return last;
    }

    /** Read-only iterator wrapper that hides {@link Iterator#remove()}. */
    private static final class ImmutableKeyIterator<E> implements Iterator<E> {
        private final Iterator<E> delegate;

        ImmutableKeyIterator(Iterator<E> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public E next() {
            if (!delegate.hasNext()) {
                throw new NoSuchElementException();
            }
            return delegate.next();
        }
    }

    /**
     * Variant of {@link ForStRsKeyedStateBackend} produced by {@link
     * #restoreFromSnapshot(ForStRsLinker, Arena, Path, TypeSerializer)}. It owns the
     * <i>database</i> and the <i>default CF</i> it was constructed with — but not the arena/linker,
     * which are owned by the caller. This split lets a single arena+linker straddle a
     * snapshot+restore boundary.
     */
    private static final class RestoredForStRsKeyedStateBackend<K>
            extends ForStRsKeyedStateBackend<K> {

        RestoredForStRsKeyedStateBackend(
                Arena arena,
                ForStRsLinker linker,
                FrsDb db,
                FrsCfHandle defaultCf,
                TypeSerializer<K> keySerializer) {
            super(arena, linker, db, defaultCf, keySerializer, /* ownsResources= */ false);
        }

        @Override
        public void close() throws IOException {
            // Release the per-state cache via the parent (ownsResources=false on the parent skips
            // the FFM-resource teardown), then explicitly close the FrsDb + default-CF that this
            // restored instance brought online — the arena+linker are caller-owned.
            super.close();
            Throwable first = null;
            try {
                getDefaultCf().close();
            } catch (Throwable t) {
                first = t;
            }
            try {
                getDb().close();
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
                throw new IOException("RestoredForStRsKeyedStateBackend close failed", first);
            }
        }
    }
}

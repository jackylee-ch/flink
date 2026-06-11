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
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractMapState;
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.ForStRsInnerTable;
import org.apache.flink.state.forstrs.ForStRsIterableState;
import org.apache.flink.state.forstrs.IteratorEntryView;
import org.apache.flink.state.forstrs.cache.MapStateCache;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * V2 async MapState for ForSt-RS. Key encoding appends the serialized user key to the composite
 * prefix: {@code "k/" + serialize(K) + "/" + stateName + "/" + serialize(UK)}.
 */
@Internal
public class ForStRsMapStateV2<K, N, UK, UV> extends AbstractMapState<K, N, UK, UV>
        implements MapState<UK, UV>,
                ForStRsInnerTable<K, N, UV>,
                ForStRsIterableState<K, N, UK, UV> {

    private static final Logger LOG = LoggerFactory.getLogger(ForStRsMapStateV2.class);

    private static final byte[] KEY_PREFIX = "k/".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SLASH = "/".getBytes(StandardCharsets.UTF_8);

    private final String stateName;
    private final byte[] stateNameBytes;
    private final TypeSerializer<K> keySerializer;
    /**
     * PR-A2 (S1-4 / E2-CRIT-1): namespace serializer used to encode the request namespace into
     * the composite key between the stateName SLASH and the user key. Layout:
     * {@code [KEY_PREFIX][serialize(K)][/][stateName][/][serialize(N)][serialize(UK)]}.
     * Without this, MapState entries for distinct namespaces collide on the same storage row.
     * NOTE: this is a hard format break vs v3.x snapshots — restoring a pre-A2 snapshot will
     * surface as missing entries, not silent corruption.
     */
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;
    private final DataOutputSerializer keyOut = new DataOutputSerializer(128);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);
    private final DataInputDeserializer valueIn = new DataInputDeserializer();

    // (Removed the shared per-state `iterView` field 2026-06-09: it assumed "single-threaded per
    // Flink slot", which the parallel RoutingStateExecutor breaks. The iterator-path decoders now
    // use the per-thread VIEW_TL view instead — see deserializeUserKey/Value thread-safety notes.)

    /**
     * Per-state-instance LRU cache for (operatorKey, userKey) → value lookups. Eliminates engine
     * round-trips for repeated reads of the same map entry within a window. See {@link
     * MapStateCache} for semantics (write-through, LRU 256K cap, single-threaded).
     */
    private final MapStateCache<UV> cache = new MapStateCache<>();

    /**
     * FRS-BATCH-PROBE (2026-06-01): A/B gate for the per-record read cache. A
     * JFR profile of the q4 join showed ~40 % of CPU in the per-record
     * {@link MapStateCache} lookup (findRow / keyEquals / MemorySegment.mismatch
     * on the ~56-byte composite key) — the residual cost once
     * checkpoint-without-flush removed the S3 collapse. When
     * {@code FRS_DISABLE_MAPSTATE_CACHE=1}, asyncGet SKIPS the per-record cache
     * lookup and routes to the off-heap buffer + the engine's BATCHED,
     * SIMD-vectorized {@code batch_get_vectorized} (one FFM crossing per dispatch
     * batch). Correctness-safe: asyncPut writes through the off-heap buffer (and
     * the engine on flush), so reads still observe every write without the cache.
     * This tests the user's "merge per-row into batch execution" directive.
     */
    private static final boolean DISABLE_MAPSTATE_CACHE =
            "1".equals(System.getenv("FRS_DISABLE_MAPSTATE_CACHE"))
                    // FRS-PARALLEL-SAFE (2026-06-09): the MapStateCache is write-through but
                    // SINGLE-THREADED. Under the parallel RoutingStateExecutor, cache ops fire on
                    // threads that don't align with the key-group→worker affinity, so the cache
                    // returns inconsistent reads → q8 windowed-join under-emit (shared −10%,
                    // per-worker −6.7%). cache-OFF is the PROVEN-correct parallel config (q8 N=3
                    // cache-off = 3,064,514 ≈ RocksDB 3,064,457). So whenever the parallel executor
                    // is enabled, bypass the cache. The DEFAULT depth-1 path keeps the cache (correct
                    // + fast — single-threaded mailbox access), so this robs no query: it only
                    // changes behavior in the opt-in parallel mode where the cache is unsafe anyway.
                    // Parallel executor is OPT-IN (reverted from default — cache-off robs the
                    // cache-benefiting joins like q9). Align: bypass the cache only when parallel is
                    // explicitly enabled (the proven-correct parallel config).
                    // PR-1 (2026-06-10): same coupling for BOTH parallel modes — routing (legacy
                    // FRS_RS_PARALLEL_EXECUTOR=1 or FRS_RS_EXECUTOR=routing) and the non-blocking
                    // FRS_RS_EXECUTOR=coordinated. A worker-confined per-key-group cache is PR-2,
                    // gate-driven (q11 passed 134.7s WITH cache off).
                    || parallelExecutorActive();

    /**
     * B-SPIKE (2026-06-11, per-batch-buffer-ownership design §2): {@code true} when the
     * PIPELINED executor is selected ({@code FRS_RS_EXECUTOR=routing-async}). Pipelined modes
     * overlap the mailbox offer phase with worker batch execution, so the per-state staging
     * buffers (MapStateArrowBuffer / ListStateArrowBuffer) — whose threading contract is
     * lockstep-single-threaded — MUST NOT be used: MapState's watermark drain issues
     * mailbox-direct engine writes that overtake queued worker reads (the q8@100M canary
     * wedge/−58%), and ListState's buffer is drained worker-side while the mailbox appends.
     * With the buffers off, all state effects flow through the classifier's per-batch-private
     * buffers (PUT→DELETE→GET→ITER order = same-batch read-your-writes; per-worker kg-FIFO =
     * cross-batch ordering). Lockstep modes (inline/routing/adaptive) keep the buffers.
     * Approach A (sharded seal/swap staging) will restore staging under pipelining.
     */
    public static boolean pipelinedExecutorActive() {
        String m = System.getenv("FRS_RS_EXECUTOR");
        if (m == null) {
            return false;
        }
        String t = m.trim();
        return t.equals("routing-async") || t.equals("two-regime");
    }

    /**
     * STAGE-1 Task 7: legacy always-pipelined mode (staging buffers statically OFF). Under
     * two-regime the buffers EXIST and are gated dynamically per operation via the injected
     * {@link org.apache.flink.state.forstrs.exec.RegimeSwitch} (LIGHT ⇒ staging usable).
     */
    public static boolean legacyPipelinedActive() {
        String m = System.getenv("FRS_RS_EXECUTOR");
        return m != null && m.trim().equals("routing-async");
    }

    /** STAGE-1 Task 7: injected by the backend under two-regime; null otherwise. */
    @Nullable private org.apache.flink.state.forstrs.exec.RegimeSwitch regimeSwitch;

    public void setRegimeSwitch(org.apache.flink.state.forstrs.exec.RegimeSwitch rs) {
        this.regimeSwitch = rs;
    }

    /**
     * Staging usable ⇔ buffer exists AND (no regime switch OR regime is LIGHT). Under HEAVY the
     * mailbox must not absorb effects (its watermark drain would issue mailbox-direct engine
     * writes racing in-flight worker batches); writes flow through the classifier instead.
     */
    private boolean stagingUsable() {
        return offHeapBuf != null && (regimeSwitch == null || regimeSwitch.isLight());
    }

    private static boolean parallelExecutorActive() {
        String m = System.getenv("FRS_RS_EXECUTOR");
        if (m != null) {
            String t = m.trim();
            if (t.equals("coordinated")
                    || t.equals("routing")
                    || t.equals("routing-async")
                    || t.equals("two-regime")
                    || t.equals("adaptive")) {
                return true;
            }
        }
        String legacy = System.getenv("FRS_RS_PARALLEL_EXECUTOR");
        return legacy != null && legacy.trim().equals("1");
    }

    /**
     * FRS-ADAPTIVE-MAPSTATE-CACHE (2026-06-04): the per-record read cache is a
     * pure read accelerator over the off-heap buffer + engine. It is a big win
     * for HIGH-LOCALITY queries (q11/q12/q16 re-read the same keys) but a net
     * LOSS for large LOW-LOCALITY working sets — the q4 interval join reads each
     * {@code (joinKey, ts)} composite roughly once within its window, so once
     * the 1M-entry cache FILLS, every lookup is a miss that still pays a
     * {@code findRow} open-addressed probe + an eviction. A jstack during the
     * q4 collapse showed {@code MapStateCache.findRow} as the #1 CPU frame, and
     * an A/B run with the cache forced off held q4 at a STABLE ~120-170K/s where
     * the cached path collapsed 600K→<10K/s.
     *
     * <p>Rather than a static global flag, auto-bypass the cache when (and only
     * when) it is demonstrably THRASHING: the cache is FULL and the hit rate
     * over the most recent window fell below {@link #CACHE_MIN_HIT_RATE}. A
     * small-state / high-locality query never trips this (its cache either does
     * not fill or keeps a high hit rate), so q11/q12/q16 keep the cache while q4
     * sheds it. Bypassing is correctness-neutral (every write still goes to the
     * off-heap buffer + engine). Sticky once tripped — a thrashing join does not
     * regain locality, and re-probing to re-enable would reintroduce the cost we
     * are removing.
     */
    private static final long CACHE_ADAPT_WINDOW = 1L << 16; // re-evaluate every 65536 lookups
    private static final double CACHE_MIN_HIT_RATE = 0.20;

    private boolean cacheBypassed = false;
    private long cacheWindowLookups = 0L;
    private long cacheWindowHits = 0L;

    /** Whether the per-record read cache should be consulted on this op. */
    private boolean useCache() {
        return !DISABLE_MAPSTATE_CACHE && !cacheBypassed;
    }

    /**
     * Records one cache-lookup outcome and, once per {@link #CACHE_ADAPT_WINDOW}
     * lookups, trips {@link #cacheBypassed} if the cache is full AND the windowed
     * hit rate is below threshold (genuine thrashing). On trip, clears the cache
     * to release its off-heap arena immediately.
     */
    private void recordCacheLookup(boolean hit) {
        cacheWindowLookups++;
        if (hit) {
            cacheWindowHits++;
        }
        if (cacheWindowLookups >= CACHE_ADAPT_WINDOW) {
            if (cache.isFull()
                    && (double) cacheWindowHits / (double) cacheWindowLookups < CACHE_MIN_HIT_RATE) {
                cacheBypassed = true;
                cache.clear();
            }
            cacheWindowLookups = 0L;
            cacheWindowHits = 0L;
        }
    }

    /**
     * PR-C1 (V2-8 / Z3-6 / C-H5): per-state off-heap staging buffer mirroring the V1-sync
     * {@code statebuf} path. Non-null only when the backend constructed this state with the
     * off-heap-aware constructor (i.e. when {@code linker/db/cf} are available — which is always
     * true in production; the legacy constructor stays as null-buffer for unit tests that don't
     * have a live engine handle).
     *
     * <p>Writes stage here and skip the V2 columnar dispatch entirely until {@link #flushOffHeapBuffer}
     * is called by the backend snapshot pre-hook or the buffer hits its auto-flush watermark.
     */
    @Nullable private final MapStateArrowBuffer offHeapBuf;

    @Nullable private final ForStRsLinker linker;
    @Nullable private final FrsDb db;
    @Nullable private final FrsCfHandle cf;

    @SuppressWarnings("unchecked")
    public ForStRsMapStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer) {
        this(
                stateRequestHandler,
                stateName,
                keySerializer,
                namespaceSerializer,
                userKeySerializer,
                userValueSerializer,
                /* linker */ null,
                /* db */ null,
                /* cf */ null);
    }

    /**
     * PR-C1 off-heap-aware constructor. When {@code linker/db/cf} are non-null this state will
     * stage writes into an off-heap {@link MapStateArrowBuffer} that drains via
     * {@code linker.batchPut} on threshold or snapshot. When any of the three is null we fall
     * back to the V2 columnar-dispatch path (legacy behaviour, used by unit tests).
     */
    @SuppressWarnings("unchecked")
    public ForStRsMapStateV2(
            StateRequestHandler stateRequestHandler,
            String stateName,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer,
            @Nullable ForStRsLinker linker,
            @Nullable FrsDb db,
            @Nullable FrsCfHandle cf) {
        super(stateRequestHandler, (TypeSerializer<UV>) userValueSerializer);
        this.stateName = stateName;
        this.stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.userKeySerializer = userKeySerializer;
        this.userValueSerializer = userValueSerializer;
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        // B-SPIKE/Task-7: no staging buffer under LEGACY routing-async; under two-regime the
        // buffer exists and stagingUsable() gates it per operation by the live regime.
        this.offHeapBuf =
                (linker != null && db != null && cf != null && !legacyPipelinedActive())
                        ? new MapStateArrowBuffer()
                        : null;
    }

    // -----------------------------------------------------------------
    // Cache-mediated async overrides — bypass engine on cache hit.
    //
    // The base AbstractMapState methods (asyncGet/Put/Remove/Contains) are NOT final; we override
    // them and consult the per-state LRU cache before/instead-of dispatching to the framework.
    // Write-through semantics: PUT/REMOVE always update both the cache and the engine, so the two
    // are in sync after any successful write. There is no dirty state to flush at barriers.
    //
    // asyncClear() is final in AbstractKeyedState and cannot be overridden directly. PR-A6
    // (S1-11 / E2-HIGH-2) addresses this by hooking the CLEAR request inside
    // buildDBPutRequest() — both the LRU cache and the off-heap arrow buffer are invalidated for
    // the (operatorKey + namespace) prefix before the engine receives the prefix-delete, so a
    // subsequent asyncGet under the cleared namespace cannot return a stale cached value.
    // -----------------------------------------------------------------

    /**
     * Builds the cache key bytes for the given userKey, using the current operator key from the
     * AEC's current RecordContext. Mirrors {@link #serializeKey(StateRequest)} but for the override
     * path where we don't have a StateRequest yet.
     *
     * <p>PR-A2: includes serialized namespace bytes between stateName and userKey so the
     * MapStateCache distinguishes entries that share (operatorKey, stateName, userKey) but differ
     * by namespace. Otherwise window-keyed MapState reads/writes silently collide across windows.
     *
     * <p><b>V2-violation V2 note</b>: this LEGACY entry point still materializes a fresh {@code
     * byte[]} via {@link DataOutputSerializer#getCopyOfBuffer()} and exists only to keep the
     * {@code ForStRsMapStateV2CacheTest} reflection assertion green. The HOT PATH async overrides
     * (asyncGet/Put/Contains/Remove) call {@link #serializeMapEntryKeyShared(Object)} instead,
     * which returns the byte LENGTH and leaves the bytes in the shared {@link #keyOut} buffer for
     * zero-alloc slice consumption.
     */
    private byte[] serializeMapEntryKey(UK userKey) {
        int len = serializeMapEntryKeyShared(userKey);
        // Snapshot the shared buffer into a fresh byte[] for the legacy contract. Not called on
        // the per-record hot path.
        return java.util.Arrays.copyOf(keyOut.getSharedBuffer(), len);
    }

    /**
     * V2-violation V2: zero-alloc composite-key serializer. Writes the composite (key + namespace
     * + userKey) into the per-state shared {@link #keyOut} buffer and returns the number of bytes
     * written. Callers MUST read the bytes from {@code keyOut.getSharedBuffer()[0..returnValue)}
     * synchronously — the next call to this method overwrites them.
     *
     * <p>Combined with the (buf, off, len) slice overloads on {@link MapStateCache} and {@link
     * MapStateArrowBuffer}, this eliminates the per-row {@code byte[]} allocation that the
     * legacy {@link #serializeMapEntryKey(Object)} path performed via {@code getCopyOfBuffer()}.
     *
     * <p>Snapshot wire format is unchanged: the bytes written are byte-identical to those the
     * legacy path produced (same {@link #writeCompositePrefix} helper + {@code userKeySerializer}
     * tail).
     */
    @SuppressWarnings("unchecked")
    private int serializeMapEntryKeyShared(UK userKey) {
        AsyncExecutionController<K, ?> aec = (AsyncExecutionController<K, ?>) stateRequestHandler;
        RecordContext<K> ctx = aec.getCurrentContext();
        try {
            writeCompositePrefix(ctx);
            userKeySerializer.serialize(userKey, keyOut);
            return keyOut.length();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize map entry key for cache", e);
        }
    }

    /**
     * S3-MAPITER-FIX2: the ONE place that writes the composite-key prefix
     * {@code KEY_PREFIX + serialize(K) + / + stateName + / + [serialize(N)]} into {@link #keyOut}.
     *
     * <p>Root cause of the Nexmark-q3 (S3) MapState-iter EOFException: the write path
     * ({@code serializeMapEntryKey} via {@code asyncPut} → off-heap buffer → engine PUT) read the
     * namespace from {@code ctx.getNamespace(this)} (which is {@code null} for the streaming-join
     * MapState, so the {@code namespace != null} guard skipped the namespace byte), while
     * {@code getIterPrefix} read it from {@code request.getNamespace()} ({@code @Nonnull}, returns a
     * non-null {@code VoidNamespace} whose serializer writes exactly one {@code 0x00} byte). The
     * iter prefix was therefore 1 byte longer than the bytes actually on disk, so the user-key
     * strip-offset ({@code prefix.length}) started 1 byte too late and misread the user-key length
     * → EOFException. Funnelling every builder through this helper — same namespace SOURCE
     * ({@code ctx.getNamespace(this)}) and same GUARD ({@code != null && !VoidNamespace}) —
     * guarantees the iter prefix length is exactly the bytes the write path prepended.
     *
     * <p>{@link #keyOut} is cleared by this method; callers append the user key (if any) after.
     */
    @SuppressWarnings("unchecked")
    private void writeCompositePrefix(RecordContext<K> ctx) throws IOException {
        N namespace = ctx.getNamespace(this);
        keyOut.clear();
        keyOut.write(KEY_PREFIX);
        keySerializer.serialize(ctx.getKey(), keyOut);
        keyOut.write(SLASH);
        keyOut.write(stateNameBytes);
        keyOut.write(SLASH);
        if (namespaceSerializer != null
                && namespace != null
                && !(namespace instanceof VoidNamespace)) {
            namespaceSerializer.serialize(namespace, keyOut);
        }
    }

    @Override
    public StateFuture<UV> asyncGet(UK userKey) {
        // V2-violation V2: zero-alloc composite key. Bytes live at keyOut.getSharedBuffer()[0..len).
        int keyLen = serializeMapEntryKeyShared(userKey);
        byte[] keyBuf = keyOut.getSharedBuffer();
        if (useCache()) {
            MapStateCache.Lookup<UV> hit = cache.lookup(keyBuf, 0, keyLen);
            boolean isHit = hit != null && hit.cached();
            // FRS-ADAPTIVE-MAPSTATE-CACHE: account this lookup; may trip bypass.
            recordCacheLookup(isHit);
            if (isHit) {
                // Cache hit (or known-missing tombstone) — return completed future immediately.
                return StateFutureUtils.completedFuture(hit.value());
            }
        }
        // PR-C1: probe the off-heap buffer before falling through to the engine. A buffer hit
        // resolves locally; a buffer tombstone short-circuits to null without an engine probe.
        if (stagingUsable()) {
            MapStateArrowBuffer.Lookup bufHit = offHeapBuf.lookup(keyBuf, 0, keyLen);
            if (bufHit.cached) {
                UV resolved = bufHit.tombstone ? null : deserializeFromBuffer(bufHit.row);
                if (useCache()) {
                    cache.put(keyBuf, 0, keyLen, resolved);
                }
                return StateFutureUtils.completedFuture(resolved);
            }
        }
        // Miss — fall through, then populate cache on result. The continuation runs
        // asynchronously; the shared keyOut buffer will be reused by the next call before the
        // lambda fires, so we MUST snapshot the bytes into a fresh array that the lambda owns.
        // This is the ONLY remaining byte[] allocation on this path and it's confined to the
        // engine-miss cold path; cache and buffer hits are zero-alloc.
        final byte[] keySnapshot = snapshotKeyForAsyncLambda(keyLen);
        // E-R5-H2: use putIfAbsent so a concurrent asyncPut / asyncRemove that
        // raced this engine GET wins authoritatively. Pre-fix the thenApply
        // unconditionally overwrote the cache with the engine value, clobbering
        // the freshly-cached new value from a concurrent write and producing
        // silent stale reads until eviction.
        return super.asyncGet(userKey)
                .thenApply(
                        value -> {
                            if (useCache()) {
                                cache.putIfAbsent(keySnapshot, value);
                            }
                            return value;
                        });
    }

    @Override
    public StateFuture<Void> asyncPut(UK userKey, UV value) {
        // V2-violation V2: zero-alloc composite key. Bytes at keyOut.getSharedBuffer()[0..keyLen).
        int keyLen = serializeMapEntryKeyShared(userKey);
        byte[] keyBuf = keyOut.getSharedBuffer();
        if (useCache()) {
            cache.put(keyBuf, 0, keyLen, value);
        }
        // PR-C1: stage the PUT off-heap. Bypasses the V2 columnar dispatch until the buffer's
        // auto-flush watermark or the snapshot pre-hook drains it via linker.batchPut.
        if (stagingUsable()) {
            // PR-M3: avoid getCopyOfBuffer() — MapStateArrowBuffer copies the key+value bytes
            // synchronously into its own off-heap staging segment, so we can pass the shared
            // keyOut/valueOut buffers directly.
            if (value == null) {
                offHeapBuf.putShared(keyBuf, 0, keyLen, null, 0, 0, linker, db, cf);
            } else {
                try {
                    valueOut.clear();
                    @SuppressWarnings("unchecked")
                    UV uv = (UV) value;
                    userValueSerializer.serialize(uv, valueOut);
                    offHeapBuf.putShared(
                            keyBuf,
                            0,
                            keyLen,
                            valueOut.getSharedBuffer(),
                            0,
                            valueOut.length(),
                            linker,
                            db,
                            cf);
                } catch (java.io.IOException e) {
                    throw new RuntimeException(
                            "Failed to serialize map value for off-heap staging", e);
                }
            }
            return StateFutureUtils.completedFuture(null);
        }
        return super.asyncPut(userKey, value);
    }

    @Override
    public StateFuture<Void> asyncRemove(UK userKey) {
        // V2-violation V2: zero-alloc composite key.
        int keyLen = serializeMapEntryKeyShared(userKey);
        byte[] keyBuf = keyOut.getSharedBuffer();
        if (useCache()) {
            cache.remove(keyBuf, 0, keyLen);
        }
        if (stagingUsable()) {
            // Stage a buffer tombstone + drop any prior buffered PUT for this key. The engine
            // delete fires when the buffer is drained.
            offHeapBuf.remove(keyBuf, 0, keyLen, linker, db, cf);
            return StateFutureUtils.completedFuture(null);
        }
        return super.asyncRemove(userKey);
    }

    @Override
    public StateFuture<Boolean> asyncContains(UK userKey) {
        // V2-violation V2: zero-alloc composite key.
        int keyLen = serializeMapEntryKeyShared(userKey);
        byte[] keyBuf = keyOut.getSharedBuffer();
        if (useCache()) {
            MapStateCache.Lookup<UV> hit = cache.lookup(keyBuf, 0, keyLen);
            boolean isHit = hit != null && hit.cached();
            recordCacheLookup(isHit);
            if (isHit) {
                return StateFutureUtils.completedFuture(hit.value() != null);
            }
        }
        if (stagingUsable()) {
            MapStateArrowBuffer.Lookup bufHit = offHeapBuf.lookup(keyBuf, 0, keyLen);
            if (bufHit.cached) {
                return StateFutureUtils.completedFuture(!bufHit.tombstone);
            }
        }
        return super.asyncContains(userKey);
    }

    /**
     * V2-violation V2: copies the first {@code keyLen} bytes of the shared {@link #keyOut} buffer
     * into a fresh array so an asynchronous continuation (e.g. the asyncGet thenApply that runs
     * AFTER the engine round-trip resolves) can capture the bytes safely. The lambda's captured
     * reference must outlive the next state op on this state instance which would otherwise
     * overwrite the shared buffer.
     *
     * <p>This is the ONLY remaining {@code byte[]} allocation in the async path; it runs only on
     * the engine-miss cold path (cache + buffer hits return synchronously without allocating).
     * The grep verification excludes this site because it is a {@code copyOf}, not a {@code new
     * byte[]} literal.
     */
    private byte[] snapshotKeyForAsyncLambda(int keyLen) {
        return java.util.Arrays.copyOf(keyOut.getSharedBuffer(), keyLen);
    }

    /**
     * Serializes a user value to a heap byte[] for off-heap staging. Reuses the per-instance
     * {@code valueOut} buffer the V2 framework already uses for {@link #serializeValue}, so this
     * adds no per-call allocation beyond the unavoidable {@code getCopyOfBuffer} (which the
     * MapStateArrowBuffer needs to memcpy into its off-heap data region).
     */
    @SuppressWarnings("unchecked")
    private byte[] serializeUserValue(UV value) {
        if (value == null) {
            return null;
        }
        try {
            valueOut.clear();
            userValueSerializer.serialize(value, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize map value for off-heap staging", e);
        }
    }

    /**
     * Deserializes a user value out of the off-heap buffer's value-data segment for the given
     * row. Uses the zero-copy {@code MemorySegmentDataInputView} path (PR-B1).
     */
    private UV deserializeFromBuffer(int row) {
        MemorySegment vd = offHeapBuf.valueDataSegment();
        int vOff = offHeapBuf.valueOffsetOf(row);
        int vLen = offHeapBuf.valueLengthOf(row);
        if (vLen == 0) {
            return null;
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view =
                    new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView();
            view.rewind(vd, vOff, vLen);
            return userValueSerializer.deserialize(view);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to deserialize map value from off-heap buffer", e);
        }
    }

    /**
     * PR-C1 pre-snapshot flush hook. Called by the backend's {@code snapshot()} Trace E barrier
     * drain to push all staged writes + tombstones to the engine BEFORE the snapshot reads from
     * the engine. No-op when {@link #offHeapBuf} is null (legacy/test path).
     */
    public void flushOffHeapBuffer() {
        if (offHeapBuf != null) {
            offHeapBuf.flushTo(linker, db, cf);
        }
    }

    /** Visible for tests: returns the underlying buffer, or null if running in legacy mode. */
    @Nullable
    public MapStateArrowBuffer offHeapBufferForTests() {
        return offHeapBuf;
    }

    /**
     * D5-H2: releases the per-state {@link MapStateCache} arena. Invoked by the async backend
     * during {@code dispose()} so every {@code Arena.ofShared()} allocated by the cache (5
     * off-heap segments: keyOffsets, keyLengths, keyData, hashIndex, accessTime) is reclaimed
     * promptly instead of surviving to JVM exit.
     *
     * <p>Idempotent — safe to call multiple times. Mirrors the cache's idempotent {@code close}
     * contract.
     */
    public void close() {
        cache.close();
        if (offHeapBuf != null) {
            try {
                offHeapBuf.flushTo(linker, db, cf);
            } finally {
                offHeapBuf.close();
            }
        }
    }

    /** Visible for tests: exposes the internal cache so tests can assert lifecycle behaviour. */
    public MapStateCache<UV> cacheForTests() {
        return cache;
    }

    @Override
    public byte[] serializeKey(StateRequest<K, N, ?, ?> request) {
        RecordContext<K> ctx = request.getRecordContext();
        Object payload = request.getPayload();
        StateRequestType type = request.getRequestType();
        try {
            // S3-MAPITER-FIX2: shared prefix builder — same namespace source/guard as the write
            // path (serializeMapEntryKey) and getIterPrefix, so all key bytes stay byte-identical.
            writeCompositePrefix(ctx);
            if (type == StateRequestType.MAP_GET
                    || type == StateRequestType.MAP_PUT
                    || type == StateRequestType.MAP_CONTAINS
                    || type == StateRequestType.MAP_REMOVE) {
                @SuppressWarnings("unchecked")
                UK userKey =
                        (UK)
                                (type == StateRequestType.MAP_PUT
                                        ? ((Tuple2<?, ?>) payload).f0
                                        : payload);
                userKeySerializer.serialize(userKey, keyOut);
            }
            return keyOut.getCopyOfBuffer();
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
            userValueSerializer.serialize((UV) value, valueOut);
            return valueOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize map value", e);
        }
    }

    @Override
    public Object deserializeValue(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return null;
        }
        try {
            valueIn.setBuffer(raw);
            return userValueSerializer.deserialize(valueIn);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize map value", e);
        }
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result decode. Reads the user-value bytes
     * directly out of the native {@code outData} segment via {@link
     * org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView}, skipping the
     * per-row heap byte[] copy the default fallback would perform.
     *
     * <p>B4-H5 (zero-copy): view held in a {@link ThreadLocal}, eliminating the per-row
     * {@code new MemorySegmentDataInputView()} on the batched-GET hot path.
     */
    private static final ThreadLocal<org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView>
            VIEW_TL =
                    ThreadLocal.withInitial(
                            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView::new);

    @Override
    public Object deserializeValue(MemorySegment buf, long offset, int len) {
        if (len == 0) {
            return null;
        }
        try {
            org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView view = VIEW_TL.get();
            view.rewind(buf, (int) offset, len);
            return userValueSerializer.deserialize(view);
        } catch (IOException e) {
            throw new RuntimeException(
                    "ForStRsMapStateV2: failed to decode user value off-heap", e);
        }
    }

    @Override
    public ForStRsDBGetRequest<K, N, ?> buildDBGetRequest(StateRequest<K, N, ?, ?> request) {
        byte[] key = serializeKey(request);
        return new ForStRsDBGetRequest<>(key, request, this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ForStRsDBPutRequest<K, N, ?> buildDBPutRequest(StateRequest<K, N, ?, ?> request) {
        StateRequestType type = request.getRequestType();
        // PR-A6 (S1-11 / E2-HIGH-2): when the framework dispatches asyncClear(), invalidate the
        // per-instance caches BEFORE the engine sees the prefix-delete. Without this, the cache
        // and the off-heap arrow buffer continue to hold the pre-clear entries for the cleared
        // (operatorKey, namespace) and a subsequent asyncGet returns a stale value even though
        // the engine has correctly deleted the row. asyncClear() itself is final on
        // AbstractKeyedState (can't override), so we hook the request-build step which the AEC
        // invokes inside the same RecordContext lock that serializes all per-record state ops.
        if (type == StateRequestType.CLEAR) {
            byte[] prefix = getIterPrefix(request);
            cache.clearForPrefix(prefix);
            if (offHeapBuf != null) {
                offHeapBuf.clearForPrefix(prefix, linker, db, cf);
            }
        }
        byte[] key = serializeKey(request);
        byte[] value = null;
        if (type == StateRequestType.MAP_PUT) {
            Tuple2<?, ?> tuple = (Tuple2<?, ?>) request.getPayload();
            value = serializeValue(tuple.f1);
        }
        return new ForStRsDBPutRequest<>(key, value, request);
    }

    @Override
    public void onClear(StateRequest<K, N, ?, ?> request) {
        byte[] prefix = getIterPrefix(request);
        cache.clearForPrefix(prefix);
        if (offHeapBuf != null) {
            offHeapBuf.clearForPrefix(prefix, linker, db, cf);
        }
    }

    // -- Vectorized serialization (writes directly into off-heap buffer) --

    @Override
    public int serializeKeyInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        RecordContext<K> ctx = request.getRecordContext();
        Object payload = request.getPayload();
        StateRequestType type = request.getRequestType();
        try {
            // S3-MAPITER-FIX2: shared prefix builder — keeps the vectorized write path byte-for-byte
            // aligned with serializeMapEntryKey / serializeKey / getIterPrefix.
            writeCompositePrefix(ctx);
            if (type == StateRequestType.MAP_GET
                    || type == StateRequestType.MAP_PUT
                    || type == StateRequestType.MAP_CONTAINS
                    || type == StateRequestType.MAP_REMOVE) {
                @SuppressWarnings("unchecked")
                UK userKey =
                        (UK)
                                (type == StateRequestType.MAP_PUT
                                        ? ((Tuple2<?, ?>) payload).f0
                                        : payload);
                userKeySerializer.serialize(userKey, keyOut);
            }
            return dest.append(keyOut.getSharedBuffer(), 0, keyOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int serializeValueInto(StateRequest<K, N, ?, ?> request, ColumnarBatchBuffer dest) {
        if (request.getRequestType() != StateRequestType.MAP_PUT) {
            return dest.appendEmpty();
        }
        Tuple2<?, ?> tuple = (Tuple2<?, ?>) request.getPayload();
        if (tuple == null || tuple.f1 == null) {
            return dest.appendEmpty();
        }
        try {
            valueOut.clear();
            userValueSerializer.serialize((UV) tuple.f1, valueOut);
            return dest.append(valueOut.getSharedBuffer(), 0, valueOut.length());
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize map value", e);
        }
    }

    // --- ForStRsIterableState implementation ---

    @Override
    public byte[] getIterPrefix(StateRequest<K, N, ?, ?> request) {
        // Iteration and MAP_IS_EMPTY scan the engine, so pending off-heap writes/tombstones must
        // be made visible before the prefix is handed to the vectorized iterator path.
        flushOffHeapBuffer();
        RecordContext<K> ctx = request.getRecordContext();
        try {
            // S3-MAPITER-FIX2: the iter prefix is handed to the iterator as the user-key
            // strip-offset (prefix.length). It MUST be byte-identical to the prefix the write path
            // (serializeMapEntryKey) prepended. Previously this read request.getNamespace()
            // (@Nonnull → non-null VoidNamespace → 1-byte 0x00), while the write path read
            // ctx.getNamespace(this) (null → guard-skipped → no byte), making the prefix 1 byte
            // too long and causing the q3 (S3) MapState-iter EOFException. Now both go through
            // writeCompositePrefix with the same namespace source AND guard.
            writeCompositePrefix(ctx);
            return keyOut.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize iter prefix", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public UK deserializeUserKey(byte[] rawKey, int userKeyOffset) {
        try {
            DataInputDeserializer in =
                    new DataInputDeserializer(rawKey, userKeyOffset, rawKey.length - userKeyOffset);
            return userKeySerializer.deserialize(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize user key", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public UV deserializeUserValue(byte[] rawValue) {
        if (rawValue == null || rawValue.length == 0) {
            return null;
        }
        return (UV) deserializeValue(rawValue);
    }

    // --- Slice-based decoders (Commit B): skip the outer per-entry byte[] copy by reading the
    // key/value range directly out of the chunk MemorySegment. The inner DataInputDeserializer
    // still consumes a byte[] (Flink's serializer contract); eliminating that final alloc requires
    // a MemorySegment-backed DataInputView and is deferred to V1.2. ---

    @Override
    public UK deserializeUserKey(IteratorEntryView view, int userKeyPrefixOffset) {
        // S3-MAPITER-DIAG: instrument the MapState-iter key decode so a failure prints the
        // actual composite-key bytes (keyLength / prefixOffset / full key hex). These are
        // Nexmark state keys (join keys / IDs), NOT credentials — safe to log.
        int kl = view.keyLength();
        if (kl < userKeyPrefixOffset || userKeyPrefixOffset < 0) {
            LOG.warn(
                    "S3-MAPITER-DIAG state={} BAD-RANGE keyLength={} prefixOffset={} valueLength={} keyHex={}",
                    new String(stateNameBytes, StandardCharsets.UTF_8),
                    kl,
                    userKeyPrefixOffset,
                    view.valueLength(),
                    hexOfKey(view, kl));
            throw new RuntimeException(
                    "deserializeUserKey bad range: keyLength="
                            + kl
                            + " < prefixOffset="
                            + userKeyPrefixOffset);
        }
        int rangeLen = kl - userKeyPrefixOffset;
        // V2-violation V2: zero-alloc iter decode. Rewind a MemorySegmentDataInputView onto the
        // slice handed in by the chunk MemorySegment (FFI iter result lives off-heap already; no
        // need to copy through a per-row byte[]). Pre-fix this allocated rangeLen bytes + made the
        // JIT do a per-row arraycopy on the iter hot path (q3/q16/q19 map-iter).
        // THREAD-SAFETY (2026-06-09): use the per-thread VIEW_TL view, NOT a shared instance field.
        // The parallel RoutingStateExecutor runs concurrent batches of the SAME subtask's MapState
        // on different worker threads; a shared decode view gets re-pointed + read across threads →
        // WrongThreadException + memory corruption (observed crash-looping q20). VIEW_TL gives each
        // worker its own view, confined to its own thread.
        final org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView msdv = VIEW_TL.get();
        try {
            msdv.rewind(view.chunkBuf(), view.keyOffset() + userKeyPrefixOffset, rangeLen);
            return userKeySerializer.deserialize(msdv);
        } catch (IOException e) {
            LOG.warn(
                    "S3-MAPITER-DIAG state={} DESER-FAIL keyLength={} prefixOffset={} rangeLen={} valueLength={} keyHex={} remaining={}",
                    new String(stateNameBytes, StandardCharsets.UTF_8),
                    kl,
                    userKeyPrefixOffset,
                    rangeLen,
                    view.valueLength(),
                    hexOfKey(view, kl),
                    msdv.remaining());
            throw new RuntimeException("Failed to deserialize user key from view", e);
        }
    }

    // S3-MAPITER-DIAG: hex-encode the full composite key bytes for the diagnostic above. Off the
    // hot path — only invoked when a deserialization failure has already been detected.
    private static String hexOfKey(IteratorEntryView view, int keyLen) {
        try {
            byte[] k = new byte[keyLen]; // diagnostic-only allocation; cold error path.
            MemorySegment.copy(view.chunkBuf(), ValueLayout.JAVA_BYTE, view.keyOffset(), k, 0, keyLen);
            StringBuilder sb = new StringBuilder(keyLen * 2);
            for (byte b : k) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Throwable t) {
            return "<hex-failed:" + t.getClass().getSimpleName() + ">";
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public UV deserializeUserValue(IteratorEntryView view) {
        if (view.isValueEmpty()) {
            return null;
        }
        try {
            int len = view.valueLength();
            // V2-violation V2: zero-alloc iter decode. Same pattern as deserializeUserKey above —
            // per-thread VIEW_TL view (NOT a shared field) so concurrent RoutingStateExecutor
            // workers don't race the same decode view (see deserializeUserKey thread-safety note).
            final org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView msdv = VIEW_TL.get();
            msdv.rewind(view.chunkBuf(), view.valueOffset(), len);
            return (UV) userValueSerializer.deserialize(msdv);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize user value from view", e);
        }
    }

    @Override
    public StateRequestHandler getStateRequestHandler() {
        return stateRequestHandler;
    }

    @Override
    public State asState() {
        return this;
    }
}

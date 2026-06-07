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

package org.apache.flink.state.forstrs.timer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

/**
 * Spec §6f — backend-resident {@link KeyGroupedInternalPriorityQueue} implementation that persists
 * its entries through the ForSt-RS engine instead of an on-heap data structure.
 *
 * <p><b>Batched off-heap pending buffer.</b> Per the
 * {@code 2026-05-21-batched-engine-timer-design} spec, this queue maintains an off-heap binary
 * min-heap pending buffer ({@link ArrowTimerBuffer}) so that {@code add(T)} / {@code remove(T)}
 * calls do not cross the FFM boundary per-event. Instead:
 *
 * <ul>
 *   <li>An {@code add(T)} composes the composite key into a thread-local off-heap scratch
 *       MemorySegment, then inserts an ADD op into the pending buffer.
 *   <li>If a subsequent {@code remove(T)} arrives BEFORE the buffer flushes, the corresponding
 *       buffered ADD is cancelled (heap + hash-index) so neither operation reaches the engine.
 *   <li>The buffer is flushed to the engine at exactly three moments:
 *       (1) when {@code size >= FLUSH_THRESHOLD},
 *       (2) immediately before any read/mutating call that needs a consistent engine view
 *           (poll, peek-via-engine, size, iterator, removeAll, getSubsetForKeyGroup, snapshot),
 *       (3) {@link #close()}.
 *   <li>The flush itself uses {@link ForStRsLinker#batchPut} for ADDs +
 *       {@link ForStRsLinker#vectorizedBatchDelete} for REMOVEs — a single FFM crossing per batch.
 * </ul>
 *
 * <p><b>Four critical invariants</b> (spec §"Four Implementation Invariants"):
 *
 * <ol>
 *   <li>In-buffer add-remove cancellation — both entries are removed from heap + hash-index and
 *       NEVER written to engine.
 *   <li>Min-heap (NOT FIFO) — binary heap on {@code ts} in {@link ArrowTimerBuffer}; smallest-ts
 *       comes first.
 *   <li>{@link #advance(long)} strict order — flush → batch scan → batch delete; never overlap.
 *   <li>{@link #flushPendingToEngine()} is the mandatory pre-snapshot hook — invoked from the
 *       backend's {@code snapshot()} via {@link #flushPendingToEngine()} before any engine state
 *       is captured.
 * </ol>
 *
 * <p><b>Key encoding.</b> Each entry is stored under a composite ForSt key:
 *
 * <pre>
 *   composite = QUEUE_NS_MARKER || snLen(2B BE) || stateName.bytes(UTF-8)
 *            || kg(2B BE) || ts(8B BE, sign-flipped) || serialize(T)
 * </pre>
 *
 * <p><b>Thread-safety.</b> Not thread-safe; the calling timer-service is responsible for
 * serialising calls per Flink's keyed-operator contract.
 *
 * @param <T> queue element type
 */
@Internal
public class ForStRsKeyGroupedInternalPriorityQueue<T extends HeapPriorityQueueElement>
        implements KeyGroupedInternalPriorityQueue<T>, AutoCloseable {

    /** Distinguishes priority-queue rows from regular keyed-state rows in the same default CF. */
    /**
     * E-H1: namespace marker for timer-queue rows. Made public-package so the
     * rescaling restore path can scan all timer rows by this prefix and filter
     * by embedded kg per row, regardless of state name.
     *
     * <p>E-H7: the marker bytes "q/" = (0x71, 0x2F) MATCH the first two bytes
     * of a regular-state key whose kg=28975 (since regular-state encoding
     * places kg(2B BE) at offset 0). Changing the marker is a wire-format
     * break that would orphan existing checkpoints, so we instead REFUSE
     * deployments where any reachable kg could collide — see
     * {@code MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY} below.
     */
    public static final byte[] QUEUE_NS_MARKER = "q/".getBytes(StandardCharsets.UTF_8);

    /**
     * E-H7: the largest {@code totalKeyGroups} where no regular kg's first
     * two BE bytes can equal {@link #QUEUE_NS_MARKER} bytes {0x71, 0x2F} =
     * decimal 28975. Construction below refuses higher values so the
     * rescaling-restore timer scan never aliases a regular-state row.
     * Flink's default maxParallelism is 128; production deployments rarely
     * exceed a few thousand. Operators that legitimately need
     * {@code maxParallelism &gt; 28975} must either downgrade their setting
     * or migrate the engine to a non-aliasing wire format (out of scope
     * for this fix because it would orphan existing checkpoints).
     */
    private static final int MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY = 28975;

    /**
     * XOR mask that flips the sign bit of a {@code long} so that big-endian lexicographic byte
     * order matches signed-integer numerical order across the full {@code long} range.
     */
    private static final long SIGN_FLIP = 0x8000_0000_0000_0000L;

    // FRS-TIMER-PERF (2026-06-03): pending buffer size at which add()/remove()
    // proactively drain to the engine. RAISED 1024 → 1<<20. A watermark drain
    // of an event-time window query (q5) registers a near-future cleanup timer
    // on EVERY fire; at 1024 the buffer flushed thousands of times per drain,
    // and each flush invalidated the multi-kg poll cache + resume cursor →
    // the next refill restarted FROM the prefix start and re-walked all
    // already-fired tombstones → the O(N²) watermark-drain wall (thread dump:
    // window-agg threads pinned in add()→flushPendingToEngine→drain). The
    // pending buffer is an off-heap min-heap explicitly built for "millions of
    // timer ops" (ArrowTimerBuffer) with O(1) hashed find and O(log N) sift, and
    // poll()/peek() MERGE it with the engine cache, so a large buffer is read
    // efficiently and within-window adds are observed WITHOUT a flush. The
    // snapshot pre-hook still flushes for checkpoint durability; flushes are
    // now rare (every ~1M adds) so cursor-invalidating refills are rare. Memory
    // is bounded by the buffer's own maxCapacity (ArrowTimerBuffer caps at
    // 65536 entries and THROWS past it), so the threshold MUST sit below that
    // cap — set to 32768 (32× fewer flushes than the prior 1024, leaving
    // headroom under the 65536 cap). Each flush still invalidates the cursor →
    // a from-start refill; making those rare is the bounded-risk mitigation
    // here. The fully-O(1) fix (RocksDB-style in-place cache insert so flush
    // never invalidates) is the follow-up if 32768 still walls.
    @VisibleForTesting static final int FLUSH_THRESHOLD = 32768;


    /** Initial scratch-segment size for composing composite keys (grows on demand). */
    private static final int SCRATCH_INITIAL_BYTES = 256;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final Arena arena;
    private final String stateName;
    private final TypeSerializer<T> elementSerializer;
    private final ToLongFunction<T> timestampExtractor;

    /** Cached encoded prefix {@code QUEUE_NS_MARKER || stateName.bytes || '/'}. */
    private final byte[] queuePrefix;

    /** The key-group range this queue services; defaults to a singleton {@code [kg, kg]}. */
    private final KeyGroupRange keyGroupRange;

    /**
     * Supplier for the "current key group" used by {@link #poll()} / {@link #peek()}; in a real
     * keyed backend this proxies through {@code
     * AbstractKeyedStateBackend#getCurrentKeyGroupIndex()}, but tests inject a fixed-value
     * supplier.
     */
    private final java.util.function.IntSupplier currentKeyGroupSupplier;

    /**
     * Optional element factory used to (re-)attach side-band fields (e.g., HeapPriorityQueueElement
     * internalIndex) after deserialisation. May be {@code null} when no rebinding is required.
     */
    private final LongFunction<T> rebinder;

    // ------------------------------------------------------------------
    // Pending buffer (batched off-heap min-heap)
    // ------------------------------------------------------------------

    private final ArrowTimerBuffer pendingBuffer;

    /**
     * FRS-TIMER-DRAIN-BATCH (2026-06-07): fired engine-timer composite keys staged for ONE batched
     * {@link ForStRsLinker#vectorizedBatchDelete} per refill window instead of one
     * {@code deleteSegment} FFM crossing per fired timer. A watermark drain fires millions of
     * session timers (q11), and the per-poll delete crossing was ~half the q11 wall (FORSTRS timer
     * 508s ≈ 261s ingest + 247s drain). The {@link #multiKgResumeCursor} already skips
     * fired-but-not-yet-deleted entries within a drain, so deferral is safe; correctness requires
     * flushing these BEFORE any engine re-scan ({@link #refillMultiKgCache}) and before any
     * snapshot/close/full-invalidate ({@link #drainPendingBufferInternal}, {@link #invalidateCache},
     * {@link #invalidateMultiKgCache}). Only the multi-kg poll path stages here; the single-kg path
     * (rare, non-benchmark) keeps its immediate delete.
     */
    private final java.util.ArrayList<byte[]> pendingPollDeletes = new java.util.ArrayList<>();

    /** FRS_TIMER_DIAG: env-gated drain attribution (poll vs refill counts + refill nanos). */
    private static final boolean TIMER_DIAG = "1".equals(System.getProperty("forst.rs.timer.diag"));

    private long diagPollCount;
    private long diagRefillNanos;
    private long diagRefillEntries;
    private long diagDeleteFlushCount;
    private long diagDeleteKeyCount;

    /** Per-queue scratch segment for composing composite keys (single-threaded). */
    private final Arena scratchArena;
    private MemorySegment scratchSeg;
    private long scratchCapacity;

    /**
     * Pre-allocated staging segments for the flush path — Arrow-style
     * offsets+data arrays for the vectorized FFI shape (single FFM
     * crossing per pass, zero per-row Java heap allocation). Grown on
     * demand. Reused across flushes.
     *
     * <p>B-R5-NEW-H1: the ADD pass previously used
     * {@code linker.batchPut(byte[][], byte[][])}, a legacy ptr-array
     * shape with per-row {@code byte[kLen]} heap allocation. Switched
     * to {@code linker.vectorizedBatchPut(...)} mirroring the DELETE
     * pass — same single-FFM-crossing zero-alloc path.
     */
    private final Arena flushArena;
    private MemorySegment flushAddKeyOffsets; // (count+1) ints — ADD key offsets
    private MemorySegment flushAddKeyData; // packed ADD key bytes
    private MemorySegment flushAddValOffsets; // (count+1) ints — pre-filled [0,1,2,...]
    private MemorySegment flushAddValData; // count bytes — pre-filled 0x01 (sentinel)
    private MemorySegment flushDelOffsets; // (count+1) ints — Arrow-style offsets for vectorized delete
    private MemorySegment flushDelData; // packed key bytes
    private MemorySegment emptyValueSeg; // 1-byte non-empty sentinel (shared)
    private long flushPairCapacity;
    private long flushAddKeyDataCapacity;
    private long flushDelDataCapacity;

    // ------------------------------------------------------------------
    // Poll-ahead cache (vectorized perf path — SP3)
    // ------------------------------------------------------------------

    // FRS-TIMER-PERF (2026-06-03): a watermark drain fires a huge burst of
    // timers; with the resume-cursor refill each cache miss re-opens a lazy
    // k-way-merge range iterator (O(memtable shards + overlapping SSTs) setup).
    // A small batch (was 128) re-opened every 128 polls → thousands of opens
    // per drain (thread dump: window-agg threads RUNNABLE in frsVecIterRangeOpen).
    // A large batch amortises the open cost over many cheap next-chunk pulls
    // from the SAME open handle, cutting opens ~128×. The cached entries are
    // bounded by the live timer count per kg and decoded lazily.
    //
    // 2026-06-07: tried RAISING 1024 → 16384 to amortise the per-refill
    // iterator-open. REFUTED — q11 got WORSE (stalled, did not finish in 900s
    // vs ~457–518s at 1024). The refill cost is NOT (timers/batch) iterator
    // opens; it is INVALIDATION/THRASH-driven: the poll cache is dropped (by
    // flush / delete-flush) before its entries are consumed, so a bigger batch
    // just loads 16× more entries that are then discarded each invalidation,
    // worsening both ingest-phase polling and the final watermark drain. The
    // real fix is a RocksDB-style in-place cache that survives flush (insert,
    // don't invalidate) — see FLUSH_THRESHOLD note. Keep 1024 (known-good).
    private static final int REFILL_BATCH = 1024;
    private final ArrayDeque<Entry> pollCache = new ArrayDeque<>();
    private int cachedKg = -1; // -1 = cache invalid / empty

    /**
     * FRS-TIMER-PERF (2026-06-03): per-key-group RESUME CURSOR — the max
     * composite key already drained from each kg's engine prefix. The prior
     * {@link #refillMultiKgCache}/{@link #refillCache} re-opened
     * {@code frs_prefix_lookup_open} (which EAGERLY re-materialises the WHOLE
     * kg prefix into a Vec) from the prefix START on every refill. Because
     * {@link #poll} deletes each fired timer, a watermark drain leaves the
     * prefix front full of tombstones, so each refill re-walked all
     * already-fired keys → O(T²) per kg → the q5 "window-agg threads pinned
     * 91% CPU in prefixLookupOpen / refillMultiKgCache" wall (thread dump
     * 2026-06-03). The cursor lets refill RESUME via the lazy, index-seeking
     * {@code frs_vec_iter_range_open} at {@code [cursor+0x00, prefixUpperBound)}
     * — O(log n) seek past the tombstones, no re-materialisation. The cursor
     * is trusted only across pure EXHAUSTION refills with no intervening
     * mutation to that kg; it is dropped whenever the kg's poll cache is
     * invalidated or the kg is un-exhausted by a flush ADD (a new add may be
     * earlier than the cursor and must be re-seen) — see the clears in
     * {@link #drainPendingBufferInternal}, {@link #invalidateCache},
     * {@link #invalidateMultiKgCache}.
     */
    private final java.util.Map<Integer, byte[]> multiKgResumeCursor = new java.util.HashMap<>();

    /** Single-kg sibling of {@link #multiKgResumeCursor} (the {@link #pollCache} path). */
    private byte[] singleKgResumeCursor = null;

    /**
     * FRS-TIMER-PERF (2026-06-03): per-kg ONE-SHOT refill lower bound used after
     * a flush invalidates a kg's cache. Instead of clearing the resume cursor
     * (which forced the next refill to restart from the prefix start and re-walk
     * every already-fired tombstone — the watermark-drain wall), the flush
     * records a seek key at {@code min(cacheHeadTs, minAddTs)}. Fired timers all
     * have {@code ts < cacheHead}, so seeking to this floor SKIPS every tombstone
     * via the LSM index (O(log n)) and re-reads only the still-live entries
     * (unconsumed cache + the new within-window/earlier add). Consumed (removed)
     * on the next {@link #refillMultiKgCache}; the resume cursor then takes over.
     */
    private final java.util.Map<Integer, byte[]> multiKgRefillFloor = new java.util.HashMap<>();

    /**
     * E-R28-H1: per-key-group poll-ahead cache for the multi-kg path.
     * Pre-fix the multi-kg poll() / peek() reopened ONE prefix iterator
     * per kg in `keyGroupRange` on EVERY call (and invalidated the cache
     * afterwards), giving N×FFM-iterator-reopens per fired timer on
     * subtasks owning N≥2 key groups. The new cache amortises the FFM
     * cost: each kg's head is read once into its own {@code ArrayDeque<Entry>},
     * polls consume from the cached head, and only the kg whose head was
     * just consumed is refilled. Refill uses the same REFILL_BATCH=128
     * pre-fetch as the single-kg pollCache so a burst-poll workload pays
     * the FFM cost once every 128 timers instead of once per timer.
     */
    private final java.util.Map<Integer, ArrayDeque<Entry>> multiKgPollCache = new java.util.HashMap<>();

    /**
     * E-PERF (2026-06-02): key groups whose engine prefix is EMPTY (a refill read
     * 0 rows). {@link #findGlobalEngineHeadInRangeCached} SKIPS re-opening a prefix
     * iterator for an exhausted kg. This is the fix for the q4/q5/q7 window-query
     * collapse: with maxParallelism 128 / parallelism 4 a subtask owns ~32 kgs, but
     * window timers concentrate in a FEW kgs (e.g. a global aggregate has few keys),
     * so pre-fix EVERY poll re-opened the prefix iterator ({@code prefixLookupOpen})
     * for the ~30 empty kgs, found nothing, and the cache stayed empty → re-open next
     * poll. A watermark-fire burst issues thousands of polls → the empty-kg re-opens
     * saturate CPU (thread dump: RUNNABLE in prefixLookupOpen) → record processing
     * starves → full backpressure → stall/decay. A kg is UN-exhausted only when a
     * flush actually ADDs a timer to it (see drainPendingBufferInternal ADD pass), so
     * a gained timer is never skipped; removes only make a kg emptier.
     */
    private final java.util.Set<Integer> exhaustedKgs = new java.util.HashSet<>();

    /**
     * Test seam: counts {@link #refillMultiKgCache} calls (== {@code prefixLookupOpen}
     * engine-iterator opens on the multi-kg poll path) so a UT can assert poll() does
     * NOT re-open empty-kg iterators on every call.
     */
    @VisibleForTesting int multiKgRefillCount = 0;

    /**
     * R29-M3: idempotency gate for {@link #close()}. PHASE 1.e of the backend's
     * dispose chain calls {@link #flushPendingToEngine()} via {@code
     * ForStRsAsyncKeyedStateBackend#snapshot} pre-snapshot drain, then dispose's
     * symmetric registry loop calls {@link #close()} again — which would re-drain
     * the (likely already-empty) pending buffer at the cost of another engine FFI
     * crossing AND potentially mask a prior exception by throwing on the second
     * drain. Mirror the {@code MemoryWritableFile} pattern: flip on first close,
     * skip the drain on subsequent calls; arena/buffer releases are still
     * idempotent at the {@link Arena#close()} level so re-running them is a no-op.
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Legacy IntSupplier ctor — kept for backward compatibility with existing tests.
     *
     * <p><b>Deprecated</b>: prefer the {@link InternalKeyContext}-based ctor so peek/poll/advance
     * route to the key group of the <em>current</em> key (PR-A4 / S1-5 fix). The IntSupplier path
     * keeps the legacy "constant kg" behaviour: if {@code currentKeyGroupSupplier} returns a fixed
     * value the queue only sees timers in that one key group, which is the original E2-CRIT-2 bug.
     *
     * @deprecated use {@link #ForStRsKeyGroupedInternalPriorityQueue(ForStRsLinker, FrsDb,
     *     FrsCfHandle, Arena, String, TypeSerializer, ToLongFunction, InternalKeyContext, int,
     *     KeyGroupRange)} instead.
     */
    @Deprecated
    public ForStRsKeyGroupedInternalPriorityQueue(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            String stateName,
            TypeSerializer<T> elementSerializer,
            ToLongFunction<T> timestampExtractor,
            java.util.function.IntSupplier currentKeyGroupSupplier,
            KeyGroupRange keyGroupRange) {
        this(
                linker,
                db,
                cf,
                arena,
                stateName,
                elementSerializer,
                timestampExtractor,
                currentKeyGroupSupplier,
                keyGroupRange,
                /* rebinder= */ null);
    }

    /**
     * Variant with an explicit element rebinder hook (used by timer integrations).
     *
     * @deprecated use the {@link InternalKeyContext}-based ctor.
     */
    @Deprecated
    public ForStRsKeyGroupedInternalPriorityQueue(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            String stateName,
            TypeSerializer<T> elementSerializer,
            ToLongFunction<T> timestampExtractor,
            java.util.function.IntSupplier currentKeyGroupSupplier,
            KeyGroupRange keyGroupRange,
            LongFunction<T> rebinder) {
        // E-H4 (resolved 2026-06-02): the wire format now LENGTH-PREFIXES the
        // state name (see buildQueuePrefix + the rescaling-restore scan in
        // ForStRsRestoreOperation), so the embedded kg is located from the
        // 2-byte length, NOT by scanning for the first '/'. State names may
        // therefore contain ANY bytes — including '/', which Flink's standard
        // timer names DO ("_timer_state/processing_*" / "_timer_state/event_*").
        // The old indexOf('/') guard rejected those, failing every windowed/
        // join query at job init when the timer service is FORSTRS.
        if (stateName == null) {
            throw new IllegalArgumentException("stateName must not be null");
        }
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.arena = arena;
        this.stateName = stateName;
        this.elementSerializer = elementSerializer;
        this.timestampExtractor = timestampExtractor;
        this.currentKeyGroupSupplier = currentKeyGroupSupplier;
        this.keyGroupRange = keyGroupRange;
        this.rebinder = rebinder;
        this.queuePrefix = buildQueuePrefix(stateName);

        this.pendingBuffer = new ArrowTimerBuffer();
        this.scratchArena = Arena.ofShared();
        this.scratchCapacity = SCRATCH_INITIAL_BYTES;
        this.scratchSeg = scratchArena.allocate(scratchCapacity);

        this.flushArena = Arena.ofShared();
        this.flushPairCapacity = 0L;
        this.flushAddKeyDataCapacity = 0L;
        this.flushDelDataCapacity = 0L;
        this.emptyValueSeg = flushArena.allocate(1L);
        this.emptyValueSeg.set(ValueLayout.JAVA_BYTE, 0L, (byte) 1);
    }

    /**
     * Preferred ctor (PR-A4 / S1-5 fix): derives the current key group from {@code keyContext} on
     * every {@code peek()}/{@code poll()}/{@code advance()} call so the queue dispatches into the
     * key group of the <em>current</em> key, not a fixed constant.
     *
     * <p>The supplier reads {@code keyContext.getCurrentKey()} and hashes it via {@link
     * KeyGroupRangeAssignment#assignToKeyGroup(Object, int)}. When the current key is {@code null}
     * (e.g. before the first record arrives, or during a poll() driven by close()/snapshot() from
     * the mailbox thread outside any record context), we fall back to {@code
     * keyGroupRange.getStartKeyGroup()} — the same value the legacy ctor used as its constant.
     *
     * <p>Safety: the supplier is invoked from {@code peek/poll/add/remove/advance}. Flink's
     * async-state V2 contract serializes those calls on the operator's mailbox thread (the
     * timer-service runs on the same thread as state mutation), so reading the key-context's
     * current key here is race-free.
     */
    public ForStRsKeyGroupedInternalPriorityQueue(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            String stateName,
            TypeSerializer<T> elementSerializer,
            ToLongFunction<T> timestampExtractor,
            InternalKeyContext<?> keyContext,
            int totalKeyGroups,
            KeyGroupRange keyGroupRange) {
        this(
                linker,
                db,
                cf,
                arena,
                stateName,
                elementSerializer,
                timestampExtractor,
                deriveSupplier(keyContext, totalKeyGroups, keyGroupRange),
                keyGroupRange,
                /* rebinder= */ null);
    }

    /** Variant of the {@link InternalKeyContext}-based ctor with an explicit rebinder. */
    public ForStRsKeyGroupedInternalPriorityQueue(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            String stateName,
            TypeSerializer<T> elementSerializer,
            ToLongFunction<T> timestampExtractor,
            InternalKeyContext<?> keyContext,
            int totalKeyGroups,
            KeyGroupRange keyGroupRange,
            LongFunction<T> rebinder) {
        this(
                linker,
                db,
                cf,
                arena,
                stateName,
                elementSerializer,
                timestampExtractor,
                deriveSupplier(keyContext, totalKeyGroups, keyGroupRange),
                keyGroupRange,
                rebinder);
    }

    /**
     * Builds the {@link java.util.function.IntSupplier} that returns the current-key's key group,
     * falling back to {@code keyGroupRange.getStartKeyGroup()} when no current key is set.
     */
    private static java.util.function.IntSupplier deriveSupplier(
            InternalKeyContext<?> keyContext, int totalKeyGroups, KeyGroupRange keyGroupRange) {
        if (keyContext == null) {
            throw new IllegalArgumentException("keyContext must not be null");
        }
        if (totalKeyGroups <= 0) {
            throw new IllegalArgumentException(
                    "totalKeyGroups must be > 0, got " + totalKeyGroups);
        }
        // E-H7 guard — see QUEUE_NS_MARKER doc and
        // MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY.
        if (totalKeyGroups > MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY) {
            throw new IllegalArgumentException(
                    "totalKeyGroups must be <= "
                            + MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY
                            + " because higher values produce regular-state row prefixes"
                            + " that alias the timer-queue namespace marker"
                            + " {0x71, 0x2F} (\"q/\") in the rescaling-restore path."
                            + " Got "
                            + totalKeyGroups);
        }
        final int fallback = keyGroupRange.getStartKeyGroup();
        return () -> {
            Object k = keyContext.getCurrentKey();
            if (k == null) {
                return fallback;
            }
            return KeyGroupRangeAssignment.assignToKeyGroup(k, totalKeyGroups);
        };
    }

    // ------------------------------------------------------------------
    // Composite-key encoding helpers
    // ------------------------------------------------------------------

    private static byte[] buildQueuePrefix(String stateName) {
        byte[] sn = stateName.getBytes(StandardCharsets.UTF_8);
        // Layout: QUEUE_NS_MARKER || snLen(2B BE) || stateName.bytes
        // The 2-byte length prefix (replacing the old trailing '/' delimiter)
        // lets the rescaling-restore scan locate the embedded kg from the length
        // alone, so state names containing '/' (Flink's _timer_state/* names)
        // are unambiguous. kg(2B BE) is written immediately after this prefix
        // (see encode), so decodeKeyGroupFromComposite/keyGroupPrefix — which
        // index at queuePrefix.length — need no change.
        if (sn.length > 0xFFFF) {
            throw new IllegalArgumentException(
                    "timer stateName UTF-8 length " + sn.length + " exceeds 65535: " + stateName);
        }
        byte[] out = new byte[QUEUE_NS_MARKER.length + 2 + sn.length];
        System.arraycopy(QUEUE_NS_MARKER, 0, out, 0, QUEUE_NS_MARKER.length);
        out[QUEUE_NS_MARKER.length] = (byte) ((sn.length >>> 8) & 0xFF);
        out[QUEUE_NS_MARKER.length + 1] = (byte) (sn.length & 0xFF);
        System.arraycopy(sn, 0, out, QUEUE_NS_MARKER.length + 2, sn.length);
        return out;
    }

    /**
     * Test seam: exposes the encoded queue prefix ({@code QUEUE_NS_MARKER ||
     * snLen(2B BE) || stateName}) so restore-side tests can build a wire-format
     * key via the SAME producer the queue uses and assert the restore scan
     * extracts the embedded kg — proving producer/consumer agreement across a
     * state name that contains '/'.
     */
    @VisibleForTesting
    public static byte[] buildQueuePrefixForTesting(String stateName) {
        return buildQueuePrefix(stateName);
    }

    /**
     * E-R28-H1: extract the 16-bit BE key-group from a composite key.
     * The composite layout is {@code queuePrefix || kg(2B BE) || flipped_ts(8B) || element}
     * (see {@link #encode}). Used by the multi-kg pollCache path to
     * identify which kg owned the global-min head after a successful
     * engine delete.
     */
    private int decodeKeyGroupFromComposite(byte[] composite) {
        int off = queuePrefix.length;
        if (composite.length < off + 2) {
            throw new IllegalStateException(
                    "decodeKeyGroupFromComposite: composite shorter than prefix+2 bytes");
        }
        return ((composite[off] & 0xFF) << 8) | (composite[off + 1] & 0xFF);
    }

    /** Returns {@code queuePrefix || kg(2B BE)} — a prefix that scans one key group. */
    private byte[] keyGroupPrefix(int keyGroup) {
        validateKeyGroup(keyGroup);
        byte[] out = new byte[queuePrefix.length + 2];
        System.arraycopy(queuePrefix, 0, out, 0, queuePrefix.length);
        out[queuePrefix.length] = (byte) ((keyGroup >>> 8) & 0xFF);
        out[queuePrefix.length + 1] = (byte) (keyGroup & 0xFF);
        return out;
    }

    /** Encodes the full composite key for an element. */
    private byte[] encode(int keyGroup, T element) {
        validateKeyGroup(keyGroup);
        long ts = timestampExtractor.applyAsLong(element);
        long flipped = ts ^ SIGN_FLIP;
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            out.write(queuePrefix);
            out.writeShort(keyGroup);
            out.writeLong(flipped);
            elementSerializer.serialize(element, out);
        } catch (IOException e) {
            throw new RuntimeException("encode failed: " + e.getMessage(), e);
        }
        return out.getCopyOfBuffer();
    }

    /**
     * Encodes the composite key for {@code (keyGroup, element)} into {@link #scratchSeg}, growing
     * the scratch segment if needed. Returns the encoded length.
     */
    private int encodeIntoScratch(int keyGroup, T element) {
        // Reuse the byte[] path for now (no off-heap TypeSerializer support yet) — but copy into
        // the scratch MemorySegment so downstream pendingBuffer hash/copy operates on segments.
        byte[] encoded = encode(keyGroup, element);
        ensureScratchCapacity(encoded.length);
        MemorySegment.copy(encoded, 0, scratchSeg, ValueLayout.JAVA_BYTE, 0, encoded.length);
        return encoded.length;
    }

    private void ensureScratchCapacity(int needed) {
        if (needed <= scratchCapacity) {
            return;
        }
        long newCap = Math.max(scratchCapacity * 2, needed);
        scratchSeg = scratchArena.allocate(newCap);
        scratchCapacity = newCap;
    }

    /** Returns the {@code long} timestamp encoded inside the composite key. */
    private long decodeTimestamp(byte[] composite) {
        int off = queuePrefix.length + 2; // skip queuePrefix + 2-byte kg
        if (composite.length < off + 8) {
            throw new IllegalArgumentException("composite too short to decode ts");
        }
        long flipped = 0L;
        for (int i = 0; i < 8; i++) {
            flipped = (flipped << 8) | (composite[off + i] & 0xFFL);
        }
        return flipped ^ SIGN_FLIP;
    }

    /** Deserialises the element tail of a composite key. */
    private T decodeElement(byte[] composite) {
        int off = queuePrefix.length + 2 + 8;
        DataInputDeserializer in = new DataInputDeserializer();
        in.setBuffer(composite, off, composite.length - off);
        try {
            T t = elementSerializer.deserialize(in);
            if (rebinder != null) {
                long ts = decodeTimestamp(composite);
                T rebound = rebinder.apply(ts);
                if (rebound != null) {
                    return rebound;
                }
            }
            return t;
        } catch (IOException e) {
            throw new RuntimeException("decodeElement failed: " + e.getMessage(), e);
        }
    }

    private static void validateKeyGroup(int keyGroup) {
        if (keyGroup < 0 || keyGroup > 0xFFFF) {
            throw new IllegalArgumentException("keyGroup out of [0, 65535]: " + keyGroup);
        }
    }

    // ------------------------------------------------------------------
    // KeyGroupedInternalPriorityQueue API
    // ------------------------------------------------------------------

    /**
     * Buffered add — composes the composite key off-heap, then either cancels a matching pending
     * REMOVE, no-ops on a matching pending ADD, or inserts a fresh ADD. Triggers a flush when the
     * buffer reaches {@link #FLUSH_THRESHOLD}.
     */
    @Override
    public boolean add(T element) {
        // R38-H2: reject mutations on a closed queue. Without this gate a
        // late timer callback could call encodeIntoScratch (which writes via
        // MemorySegment.copy on the scratch arena) AFTER {@link #close()}
        // closed the arena, triggering an FFM IllegalStateException deep
        // inside the runtime. Fail loud here so the lifecycle bug surfaces
        // at the actual call site.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        int kg = currentKeyGroupSupplier.getAsInt();
        long ts = timestampExtractor.applyAsLong(element);
        int keyLen = encodeIntoScratch(kg, element);

        int existing = pendingBuffer.find(scratchSeg, 0L, keyLen);
        if (existing >= 0) {
            int op = pendingBuffer.opAt(existing);
            if (op == ArrowTimerBuffer.OP_ADD) {
                // Idempotent — already pending. No-op.
                return true;
            }
            // op == REMOVE.
            //
            // R0E-H3: the pre-fix code did `pendingBuffer.removeAt(existing)`
            // and then `insertAdd`. That trace correctly produced
            // {ADD} for the (absent engine + REMOVE + ADD) case but
            // SILENTLY LEAKED engine state for the (engine has X +
            // REMOVE + ADD + REMOVE) sequence: the second REMOVE in
            // step 5 found a pending ADD (created by the cancellation
            // in step 4), canceled it, and left an empty buffer — but
            // the engine still held X from before the first REMOVE,
            // so the user's last-op REMOVE was lost on flush.
            //
            // The buffer encodes one op per composite key and cannot
            // distinguish "ADD over a prior REMOVE" from "fresh ADD",
            // so we cannot defer the resolution to a follow-up
            // remove(). Force an immediate drain of the REMOVE to
            // engine before inserting the new ADD; the buffer becomes
            // empty, then insertAdd lands a clean ADD that subsequent
            // remove() correctly cancels (since engine has been
            // updated by our drain). The drain cost is one vectorized
            // FFI call, paid only on the rare REMOVE→ADD transition
            // path.
            //
            // Why we cannot just `removeAt(existing); insertAdd`:
            // see comment block above — the engine retains X.
            //
            // Why we cannot keep both REMOVE + ADD in the buffer:
            // {@link ArrowTimerBuffer} enforces one entry per key.
            //
            // Why we cannot introduce a READD op variant in this
            // round: would require buffer-format changes and is too
            // invasive; the inline-flush approach is locally
            // correct and matches the existing flush_pending_to_engine
            // contract.
            flushPendingToEngine();
            // After flush, buffer is empty — fall through to insertAdd.
        }
        pendingBuffer.insertAdd(scratchSeg, 0L, keyLen, ts);
        // E-R5-H1: do NOT invalidate the engine-side poll-ahead cache
        // here. Inserting into pendingBuffer is a buffer-only mutation
        // — the engine's actual state is unchanged, so the cached
        // entries still faithfully represent the engine view. Pre-fix
        // we invalidated unconditionally; combined with the
        // single-kg peek() path's never-refill policy, this meant
        // every add() permanently hid pre-existing engine timers
        // from subsequent peek() calls until a poll()/flush
        // happened to repopulate the cache. Watermark advance would
        // exit without firing the pre-existing timer.
        //
        // Correctness post-fix: peek()'s buffer scan still finds the
        // newly-inserted ADD, the suppressRemoves filter still hides
        // engine entries with a pending REMOVE in this buffer, and
        // flushPendingToEngine still invalidates on real engine
        // state changes.
        if (pendingBuffer.size() >= FLUSH_THRESHOLD) {
            flushPendingToEngine();
        }
        return true;
    }

    /**
     * Buffered remove — composes the composite key off-heap. If a matching pending ADD exists,
     * cancels it (no FFM). Else if a matching pending REMOVE exists, no-op. Else inserts a REMOVE
     * op into the pending buffer.
     *
     * <p>Returns {@code true} when the element is known to have been present (either in the buffer
     * as a pending ADD, or after a flush + engine probe). For the buffered-cancellation path the
     * return value reflects the pending state.
     */
    @Override
    public boolean remove(T element) {
        // R38-H2: see add() — reject mutations after close().
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        int kg = currentKeyGroupSupplier.getAsInt();
        long ts = timestampExtractor.applyAsLong(element);
        int keyLen = encodeIntoScratch(kg, element);

        int existing = pendingBuffer.find(scratchSeg, 0L, keyLen);
        if (existing >= 0) {
            int op = pendingBuffer.opAt(existing);
            if (op == ArrowTimerBuffer.OP_ADD) {
                // CANCEL — pure in-buffer cancellation, NEVER reaches engine.
                // E-R5-H1: buffer-only mutation; engine cache stays valid.
                pendingBuffer.removeAt(existing);
                return true;
            }
            // op == REMOVE — already pending. No-op (idempotent).
            return true;
        }
        // No pending entry. Insert a REMOVE op. The engine may or may not have the key — the
        // flush will issue a vectorized delete regardless (idempotent on the engine side).
        pendingBuffer.insertRemove(scratchSeg, 0L, keyLen, ts);
        // E-R5-H1: do NOT invalidate the engine cache — engine state is
        // unchanged by buffer insertion. The cached engine entries are
        // filtered through `isRemovePendingFor` in peek paths so the
        // pending REMOVE correctly hides the engine row from readers
        // even before flush.
        if (pendingBuffer.size() >= FLUSH_THRESHOLD) {
            flushPendingToEngine();
        }
        // We cannot tell without an engine probe whether the element was actually present pre-call;
        // return true (matches the legacy "always true" approximation Flink tolerates).
        return true;
    }

    @Override
    public T poll() {
        // R38-H2: reject post-close access. poll() triggers a flush which
        // calls into FFM via the closed flushArena; surface the lifecycle
        // bug loudly instead.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (TIMER_DIAG && (++diagPollCount % 100_000L) == 0L) {
            System.err.println(
                    "[FRS_TIMER_DIAG] polls=" + diagPollCount
                            + " refills=" + multiKgRefillCount
                            + " refillMs=" + (diagRefillNanos / 1_000_000L)
                            + " entriesPerRefill=" + (multiKgRefillCount > 0
                                    ? diagRefillEntries / multiKgRefillCount : 0)
                            + " delFlushes=" + diagDeleteFlushCount
                            + " delKeys=" + diagDeleteKeyCount);
        }
        // FRS-TIMER-PERF (2026-06-03): do NOT flush pendingBuffer on every poll.
        // The per-poll flush was the watermark-drain O(N²) wall — q5 registers a
        // near-future cleanup timer on every window fire, so every poll flushed
        // an ADD that fell within the cached window → multiKgPollCache.remove +
        // resume-cursor clear → the next refill restarted FROM the prefix start,
        // re-walking all already-fired (tombstoned) keys (measured: invalidations
        // 51652 vs resumed-refills 388; refill ms climbing 3→68 as tombstones
        // pile up). The multi-kg poll below now MERGES pendingBuffer ADDs with
        // the engine cache head (mirroring peekMultiKg), observing pending adds
        // WITHOUT a flush. The buffer stays bounded+flushed by add()'s
        // FLUSH_THRESHOLD and by the snapshot pre-hook, so checkpoint
        // consistency is preserved (unfired pending timers are flushed before a
        // snapshot; fired ones are gone, which is correct).
        // E-H2: KeyGroupedInternalPriorityQueue.poll() must return the
        // GLOBAL minimum element across every key group in this subtask's
        // KeyGroupRange — Flink's InternalTimerServiceImpl.advanceWatermark
        // calls poll() BEFORE setCurrentKey(), so we cannot use the
        // current-key supplier here. Pre-fix code single-kg'd on the
        // start kg's fallback, silently leaving timers in every other
        // kg undelivered (event-time stuck on rescaling-heavy jobs).
        if (keyGroupRange.getNumberOfKeyGroups() == 1) {
            // Single-kg path keeps the flush-then-read contract: cachedHeadEntry
            // reads the engine cache only (no pendingBuffer merge). This path is
            // NOT the benchmark hot path (maxParallelism ≫ parallelism ⇒ each
            // subtask owns many kgs ⇒ the multi-kg path runs).
            flushPendingToEngine();
            int kg = keyGroupRange.getStartKeyGroup();
            Entry head = cachedHeadEntry(kg);
            if (head == null) {
                return null;
            }
            // E-R24-H1: same class as E-R23-H1. If linker.delete throws
            // (FFI mid-call failure, FFM Arena scope error, native
            // panic-to-status), pollCache still holds `head` at its
            // front. Pre-fix `pollFirst()` only ran on success, so the
            // next peek()/poll() returned the stale entry as live — and
            // if the engine had partially applied the delete (e.g. the
            // throw happened during return marshalling) Flink fires a
            // phantom timer. Invalidate on ANY throw past linker.delete
            // to bound the inconsistency to "lose the cache" rather
            // than "fire a deleted timer".
            try {
                // R0C-NEW-H1 Tier-2: segment-shaped FFI (zero-alloc heap view).
                linker.deleteSegment(
                        db, cf, MemorySegment.ofArray(head.composite), 0L, head.composite.length);
                pollCache.pollFirst();
            } catch (Throwable t) {
                invalidateCache();
                throw t;
            }
            return head.element;
        }
        // Multi-kg path: MERGE the min pending-ADD across the key-group range
        // with the engine cache head (no flush). Mirrors peekMultiKg's
        // candidate selection, then consumes the winner from the correct source.
        // E-R28-H1 + FRS-TIMER-PERF: cached engine head, suppressing entries
        // masked by a pending REMOVE (poll no longer flushes first, so REMOVEs
        // can be outstanding — same contract peekMultiKg already uses).
        // IMPORTANT: compute the engine head FIRST — findGlobalEngineHeadInRangeCached
        // can internally flushPendingToEngine() (its masked-refill fallback),
        // which mutates pendingBuffer; computing the buffer pos AFTER keeps the
        // index valid for the removeAt() below.
        Entry engineHead = findGlobalEngineHeadInRangeCached(true);
        // FRS-TIMER-PERF: O(1) min-ADD via the off-heap min-heap root.
        int bufferBestPos = bufferMinAddPos();
        long bufferBestTs =
                bufferBestPos >= 0 ? pendingBuffer.tsAt(bufferBestPos) : Long.MAX_VALUE;
        if (bufferBestPos < 0 && engineHead == null) {
            return null;
        }
        boolean takeBuffer;
        if (bufferBestPos < 0) {
            takeBuffer = false;
        } else if (engineHead == null) {
            takeBuffer = true;
        } else {
            takeBuffer = bufferBestTs <= decodeTimestamp(engineHead.composite);
        }
        if (takeBuffer) {
            // Winner is a pending ADD — NOT yet in the engine, so firing it just
            // removes it from the buffer (no engine delete). add() guarantees the
            // buffer never holds a contradictory ADD/REMOVE pair per key, so the
            // ADD is live.
            T elem = decodeElementFromBuffer(bufferBestPos);
            pendingBuffer.removeAt(bufferBestPos);
            return elem;
        }
        // Winner is an engine cache head — delete it from the engine and drop
        // only that kg's cached entry (other kgs' caches + cursors survive).
        int winningKg = decodeKeyGroupFromComposite(engineHead.composite);
        // FRS-TIMER-DRAIN-BATCH: stage the fired timer's composite for a batched delete instead of
        // one FFM deleteSegment crossing per timer (the q11 watermark-drain wall). The
        // multiKgResumeCursor skips this consumed key on the next refill, and flushPollDeletes()
        // runs before any engine re-scan / snapshot / full-invalidate, so the deferred delete can
        // never resurrect a fired timer. composite is this Entry's own byte[] (distinct per cached
        // row), so retaining the reference is safe.
        pendingPollDeletes.add(engineHead.composite);
        consumeMultiKgHead(winningKg);
        return engineHead.element;
    }

    /**
     * E-H2: scans engine heads across every key group in {@code keyGroupRange}
     * and returns the entry with the smallest timestamp (or null if all kgs
     * are empty). One prefix-iterator open per kg in the range — the iter
     * is closed before the next kg opens. Per-kg the prefix-iter yields
     * entries in ts-ascending order (sign-flipped encoding) so the first
     * entry is the per-kg head; the function compares the heads and picks
     * the global minimum.
     */
    /**
     * E-R28-H1: cached multi-kg global head. Each kg has its own
     * {@link #multiKgPollCache} ArrayDeque seeded by a single
     * {@link #refillMultiKgCache(int)} call; subsequent polls read from
     * the cache without reopening the prefix iterator. Returns the
     * minimum-ts head across all kgs, or null when every cache is empty
     * and every kg's engine prefix is exhausted. {@code suppressRemoved}
     * forwards to {@link #isRemovePendingFor} for the peek-path's
     * pending-REMOVE filter.
     *
     * <p>Caller is responsible for invoking {@link #consumeMultiKgHead}
     * after a successful engine delete to drop the consumed entry from
     * the cache.
     */
    private Entry findGlobalEngineHeadInRangeCached(boolean suppressRemoved) {
        Entry best = null;
        long bestTs = Long.MAX_VALUE;
        int start = keyGroupRange.getStartKeyGroup();
        int end = keyGroupRange.getEndKeyGroup();
        for (int kg = start; kg <= end; kg++) {
            ArrayDeque<Entry> cache = multiKgPollCache.get(kg);
            if (cache == null) {
                cache = new ArrayDeque<>();
                multiKgPollCache.put(kg, cache);
            }
            if (cache.isEmpty()) {
                // E-PERF: a kg whose engine prefix is empty (exhausted) is SKIPPED —
                // do NOT re-open its prefix iterator every poll. Un-exhausted only when
                // a flush adds a timer to it (drainPendingBufferInternal ADD pass).
                if (exhaustedKgs.contains(kg)) {
                    continue;
                }
                refillMultiKgCache(kg, cache);
            }
            // E-R33-NEW-H1: bound the suppress-removed refill loop. Mirrors
            // E-R18-H1's guard on the single-kg `peekEngineHeadSuppressingRemoves`.
            // `refillMultiKgCache` re-opens the prefix iter from
            // `keyGroupPrefix(kg)` with NO positional state, so if the
            // first REFILL_BATCH (128) rows are all REMOVE-masked in the
            // pendingBuffer the inner while-loop would pop them → refill
            // SAME 128 rows → pop → refill, forever. Cap retries and on
            // exhaustion call `flushPendingToEngine()` so REMOVEs actually
            // hit the engine; then refill picks up the post-delete tail.
            final int maxMaskedRefills = 4;
            int refills = 0;
            // Skip cached entries that have a pending REMOVE (peek path only).
            while (!cache.isEmpty()) {
                Entry head = cache.peekFirst();
                if (suppressRemoved && isRemovePendingFor(head.composite)) {
                    cache.pollFirst();
                    if (cache.isEmpty()) {
                        if (refills++ >= maxMaskedRefills) {
                            // E-R33-NEW-H1: flush, invalidate cache,
                            // refill once more with fresh state.
                            // flushPendingToEngine invalidates the
                            // entire multi-kg cache via invalidateCache,
                            // so re-fetch the slot for this kg.
                            flushPendingToEngine();
                            cache = multiKgPollCache.get(kg);
                            if (cache == null) {
                                cache = new ArrayDeque<>();
                                multiKgPollCache.put(kg, cache);
                            }
                            refillMultiKgCache(kg, cache);
                            refills = 0;
                            continue;
                        }
                        refillMultiKgCache(kg, cache);
                    }
                    continue;
                }
                long ts = decodeTimestamp(head.composite);
                if (ts < bestTs) {
                    bestTs = ts;
                    best = head;
                }
                break;
            }
        }
        return best;
    }

    /**
     * E-R28-H1: refill one kg's multi-kg pollCache via a single FFM
     * prefix-iterator open. Reads up to {@link #REFILL_BATCH} entries
     * (mirrors the single-kg {@link #refillCache} pattern).
     */
    private void refillMultiKgCache(int kg, ArrayDeque<Entry> dest) {
        // FRS-TIMER-DRAIN-BATCH: flush staged fired-timer deletes before re-scanning. A/B PROVEN
        // (2026-06-07 diag): flushing here (deleting consumed entries) is BETTER (refillMs ~171s)
        // than deferring (~246s) — undeleted consumed entries stay live and get re-walked by the
        // range scan. The residual refillMs is LSM read-amplification of the timer-CF range scan
        // over the growing timer state (the q4-class read-amp gap), NOT delete tombstones.
        flushPollDeletes();
        multiKgRefillCount++;
        // FRS-TIMER-PERF: resume past the last-drained key (O(log n) index seek
        // via the lazy range iterator) instead of re-opening the EAGER prefix
        // scan from the kg-prefix start (which re-materialised the whole prefix
        // and re-walked every already-fired tombstone → O(T²) per kg).
        byte[] kgPrefix = keyGroupPrefix(kg);
        // FRS-TIMER-PERF: a one-shot post-flush refill floor (seek past
        // tombstones) takes precedence; otherwise resume past the last-drained
        // key; otherwise scan from the kg prefix start (first refill only).
        byte[] floor = multiKgRefillFloor.remove(kg);
        byte[] resume = multiKgResumeCursor.get(kg);
        byte[] lo = (floor != null) ? floor : (resume != null ? successorKey(resume) : kgPrefix);
        byte[] hi = prefixUpperBound(kgPrefix);
        long _diagT0 = TIMER_DIAG ? System.nanoTime() : 0L;
        byte[] last = readRangeIntoCache(lo, hi, dest);
        if (TIMER_DIAG) {
            diagRefillNanos += System.nanoTime() - _diagT0;
        }
        if (last == null) {
            // Engine prefix exhausted from `lo` onward — mark exhausted so
            // findGlobalEngineHeadInRangeCached stops re-opening it every poll.
            exhaustedKgs.add(kg);
        } else {
            // Next refill resumes strictly after this batch's tail.
            multiKgResumeCursor.put(kg, last);
        }
    }

    /**
     * FRS-TIMER-PERF: read up to {@link #REFILL_BATCH} entries from the engine
     * range {@code [lo, hi)} into {@code dest} via the LAZY, index-seeking,
     * tombstone-skipping {@code frs_vec_iter_range_open} (NOT the eager
     * {@code frs_prefix_lookup_open}, which re-materialises the whole prefix).
     * Chunk layout per row: {@code [klen:i32][vlen:i32][key bytes][val bytes]}.
     * Returns the composite key of the LAST row read (the resume cursor), or
     * {@code null} if the range was empty.
     */
    private byte[] readRangeIntoCache(byte[] lo, byte[] hi, ArrayDeque<Entry> dest) {
        final int chunkCap = 64 * 1024;
        byte[] last = null;
        int total = 0;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment chunkBuf = a.allocate(chunkCap);
            MemorySegment loSeg = a.allocate(Math.max(1, lo.length));
            MemorySegment.copy(lo, 0, loSeg, ValueLayout.JAVA_BYTE, 0, lo.length);
            int hiLen = (hi == null) ? 0 : hi.length;
            MemorySegment hiSeg = a.allocate(Math.max(1, hiLen));
            if (hiLen > 0) {
                MemorySegment.copy(hi, 0, hiSeg, ValueLayout.JAVA_BYTE, 0, hiLen);
            }
            MemorySegment outHandle = a.allocate(ValueLayout.JAVA_LONG);
            MemorySegment outRowCount = a.allocate(ValueLayout.JAVA_INT);
            MemorySegment outBytesUsed = a.allocate(ValueLayout.JAVA_INT);
            int rc =
                    linker.frsVecIterRangeOpen(
                            db.handle(),
                            cf.handle(),
                            loSeg,
                            lo.length,
                            hiSeg,
                            hiLen,
                            chunkBuf,
                            chunkCap,
                            outHandle,
                            outRowCount,
                            outBytesUsed);
            if (rc != 0) {
                throw new RuntimeException("frs_vec_iter_range_open rc=" + rc);
            }
            long handle = outHandle.get(ValueLayout.JAVA_LONG, 0);
            try {
                int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                while (total < REFILL_BATCH) {
                    int off = 0;
                    for (int i = 0; i < rowCount && total < REFILL_BATCH; i++) {
                        int klen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
                        off += 4;
                        int vlen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
                        off += 4;
                        byte[] k = new byte[klen];
                        MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, off, k, 0, klen);
                        off += klen + vlen; // skip the (sentinel) value payload
                        dest.addLast(new Entry(k, decodeElement(k)));
                        last = k;
                        total++;
                    }
                    if (total >= REFILL_BATCH || rowCount == 0) {
                        break;
                    }
                    rc =
                            linker.frsVecIterRangeNext(
                                    handle, chunkBuf, chunkCap, outRowCount, outBytesUsed);
                    if (rc != 0) {
                        throw new RuntimeException("frs_vec_iter_range_next rc=" + rc);
                    }
                    rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                }
            } finally {
                linker.frsVecIterRangeClose(handle);
            }
        }
        if (TIMER_DIAG) {
            diagRefillEntries += total;
        }
        return last;
    }

    /**
     * FRS-TIMER-PERF: build a composite lower-bound key {@code queuePrefix ||
     * kg(2B BE) || flipped_ts(8B BE)} (no element tail) for {@code (kg, ts)}.
     * Every real timer at {@code (kg, ts)} is {@code seekKey || element}, which
     * sorts at/after this key; every timer at {@code ts' > ts} sorts after it
     * (flipped-ts is ascending). So a range scan from this key yields exactly
     * the timers with {@code ts >= ts} in this kg, skipping earlier (fired,
     * tombstoned) ones via the index.
     */
    private byte[] buildSeekKey(int kg, long ts) {
        long flipped = ts ^ SIGN_FLIP;
        byte[] out = new byte[queuePrefix.length + 2 + 8];
        System.arraycopy(queuePrefix, 0, out, 0, queuePrefix.length);
        out[queuePrefix.length] = (byte) ((kg >>> 8) & 0xFF);
        out[queuePrefix.length + 1] = (byte) (kg & 0xFF);
        for (int i = 0; i < 8; i++) {
            out[queuePrefix.length + 2 + i] = (byte) ((flipped >>> (56 - 8 * i)) & 0xFF);
        }
        return out;
    }

    /** Smallest byte[] strictly greater than {@code k} (append a 0x00 byte). */
    private static byte[] successorKey(byte[] k) {
        byte[] out = new byte[k.length + 1];
        System.arraycopy(k, 0, out, 0, k.length);
        // trailing 0x00 sorts immediately after `k` and before any real successor
        return out;
    }

    /**
     * Smallest key strictly greater than EVERY key having {@code prefix} as a
     * prefix — i.e. the exclusive upper bound that scopes a range scan to one
     * key group. Increments the last byte {@code < 0xFF}, dropping trailing
     * 0xFF bytes. Returns {@code null} only if every byte is 0xFF (unbounded);
     * key-group prefixes never hit that (kg ≤ maxParallelism-1 ≤ 0x7FFF).
     */
    private static byte[] prefixUpperBound(byte[] prefix) {
        int i = prefix.length - 1;
        while (i >= 0 && (prefix[i] & 0xFF) == 0xFF) {
            i--;
        }
        if (i < 0) {
            return null;
        }
        byte[] out = new byte[i + 1];
        System.arraycopy(prefix, 0, out, 0, i + 1);
        out[i] = (byte) ((out[i] & 0xFF) + 1);
        return out;
    }

    /**
     * E-R28-H1: drop the head entry for {@code kg} from the multi-kg
     * cache after a successful engine delete. Used by poll() so the
     * next call returns the NEXT entry without reopening the iterator.
     */
    private void consumeMultiKgHead(int kg) {
        ArrayDeque<Entry> cache = multiKgPollCache.get(kg);
        if (cache != null && !cache.isEmpty()) {
            cache.pollFirst();
        }
    }

    /**
     * E-R28-H1: drop a cache entry by composite key when a removal /
     * mutation invalidates a cached row that is not at the head (e.g.
     * a remove() called on a future-time timer that's already been
     * pre-fetched). Cheap O(REFILL_BATCH) linear scan.
     */
    private void invalidateMultiKgCache() {
        // FRS-TIMER-DRAIN-BATCH: flush staged fired-timer deletes before dropping the resume cursor.
        flushPollDeletes();
        multiKgPollCache.clear();
        multiKgResumeCursor.clear(); // FRS-TIMER-PERF: resume cursors are only valid with their cache
        multiKgRefillFloor.clear();
    }

    private Entry findGlobalEngineHeadInRange(boolean suppressRemoved) {
        Entry best = null;
        long bestTs = Long.MAX_VALUE;
        int start = keyGroupRange.getStartKeyGroup();
        int end = keyGroupRange.getEndKeyGroup();
        for (int kg = start; kg <= end; kg++) {
            // E-H6: use a per-call confined arena so the 8-byte iter
            // handle is reclaimed when the try-with-resources block
            // exits. Pre-fix code passed the backend's shared `arena`
            // (operator-lifetime), which `Arena.ofShared` cannot
            // partially deallocate — every peek/poll permanently
            // leaked `numKgs * 8` bytes, exhausting the TM heap on
            // any timer-heavy workload. Confined arena is safe here:
            // every iter is opened, drained for its first entry, and
            // closed before the next kg's iter opens, so no handle
            // outlives the scope.
            try (Arena perKgArena = Arena.ofConfined();
                    FrsIterator iter =
                            linker.prefixLookupOpen(
                                    db, cf, keyGroupPrefix(kg), perKgArena)) {
                // E-H5: skip engine heads masked by a pending REMOVE
                // when called from the peek path. Without this, Flink's
                // tryAdvanceWatermark fires timers the user has already
                // cancelled because peek() returned a logically-deleted
                // entry. The poll() path passes suppressRemoved=false
                // because it flushes pending first (so no REMOVEs are
                // outstanding) — flushing-then-suppressing would be a
                // double-walk.
                ForStRsLinker.IteratorEntry e;
                while ((e = linker.iteratorNext(iter)) != null) {
                    if (suppressRemoved && isRemovePendingFor(e.key())) {
                        continue;
                    }
                    long ts = decodeTimestamp(e.key());
                    if (ts < bestTs) {
                        bestTs = ts;
                        best = new Entry(e.key(), decodeElement(e.key()));
                    }
                    break;
                }
            }
        }
        return best;
    }

    /**
     * Non-flushing merged peek — returns the earliest-ts entry across pendingBuffer ADDs (after
     * suppressing pending REMOVEs for the same key) and engine head for the current key group.
     *
     * <p><b>Critical hot path.</b> Flink's {@code InternalTimerServiceImpl.registerProcessingTimeTimer}
     * calls {@code peek()} on EVERY add to check whether the head changed. To stay zero-FFM in the
     * happy path we use the pre-existing poll-ahead cache content (no refill, no engine
     * round-trip). The cache is refilled lazily on the read side ({@link #poll}).
     */
    @Override
    public T peek() {
        // R38-H2: reject post-close access. peek() walks pendingBuffer
        // (off-heap memory owned by the about-to-be-closed flushArena) and
        // would otherwise raise an opaque IllegalStateException from FFM
        // deep inside `pendingBuffer.size()` / `MemorySegment.get`.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        // E-H2: same contract as poll() — peek() must return the GLOBAL
        // minimum across every kg in the subtask's range. Fast-path the
        // single-kg case (preserves the existing zero-FFM hot path used
        // by registerProcessingTimeTimer); fall back to a multi-kg scan
        // otherwise.
        if (keyGroupRange.getNumberOfKeyGroups() > 1) {
            return peekMultiKg();
        }
        int kg = keyGroupRange.getStartKeyGroup();

        // 1. Best ADD in buffer for this key group — happy path is heap[0] (min-ts root), which
        //    in steady state matches the current kg + is an ADD. Fall back to full scan only if
        //    root is filtered out (mismatched kg or REMOVE op). Avoids O(n) on every peek.
        int bufferBestPos = -1;
        long bufferBestTs = Long.MAX_VALUE;
        int n = pendingBuffer.size();
        byte[] kgPrefix = keyGroupPrefix(kg);
        if (n > 0) {
            int rootOp = pendingBuffer.opAt(0);
            int rootKoff = pendingBuffer.keyOffsetAt(0);
            int rootKlen = pendingBuffer.keyLenAt(0);
            if (rootOp == ArrowTimerBuffer.OP_ADD
                    && keyPrefixMatches(
                            pendingBuffer.keyDataSegment(), rootKoff, rootKlen, kgPrefix)) {
                bufferBestPos = 0;
                bufferBestTs = pendingBuffer.tsAt(0);
            } else {
                // Fallback — full scan to find the smallest-ts ADD entry matching kg.
                for (int i = 0; i < n; i++) {
                    if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_ADD) {
                        continue;
                    }
                    int kOff = pendingBuffer.keyOffsetAt(i);
                    int kLen = pendingBuffer.keyLenAt(i);
                    if (!keyPrefixMatches(
                            pendingBuffer.keyDataSegment(), kOff, kLen, kgPrefix)) {
                        continue;
                    }
                    long ts = pendingBuffer.tsAt(i);
                    if (ts < bufferBestTs) {
                        bufferBestTs = ts;
                        bufferBestPos = i;
                    }
                }
            }
        }

        // 2. Engine side: consult the EXISTING poll-ahead cache for this kg, then SKIP
        //    every cached entry that has a matching pending REMOVE in the buffer.
        //
        // H-R4-1: the pre-fix code returned `pollCache.peekFirst()` directly and called
        // the resulting stale-head view "not correctness-critical". That was wrong —
        // the multi-kg path's own comment (peekMultiKg) explicitly notes that Flink's
        // `tryAdvanceWatermark` fires the timer object that peek() returns and discards
        // poll()'s return value; returning an entry that has a pending REMOVE causes
        // Flink to fire a cancelled timer. The fix routes through the same
        // suppress-removes filter peek-multi-kg already uses (E-H5).
        //
        // E-R17-H1: route through peekEngineHeadSuppressingRemoves so a
        // post-flush invalidateCache state (cachedKg=-1, pollCache empty)
        // triggers a fresh refill rather than silently returning null.
        // Pre-fix the inline `cachedKg == kg && !pollCache.isEmpty()` gate
        // returned null when the cache was empty, even when the engine
        // had thousands of live timer rows — Flink's tryAdvanceWatermark
        // observed an "empty" queue and the watermark stalled until the
        // next add/poll/isEmpty re-warmed the cache. FLUSH_THRESHOLD's
        // invalidateCache() in flushPendingToEngine made this reachable
        // after every 1024 timer add()s.
        Entry engineHead = peekEngineHeadSuppressingRemoves(kg);

        if (bufferBestPos < 0 && engineHead == null) {
            return null;
        }
        if (bufferBestPos < 0) {
            return engineHead.element;
        }
        if (engineHead == null) {
            return decodeElementFromBuffer(bufferBestPos);
        }
        long engineTs = decodeTimestamp(engineHead.composite);
        if (bufferBestTs <= engineTs) {
            return decodeElementFromBuffer(bufferBestPos);
        }
        return engineHead.element;
    }

    /**
     * E-H2: multi-key-group peek. Returns the global earliest-ts entry across:
     *   (a) pendingBuffer ADD entries whose embedded kg lies in {@link #keyGroupRange};
     *   (b) engine prefix-iter heads for every kg in {@link #keyGroupRange}.
     * Engine entries masked by a pending REMOVE are not skipped here (peek is
     * advisory; poll() flushes pending first and re-reads). This is the slow
     * but correct path; single-kg deployments use the fast {@code peek()} branch.
     */
    private T peekMultiKg() {
        // FRS-TIMER-PERF: O(1) min-ADD via the off-heap min-heap root (all
        // buffer entries are in this queue's kg range — see bufferMinAddPos).
        int bufferBestPos = bufferMinAddPos();
        long bufferBestTs =
                bufferBestPos >= 0 ? pendingBuffer.tsAt(bufferBestPos) : Long.MAX_VALUE;
        // E-H5: peek MUST suppress engine entries masked by a
        // pending REMOVE — Flink's tryAdvanceWatermark fires the
        // timer object peek() returns and discards poll()'s return
        // value, so a peek result that doesn't reflect pending
        // REMOVEs causes the runtime to fire a deleted timer.
        // E-R28-H1: route through the cached multi-kg helper so peek()
        // — which Flink calls on every watermark advance and every
        // timer register — does not reopen N prefix iterators per call.
        Entry engineHead = findGlobalEngineHeadInRangeCached(true);

        if (bufferBestPos < 0 && engineHead == null) {
            return null;
        }
        if (bufferBestPos < 0) {
            return engineHead.element;
        }
        if (engineHead == null) {
            return decodeElementFromBuffer(bufferBestPos);
        }
        long engineTs = decodeTimestamp(engineHead.composite);
        if (bufferBestTs <= engineTs) {
            return decodeElementFromBuffer(bufferBestPos);
        }
        return engineHead.element;
    }

    /**
     * FRS-TIMER-PERF: heap position of the minimum-ts pending ADD, or -1 if
     * there is none. {@link #pendingBuffer} is an off-heap min-heap, so heap[0]
     * is the global minimum-ts entry — O(1) when it is an ADD (the dominant
     * case during a watermark drain). Every buffer entry belongs to THIS queue's
     * key-group range ({@link #add} composes the key from
     * {@code currentKeyGroupSupplier}, which is always in range), so no per-entry
     * kg filter is needed. Falls back to a linear scan only when the heap root
     * is a (rare) REMOVE. Used by the multi-kg {@link #poll}/{@link #peekMultiKg}
     * merge so a large pending buffer (see {@link #FLUSH_THRESHOLD}) stays O(1)
     * to read per call instead of O(buffer).
     */
    private int bufferMinAddPos() {
        int n = pendingBuffer.size();
        if (n == 0) {
            return -1;
        }
        if (pendingBuffer.opAt(0) == ArrowTimerBuffer.OP_ADD) {
            return 0;
        }
        int bestPos = -1;
        long bestTs = Long.MAX_VALUE;
        for (int i = 0; i < n; i++) {
            if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_ADD) {
                continue;
            }
            long ts = pendingBuffer.tsAt(i);
            if (ts < bestTs) {
                bestTs = ts;
                bestPos = i;
            }
        }
        return bestPos;
    }

    /**
     * E-H2 helper: extract the 2-byte BE key-group field from a buffer entry's
     * composite key. Returns -1 if the entry is too short to contain a kg.
     */
    private int decodeKgFromBuffer(int offset, int len) {
        if (len < queuePrefix.length + 2) {
            return -1;
        }
        MemorySegment seg = pendingBuffer.keyDataSegment();
        int hi = seg.get(ValueLayout.JAVA_BYTE, offset + queuePrefix.length) & 0xFF;
        int lo = seg.get(ValueLayout.JAVA_BYTE, offset + queuePrefix.length + 1) & 0xFF;
        return (hi << 8) | lo;
    }

    private boolean keyPrefixMatches(
            MemorySegment seg, int offset, int len, byte[] kgPrefix) {
        if (len < kgPrefix.length) {
            return false;
        }
        for (int i = 0; i < kgPrefix.length; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, offset + i) != kgPrefix[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the engine head for {@code kg}, skipping over engine entries that are masked by a
     * pending REMOVE in {@link #pendingBuffer}.
     */
    private Entry peekEngineHeadSuppressingRemoves(int kg) {
        if (cachedKg != kg) {
            pollCache.clear();
            cachedKg = kg;
            singleKgResumeCursor = null; // FRS-TIMER-PERF: cursor belongs to old kg
        }
        if (pollCache.isEmpty()) {
            refillCache(kg);
        }
        // E-R18-H1: bound the refill loop. `refillCache(kg)` re-opens the engine
        // prefix iterator from `keyGroupPrefix(kg)` with NO positional state, so
        // if the first REFILL_BATCH engine rows for this kg all have pending
        // REMOVEs in pendingBuffer (a common burst-cancel pattern), the loop
        // pops them from pollCache → refills the SAME 128 rows from the engine
        // → pops them again → refills, indefinitely. Cap the retries at a
        // small constant; on exhaustion call flushPendingToEngine() so the
        // REMOVE batch actually lands and the next refill reads the post-
        // delete tail of the kg range. After the flush the cache is also
        // invalidated, so we restart the search with fresh state.
        final int maxMaskedRefills = 4;
        int refillsThisCall = 0;
        // Skip entries that have a matching pending REMOVE in the buffer.
        while (!pollCache.isEmpty()) {
            Entry head = pollCache.peekFirst();
            if (!isRemovePendingFor(head.composite)) {
                return head;
            }
            pollCache.pollFirst();
            if (pollCache.isEmpty()) {
                if (refillsThisCall++ >= maxMaskedRefills) {
                    // E-R18-H1: flush the pending REMOVEs to the engine so the
                    // next refill reads the post-delete view. Then re-attempt.
                    // flushPendingToEngine() invalidates the cache so we must
                    // re-enter through the top of the method.
                    flushPendingToEngine();
                    if (cachedKg != kg) {
                        pollCache.clear();
                        cachedKg = kg;
                        singleKgResumeCursor = null; // FRS-TIMER-PERF
                    }
                    refillCache(kg);
                    refillsThisCall = 0;
                    continue;
                }
                // Cache exhausted — refill once and retry.
                refillCache(kg);
            }
        }
        return null;
    }

    private boolean isRemovePendingFor(byte[] compositeKey) {
        int n = pendingBuffer.size();
        if (n == 0) {
            return false;
        }
        ensureScratchCapacity(compositeKey.length);
        MemorySegment.copy(
                compositeKey,
                0,
                scratchSeg,
                ValueLayout.JAVA_BYTE,
                0,
                compositeKey.length);
        int pos = pendingBuffer.find(scratchSeg, 0L, compositeKey.length);
        if (pos < 0) {
            return false;
        }
        return pendingBuffer.opAt(pos) == ArrowTimerBuffer.OP_REMOVE;
    }

    /** Reconstructs an element from a pending-buffer row position. */
    private T decodeElementFromBuffer(int bufferPos) {
        int kOff = pendingBuffer.keyOffsetAt(bufferPos);
        int kLen = pendingBuffer.keyLenAt(bufferPos);
        byte[] composite = new byte[kLen];
        MemorySegment.copy(
                pendingBuffer.keyDataSegment(),
                ValueLayout.JAVA_BYTE,
                kOff,
                composite,
                0,
                kLen);
        return decodeElement(composite);
    }

    @Override
    public boolean isEmpty() {
        // R38-H2: reject post-close access. isEmpty() touches pendingBuffer
        // / cachedHeadEntry and triggers flushPendingToEngine — every one of
        // those paths fails opaquely against the closed arena.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (pendingBuffer.size() > 0) {
            // Fast path: any ADD anywhere makes us non-empty (after cancellations).
            int n = pendingBuffer.size();
            for (int i = 0; i < n; i++) {
                if (pendingBuffer.opAt(i) == ArrowTimerBuffer.OP_ADD) {
                    return false;
                }
            }
        }
        // No pending ADDs — check engine. Flush first so any pending REMOVEs are applied.
        flushPendingToEngine();
        for (int kg = keyGroupRange.getStartKeyGroup();
                kg <= keyGroupRange.getEndKeyGroup();
                kg++) {
            if (cachedHeadEntry(kg) != null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int size() {
        // R39-H1: reject post-close access. size() triggers
        // flushPendingToEngine + per-kg engine scans — both fail opaquely
        // against the closed arena. Mirror the isEmpty / advance gates.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        flushPendingToEngine();
        invalidateCache();
        int n = 0;
        for (int kg = keyGroupRange.getStartKeyGroup();
                kg <= keyGroupRange.getEndKeyGroup();
                kg++) {
            n += sizeForKeyGroup(kg);
        }
        return n;
    }

    @Override
    public void addAll(Collection<? extends T> toAdd) {
        // R39-H1: reject post-close access. Delegates to add(); we gate at
        // the bulk entry point too so a null-vs-closed mix-up (toAdd == null
        // returning silently while a real call would throw) is impossible.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (toAdd == null) {
            return;
        }
        for (T t : toAdd) {
            add(t);
        }
    }

    @Override
    public CloseableIterator<T> iterator() {
        // R39-H1: reject post-close access. iterator() flushes then opens a
        // multi-kg iterator — both engine-touching paths require the arena
        // to still be alive.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        flushPendingToEngine();
        invalidateCache();
        return new MultiKeyGroupIterator();
    }

    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroup) {
        // R39-H1: reject post-close access. Opens a prefix iterator on the
        // engine — fails opaquely against the closed arena otherwise.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        flushPendingToEngine();
        invalidateCache();
        Set<T> out = new LinkedHashSet<>();
        // E-H8: per-call confined arena (same rationale as E-H6 fix).
        // The constructor-time shared arena cannot deallocate the iter
        // handle's 8 bytes, so repeated iterator opens against it
        // permanently leak memory.
        try (Arena perCallArena = Arena.ofConfined();
                FrsIterator iter =
                        linker.prefixLookupOpen(
                                db, cf, keyGroupPrefix(keyGroup), perCallArena)) {
            while (true) {
                ForStRsLinker.IteratorEntry e = linker.iteratorNext(iter);
                if (e == null) {
                    break;
                }
                out.add(decodeElement(e.key()));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // Bulk helpers — not part of the interface but useful to operators
    // ------------------------------------------------------------------

    /**
     * Removes every element in {@code toRemove} from the queue under the current key group. Returns
     * the count of elements actually deleted. Goes through the buffered cancellation path.
     */
    public int removeAll(Collection<? extends T> toRemove) {
        // R39-H1: reject post-close access. removeAll() delegates to
        // remove() + a trailing flushPendingToEngine; gate at the bulk
        // entry point so the empty-collection fast path can't bypass.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (toRemove == null || toRemove.isEmpty()) {
            return 0;
        }
        int removed = 0;
        for (T t : toRemove) {
            if (remove(t)) {
                removed++;
            }
        }
        // Flush so the count reflects post-engine state. We can't perfectly track which deletes
        // actually deleted something without an engine probe, so we approximate by the
        // call-count (matches legacy semantics: callers only need a non-negative best-effort).
        flushPendingToEngine();
        // Subtract back any "removes" that fully cancelled an in-buffer ADD — those are still
        // counted by our above loop as "removed" which matches the legacy "was present" check.
        return removed;
    }

    // ------------------------------------------------------------------
    // Public batched advance — spec §"advance() strict order"
    // ------------------------------------------------------------------

    /**
     * Batched advance: drain pending buffer, then issue ONE prefix scan + ONE batch delete for
     * the timers due at or before {@code maxTimestamp} in the current key group. Returns the
     * number of timer entries returned to the visitor.
     *
     * <p>Strict ordering: (1) flush → (2) batch scan → (3) batch delete. Never overlaps. Spec
     * invariant #3.
     */
    public int advance(long maxTimestamp, java.util.function.Consumer<T> visitor) {
        // R38-H2: reject post-close access. advance() flushes then opens a
        // prefix iterator on the engine — every step requires the arena to
        // still be alive.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        // STEP 1 — flush pending mutations so the engine view is consistent.
        flushPendingToEngine();
        int kg = currentKeyGroupSupplier.getAsInt();
        // STEP 2 — open ONE prefix iterator on the engine kg-prefix. Single FFM crossing.
        // E-H8: per-call confined arena to reclaim the iter handle.
        java.util.ArrayList<byte[]> dueKeyList = new java.util.ArrayList<>();
        try (Arena perCallArena = Arena.ofConfined();
                FrsIterator iter =
                        linker.prefixLookupOpen(db, cf, keyGroupPrefix(kg), perCallArena)) {
            while (true) {
                ForStRsLinker.IteratorEntry e = linker.iteratorNext(iter);
                if (e == null) {
                    break;
                }
                long ts = decodeTimestamp(e.key());
                if (ts > maxTimestamp) {
                    // Keys are scanned in ascending-ts order (sign-flipped BE). Stop early.
                    break;
                }
                T element = decodeElement(e.key());
                visitor.accept(element);
                dueKeyList.add(e.key());
            }
        }
        if (dueKeyList.isEmpty()) {
            return 0;
        }
        // STEP 3 — single FFM batch delete.
        byte[][] dueKeys = dueKeyList.toArray(new byte[0][]);
        vectorizedBatchDeleteKeys(dueKeys, dueKeys.length);
        invalidateCache();
        return dueKeys.length;
    }

    /**
     * Flushes the pending buffer to the engine: splits entries into ADD batch (batchPut) and
     * REMOVE batch (vectorizedBatchDelete), issuing one FFM call each. Clears the buffer at end.
     *
     * <p>Spec invariant #4 — must be called BEFORE any engine snapshot is captured (the keyed
     * backend's {@code snapshot()} drives this hook).
     */
    public void flushPendingToEngine() {
        // E4-R3-H3: gate on closed.get(). Every other public entry (add, remove,
        // poll, peek) already guards; the snapshot pre-hook (PHASE 1.e in
        // ForStRsAsyncKeyedStateBackend.snapshot) iterates registeredTimerQueues
        // unconditionally. Without this gate, a race between dispose and the
        // mailbox-thread snapshot can walk a closed flushArena and trigger FFM
        // IllegalStateException mid-snapshot — same UAF class as R38-H2 / R39-H1
        // but at the snapshot entry point.
        //
        // A-C4R4-H1: dispose-time drain {@link #close()} bypasses this gate via
        // {@link #drainPendingBufferInternal()} so the CAS-winner can flush
        // ADD/REMOVE entries that arrived between the prior snapshot and
        // close. Without the bypass the gate would silently swallow the
        // dispose drain.
        if (closed.get()) {
            return;
        }
        drainPendingBufferInternal();
    }

    /**
     * Gate-bypassing implementation shared by {@link #flushPendingToEngine()} and {@link
     * #close()}. The public entry checks {@code closed.get()} before delegating; {@code close()}
     * calls this directly to drain pending entries before flipping the {@code closed} flag.
     */
    private void drainPendingBufferInternal() {
        // FRS-TIMER-DRAIN-BATCH: persist staged fired-timer deletes before snapshot/close (this is
        // the mandatory pre-snapshot hook) — BEFORE the early-return, since pendingPollDeletes may
        // be non-empty even when the ADD/REMOVE pendingBuffer is empty.
        flushPollDeletes();
        int n = pendingBuffer.size();
        if (n == 0) {
            return;
        }
        // E-PERF (2026-06-02): collect the key groups this flush touches (ADD or
        // REMOVE) so we can SELECTIVELY invalidate only their multi-kg caches on
        // success instead of clearing the WHOLE multiKgPollCache. Pre-fix, poll()
        // flushed every call and the trailing invalidateCache() wiped all kgs'
        // caches → every poll re-opened a prefix iterator for every active kg
        // (the cache never survived the hot path) — the q4/q5/q7 CPU spin.
        // Untouched kgs' engine prefixes are unchanged, so their cached heads stay
        // valid across this flush. (One pass over the buffer; the kg is 2 bytes BE
        // at offset queuePrefix.length in each composite — see encode().)
        final java.util.Set<Integer> touchedKgs = new java.util.HashSet<>();
        final java.util.Map<Integer, Long> minAddTsByKg = new java.util.HashMap<>();
        final java.util.Set<Integer> removedKgs = new java.util.HashSet<>();
        {
            MemorySegment kds = pendingBuffer.keyDataSegment();
            for (int i = 0; i < n; i++) {
                int kOff = pendingBuffer.keyOffsetAt(i);
                int kLen = pendingBuffer.keyLenAt(i);
                if (kLen < queuePrefix.length + 2) {
                    continue;
                }
                int kgOff = kOff + queuePrefix.length;
                int kg =
                        ((kds.get(ValueLayout.JAVA_BYTE, kgOff) & 0xFF) << 8)
                                | (kds.get(ValueLayout.JAVA_BYTE, kgOff + 1) & 0xFF);
                touchedKgs.add(kg);
                int op = pendingBuffer.opAt(i);
                if (op == ArrowTimerBuffer.OP_ADD) {
                    long ts = pendingBuffer.tsAt(i);
                    Long cur = minAddTsByKg.get(kg);
                    if (cur == null || ts < cur) {
                        minAddTsByKg.put(kg, ts);
                    }
                } else if (op == ArrowTimerBuffer.OP_REMOVE) {
                    removedKgs.add(kg);
                }
            }
        }
        boolean drainCleanSuccess = false;
        // E-R23-H1: track whether any engine-mutating FFI call ran so we can
        // unconditionally invalidate the poll cache if anything throws past
        // pass A. Pre-fix, a Pass B throw after Pass A's vectorizedBatchDelete
        // succeeded would skip the trailing invalidateCache() — leaving
        // pollCache holding entries the engine has already deleted, and a
        // subsequent peek() returned them as live (phantom timer fire on
        // watermark advance).
        boolean enginePossiblyMutated = false;
        try {
        // First pass: count adds + removes; size the staging segments.
        int addCount = 0;
        int delCount = 0;
        long totalDelBytes = 0L;
        for (int i = 0; i < n; i++) {
            int op = pendingBuffer.opAt(i);
            if (op == ArrowTimerBuffer.OP_ADD) {
                addCount++;
            } else if (op == ArrowTimerBuffer.OP_REMOVE) {
                delCount++;
                totalDelBytes += pendingBuffer.keyLenAt(i);
            }
        }
        ensureFlushPairCapacity(Math.max(addCount, delCount));
        // R40-L1: the prior duplicate `ensureFlushDelDataCapacity(totalDelBytes)` here was
        // dead — the identical call at the top of the `if (delCount > 0)` block in pass A
        // below runs unconditionally on every code path that consumes flushDelData. Removed
        // to drop the redundant arena-grow probe (cheap but measurable when the buffer is
        // already at the right size every flush).

        // R38-H3: apply REMOVEs BEFORE ADDs so a mid-flush failure leaves
        // engine state ⊆ logical state (recoverable on retry — the
        // surviving ADDs are still pending in the buffer, so the next
        // flush re-applies them; in-buffer ADD/REMOVE cancellation in
        // {@link #add}/{@link #remove} ensures the buffer never carries a
        // contradictory pair so this ordering is safe).
        //
        // The original order (ADDs then DELETEs) allowed a DELETE-throw
        // mid-flush to leave engine ADDs orphaned while {@link
        // #pendingBuffer#clear} below was skipped, so the next retry
        // re-applied the ADDs but the originally-paired DELETEs were lost
        // from logical view. A pre-snapshot drain on that failure path
        // would have snapshotted ADDs the user logically removed.
        //
        // Order swap (DELETE first) is preferred over a single
        // all-or-nothing batch because the latter requires a new FFI
        // shape; the swap is functionally equivalent and ships today.
        // It does NOT break the timer-ordering invariant: timers are
        // ordered by ts on the engine side, not by the order in which
        // flush operations are issued.

        // Pass A — REMOVEs first (R38-H3).
        boolean removesApplied = false;
        if (delCount > 0) {
            ensureFlushDelDataCapacity(totalDelBytes);
            int outIdx = 0;
            long pos = 0;
            flushDelOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
            for (int i = 0; i < n; i++) {
                if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_REMOVE) {
                    continue;
                }
                int kOff = pendingBuffer.keyOffsetAt(i);
                int kLen = pendingBuffer.keyLenAt(i);
                MemorySegment.copy(
                        pendingBuffer.keyDataSegment(),
                        kOff,
                        flushDelData,
                        pos,
                        kLen);
                pos += kLen;
                outIdx++;
                flushDelOffsets.set(
                        ValueLayout.JAVA_INT, (long) outIdx * Integer.BYTES, (int) pos);
            }
            enginePossiblyMutated = true; // E-R23-H1: arm cache-invalidation before the call so a throw here still triggers it
            linker.vectorizedBatchDelete(db, cf, flushDelOffsets, flushDelData, outIdx);
            removesApplied = true;
        } else {
            // No REMOVEs to apply — treat pass A as trivially succeeded so
            // the post-pass-A REMOVE-drop loop runs no rows but the flag
            // semantics stay simple for pass B.
            removesApplied = true;
        }

        // R39-M2: after pass A succeeds, drop the REMOVE rows from the
        // pending buffer so a subsequent ADD-throw in pass B does NOT
        // leave the buffer holding applied REMOVEs alongside un-flushed
        // ADDs (the prior shape risked a wedged buffer that never
        // shrank, because the trailing clear() was bypassed by the
        // throw and the next retry would re-issue idempotent REMOVEs
        // without ever cleaning them out). REMOVEs are idempotent on
        // the engine side so re-issue would have been safe; the wedge
        // here was about buffer-occupancy unboundedness, not engine
        // correctness. Walking high-to-low keeps removeAt indices
        // valid (removeAt shifts higher positions down).
        if (removesApplied && delCount > 0) {
            for (int i = n - 1; i >= 0; i--) {
                if (pendingBuffer.opAt(i) == ArrowTimerBuffer.OP_REMOVE) {
                    pendingBuffer.removeAt(i);
                }
            }
        }

        // Pass B — ADDs after REMOVEs have succeeded. After this call
        // returns successfully the remaining ADD rows are dropped; if it
        // throws the buffer still holds the un-applied ADDs and a retry
        // can reissue them (the dropped REMOVE rows from pass A are
        // already on the engine so the retry's pass-A no-ops harmlessly).
        if (addCount > 0) {
            // B-R5-NEW-H1: vectorized FFI path. Pre-fix the ADD pass
            // allocated `byte[addCount][]` outer arrays plus a
            // per-row `byte[kLen]` heap copy, then dispatched via the
            // legacy `linker.batchPut(byte[][], byte[][])` shape — every
            // entry crossed FFM as a separate pointer-array materialised
            // on the native side. For Q12-style workloads this drove
            // millions of `byte[]` allocations per second on the timer
            // hot path. Switched to `vectorizedBatchPut` (single FFM
            // crossing, off-heap packed key/val data) mirroring Pass A's
            // `vectorizedBatchDelete` route. Vals are the fixed 0x01
            // sentinel, pre-filled in `ensureFlushPairCapacity` so the
            // hot path only writes the key side.
            long totalAddBytes = 0L;
            int remaining = pendingBuffer.size();
            for (int i = 0; i < remaining; i++) {
                if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_ADD) {
                    continue;
                }
                totalAddBytes += pendingBuffer.keyLenAt(i);
            }
            ensureFlushAddKeyDataCapacity(totalAddBytes);
            MemorySegment keyDataSeg = pendingBuffer.keyDataSegment();
            flushAddKeyOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
            long pos = 0L;
            int outIdx = 0;
            for (int i = 0; i < remaining; i++) {
                if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_ADD) {
                    continue;
                }
                int kOff = pendingBuffer.keyOffsetAt(i);
                int kLen = pendingBuffer.keyLenAt(i);
                MemorySegment.copy(keyDataSeg, kOff, flushAddKeyData, pos, kLen);
                pos += kLen;
                outIdx++;
                flushAddKeyOffsets.set(
                        ValueLayout.JAVA_INT, (long) outIdx * Integer.BYTES, (int) pos);
                // E-PERF: this kg now has a timer in the engine → un-exhaust it so the
                // next poll re-checks it (else findGlobalEngineHeadInRangeCached would
                // skip it forever and the timer would never fire). The composite kg is
                // 2 bytes BE at offset queuePrefix.length (see encode()/keyGroupPrefix).
                if (!exhaustedKgs.isEmpty() && kLen >= queuePrefix.length + 2) {
                    int kgOff = kOff + queuePrefix.length;
                    int hi = keyDataSeg.get(ValueLayout.JAVA_BYTE, kgOff) & 0xFF;
                    int lo = keyDataSeg.get(ValueLayout.JAVA_BYTE, kgOff + 1) & 0xFF;
                    exhaustedKgs.remove((hi << 8) | lo);
                }
            }
            // E-R13-H3: tie the in-buffer ADD-drop atomically to batchPut
            // success. Pre-fix the ADD rows lived in the pendingBuffer
            // until the trailing `clear()`; if anything interleaved
            // between batchPut's return and that clear() (an unchecked
            // Throwable, or a re-entrant invalidateCache listener), the
            // ADDs were durable on the engine AND still in the buffer.
            // A subsequent remove() observing the in-buffer ADD then
            // took the "cancel pending ADD" no-op branch (add/remove
            // path) and silently failed to issue the engine REMOVE,
            // leaving an orphan engine row that fires on watermark
            // advance — same leak class as R0E-H3.
            boolean putSucceeded = false;
            try {
                enginePossiblyMutated = true; // E-R23-H1: arm before the call so a throw mid-call still invalidates
                linker.vectorizedBatchPut(
                        db,
                        cf,
                        flushAddKeyOffsets,
                        flushAddKeyData,
                        flushAddValOffsets,
                        flushAddValData,
                        outIdx);
                putSucceeded = true;
            } finally {
                if (putSucceeded) {
                    for (int i = pendingBuffer.size() - 1; i >= 0; i--) {
                        if (pendingBuffer.opAt(i) == ArrowTimerBuffer.OP_ADD) {
                            pendingBuffer.removeAt(i);
                        }
                    }
                }
            }
        }
        // Pass B succeeded — clear any residue (a no-op when the ADD
        // drop loop above already emptied the buffer; defensive against
        // future code that leaves non-ADD non-REMOVE rows).
        pendingBuffer.clear();
        drainCleanSuccess = true;
        } finally {
            // E-R23-H1: invalidate pollCache whenever Pass A or Pass B
            // potentially mutated engine state, EVEN ON THE THROW PATH.
            // Pre-fix the trailing invalidateCache() was bypassed by a
            // Pass-B throw; subsequent peek() returned stale entries
            // already removed from the engine, firing phantom timers on
            // watermark advance.
            if (enginePossiblyMutated) {
                if (drainCleanSuccess) {
                    // E-PERF: invalidate the MINIMUM cache needed. The single-kg
                    // pollCache covers one kg and is cheap to always refresh.
                    pollCache.clear();
                    cachedKg = -1;
                    for (Integer kg : touchedKgs) {
                        ArrayDeque<Entry> dq = multiKgPollCache.get(kg);
                        long headTs =
                                (dq != null && !dq.isEmpty())
                                        ? decodeTimestamp(dq.peekFirst().composite)
                                        : Long.MAX_VALUE;
                        Long minAdd = minAddTsByKg.get(kg);
                        boolean invalidate;
                        if (removedKgs.contains(kg)) {
                            // A remove may delete a cached entry → must refresh.
                            invalidate = true;
                        } else if (dq == null || dq.isEmpty()) {
                            // No live cache → must refill to observe the add
                            // (also re-checks a previously-exhausted kg).
                            invalidate = true;
                        } else {
                            // KEEP the cached prefix iff every add to this kg is
                            // BEYOND the cached tail: such an add lands in the
                            // engine tail and is read on the next refill (after the
                            // cached prefix drains), so the prefix stays valid.
                            // Invalidate only if an add falls WITHIN the cached
                            // window (it would otherwise be skipped).
                            long tailTs = decodeTimestamp(dq.peekLast().composite);
                            invalidate = (minAdd != null && minAdd <= tailTs);
                        }
                        if (!invalidate) {
                            continue;
                        }
                        // FRS-TIMER-PERF: instead of clearing the resume cursor
                        // (→ the next refill restarts from the prefix start and
                        // re-walks every fired tombstone — the watermark-drain
                        // wall), record a one-shot refill FLOOR at
                        // min(cacheHeadTs, minAddTs). Fired timers all have
                        // ts < cacheHead, so the next refill SEEKS past them via
                        // the index and re-reads only the still-live rows
                        // (unconsumed cache + the new within-window/earlier add).
                        long floorTs;
                        if (headTs != Long.MAX_VALUE && minAdd != null) {
                            floorTs = Math.min(headTs, minAdd);
                        } else if (headTs != Long.MAX_VALUE) {
                            floorTs = headTs;
                        } else if (minAdd != null) {
                            floorTs = minAdd;
                        } else {
                            // Remove on an empty cache, no add: nothing to re-read
                            // here; the cursor-resumed refill's lazy scan filters
                            // the deleted row. Keep the cursor, set no floor.
                            multiKgPollCache.remove(kg);
                            continue;
                        }
                        multiKgPollCache.remove(kg);
                        multiKgRefillFloor.put(kg, buildSeekKey(kg, floorTs));
                        // Keep the resume cursor: the one-shot floor overrides the
                        // next refill, after which the cursor resumes normally.
                    }
                } else {
                    // Partial/failed flush: fall back to full invalidation (safe).
                    invalidateCache();
                }
            }
        }
    }

    private void ensureFlushPairCapacity(int neededRows) {
        if (neededRows <= flushPairCapacity) {
            return;
        }
        long newCap = Math.max(flushPairCapacity == 0 ? 256 : flushPairCapacity * 2, neededRows);
        // B-R5-NEW-H1: Arrow-style offsets + packed val data for the
        // vectorized ADD FFI shape, mirroring the DEL side.
        flushAddKeyOffsets = flushArena.allocate((newCap + 1) * Integer.BYTES);
        flushAddValOffsets = flushArena.allocate((newCap + 1) * Integer.BYTES);
        flushAddValData = flushArena.allocate(newCap);
        // Each timer's value is a fixed 0x01 sentinel; pre-fill valData
        // and valOffsets with the constant pattern [0,1,2,...] so the
        // flush hot path only writes the key side.
        flushAddValData.fill((byte) 1);
        for (long i = 0; i <= newCap; i++) {
            flushAddValOffsets.set(ValueLayout.JAVA_INT, i * Integer.BYTES, (int) i);
        }
        flushDelOffsets = flushArena.allocate((newCap + 1) * Integer.BYTES);
        flushPairCapacity = newCap;
    }

    private void ensureFlushAddKeyDataCapacity(long neededBytes) {
        if (neededBytes <= flushAddKeyDataCapacity) {
            return;
        }
        long newCap = Math.max(
                flushAddKeyDataCapacity == 0 ? 4096 : flushAddKeyDataCapacity * 2,
                neededBytes);
        flushAddKeyData = flushArena.allocate(newCap);
        flushAddKeyDataCapacity = newCap;
    }

    private void ensureFlushDelDataCapacity(long neededBytes) {
        if (neededBytes <= flushDelDataCapacity) {
            return;
        }
        long newCap = Math.max(flushDelDataCapacity == 0 ? 4096 : flushDelDataCapacity * 2, neededBytes);
        flushDelData = flushArena.allocate(newCap);
        flushDelDataCapacity = newCap;
    }

    /**
     * Issues a single vectorized batch delete for {@code count} keys via the linker. Stages keys
     * into the pre-allocated del staging segments.
     */
    /**
     * FRS-TIMER-DRAIN-BATCH: flush all staged fired-timer composites in ONE batched
     * {@link ForStRsLinker#vectorizedBatchDelete} crossing. Idempotent / no-op when empty, so it is
     * safe to call from every engine-read / snapshot / invalidate entry point. MUST run before any
     * engine re-scan or snapshot so a deferred delete never resurrects a fired timer.
     */
    private void flushPollDeletes() {
        int n = pendingPollDeletes.size();
        if (n == 0) {
            return;
        }
        vectorizedBatchDeleteKeys(pendingPollDeletes.toArray(new byte[0][]), n);
        pendingPollDeletes.clear();
        if (TIMER_DIAG) {
            diagDeleteFlushCount++;
            diagDeleteKeyCount += n;
        }
    }

    private void vectorizedBatchDeleteKeys(byte[][] keys, int count) {
        if (count <= 0) {
            return;
        }
        long totalBytes = 0L;
        for (int i = 0; i < count; i++) {
            totalBytes += keys[i].length;
        }
        ensureFlushPairCapacity(count);
        ensureFlushDelDataCapacity(totalBytes);
        flushDelOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
        long pos = 0L;
        for (int i = 0; i < count; i++) {
            byte[] k = keys[i];
            MemorySegment.copy(k, 0, flushDelData, ValueLayout.JAVA_BYTE, pos, k.length);
            pos += k.length;
            flushDelOffsets.set(
                    ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES, (int) pos);
        }
        linker.vectorizedBatchDelete(db, cf, flushDelOffsets, flushDelData, count);
    }

    // ------------------------------------------------------------------
    // Close — flush and release off-heap resources
    // ------------------------------------------------------------------

    @Override
    public void close() {
        close(false);
    }

    /**
     * E5-R5-H1: skip-drain variant. When the dispose path detects {@code awaitOutstandingSnapshots}
     * timed out (snapshot worker still in FFI), the timer-queue drain via
     * {@link #drainPendingBufferInternal()} would race the stuck worker's db/cf access — a real
     * UAF the V1-sync backend already guards via its {@code if (snapshotsTimedOut) continue;}
     * skip at {@code ForStRsAbstractKeyedStateBackend.java:1240}. {@code skipDrain=true} replays
     * the pre-A-C4R4-H1 behavior (release arenas only, drop any pending ADDs/REMOVEs that arrived
     * after the prior snapshot — same data-loss surface the original gate-blocked drain produced).
     */
    public void close(boolean skipDrain) {
        // R29-M3: idempotent close. PHASE 1.e of dispose already drained the
        // pending buffer via {@link #flushPendingToEngine()}; the symmetric
        // registry close() loop must NOT re-drain (extra FFI call) and must NOT
        // throw on the second invocation (which would mask the real dispose
        // cause). On second+ entry we skip the drain and proceed directly to
        // arena/buffer release (those releases are themselves idempotent at the
        // {@link Arena#close()} layer).
        //
        // A-C4R4-H1: the dispose drain MUST bypass the E4-R3-H3 gate in
        // {@link #flushPendingToEngine()}. The CAS-winner gets the single
        // dispose-time chance to flush ADD/REMOVE entries sitting in
        // `pendingBuffer`; the gate (which exists to guard a concurrent
        // snapshot+close UAF) would silently swallow that drain and the
        // entries would be lost. We CAS first (claims ownership of the
        // single drain + arena release), then call the gate-bypassing
        // {@link #drainPendingBufferInternal()} directly. Concurrent
        // close() losers see firstClose=false and skip drain — same
        // semantics as the pre-fix CAS-based idempotent close.
        boolean firstClose = closed.compareAndSet(false, true);
        try {
            if (firstClose && !skipDrain) {
                drainPendingBufferInternal();
            }
        } finally {
            try {
                pendingBuffer.close();
            } catch (Exception ignored) {
            }
            try {
                scratchArena.close();
            } catch (Exception ignored) {
            }
            try {
                flushArena.close();
            } catch (Exception ignored) {
            }
        }
    }

    // ------------------------------------------------------------------
    // Test/observability hooks
    // ------------------------------------------------------------------

    @VisibleForTesting
    public int pendingBufferSize() {
        return pendingBuffer.size();
    }

    // ------------------------------------------------------------------
    // Implementation helpers
    // ------------------------------------------------------------------

    /** A composite-key + decoded-element tuple returned by {@link #cachedHeadEntry(int)}. */
    private final class Entry {
        final byte[] composite;
        final T element;

        Entry(byte[] composite, T element) {
            this.composite = composite;
            this.element = element;
        }
    }

    /**
     * Returns the head entry for {@code kg}, populating the poll-ahead cache from the engine on a
     * miss. The returned entry stays at the front of {@link #pollCache} so that subsequent {@link
     * #peek()} calls are idempotent; {@link #poll()} pops it after the engine delete succeeds.
     */
    private Entry cachedHeadEntry(int kg) {
        if (cachedKg != kg) {
            pollCache.clear();
            cachedKg = kg;
            // FRS-TIMER-PERF: switching key group invalidates the resume cursor
            // (it belongs to the previously-cached kg).
            singleKgResumeCursor = null;
        }
        if (pollCache.isEmpty()) {
            refillCache(kg);
        }
        return pollCache.peekFirst();
    }

    /**
     * Refills {@link #pollCache} from the engine for {@code kg} by opening a prefix iterator and
     * reading up to {@link #REFILL_BATCH} entries in one go.
     */
    private void refillCache(int kg) {
        // FRS-TIMER-PERF: resume-cursor + lazy range scan (see refillMultiKgCache).
        byte[] resume = singleKgResumeCursor;
        byte[] kgPrefix = keyGroupPrefix(kg);
        byte[] lo = (resume != null) ? successorKey(resume) : kgPrefix;
        byte[] hi = prefixUpperBound(kgPrefix);
        byte[] last = readRangeIntoCache(lo, hi, pollCache);
        if (last != null) {
            singleKgResumeCursor = last;
        }
    }

    /** Invalidates the poll-ahead cache. Cheap (just clears the deque). */
    private void invalidateCache() {
        // FRS-TIMER-DRAIN-BATCH: flush staged fired-timer deletes before dropping the resume cursor
        // (a subsequent from-start refill must not re-read them). No-op when nothing is staged.
        flushPollDeletes();
        pollCache.clear();
        cachedKg = -1;
        // FRS-TIMER-PERF: drop resume cursors + refill floors alongside their caches.
        singleKgResumeCursor = null;
        multiKgResumeCursor.clear();
        multiKgRefillFloor.clear();
        // E-R28-H1: multi-kg pollCache is invalidated alongside the
        // single-kg one so any caller of invalidateCache() (e.g.
        // flushPendingToEngine, exception paths in poll()/peek())
        // gets consistent semantics across both caches.
        multiKgPollCache.clear();
    }

    private int sizeForKeyGroup(int kg) {
        int n = 0;
        // E-H8: per-call confined arena to reclaim the iter handle.
        try (Arena perCallArena = Arena.ofConfined();
                FrsIterator iter =
                        linker.prefixLookupOpen(db, cf, keyGroupPrefix(kg), perCallArena)) {
            while (linker.iteratorNext(iter) != null) {
                n++;
            }
        }
        return n;
    }

    /** Iterator that walks every covered key group end-to-end, decoding elements lazily. */
    private final class MultiKeyGroupIterator implements CloseableIterator<T> {
        private int kg = keyGroupRange.getStartKeyGroup();
        private FrsIterator current;
        // E-H8: per-kg arena lives for one iter handle's lifetime. We
        // close it in `closeCurrent` so the handle's 8 bytes are
        // reclaimed before opening the next kg's iter, mirroring the
        // E-H6 pattern but adapted to a stateful iterator.
        private Arena currentArena;
        private T next;
        private boolean closed;

        MultiKeyGroupIterator() {
            advance();
        }

        private void advance() {
            while (!closed) {
                if (current == null) {
                    if (kg > keyGroupRange.getEndKeyGroup()) {
                        next = null;
                        return;
                    }
                    currentArena = Arena.ofConfined();
                    current = linker.prefixLookupOpen(
                            db, cf, keyGroupPrefix(kg), currentArena);
                }
                ForStRsLinker.IteratorEntry e = linker.iteratorNext(current);
                if (e == null) {
                    closeCurrent();
                    kg++;
                    continue;
                }
                next = decodeElement(e.key());
                return;
            }
            next = null;
        }

        private void closeCurrent() {
            if (current != null) {
                current.close();
                current = null;
            }
            if (currentArena != null) {
                currentArena.close();
                currentArena = null;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public T next() {
            if (next == null) {
                throw new java.util.NoSuchElementException();
            }
            T out = next;
            advance();
            return out;
        }

        @Override
        public void close() {
            closed = true;
            closeCurrent();
        }
    }
}

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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.state.forstrs.exec.IterLifetimeWatchdog;
import org.apache.flink.state.forstrs.exec.SlotArenaScope;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsAbi;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;
import org.apache.flink.state.forstrs.state.ArrowBinaryBuffer;
import org.apache.flink.state.forstrs.state.ArrowBinaryBufferAutoTuner;
import org.apache.flink.state.forstrs.state.ForStRsAggregatingState;
import org.apache.flink.state.forstrs.state.ForStRsListState;
import org.apache.flink.state.forstrs.state.ForStRsMapState;
import org.apache.flink.state.forstrs.state.ForStRsReducingState;
import org.apache.flink.state.forstrs.state.ForStRsValueState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

    private static final Logger LOG = LoggerFactory.getLogger(ForStRsKeyedStateBackend.class);

    /** Initial buffer size for currentKey serialization (grows on demand). */
    private static final int DEFAULT_KEY_BUFFER = 32;

    private static final long DEFAULT_SLOT_TURN_BYTES = 8L * 1024 * 1024;
    private static final long DEFAULT_SLOT_CACHE_BYTES = 64L * 1024 * 1024;

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

    /**
     * Key-group range this backend services. Set via constructor — production wires the real range
     * from {@code KeyedStateBackendParameters.getKeyGroupRange()}; legacy/test constructors default
     * to {@code KeyGroupRange.of(0, 127)} so existing call sites compile without churn.
     *
     * <p>PR-A3 (S1-6 / E-CRIT-3): replaces the hard-coded {@code () -> 0} supplier so V1-sync
     * keyed state correctly partitions keys across rescaling boundaries.
     */
    private final KeyGroupRange keyGroupRange;

    /** Total number of key-groups (a.k.a. Flink max-parallelism) for keygroup assignment. */
    private final int numberOfKeyGroups;

    private SlotArenaScope slotArenaScope;

    private final DataOutputSerializer keyOutBuffer = new DataOutputSerializer(DEFAULT_KEY_BUFFER);

    /**
     * Cache of currently-bound state objects keyed by {@code stateName}. Cleared on every {@link
     * #setCurrentKey(Object)} call because the per-state {@code keyPrefix} embeds the current key.
     */
    private final Map<String, Object> stateCache = new HashMap<>();

    /**
     * Registry of kg-prefixed MapState instances that persist across key changes. These use dynamic
     * prefix/composite-key computers and accumulate writes in their internal cache. Flushed on
     * checkpoint via {@link #flushAllMapStates()}.
     */
    private final Map<String, ForStRsMapState<?, ?>> mapStateRegistry = new HashMap<>();

    // ------------------------------------------------------------------
    // Write-behind buffer: defers native put calls and serves subsequent
    // reads from the buffer. Flushed in batch (batchPut) every
    // WRITE_BUFFER_FLUSH_THRESHOLD writes or on checkpoint/close.
    // ------------------------------------------------------------------

    /**
     * Flush threshold: number of buffered writes before auto-flush. 64 entries × 8 bytes/pointer =
     * 512 bytes = one L1 cache line. The batch-put call processes all key/value pointers in one
     * tight loop that fits entirely in L1, maximizing cache hits during the flush.
     */
    private static final int WRITE_BUFFER_FLUSH_THRESHOLD = 64;

    private static final int ADAPTIVE_SAMPLE_WINDOW = 1024;
    private static final double DISABLE_THRESHOLD = 0.10;
    private static final double ENABLE_THRESHOLD = 0.50;
    private static final int MAX_BUFFER_ENTRIES = 4096;

    /**
     * A9-M1: upper bound on the byte length of a single buffered value. Rejected at {@link
     * #putToWriteBuffer} entry so:
     *
     * <ul>
     *   <li>the packed {@code (off << 32) | (len & 0xFFFFFFFFL)} encoding never loses precision on
     *       either half — both off and len stay within 32 bits;
     *   <li>the arena-growth comparison can be done in {@code long} space without {@code valLen}
     *       ever overflowing the int-arithmetic side of {@code writeArenaPos + valLen}, which
     *       pre-fix could wrap negative and either silently corrupt the buffer or trigger an
     *       {@link ArrayIndexOutOfBoundsException} on the {@link System#arraycopy} that follows;
     *   <li>even an adversarial caller can't tie up more than 256 MiB of buffered memory per
     *       entry before the next threshold flush.
     * </ul>
     *
     * <p>256 MiB is well above any realistic Flink value-state record (the community ForSt
     * backend rejects values above 16 MiB by default) but small enough that the per-flush buffer
     * never approaches {@code Long.MAX_VALUE} even with {@code MAX_BUFFER_ENTRIES = 4096} entries.
     */
    public static final int MAX_VALUE_BYTES = 256 * 1024 * 1024;

    /**
     * Write-behind buffer: maps full composite ForSt keys to packed {@code (offset << 32) | length}
     * indices into {@link #writeValueArena}. Shared across all ValueState instances on this
     * backend. Reads check this buffer first (0-cost hit); writes go here instead of native.
     * Flushed via {@link #flushWriteBuffer()} on threshold, checkpoint, or close.
     *
     * <p>B8-H2: indices replace the previous {@code Map<…, byte[]>} so {@code putToWriteBuffer}
     * can accept a value slice ({@code valBuf, valOff, valLen}) and copy directly into the arena.
     * That eliminates the per-{@code update()} {@code outputBuffer.getCopyOfBuffer()} allocation
     * on the Q11 V1-sync hot path. On a duplicate key write the new bytes are appended fresh and
     * the index is repointed; the old arena slot leaks until the next flush (acceptable because
     * the buffer is bounded at {@code MAX_BUFFER_ENTRIES} and flushed on threshold).
     *
     * <p>B9-H1: the map is now {@link ByteArrayLongMap} (open-addressing, primitive {@code long}
     * values, raw {@code byte[]} keys). This eliminates both the {@code ByteArrayWrapper}
     * allocation per put/get/remove AND the boxed {@code Long} allocation for pointer-magnitude
     * packed indices (which fall outside JDK's -128..127 Long cache and were freshly autoboxed
     * per call). Together with B9-H3 — which shares this same map — the V1-sync ValueState
     * {@code update()}/{@code value()} hot path is now zero-allocation past steady-state arena
     * grow.
     */
    private final ByteArrayLongMap writeBuffer = new ByteArrayLongMap();

    /**
     * B8-H2: contiguous arena holding the buffered value bytes for {@link #writeBuffer}. Grown by
     * doubling on overflow. Reset (via {@link #writeArenaPos} = 0) on every flush so a steady-state
     * Q11 workload never re-grows beyond the high-water mark.
     */
    private byte[] writeValueArena = new byte[16 * 1024];

    /**
     * B8-H2: write-pointer (in bytes) into {@link #writeValueArena}.
     *
     * <p>A9-M1: widened from {@code int} to {@code long} so {@code writeArenaPos + valLen} can be
     * computed in long space — the int form could wrap negative for a sufficiently large
     * adversarial value (or cumulative arena past 2 GiB before a flush) and the bounds-check
     * comparison {@code (pos + len) > arena.length} would then succeed against a NEGATIVE
     * left-hand side, skipping the grow step and silently corrupting the buffer (or throwing
     * AIOOBE on the {@link System#arraycopy} immediately below). The arena itself is still a
     * {@code byte[]} (capped at {@link Integer#MAX_VALUE} by the JVM), and we additionally
     * bound per-entry size to {@link #MAX_VALUE_BYTES} at put-time so the long value here never
     * exceeds {@link Integer#MAX_VALUE} in practice — but the long type makes the bounds-check
     * arithmetic robust against future regressions of either invariant.
     */
    private long writeArenaPos = 0L;

    /** Running count of buffered writes since last flush. */
    private int writeBufferCount = 0;

    private int sampleHits = 0;
    private int sampleTotal = 0;
    private boolean bufferEnabled = true;

    // ------------------------------------------------------------------
    // Pre-allocated flush staging buffers (Optimization 2).
    //
    // Instead of allocating a fresh Arena + N key/value native segments on
    // every flush, we pre-allocate four pointer/length arrays sized to
    // WRITE_BUFFER_FLUSH_THRESHOLD and a contiguous data arena that is
    // reused across flushes. This eliminates ~50ns × N Arena.allocate
    // overhead per flush and keeps the pointer arrays in L1 (512 bytes
    // for 64 entries × 8 bytes/pointer).
    // ------------------------------------------------------------------

    /** Long-lived arena for the pre-allocated flush staging segments. */
    private final Arena flushArena = Arena.ofAuto();

    /**
     * Pre-allocated: FLUSH_THRESHOLD × ADDRESS slots for key pointers.
     *
     * <p>D10-M1: explicit 8-byte alignment. {@code JAVA_LONG.set}/{@code ADDRESS.set} require
     * segment alignment ≥ 8; the default {@code allocate(byteSize)} grants alignment=1 and only
     * works by happenstance of allocator natural alignment.
     */
    private final MemorySegment flushKeyPtrs =
            flushArena.allocate(
                    (long) WRITE_BUFFER_FLUSH_THRESHOLD * ValueLayout.ADDRESS.byteSize(), 8);

    /** Pre-allocated: FLUSH_THRESHOLD × JAVA_LONG slots for key lengths. D10-M1: align=8. */
    private final MemorySegment flushKeyLens =
            flushArena.allocate(
                    (long) WRITE_BUFFER_FLUSH_THRESHOLD * ValueLayout.JAVA_LONG.byteSize(), 8);

    /** Pre-allocated: FLUSH_THRESHOLD × ADDRESS slots for value pointers. D10-M1: align=8. */
    private final MemorySegment flushValuePtrs =
            flushArena.allocate(
                    (long) WRITE_BUFFER_FLUSH_THRESHOLD * ValueLayout.ADDRESS.byteSize(), 8);

    /** Pre-allocated: FLUSH_THRESHOLD × JAVA_LONG slots for value lengths. D10-M1: align=8. */
    private final MemorySegment flushValueLens =
            flushArena.allocate(
                    (long) WRITE_BUFFER_FLUSH_THRESHOLD * ValueLayout.JAVA_LONG.byteSize(), 8);

    // ------------------------------------------------------------------
    // Off-heap (Arrow) state plumbing (Task 1b.2).
    //
    // Wires ForStRsValueState's new off-heap constructor:
    //   - scratchArenaTL: per-thread scratch MemorySegment used by the
    //     composite-key encoder; reset by each ValueState entry point
    //     (encoder writes from offset 0). 64 KB plenty for Q5 / Q11 / Q13.
    //   - offheapKeyGroupSupplier: this backend is a stepping-stone that
    //     does not yet track Flink key-group; suppling 0 keeps key encoding
    //     stable (all keys land in the same key-group prefix) and matches
    //     the regime used in ForStRsValueStateOffheapTest.
    //   - offheapKeySupplier: hands the off-heap encoder the current key
    //     object (read on each call via getCurrentKey()).
    //   - ownedBuffers: ArrowBinaryBuffers created by getValueState(...);
    //     closed by close() to release off-heap memory.
    // ------------------------------------------------------------------

    /**
     * Per-thread scratch off-heap memory for V1-sync off-heap state operations. Reset by each
     * ValueState entry point (encoder writes from offset 0). 64 KB is plenty for Q5 (max composite
     * key ~100 B + value ~16 B; reused across the 5 panes within one event).
     */
    /**
     * PR-M1 (D-R2-5 fix): the previous {@code ThreadLocal<MemorySegment>} initializer was
     * {@code () -> Arena.ofShared().allocate(65536)} — the {@code Arena} reference was dropped,
     * so it never closed (bounded 64 KB leak per task thread). Now stores both Arena + segment
     * and tracks all per-thread Arenas in {@link #threadLocalArenas} so {@link #close()} can
     * dispose them.
     */
    private final java.util.List<Arena> threadLocalArenas =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    /**
     * D-R5R-H2: virtual-thread aware scratch arena allocation.
     *
     * <p>Pre-fix, the ThreadLocal initializer always allocated an {@link Arena#ofShared()} and
     * added it to {@link #threadLocalArenas} for cleanup at backend close. That design assumed
     * platform threads (bounded). JDK 25 virtual threads spawned per task via
     * {@code Executors.newVirtualThreadPerTaskExecutor()} caused a permanent off-heap leak:
     * every distinct virtual thread that touched a V1-sync path allocated a fresh 64 KiB arena
     * AND a strong reference in {@code threadLocalArenas} that survived the virtual thread's
     * death. A high-throughput async job that touched N distinct virtual threads accumulated
     * N × 64 KiB native plus N JVM handshake-table entries.
     *
     * <p>The fix routes platform threads through the original tracked-arena path (so backend
     * close() still reclaims them) and virtual threads through a {@code Cleaner.register} hook
     * keyed on the per-thread {@code ThreadLocal} value, so the arena closes when the virtual
     * thread (and its ThreadLocal entry) becomes unreachable. Backend close still drains
     * platform-thread arenas eagerly.
     */
    private static final java.lang.ref.Cleaner VT_ARENA_CLEANER = java.lang.ref.Cleaner.create();

    /**
     * E-R6-H3: holder that ties Cleaner cleanup to the ThreadLocal entry's
     * lifetime. The ThreadLocal stores instances of this class (NOT the raw
     * {@link MemorySegment}) so the holder stays reachable for the lifetime
     * of the virtual thread that owns the ThreadLocal map entry. When the
     * virtual thread terminates and the entry is GC'd, the Cleaner fires
     * {@code arena.close()} — reclaiming the off-heap segment without
     * leaking it via {@link #threadLocalArenas}.
     *
     * <p>Pre-fix the holder was a local variable inside the ThreadLocal
     * lambda — it became GC-eligible immediately after the lambda returned,
     * so the Cleaner fired on the next GC cycle and invalidated the segment
     * while the ThreadLocal still handed it out, producing
     * {@code IllegalStateException} on the next state op.
     */
    private static final class CleanableScratch {
        final MemorySegment segment;

        CleanableScratch(MemorySegment segment) {
            this.segment = segment;
        }
    }

    private final ThreadLocal<CleanableScratch> scratchArenaTL =
            ThreadLocal.withInitial(
                    () -> {
                        Arena a = Arena.ofShared();
                        MemorySegment seg = a.allocate(65536);
                        CleanableScratch holder = new CleanableScratch(seg);
                        if (Thread.currentThread().isVirtual()) {
                            // Cleaner is keyed on `holder`; the ThreadLocal
                            // anchors `holder` for the virtual thread's
                            // lifetime, so the Cleaner only fires after the
                            // thread (and its TL entry) become unreachable.
                            VT_ARENA_CLEANER.register(holder, a::close);
                        } else {
                            // Platform-thread path keeps the original
                            // tracked-arena lifecycle so backend.close()
                            // reclaims eagerly.
                            threadLocalArenas.add(a);
                        }
                        return holder;
                    });

    /**
     * Returns the per-thread scratch {@link MemorySegment}. Indirection through
     * {@link CleanableScratch} keeps the holder reachable for the lifetime of
     * the ThreadLocal entry — see {@link CleanableScratch} javadoc for the
     * E-R6-H3 rationale.
     */
    private MemorySegment scratchSegment() {
        return scratchArenaTL.get().segment;
    }

    /**
     * Supplier passed to off-heap ForStRsValueState that resolves the current key's key-group.
     *
     * <p>PR-A3 (S1-6 / E-CRIT-3): previously returned 0 unconditionally, which made V1-sync state
     * routing collide across key-groups and break rescaling. Now delegates to
     * {@link #computeCurrentKeyGroup()} which computes the key-group from {@link #getCurrentKey()}
     * via {@link KeyGroupRangeAssignment#assignToKeyGroup(Object, int)} using the {@link
     * #numberOfKeyGroups} the constructor was given.
     *
     * <p>When the current key is {@code null} (i.e. a state object was instantiated before
     * {@link #setCurrentKey(Object)} was called — possible in some test paths) the supplier falls
     * back to the range's start key-group so the encoder still produces a valid, in-range prefix.
     *
     * <p>The supplier is a method reference (not an inline lambda) so the field initializer
     * doesn't trigger the JLS "definite assignment" check on the {@code final} key-group fields
     * — those are assigned in the constructor body, after this field initializer runs, but
     * are only <i>read</i> on each {@code getAsInt()} invocation (which is always strictly
     * later than the constructor return).
     */
    private final java.util.function.IntSupplier offheapKeyGroupSupplier =
            this::computeCurrentKeyGroup;

    private int computeCurrentKeyGroup() {
        Object cur = getCurrentKey();
        if (cur == null) {
            return keyGroupRange.getStartKeyGroup();
        }
        return KeyGroupRangeAssignment.assignToKeyGroup(cur, numberOfKeyGroups);
    }

    private final java.util.function.Supplier<Object> offheapKeySupplier =
            () -> (Object) getCurrentKey();

    /** ArrowBinaryBuffers owned by this backend, closed in {@link #close()}. */
    private final java.util.List<ArrowBinaryBuffer> ownedBuffers = new java.util.ArrayList<>();

    private K currentKey;
    private byte[] currentKeyBytes;

    /**
     * Monotonically increasing generation counter, bumped on every effective {@link
     * #setCurrentKey(Object)} call (i.e. when the key actually changes). Used by adapters to detect
     * stale cached state objects without a HashMap lookup on every state access (Phase B1+B3
     * optimization).
     */
    private long keyGeneration;

    // R71-M2: `closed` must be `volatile` so concurrent close() / dispose()
    // calls observe each other's flag flip (preventing a double-close UAF on
    // defaultCf / db / arena) AND so in-flight state ops on other threads
    // checking `if (closed) throw IllegalStateException(...)` reliably see
    // the transition. The Flink contract is typically single-threaded close,
    // but cancel paths and async snapshot threads (and post R67-H1's
    // exception-rethrowing flush phase) make the assumption fragile.
    private volatile boolean closed = false;

    private IterLifetimeWatchdog iterWatchdog;

    /**
     * Default sentinel key-group range used by legacy/test constructors that don't care about
     * rescaling. Matches the canonical Flink "single TM, default max-parallelism = 128" layout.
     */
    private static final KeyGroupRange DEFAULT_TEST_KEY_GROUP_RANGE = KeyGroupRange.of(0, 127);

    /** Default sentinel number-of-key-groups for legacy/test constructors. */
    private static final int DEFAULT_TEST_NUMBER_OF_KEY_GROUPS = 128;

    /**
     * Legacy 5-arg constructor preserved so existing test sites compile without churn. Defaults
     * the key-group range to {@link #DEFAULT_TEST_KEY_GROUP_RANGE} and number-of-key-groups to
     * {@link #DEFAULT_TEST_NUMBER_OF_KEY_GROUPS}. Production code paths should use the 7-arg
     * constructor that passes the real values from
     * {@code KeyedStateBackendParameters.getKeyGroupRange() / getNumberOfKeyGroups()}.
     */
    public ForStRsKeyedStateBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle defaultCf,
            TypeSerializer<K> keySerializer) {
        this(
                arena,
                linker,
                db,
                defaultCf,
                keySerializer,
                /* ownsResources= */ true,
                DEFAULT_TEST_KEY_GROUP_RANGE,
                DEFAULT_TEST_NUMBER_OF_KEY_GROUPS);
    }

    /**
     * Legacy 6-arg constructor preserved so existing test sites compile without churn. Defaults
     * the key-group range and number-of-key-groups as in the 5-arg variant.
     */
    public ForStRsKeyedStateBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle defaultCf,
            TypeSerializer<K> keySerializer,
            boolean ownsResources) {
        this(
                arena,
                linker,
                db,
                defaultCf,
                keySerializer,
                ownsResources,
                DEFAULT_TEST_KEY_GROUP_RANGE,
                DEFAULT_TEST_NUMBER_OF_KEY_GROUPS);
    }

    /**
     * Production constructor (PR-A3 / S1-6 fix): accepts explicit {@code keyGroupRange} and
     * {@code numberOfKeyGroups} so {@link #offheapKeyGroupSupplier} routes V1-sync keys to the
     * correct key-group via {@link KeyGroupRangeAssignment#assignToKeyGroup(Object, int)}. The
     * {@code ownsResources=true} variant is the one the production
     * {@code ForStRsStateBackend.createKeyedStateBackend} factory uses.
     */
    public ForStRsKeyedStateBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle defaultCf,
            TypeSerializer<K> keySerializer,
            KeyGroupRange keyGroupRange,
            int numberOfKeyGroups) {
        this(
                arena,
                linker,
                db,
                defaultCf,
                keySerializer,
                /* ownsResources= */ true,
                keyGroupRange,
                numberOfKeyGroups);
    }

    /**
     * Full production constructor with both {@code ownsResources} and explicit key-group plumbing.
     * When {@code ownsResources} is {@code false}, {@link #close()} only releases the per-state
     * cache and leaves the linker/db/cf/arena untouched — useful for tests that want to share an
     * Arena across multiple backends and for {@link #restoreFromSnapshot}.
     */
    public ForStRsKeyedStateBackend(
            Arena arena,
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle defaultCf,
            TypeSerializer<K> keySerializer,
            boolean ownsResources,
            KeyGroupRange keyGroupRange,
            int numberOfKeyGroups) {
        FrsAbi.verifyAgainst(linker::frsAbiVersion);
        if (keyGroupRange == null) {
            throw new NullPointerException("keyGroupRange must not be null");
        }
        if (numberOfKeyGroups <= 0) {
            throw new IllegalArgumentException(
                    "numberOfKeyGroups must be > 0 (was " + numberOfKeyGroups + ")");
        }
        this.arena = arena;
        this.linker = linker;
        this.db = db;
        this.defaultCf = defaultCf;
        this.keySerializer = keySerializer;
        this.ownsResources = ownsResources;
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.slotArenaScope =
                SlotArenaScope.openForSlot(DEFAULT_SLOT_TURN_BYTES, DEFAULT_SLOT_CACHE_BYTES);
        this.iterWatchdog = new IterLifetimeWatchdog(slotArenaScope);
        this.iterWatchdog.start();
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
        // ValueState now lazily computes composite keys per call from currentKeyBytes
        // (kgSerializer.encodeForStateOffheap) via the off-heap path; instance survives
        // setCurrentKey. setCurrentKey only bumps keyGeneration for stale-adapter detection.
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
        ForStRsValueState<T> existing;
        // E-C4R8-H3/H4: stateCache + ownedBuffers reads/writes synchronized on stateCache.
        // close() iterates+clears stateCache (line ~1721) and ownedBuffers (line ~1631) — a
        // mailbox-thread factory call that races a task-canceler-thread close could observe
        // closed=false at ensureCurrentKey(), publish into stateCache after close()'s clear,
        // and return a state bound to a soon-to-be-freed db/cf handle (UAF). Mirror the
        // A-C4R7-H1 fix already in place for getMapState.
        synchronized (stateCache) {
            if (closed) {
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot access ValueState: "
                                + stateName);
            }
            existing = (ForStRsValueState<T>) stateCache.get(valueStateCacheKey(stateName));
        }
        if (existing != null) {
            return existing;
        }
        // Task 1b.2: wire the off-heap ForStRsValueState constructor with a per-instance
        // ArrowBinaryBuffer + ArrowBinaryBufferAutoTuner. The composite-key encoder uses
        // the per-thread scratch MemorySegment owned by this backend. The state instance
        // survives setCurrentKey because the encoder reads the current key on each call.
        ForStRsKeyGroupedSerializer<K> kgSer = new ForStRsKeyGroupedSerializer<>(keySerializer);
        ArrowBinaryBufferAutoTuner tuner =
                new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
        // ValueState uses the global MAX_CAPACITY (1 048 576 — lifted in size-aware AutoTune
        // design 2026-05-21) so Q5's HOP working set (~150K) fits. Q11-style small-WS workloads
        // stay at MIN_CAPACITY because the tuner's occupancy gate refuses to grow a buffer
        // whose fill rate stays below 70%.
        ArrowBinaryBuffer buf =
                new ArrowBinaryBuffer(
                        ArrowBinaryBuffer.MIN_CAPACITY, ArrowBinaryBuffer.MAX_CAPACITY, tuner);
        ForStRsValueState<T> created =
                new ForStRsValueState<>(
                        linker,
                        db,
                        defaultCf,
                        valueSerializer,
                        this::scratchSegment,
                        kgSer,
                        stateName,
                        offheapKeyGroupSupplier,
                        offheapKeySupplier,
                        buf,
                        tuner);
        // E-C4R8-H1/H4: publish + buffer-register under stateCache monitor. Re-check closed
        // — close() may have flipped it between our read above and the publish below.
        synchronized (stateCache) {
            if (closed) {
                try {
                    buf.close();
                } catch (Throwable ignore) {
                    // best-effort — the just-allocated buffer was never published.
                }
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot register ValueState: "
                                + stateName);
            }
            ownedBuffers.add(buf);
            stateCache.put(valueStateCacheKey(stateName), created);
        }
        return created;
    }

    /** Returns a {@link ForStRsListState} bound to the current key + state-id. */
    public <T> ForStRsListState<T> getListState(
            String stateName, TypeSerializer<T> elementSerializer) {
        ensureCurrentKey();
        @SuppressWarnings("unchecked")
        ForStRsListState<T> existing;
        // E-C4R8-H3/H4: see getValueState — same close-race rationale.
        synchronized (stateCache) {
            if (closed) {
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot access ListState: "
                                + stateName);
            }
            existing = (ForStRsListState<T>) stateCache.get(listStateCacheKey(stateName));
        }
        if (existing != null) {
            return existing;
        }
        byte[] prefix = buildPrefix(stateName);
        ForStRsListState<T> created =
                new ForStRsListState<>(linker, db, defaultCf, prefix, elementSerializer);
        synchronized (stateCache) {
            if (closed) {
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot register ListState: "
                                + stateName);
            }
            stateCache.put(listStateCacheKey(stateName), created);
        }
        return created;
    }

    /**
     * Returns a {@link ForStRsMapState} bound to the current key + state-id. Uses the kg-prefixed
     * constructor so the same instance persists across key changes — its write cache accumulates
     * entries from all keys and flushes in batch on checkpoint.
     */
    @SuppressWarnings("unchecked")
    public <UK, UV> ForStRsMapState<UK, UV> getMapState(
            String stateName, TypeSerializer<UK> keySer, TypeSerializer<UV> valueSer) {
        ensureCurrentKey();
        ForStRsMapState<UK, UV> existing;
        // D-R5-H4: registry reads/writes synchronized on the registry monitor.
        // close() iterates via the same monitor (line ~1640) so a mailbox put
        // can't race a canceler-thread close iteration and corrupt the table.
        //
        // A-C4R7-H1: re-check `closed` UNDER the registry monitor for the
        // existing-branch too. The A-C4R6-H2 fix only protected the publish
        // branch; the existing-branch could still observe closed=false at
        // ensureCurrentKey(), then close() flips closed=true and starts
        // clearing the registry — but T1 still acquires the monitor BEFORE
        // close()'s snapshot+clear at line ~1659 and returns a non-null
        // `existing`. The returned state's subsequent FFI calls then race
        // db.close() / defaultCf.close() — UAF.
        synchronized (mapStateRegistry) {
            if (closed) {
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot access MapState: "
                                + stateName);
            }
            existing = (ForStRsMapState<UK, UV>) mapStateRegistry.get(stateName);
        }
        if (existing != null) {
            return existing;
        }
        // FRS-Q11-DIAG (2026-05-30, gated by -Dforst.rs.mapstate.legacy=true): force the LEGACY
        // (non-statebuf) MapState constructor to test whether the off-heap statebuf forEachEntry
        // merge/dedup is the cause of the q11 MergingWindowSet "not in in-flight set" crash. If
        // q11 runs clean with this flag, the off-heap MapState statebuf path is confirmed.
        if (Boolean.getBoolean("forst.rs.mapstate.legacy")) {
            ForStRsMapState<UK, UV> legacy =
                    new ForStRsMapState<>(
                            linker,
                            db,
                            defaultCf,
                            keySer,
                            valueSer,
                            () -> buildPrefix(stateName),
                            (uk) -> buildCompositeMapKey(stateName, keySer, uk));
            synchronized (stateCache) {
                synchronized (mapStateRegistry) {
                    if (closed) {
                        throw new IllegalStateException(
                                "ForStRsKeyedStateBackend already closed; cannot register MapState: "
                                        + stateName);
                    }
                    mapStateRegistry.put(stateName, legacy);
                }
            }
            return legacy;
        }
        // 1c.1: wire the off-heap ForStRsMapState constructor with a per-instance
        // ArrowBinaryBuffer + ArrowBinaryBufferAutoTuner. Composite-key encoder uses the per-
        // thread scratch MemorySegment owned by this backend (shared with ValueState — both
        // wait for the encoder + value-serialize to complete before the scratch region is
        // reusable, and only one state operation is in flight per slot).
        ForStRsKeyGroupedSerializer<K> kgSer = new ForStRsKeyGroupedSerializer<>(keySerializer);
        // MapState gets a larger per-instance cap (matches legacy MAP_WRITE_BUFFER_THRESHOLD)
        // to avoid Q19-style high-cardinality top-N RMW thrashing the FFI boundary at the
        // ValueState default. The size-aware tuner is wired so small-WS MapState workloads
        // (Q15 / Q19 patterns with bounded distinct user-keys) stay near MIN_CAPACITY rather
        // than paying repeated resize churn.
        ArrowBinaryBufferAutoTuner tuner =
                new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
        ArrowBinaryBuffer buf =
                new ArrowBinaryBuffer(
                        ArrowBinaryBuffer.MIN_CAPACITY,
                        ArrowBinaryBuffer.MAX_CAPACITY_MAP_STATE,
                        tuner);
        ForStRsMapState<UK, UV> created =
                new ForStRsMapState<>(
                        linker,
                        db,
                        defaultCf,
                        keySer,
                        valueSer,
                        this::scratchSegment,
                        kgSer,
                        stateName,
                        offheapKeyGroupSupplier,
                        offheapKeySupplier,
                        buf,
                        tuner,
                        // FRS-Q11-FIX (2026-05-31): the off-heap statebuf + engine keys are stored
                        // in the encodeForMapOffheap layout [kg(2 BE) | key | SEP | stateName | SEP
                        // | mapKey]. The prefix-computer MUST produce the matching prefix
                        // [kg(2 BE) | key | SEP | stateName | SEP] so forEachEntry / isEmpty /
                        // clear / entries can prefix-match. The previous wiring used
                        // buildPrefix(stateName) = ["k/" | key | "/" | stateName | "/"], a DIFFERENT
                        // layout (KEYED_NS_MARKER "k/" vs 2-byte keyGroup) — so every off-heap
                        // prefix scan matched ZERO rows: isEmpty()==true always, clear()==no-op,
                        // entries()/forEachEntry()==empty. That empty reload made MergingWindowSet
                        // (q11/q15 session windows) throw "Window not in the in-flight window set".
                        // get/put/remove were unaffected (they use the off-heap encoder directly).
                        () ->
                                kgSer.encodeForState(
                                        offheapKeyGroupSupplier.getAsInt(),
                                        (K) offheapKeySupplier.get(),
                                        stateName));
        // A-C4R10-H1: ownedBuffers.add + mapStateRegistry.put must be atomic
        // with the closed re-check. Pre-fix split them across two monitors —
        // `ownedBuffers.add` succeeded under stateCache, close() then ran
        // its F-C4R9-H1 snapshot + arena.close(), then T1 acquired
        // mapStateRegistry, observed closed=true, threw — but the buf was
        // already in `ownedBuffers` AFTER close()'s snapshot, never closed
        // → its MemorySegments reference a now-closed arena → IllegalState
        // / UAF when the buf is touched later.
        //
        // Hold BOTH monitors during the publish: stateCache to register the
        // buf so close()'s snapshot will see it AND see closed=true here, plus
        // mapStateRegistry for the state publish itself. Lock acquisition
        // order: stateCache OUTER, mapStateRegistry INNER (matches close()'s
        // order — stateCache snapshot at line ~1738 → mapStateRegistry snapshot
        // at line ~1754).
        synchronized (stateCache) {
            synchronized (mapStateRegistry) {
                if (closed) {
                    try {
                        buf.close();
                    } catch (Throwable ignore) {
                        // best-effort — buf was never published.
                    }
                    throw new IllegalStateException(
                            "ForStRsKeyedStateBackend already closed; cannot register MapState: "
                                    + stateName);
                }
                ownedBuffers.add(buf);
                mapStateRegistry.put(stateName, created);
            }
        }
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
        ForStRsReducingState<T> existing;
        // E-C4R8-H3/H4: see getValueState.
        synchronized (stateCache) {
            if (closed) {
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot access ReducingState: "
                                + stateName);
            }
            existing =
                    (ForStRsReducingState<T>) stateCache.get(reducingStateCacheKey(stateName));
        }
        if (existing != null) {
            return existing;
        }
        byte[] prefix = buildPrefix(stateName);
        // FRS-V1-VEC (2026-06-01): off-heap Arrow batch-execution statebuf (mirrors
        // ValueState). add()=read-(off-heap)-reduce-insert; drains in one batch via the
        // checkpoint hook (flushAllOffHeapValueStateBuffers handles ReducingState below).
        ArrowBinaryBufferAutoTuner tuner =
                new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
        ArrowBinaryBuffer buf =
                new ArrowBinaryBuffer(
                        ArrowBinaryBuffer.MIN_CAPACITY, ArrowBinaryBuffer.MAX_CAPACITY, tuner);
        ForStRsReducingState<T> created =
                new ForStRsReducingState<>(
                        linker,
                        db,
                        defaultCf,
                        prefix,
                        elementSerializer,
                        reduceFunction,
                        this::scratchSegment,
                        buf,
                        tuner);
        synchronized (stateCache) {
            if (closed) {
                try {
                    buf.close();
                } catch (Throwable ignore) {
                    // best-effort: buffer never published.
                }
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot register ReducingState: "
                                + stateName);
            }
            ownedBuffers.add(buf);
            stateCache.put(reducingStateCacheKey(stateName), created);
        }
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
        ForStRsAggregatingState<IN, ACC, OUT> existing;
        // E-C4R8-H3/H4: see getValueState.
        synchronized (stateCache) {
            if (closed) {
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot access AggregatingState: "
                                + stateName);
            }
            existing =
                    (ForStRsAggregatingState<IN, ACC, OUT>)
                            stateCache.get(aggregatingStateCacheKey(stateName));
        }
        if (existing != null) {
            return existing;
        }
        byte[] prefix = buildPrefix(stateName);
        // FRS-V1-VEC (2026-06-01): off-heap Arrow batch-execution statebuf (drained at
        // checkpoint via flushAllOffHeapValueStateBuffers).
        ArrowBinaryBufferAutoTuner aggTuner =
                new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
        ArrowBinaryBuffer aggBuf =
                new ArrowBinaryBuffer(
                        ArrowBinaryBuffer.MIN_CAPACITY, ArrowBinaryBuffer.MAX_CAPACITY, aggTuner);
        ForStRsAggregatingState<IN, ACC, OUT> created =
                new ForStRsAggregatingState<>(
                        linker,
                        db,
                        defaultCf,
                        prefix,
                        accSerializer,
                        aggregateFunction,
                        this::scratchSegment,
                        aggBuf,
                        aggTuner);
        synchronized (stateCache) {
            if (closed) {
                try {
                    aggBuf.close();
                } catch (Throwable ignore) {
                    // best-effort: buffer never published.
                }
                throw new IllegalStateException(
                        "ForStRsKeyedStateBackend already closed; cannot register AggregatingState: "
                                + stateName);
            }
            ownedBuffers.add(aggBuf);
            stateCache.put(aggregatingStateCacheKey(stateName), created);
        }
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
        //
        // R69-M1: each flush in its own try/catch so one phase's throw does not
        // skip the others, leaving the engine only partially flushed and the
        // iterator-based count silently undercounted. If any flush fails we
        // refuse to return a fractional count and surface the captured throwable
        // — better a loud failure than a silently wrong answer.
        Throwable flushError = null;
        try {
            flushWriteBuffer();
        } catch (Throwable t) {
            flushError = t;
        }
        try {
            flushAllMapStates();
        } catch (Throwable t) {
            if (flushError == null) {
                flushError = t;
            } else {
                flushError.addSuppressed(t);
            }
        }
        try {
            flushAllOffHeapValueStateBuffers();
        } catch (Throwable t) {
            if (flushError == null) {
                flushError = t;
            } else {
                flushError.addSuppressed(t);
            }
        }
        if (flushError != null) {
            throw new RuntimeException(
                    "numKeyValueStateEntries: flush phase failed; count is not safe to return",
                    flushError);
        }
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
        // R26-M3: mirror the R24-H1 try/finally pattern from close(). If
        // flushAllMapStates throws (R26-M2 still records aggregate failures but rethrows),
        // pre-R26-M3 the subsequent flushAllOffHeapValueStateBuffers + createCheckpoint were
        // skipped — value-state buffer entries that DID drain to the engine on prior flushes
        // would still be checkpointed, but any value-state writes still living in the
        // off-heap buffer were lost and the engine snapshot was incomplete in a hidden way.
        // The try/finally chain gives every flush phase a chance to drain before any throw
        // propagates, and we deliberately SKIP createCheckpoint if a flush failed (an
        // incomplete checkpoint is worse than no checkpoint — fail loud).
        Throwable flushError = null;
        try {
            flushWriteBuffer();
        } catch (Throwable t) {
            flushError = t;
        }
        try {
            flushAllMapStates();
        } catch (Throwable t) {
            if (flushError == null) {
                flushError = t;
            } else {
                flushError.addSuppressed(t);
            }
        }
        try {
            flushAllOffHeapValueStateBuffers();
        } catch (Throwable t) {
            if (flushError == null) {
                flushError = t;
            } else {
                flushError.addSuppressed(t);
            }
        }
        if (flushError != null) {
            if (flushError instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Flush phase failed before snapshot", flushError);
        }
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
        // R26-M3: same try/finally chain as {@link #snapshot}: each flush phase gets a chance
        // to drain before any throw propagates. If any flush fails the scan is aborted —
        // returning an iterator over a partially-flushed engine would silently omit keys.
        Throwable flushError = null;
        try {
            flushWriteBuffer();
        } catch (Throwable t) {
            flushError = t;
        }
        try {
            flushAllMapStates();
        } catch (Throwable t) {
            if (flushError == null) {
                flushError = t;
            } else {
                flushError.addSuppressed(t);
            }
        }
        try {
            flushAllOffHeapValueStateBuffers();
        } catch (Throwable t) {
            if (flushError == null) {
                flushError = t;
            } else {
                flushError.addSuppressed(t);
            }
        }
        if (flushError != null) {
            if (flushError instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Flush phase failed before keys() scan", flushError);
        }
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

    /**
     * Returns the per-slot Arena scope. Throws {@link IllegalStateException} if called after {@link
     * #close()} or {@link #dispose()} so stale callers fail loudly.
     */
    public SlotArenaScope slotArenaScope() {
        if (slotArenaScope == null) {
            throw new IllegalStateException("Backend disposed");
        }
        return slotArenaScope;
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

    /** Returns the key-group range this backend services (PR-A3 / S1-6). */
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    /** Returns the total number of key-groups for keygroup assignment (PR-A3 / S1-6). */
    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    /**
     * Returns the key-group assigned to the current key (or the range's start when no current key
     * is set). Exposed so tests can verify {@link #offheapKeyGroupSupplier} routes V1-sync state
     * correctly under rescaling.
     */
    public int getCurrentKeyGroup() {
        return offheapKeyGroupSupplier.getAsInt();
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
        if (!bufferEnabled) {
            return null;
        }
        // B8-H2: lookup returns the packed (off,len) index into the value arena; the caller
        // (ValueState.value()) expects a freshly-owned byte[], so we materialize one here. This is
        // the read path — cold relative to update() which is what the arena optimizes.
        // B9-H1/H3: primitive-keyed open-addressing map → zero allocations on the get path
        // (no ByteArrayWrapper, no boxed Long). The result byte[] is the only remaining alloc
        // and is dictated by the caller's borrow contract.
        long packed = writeBuffer.get(key);
        byte[] result = null;
        if (packed != ByteArrayLongMap.ABSENT) {
            int off = (int) (packed >>> 32);
            int len = (int) packed;
            result = new byte[len];
            System.arraycopy(writeValueArena, off, result, 0, len);
        }
        sampleTotal++;
        if (result != null) {
            sampleHits++;
        }
        if (sampleTotal >= ADAPTIVE_SAMPLE_WINDOW) {
            double hitRate = (double) sampleHits / sampleTotal;
            if (hitRate < DISABLE_THRESHOLD) {
                bufferEnabled = false;
                flushWriteBuffer();
            }
            sampleHits = 0;
            sampleTotal = 0;
        }
        return result;
    }

    /**
     * Buffers a write for the given composite ForSt key. The write is NOT sent to native
     * immediately — it will be flushed in batch when the threshold is reached, on checkpoint, or on
     * close. Called by {@link ForStRsValueState#update(Object)}.
     *
     * <p>B8-H2: slice-based PUT — the caller (ValueState.update) passes the serializer's shared
     * buffer directly ({@code valBuf, valOff, valLen}) instead of a freshly-allocated owned
     * {@code byte[]}. We copy into {@link #writeValueArena} so the shared buffer can be reused
     * on the next update() call without corrupting buffered entries.
     */
    public void putToWriteBuffer(byte[] key, byte[] valBuf, int valOff, int valLen) {
        // A9-M1: hard cap per-entry value length BEFORE any arithmetic on writeArenaPos +
        // valLen. Pre-fix an adversarial value > 2 GiB (or cumulative arena past 2 GiB before
        // flush) could wrap the int boundary check negative → bounds check passed against a
        // negative left-hand side → either silent buffer corruption or AIOOBE on arraycopy.
        // Reject early with a typed exception so the caller sees a deterministic failure mode
        // instead of either pathology. valLen < 0 is also rejected (defensive — the slice API
        // contract is non-negative len, but a buggy caller could break it).
        if (valLen < 0 || valLen > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException(
                    "valLen out of bounds: "
                            + valLen
                            + " (must be in [0, "
                            + MAX_VALUE_BYTES
                            + "]); raise MAX_VALUE_BYTES if a larger value is genuinely needed");
        }
        if (!bufferEnabled) {
            // Slice-based native put: pass the slice directly to ForStRsLinker. The single
            // overload that accepts (key, value, valueOff, valueLen) avoids the per-update copy.
            // R0C-NEW-H1 Tier-2: segment FFI surface.
            linker.putSegment(
                    db,
                    defaultCf,
                    java.lang.foreign.MemorySegment.ofArray(key),
                    0L,
                    key.length,
                    java.lang.foreign.MemorySegment.ofArray(valBuf),
                    valOff,
                    valLen);
            return;
        }
        // Grow arena if needed (doubling). Steady-state Q11 hits the high-water mark once and
        // then never re-grows because the arena is reset on every flush.
        //
        // A9-M1: writeArenaPos is now {@code long} so the addition cannot wrap negative even on
        // an adversarial cumulative path; the {@code MAX_VALUE_BYTES} cap on valLen plus the
        // {@code MAX_BUFFER_ENTRIES} cap on the in-flight buffer keep the sum well below
        // {@link Integer#MAX_VALUE} so the int-cast for the {@code byte[]} allocation below is
        // safe (any future regression on either cap would surface as a {@link
        // NegativeArraySizeException} on the {@code new byte[newLen]} allocation, not silent
        // corruption).
        if (writeArenaPos + valLen > writeValueArena.length) {
            long required = writeArenaPos + valLen;
            if (required > Integer.MAX_VALUE) {
                // Defense-in-depth: even with MAX_VALUE_BYTES + MAX_BUFFER_ENTRIES this branch
                // should be unreachable, but if both caps are raised we'd rather throw than
                // silently truncate via the int cast on `new byte[(int) required]`.
                throw new IllegalStateException(
                        "writeValueArena required size " + required + " exceeds Integer.MAX_VALUE");
            }
            // D10-H2: use long arithmetic to avoid `int` overflow if the doubling loop ever
            // approaches Integer.MAX_VALUE (sign-bit flip → infinite loop). The `required >
            // Integer.MAX_VALUE` guard above bounds the target, but the multiplier itself
            // must also be computed in `long` so a single `<<= 1` past 2^30 doesn't wrap.
            long doubled = (long) writeValueArena.length;
            while (doubled < required) {
                doubled <<= 1;
            }
            if (doubled > Integer.MAX_VALUE) {
                throw new IllegalStateException(
                        "writeValueArena grown size " + doubled + " exceeds Integer.MAX_VALUE");
            }
            int newLen = (int) doubled;
            byte[] grown = new byte[newLen];
            // writeArenaPos is bounded by writeValueArena.length here (int), so the int cast is
            // safe — the loop above grows newLen only on the next iteration if it would exceed
            // the cap, and we already rejected required > Integer.MAX_VALUE above.
            System.arraycopy(writeValueArena, 0, grown, 0, (int) writeArenaPos);
            writeValueArena = grown;
        }
        int off = (int) writeArenaPos;
        System.arraycopy(valBuf, valOff, writeValueArena, off, valLen);
        writeArenaPos += valLen;
        long packed = ((long) off << 32) | ((long) valLen & 0xFFFFFFFFL);
        // B9-H1: primitive-keyed map → no ByteArrayWrapper alloc, no boxed Long alloc.
        writeBuffer.put(key, packed);
        writeBufferCount++;
        if (writeBufferCount >= WRITE_BUFFER_FLUSH_THRESHOLD
                || writeBuffer.size() >= MAX_BUFFER_ENTRIES) {
            flushWriteBuffer();
        }
    }

    /**
     * B8-H2 legacy-compatibility shim: kept so existing call sites (none on the hot path) that
     * still pass an owned byte[] continue to compile. Forwards to the slice overload with
     * {@code (value, 0, value.length)}.
     */
    public void putToWriteBuffer(byte[] key, byte[] value) {
        putToWriteBuffer(key, value, 0, value.length);
    }

    /**
     * Removes a key from the write buffer and issues a native delete. Called by {@link
     * ForStRsValueState#clear()} to ensure the delete reaches the engine and the buffer doesn't
     * serve stale data.
     *
     * <p>E-R4-H3: the pre-fix shape did {@code writeBuffer.remove(key); linker.delete(...);}
     * which DROPPED only this key's pending put and let the DELETE bypass the rest of the
     * buffer. If a checkpoint barrier arrived between the synchronous DELETE and the next
     * {@link #flushWriteBuffer()} call, the snapshot captured the DELETE without the
     * still-buffered PUTs — restoring from that checkpoint silently lost user writes.
     * Same hazard on crash: the DELETE persists while buffered PUTs do not. The fix flushes
     * the ENTIRE write buffer first (ordering: all buffered PUTs land, then this DELETE
     * lands, then return). This preserves the write-buffer's contract that "engine state
     * matches the sequence of user-issued ops" across snapshot and crash boundaries.
     */
    public void deleteFromWriteBuffer(byte[] key) {
        // B9-H1: primitive-keyed map → no ByteArrayWrapper alloc.
        writeBuffer.remove(key);
        // 2026-05-29 PERF-RESTORE-#2 (v3.8 parity): the per-delete flushWriteBuffer()
        // (originally E-R4-H3) was costing q4/q5/q7/q9/q15-q19 their 5×-15× wins —
        // windowed-join CLEARs at window close each forced a full buffer drain.
        // The correctness invariant ("all PUTs land before this DELETE") is preserved
        // at the SNAPSHOT BOUNDARY via ForStRsAbstractKeyedStateBackend.snapshot()'s
        // pre-snapshot flushWriteBuffer() call, which is where v3.8 enforced it. We
        // additionally drain in flushWriteBuffer()-on-close so terminal state is
        // consistent. Per-record CLEAR pays only the native delete FFI hop.
        // R0C-NEW-H1 Tier-2: segment FFI surface.
        linker.deleteSegment(
                db, defaultCf, java.lang.foreign.MemorySegment.ofArray(key), 0L, key.length);
    }

    /**
     * Flushes all buffered writes to the engine in batched {@code batchPut} calls. Must be called
     * before checkpoint (correctness) and before close (durability). Safe to call when the buffer
     * is empty (no-op).
     *
     * <p>D9-H1 fix: the buffer can grow up to {@link #MAX_BUFFER_ENTRIES} (= 4096) entries — the
     * count-based fast-path threshold ({@link #WRITE_BUFFER_FLUSH_THRESHOLD} = 64) is hit first in
     * the common case, but the {@code writeBuffer.size() >= MAX_BUFFER_ENTRIES} branch in {@link
     * #putToWriteBuffer} could trigger a flush with up to 4096 entries while the pre-allocated
     * staging segments ({@link #flushKeyPtrs}, {@link #flushKeyLens}, {@link #flushValuePtrs},
     * {@link #flushValueLens}) are sized for only {@code WRITE_BUFFER_FLUSH_THRESHOLD} slots.
     * Iterating past slot 63 walked off the segments. To remove this invariant entirely we iterate
     * the buffer in chunks of {@code WRITE_BUFFER_FLUSH_THRESHOLD}, reusing the same 64-slot
     * staging segments for each chunk and issuing one {@code batchPut} per chunk. This also keeps
     * the per-call FFI cost small (smaller batch → shorter native synchronous window) which
     * matches the same bounded-latency rationale as D6-H2 / D8-H1 / D9-H2.
     *
     * <p>Uses pre-allocated staging segments ({@link #flushKeyPtrs}, {@link #flushKeyLens}, {@link
     * #flushValuePtrs}, {@link #flushValueLens}) to avoid per-flush Arena allocation for the
     * pointer/length arrays. The key/value byte[] payloads are staged into a short-lived confined
     * Arena (one allocation per payload) opened once per chunk so the staging memory is reclaimed
     * between chunks instead of growing to the full buffer size.
     *
     * <p>A9-M2 atomicity contract — no double-write on FFI throw:
     *
     * <ul>
     *   <li><b>All chunks succeed:</b> buffer state is cleared exactly once, after the loop.
     *       Re-invoking flush is a no-op.
     *   <li><b>Any chunk throws:</b> the {@code linker.batchPut} call lets the exception escape
     *       the inner try-with-resources (which only owns the payload Arena) and then escape the
     *       outer while loop. Critically, {@link #writeBuffer}, {@link #writeArenaPos}, and
     *       {@link #writeBufferCount} are <i>not</i> touched on the throw path — the buffer
     *       retains <i>every</i> entry it held when flush began, including those that may have
     *       been successfully written to the engine in earlier chunks of this same flush.
     *   <li><b>Retry policy for partial-success throws:</b> a naive retry of this method would
     *       re-send entries that earlier chunks already wrote to the engine. The engine-side
     *       contract therefore <b>MUST</b> be idempotent for batchPut (overwrites with the same
     *       key+value are byte-equal — true for ForSt-RS's LSM put which appends a new MemTable
     *       entry that subsumes the prior one). The standard Flink retry path on snapshot failure
     *       is to fail the task and trigger a fresh restore from the previous checkpoint, which
     *       discards both the buffer and the engine state — so retry-with-double-write is the
     *       degenerate case for a checkpoint that already failed.
     * </ul>
     *
     * <p>The clear-state lines are intentionally placed AFTER the outer while loop (not inside a
     * try/finally) so that a throw from batchPut leaves the buffer untouched. Do NOT move them
     * into a finally block — that would defeat the contract above.
     */
    public void flushWriteBuffer() {
        if (writeBuffer.isEmpty()) {
            return;
        }
        // B9-H1: slot-cursor iteration over the primitive-keyed map avoids the per-Entry box that
        // the legacy HashMap.entrySet().iterator() allocated on each next() call. `slot` persists
        // across chunk boundaries so we resume exactly where the previous chunk stopped.
        final int cap = writeBuffer.capacity();
        int slot = 0;
        // B10-H1: hoist Arena.ofConfined OUTSIDE the chunk loop. The prior shape opened a fresh
        // Arena per chunk — fine on the normal Q11 V1-sync path (count threshold 64 fires first
        // so 1 arena per flush) but pathological on the heavy Map/duplicate path where 4096
        // entries / 64 chunk size = 64 arena opens per flushWriteBuffer call. Each
        // Arena.ofConfined is ~µs (Cleaner registration + native allocator setup), adding
        // multi-ms latency per flush.
        //
        // Lifetime safety: each chunk's per-slot payload allocations (ks, vs) are scoped to
        // payloadArena. They are referenced by flushKeyPtrs/flushValuePtrs/flushKeyLens/
        // flushValueLens — staging segments that the engine reads synchronously inside batchPut
        // and copies into the LSM MemTable. Once batchPut returns for a chunk, the staging
        // segments are free to be overwritten by the next chunk's allocations. payloadArena's
        // try-with-resources close at method exit then releases all chunk allocations as a unit
        // — no UAF because batchPut has fully returned for every chunk by then.
        //
        // The staging segments themselves (flushKeyPtrs/Lens etc) are field-initialized in the
        // global arena (see field declarations) and live for the backend lifetime — they are
        // overwritten per chunk, so they do not depend on payloadArena's lifetime.
        try (Arena payloadArena = Arena.ofConfined()) {
            while (slot < cap) {
                // Fill one chunk of up to WRITE_BUFFER_FLUSH_THRESHOLD rows; the staging segments
                // are sized exactly for this chunk size so we never write past their bounds.
                int chunk = 0;
                while (chunk < WRITE_BUFFER_FLUSH_THRESHOLD && slot < cap) {
                    byte[] k = writeBuffer.keyAt(slot);
                    if (k == null) {
                        slot++;
                        continue;
                    }
                    long p = writeBuffer.valueAt(slot);
                    slot++;
                    int vOff = (int) (p >>> 32);
                    int vLen = (int) p;
                    // A11-H1 / D11-H2 (DATA CORRUPTION): REVERT D10-M3. The 1-byte dummy
                    // allocation is load-bearing — it preserves PUT semantics for legitimately
                    // empty keys / values.
                    //
                    // The Rust FFI {@code frs_batch_put} interprets {@code value_ptrs[i].is_null()}
                    // as DELETE (lib.rs:1339-1348) and {@code key_ptrs[i].is_null()} as
                    // FRS_STATUS_NULL_ARG (aborts the whole batch). If we passed
                    // {@link MemorySegment#NULL} (C pointer 0) for an empty value, ANY state
                    // whose serialized form is zero bytes would be silently transformed into a
                    // tombstone at flush — silent data corruption. Storing NULL for an empty
                    // key would abort the entire batch.
                    //
                    // The 1-byte sentinel guarantees the pointer is non-NULL while the matching
                    // length slot remains 0, so the engine sees a PUT with an empty payload
                    // ({@code slice::from_raw_parts(ptr, 0)} is a valid empty slice). Memcopy of
                    // length 0 is a no-op so we never read the sentinel byte's contents.
                    //
                    // Arena cost: 1 byte × 64 entries = 64 bytes/chunk peak — negligible vs the
                    // correctness cost of silent tombstoning.
                    MemorySegment ks =
                            payloadArena.allocate(k.length == 0 ? 1 : k.length, 1);
                    MemorySegment vs =
                            payloadArena.allocate(vLen == 0 ? 1 : vLen, 1);
                    if (k.length > 0) {
                        MemorySegment.copy(k, 0, ks, ValueLayout.JAVA_BYTE, 0, k.length);
                    }
                    if (vLen > 0) {
                        // B8-H2: copy from the value arena slice rather than from a separately-
                        // owned byte[]. The arena slice was populated by putToWriteBuffer directly
                        // from the serializer's shared buffer — one copy total (vs two in the
                        // legacy owned-byte[] path: serializer→getCopyOfBuffer→arena).
                        MemorySegment.copy(
                                writeValueArena, vOff, vs, ValueLayout.JAVA_BYTE, 0, vLen);
                    }
                    flushKeyPtrs.set(
                            ValueLayout.ADDRESS,
                            (long) chunk * ValueLayout.ADDRESS.byteSize(),
                            ks);
                    flushValuePtrs.set(
                            ValueLayout.ADDRESS,
                            (long) chunk * ValueLayout.ADDRESS.byteSize(),
                            vs);
                    flushKeyLens.set(
                            ValueLayout.JAVA_LONG,
                            (long) chunk * ValueLayout.JAVA_LONG.byteSize(),
                            (long) k.length);
                    flushValueLens.set(
                            ValueLayout.JAVA_LONG,
                            (long) chunk * ValueLayout.JAVA_LONG.byteSize(),
                            (long) vLen);
                    chunk++;
                }
                if (chunk > 0) {
                    // A9-M2: if batchPut throws, the exception escapes both the inner try (which
                    // owns payloadArena via its try-with-resources) and the outer while loop,
                    // bypassing the post-loop
                    // clear-state lines below. The buffer therefore retains every entry it held
                    // when flush began — engine state is UNKNOWN (the batch may have partially
                    // committed) but the buffer is in a consistent pre-flush state, and the
                    // standard Flink response (fail the snapshot → fail the task → restore from
                    // the previous checkpoint) discards both. See the method-level contract above.
                    linker.batchPut(
                            db,
                            defaultCf,
                            flushKeyPtrs,
                            flushKeyLens,
                            flushValuePtrs,
                            flushValueLens,
                            chunk);
                }
            }
        }
        // A9-M2: reached ONLY on success of every chunk (any throw bypasses these lines and
        // leaves the buffer populated). Order: clear the map first so a re-entrant flushWriteBuffer
        // call (e.g. from a concurrent close on another thread — guarded elsewhere but defensive
        // here) sees an empty buffer and returns at the isEmpty() fast path. Reset the value arena
        // and counter after so steady-state Q11 reuses the same backing storage indefinitely
        // (zero allocation per flush after the initial high-water mark).
        writeBuffer.clear();
        writeArenaPos = 0L;
        writeBufferCount = 0;
    }

    /**
     * Flushes all registered kg-prefixed MapState instances. Called on checkpoint and close to
     * ensure all buffered map-state writes reach the engine before snapshot.
     *
     * <p>R25-M2: pre-fix this method iterated without a per-state try/catch — a single
     * {@code ms.flush()} throw aborted the loop and skipped every subsequent MapState's
     * pending write buffer, silently dropping their data on checkpoint. The fix mirrors
     * the best-effort pattern at {@link #flushAllOffHeapValueStateBuffers}: a per-state
     * try/catch records the first failure (with any subsequent failures attached as
     * {@code addSuppressed}) and continues, then surfaces the aggregate at the end so
     * callers still observe the failure but every state has had a chance to drain.
     */
    public void flushAllMapStates() {
        Throwable firstFailure = null;
        // R26-M2: snapshot the entry set to a local list BEFORE iterating. {@code
        // mapStateRegistry} is a plain HashMap and {@code ms.flush()} can — via the engine's
        // write-buffer flush callbacks — re-enter the backend on the same thread and mutate
        // the registry (e.g. a registerMapStateForFlush call from a downstream listener), or
        // a concurrent {@code dispose()} could clear the map mid-iteration. Either path
        // throws {@code ConcurrentModificationException} on the live entrySet view. A
        // defensive copy decouples the iteration from concurrent structural mutations; the
        // entries themselves still point at the same ForStRsMapState instances, so each
        // flush reaches the live state.
        java.util.List<Map.Entry<String, ForStRsMapState<?, ?>>> snapshot =
                new java.util.ArrayList<>(mapStateRegistry.entrySet());
        for (Map.Entry<String, ForStRsMapState<?, ?>> entry : snapshot) {
            try {
                entry.getValue().flush();
            } catch (Throwable t) {
                LOG.warn("MapState flush failed for state '{}'", entry.getKey(), t);
                if (firstFailure == null) {
                    firstFailure = t;
                } else {
                    firstFailure.addSuppressed(t);
                }
            }
        }
        if (firstFailure != null) {
            throw new RuntimeException(
                    "One or more MapState flushes failed during flushAllMapStates",
                    firstFailure);
        }
    }

    /**
     * 1b.3: Flushes all off-heap ArrowBinaryBuffers owned by ValueState instances in {@code
     * stateCache} so their buffered writes reach the engine. Without this, checkpoint/close
     * silently drops every buffered value-state write — a correctness bug.
     *
     * <p>R67-H1: this sister method to {@link #flushAllMapStates} was previously
     * "best-effort" (swallowed all per-state failures). On the checkpoint path (called from
     * {@code snapshot}/{@code snapshotInProgress}), a failing flush left buffered writes
     * lost while {@code linker.createCheckpoint} proceeded — producing an incomplete
     * checkpoint with no error surface. We now mirror the R25-M2 pattern verbatim: record
     * the first failure, attach subsequent ones as suppressed, then rethrow as a
     * {@code RuntimeException} after every state has had a chance to drain.
     */
    public void flushAllOffHeapValueStateBuffers() {
        Throwable firstFailure = null;
        // Defensive copy: same rationale as R26-M2 — a flush callback may reach
        // back into the backend and mutate `stateCache` on the same thread,
        // throwing ConcurrentModificationException on the live values() view.
        java.util.List<Object> snapshot = new java.util.ArrayList<>(stateCache.values());
        for (Object v : snapshot) {
            try {
                // FRS-V1-VEC (2026-06-01): drain the off-heap Arrow statebuf of every
                // off-heap-backed V1 state (ValueState + now ReducingState/AggregatingState)
                // to the engine in one batch before the checkpoint snapshot.
                if (v instanceof ForStRsValueState<?> vs) {
                    vs.flushStateBuffer();
                } else if (v instanceof org.apache.flink.state.forstrs.state.ForStRsReducingState<?> rs) {
                    rs.flushStateBuffer();
                } else if (v
                        instanceof org.apache.flink.state.forstrs.state.ForStRsAggregatingState<?, ?, ?> as) {
                    as.flushStateBuffer();
                }
            } catch (Throwable t) {
                LOG.warn("Off-heap state-buffer flush failed", t);
                if (firstFailure == null) {
                    firstFailure = t;
                } else {
                    firstFailure.addSuppressed(t);
                }
            }
        }
        if (firstFailure != null) {
            throw new RuntimeException(
                    "One or more ValueState flushes failed during "
                            + "flushAllOffHeapValueStateBuffers",
                    firstFailure);
        }
    }

    /**
     * Releases all per-state-cache entries. When this backend was constructed with {@code
     * ownsResources=true}, also closes (in order) the default CF, the database, and the Arena that
     * owns the linker's symbol lookup.
     */
    @Override
    public void close() throws IOException {
        // R71-M2: serialize the `closed = true` transition under `this` so two
        // concurrent close()/dispose() callers cannot both observe closed=false
        // and both proceed to free defaultCf / db / arena (double-close UAF).
        // The synchronized region is intentionally tiny — only the check and
        // flag flip — so the long flush+release work below runs without
        // holding the monitor; subsequent flag checks (e.g. by in-flight
        // state ops on other threads) rely on the `volatile closed` declared
        // above for the happens-before edge.
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        // R24-H1: close() previously set {@code closed=true} BEFORE running
        // {@link #flushAllOffHeapValueStateBuffers}, {@link #flushAllMapStates}, and {@link
        // #flushWriteBuffer} without a try/finally guard around the resource-release block. A
        // throw from any of those flush calls leaked the slot arena scope, defaultCf, db, and
        // backing arena because {@code closed=true} blocked any retry through {@link
        // #dispose}. The fix wraps the FLUSH PHASE in a try-block that captures (but does not
        // swallow) the first flush error, runs the full RESOURCE-RELEASE PHASE inside the
        // finally, and surfaces the flush error AFTER all native handles are freed. The
        // release phase itself follows the existing first-error capture pattern so a failure
        // in one step does not skip the next.
        Throwable flushError = null;
        try {
            // 1b.3 + 1c.1: drain off-heap ValueState AND MapState buffers BEFORE closing the
            // underlying ArrowBinaryBuffers. Without this, any state writes that never crossed
            // the auto-flush threshold are silently lost on shutdown.
            //
            // R68-M1: each flush call is independently guarded so a ValueState-flush
            // failure (now a throw, per R67-H1) does not skip MapState's drain — which
            // would silently lose MapState buffered writes during close. Mirrors the
            // multi-phase suppression pattern used by snapshot() / keys() / restore.
            try {
                flushAllOffHeapValueStateBuffers();
            } catch (Throwable t) {
                flushError = t;
            }
            try {
                flushAllMapStates();
            } catch (Throwable t) {
                if (flushError == null) {
                    flushError = t;
                } else {
                    flushError.addSuppressed(t);
                }
            }
            // Release off-heap ArrowBinaryBuffers owned by ValueState + MapState instances.
            //
            // R69-M2: log close failures at WARN. Pre-fix the catch swallowed every
            // throwable silently, which could hide an Arena-double-close UAF (a real
            // sharing-bug symptom) until later allocations corrupt unrelated native
            // memory. Logging keeps close() best-effort while preserving diagnostic
            // visibility for sharing-bug regressions.
            // F-C4R9-H1: snapshot ownedBuffers under stateCache monitor. A
            // publisher inside the same monitor in get{Value,List,Reducing,
            // Aggregating,Map}State that read closed=false BEFORE close()
            // flipped it can still be adding a buffer concurrent with this
            // iteration. Snapshot+clear under the monitor; close the
            // snapshot outside it (b.close() can be slow on shared-arena).
            java.util.List<ArrowBinaryBuffer> buffersToClose;
            synchronized (stateCache) {
                buffersToClose = new java.util.ArrayList<>(ownedBuffers);
                ownedBuffers.clear();
            }
            for (ArrowBinaryBuffer b : buffersToClose) {
                try {
                    b.close();
                } catch (Throwable t) {
                    LOG.warn("ArrowBinaryBuffer close failed during backend close", t);
                }
            }
            // D-R3-H1: release each ForStRsMapState's lazily-allocated offHeapArena.
            // Pre-fix this arena leaked for the JVM lifetime once any put() ran on a
            // state instance — silent native-memory growth bounded only by JVM exit.
            //
            // D-R5-H4: snapshot the registry under the backend's monitor before
            // iteration. `mapStateRegistry` is a plain HashMap and `getMapState` at
            // line ~734 inserts new entries from the mailbox thread; if close() runs
            // on a task-canceler thread (per the D-R4-H1 cross-thread invariant), a
            // concurrent put could corrupt the iteration. We already use the same
            // synchronized-snapshot pattern at line ~1497 for flushAllMapStates.
            java.util.List<ForStRsMapState<?, ?>> closeSnapshot;
            synchronized (mapStateRegistry) {
                closeSnapshot = new java.util.ArrayList<>(mapStateRegistry.values());
                mapStateRegistry.clear();
            }
            for (ForStRsMapState<?, ?> ms : closeSnapshot) {
                try {
                    ms.close();
                } catch (Throwable t) {
                    LOG.warn("ForStRsMapState close failed during backend close", t);
                }
            }
            // PR-M1 (D-R2-5): close all per-thread scratch Arenas so their 64 KB allocations
            // are reclaimed instead of leaking until JVM shutdown.
            synchronized (threadLocalArenas) {
                for (Arena a : threadLocalArenas) {
                    try {
                        a.close();
                    } catch (Throwable ignore) {
                        // best-effort
                    }
                }
                threadLocalArenas.clear();
            }
            // Flush any buffered writes before releasing resources.
            flushWriteBuffer();
        } catch (Throwable t) {
            // Capture and proceed — the finally block MUST run the resource-release chain so
            // we don't leak slotArenaScope / defaultCf / db / arena on flush throw. Re-thrown
            // below after release completes.
            //
            // R68-M1: preserve any flushError already captured by the inner per-flush
            // try/catches (ValueState/MapState) rather than overwriting it.
            if (flushError == null) {
                flushError = t;
            } else {
                flushError.addSuppressed(t);
            }
        } finally {
            // F-C4R9-H1: clear under stateCache monitor to avoid racing a
            // publisher mid-`stateCache.put(...)` (see ownedBuffers snapshot
            // above for the rationale).
            synchronized (stateCache) {
                stateCache.clear();
            }
            if (iterWatchdog != null) {
                try {
                    iterWatchdog.stop();
                } catch (Throwable ignore) {
                    // best-effort: must not block resource release below.
                }
                iterWatchdog = null;
            }
            if (slotArenaScope != null) {
                try {
                    slotArenaScope.closeSlot();
                } catch (Throwable ignore) {
                    // best-effort
                }
                slotArenaScope = null;
            }
            if (ownsResources) {
                // Close in reverse order of construction; each step swallows-and-rethrows the
                // first exception so that we always attempt the full chain. Releasing the
                // native handles must NEVER be skipped by an earlier failure — that was the
                // original R24-H1 leak.
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
                    // If the flush phase ALSO threw, prefer the flush error (more
                    // actionable for users — it is what caused the close) and attach the
                    // release error as a suppressed exception. Otherwise surface the
                    // release error directly.
                    if (flushError != null) {
                        flushError.addSuppressed(first);
                    } else if (first instanceof RuntimeException re) {
                        throw re;
                    } else if (first instanceof Error err) {
                        throw err;
                    } else {
                        throw new IOException("ForStRsKeyedStateBackend close failed", first);
                    }
                }
            }
        }
        if (flushError != null) {
            if (flushError instanceof RuntimeException re) {
                throw re;
            }
            if (flushError instanceof Error err) {
                throw err;
            }
            throw new IOException(
                    "ForStRsKeyedStateBackend close failed during flush phase", flushError);
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

    /**
     * Builds the full composite ForSt key for a MapState user key: {@code "k/" ||
     * serialize(currentKey) || "/" || stateName.bytes || "/" || serialize(uk)}. Used by the
     * kg-prefixed MapState constructor's {@code compositeKeyComputer}.
     */
    private <UK> byte[] buildCompositeMapKey(
            String stateName, TypeSerializer<UK> ukSerializer, UK userKey) {
        byte[] prefix = buildPrefix(stateName);
        DataOutputSerializer ukOut = new DataOutputSerializer(32);
        try {
            ukSerializer.serialize(userKey, ukOut);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize MapState user key", e);
        }
        byte[] ukBytes = ukOut.getCopyOfBuffer();
        byte[] full = new byte[prefix.length + ukBytes.length];
        System.arraycopy(prefix, 0, full, 0, prefix.length);
        System.arraycopy(ukBytes, 0, full, prefix.length, ukBytes.length);
        return full;
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

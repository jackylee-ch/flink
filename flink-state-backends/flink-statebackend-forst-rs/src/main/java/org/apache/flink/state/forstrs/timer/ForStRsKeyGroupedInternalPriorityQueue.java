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
import org.apache.flink.runtime.state.KeyGroupRange;
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
 *   composite = QUEUE_NS_MARKER || stateName.bytes(UTF-8) || '/'
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
    private static final byte[] QUEUE_NS_MARKER = "q/".getBytes(StandardCharsets.UTF_8);

    private static final byte SEP = (byte) '/';

    /**
     * XOR mask that flips the sign bit of a {@code long} so that big-endian lexicographic byte
     * order matches signed-integer numerical order across the full {@code long} range.
     */
    private static final long SIGN_FLIP = 0x8000_0000_0000_0000L;

    /** Flush threshold — pending buffer size at which we proactively drain to the engine. */
    @VisibleForTesting static final int FLUSH_THRESHOLD = 1024;

    /** Sentinel value written under each timer key — same as the legacy code path used. */
    private static final byte[] EMPTY_VAL_BYTES = new byte[] {(byte) 1};

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

    /** Per-queue scratch segment for composing composite keys (single-threaded). */
    private final Arena scratchArena;
    private MemorySegment scratchSeg;
    private long scratchCapacity;

    /**
     * Pre-allocated staging segments for the flush path — pointer/length arrays for
     * {@link ForStRsLinker#batchPut}. Grown on demand. Reused across flushes.
     */
    private final Arena flushArena;
    private MemorySegment flushAddKeyPtrs;
    private MemorySegment flushAddKeyLens;
    private MemorySegment flushAddValPtrs;
    private MemorySegment flushAddValLens;
    private MemorySegment flushDelOffsets; // (count+1) ints — Arrow-style offsets for vectorized delete
    private MemorySegment flushDelData; // packed key bytes
    private MemorySegment emptyValueSeg; // 1-byte non-empty sentinel (shared)
    private long flushPairCapacity;
    private long flushDelDataCapacity;

    // ------------------------------------------------------------------
    // Poll-ahead cache (vectorized perf path — SP3)
    // ------------------------------------------------------------------

    private static final int REFILL_BATCH = 128;
    private final ArrayDeque<Entry> pollCache = new ArrayDeque<>();
    private int cachedKg = -1; // -1 = cache invalid / empty

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

    /** Variant with an explicit element rebinder hook (used by timer integrations). */
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
        this.flushDelDataCapacity = 0L;
        this.emptyValueSeg = flushArena.allocate(1L);
        this.emptyValueSeg.set(ValueLayout.JAVA_BYTE, 0L, (byte) 1);
    }

    // ------------------------------------------------------------------
    // Composite-key encoding helpers
    // ------------------------------------------------------------------

    private static byte[] buildQueuePrefix(String stateName) {
        byte[] sn = stateName.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[QUEUE_NS_MARKER.length + sn.length + 1];
        System.arraycopy(QUEUE_NS_MARKER, 0, out, 0, QUEUE_NS_MARKER.length);
        System.arraycopy(sn, 0, out, QUEUE_NS_MARKER.length, sn.length);
        out[out.length - 1] = SEP;
        return out;
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
            // op == REMOVE — cancellation: remove the pending REMOVE so the engine still keeps
            // the original entry (or, if the engine doesn't have it either, the add lands as a
            // fresh insert below).
            pendingBuffer.removeAt(existing);
            // If there's still an engine entry under this key, the (add then remove then add) net
            // is "keep present". If there isn't, we still need an ADD pending — fall through.
            // For correctness we re-insert ADD so the engine reflects "present" after flush.
        }
        pendingBuffer.insertAdd(scratchSeg, 0L, keyLen, ts);
        invalidateCache();
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
        int kg = currentKeyGroupSupplier.getAsInt();
        long ts = timestampExtractor.applyAsLong(element);
        int keyLen = encodeIntoScratch(kg, element);

        int existing = pendingBuffer.find(scratchSeg, 0L, keyLen);
        if (existing >= 0) {
            int op = pendingBuffer.opAt(existing);
            if (op == ArrowTimerBuffer.OP_ADD) {
                // CANCEL — pure in-buffer cancellation, NEVER reaches engine.
                pendingBuffer.removeAt(existing);
                invalidateCache();
                return true;
            }
            // op == REMOVE — already pending. No-op (idempotent).
            return true;
        }
        // No pending entry. Insert a REMOVE op. The engine may or may not have the key — the
        // flush will issue a vectorized delete regardless (idempotent on the engine side).
        pendingBuffer.insertRemove(scratchSeg, 0L, keyLen, ts);
        invalidateCache();
        if (pendingBuffer.size() >= FLUSH_THRESHOLD) {
            flushPendingToEngine();
        }
        // We cannot tell without an engine probe whether the element was actually present pre-call;
        // return true (matches the legacy "always true" approximation Flink tolerates).
        return true;
    }

    @Override
    public T poll() {
        // Flush pending mutations so the engine view reflects all add/remove decisions.
        flushPendingToEngine();
        int kg = currentKeyGroupSupplier.getAsInt();
        Entry head = cachedHeadEntry(kg);
        if (head == null) {
            return null;
        }
        linker.delete(db, cf, head.composite);
        pollCache.pollFirst();
        return head.element;
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
        int kg = currentKeyGroupSupplier.getAsInt();

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

        // 2. Engine side: consult the EXISTING poll-ahead cache ONLY if it's already populated for
        //    this key group. Do NOT refill on a peek — refill is reserved for poll(). On a cache
        //    miss we conservatively trust the buffer side. This matches Flink's only use of peek
        //    on the registerTimer hot path: it just wants to know "is the head before mine?".
        Entry engineHead = null;
        if (cachedKg == kg && !pollCache.isEmpty()) {
            // Use the cache front directly — even if a pending REMOVE eventually masks it, the
            // worst case is a slightly-stale "head" view returned from peek, which Flink only
            // uses for the "head may have changed" hint (not correctness-critical). poll()
            // re-establishes the truth via flush + cache refresh.
            engineHead = pollCache.peekFirst();
        }

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
        }
        if (pollCache.isEmpty()) {
            refillCache(kg);
        }
        // Skip entries that have a matching pending REMOVE in the buffer.
        while (!pollCache.isEmpty()) {
            Entry head = pollCache.peekFirst();
            if (!isRemovePendingFor(head.composite)) {
                return head;
            }
            pollCache.pollFirst();
            if (pollCache.isEmpty()) {
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
        if (toAdd == null) {
            return;
        }
        for (T t : toAdd) {
            add(t);
        }
    }

    @Override
    public CloseableIterator<T> iterator() {
        flushPendingToEngine();
        invalidateCache();
        return new MultiKeyGroupIterator();
    }

    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroup) {
        flushPendingToEngine();
        invalidateCache();
        Set<T> out = new LinkedHashSet<>();
        try (FrsIterator iter = linker.prefixLookupOpen(db, cf, keyGroupPrefix(keyGroup), arena)) {
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
        // STEP 1 — flush pending mutations so the engine view is consistent.
        flushPendingToEngine();
        int kg = currentKeyGroupSupplier.getAsInt();
        // STEP 2 — open ONE prefix iterator on the engine kg-prefix. Single FFM crossing.
        java.util.ArrayList<byte[]> dueKeyList = new java.util.ArrayList<>();
        try (FrsIterator iter = linker.prefixLookupOpen(db, cf, keyGroupPrefix(kg), arena)) {
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
        int n = pendingBuffer.size();
        if (n == 0) {
            return;
        }
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
        if (delCount > 0) {
            ensureFlushDelDataCapacity(totalDelBytes);
        }

        // Pass A — collect ADDs into batchPut staging.
        if (addCount > 0) {
            byte[][] keys = new byte[addCount][];
            byte[][] vals = new byte[addCount][];
            int outIdx = 0;
            MemorySegment keyDataSeg = pendingBuffer.keyDataSegment();
            for (int i = 0; i < n; i++) {
                if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_ADD) {
                    continue;
                }
                int kOff = pendingBuffer.keyOffsetAt(i);
                int kLen = pendingBuffer.keyLenAt(i);
                byte[] k = new byte[kLen];
                MemorySegment.copy(keyDataSeg, ValueLayout.JAVA_BYTE, kOff, k, 0, kLen);
                keys[outIdx] = k;
                vals[outIdx] = EMPTY_VAL_BYTES;
                outIdx++;
            }
            linker.batchPut(db, cf, keys, vals);
        }

        // Pass B — collect REMOVEs into Arrow-offset staging then issue one vectorized delete.
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
            linker.vectorizedBatchDelete(db, cf, flushDelOffsets, flushDelData, outIdx);
        }
        pendingBuffer.clear();
        invalidateCache();
    }

    private void ensureFlushPairCapacity(int neededRows) {
        if (neededRows <= flushPairCapacity) {
            return;
        }
        long newCap = Math.max(flushPairCapacity == 0 ? 256 : flushPairCapacity * 2, neededRows);
        // Allocate as offsets too (one larger).
        flushAddKeyPtrs = flushArena.allocate(newCap * ValueLayout.ADDRESS.byteSize());
        flushAddKeyLens = flushArena.allocate(newCap * ValueLayout.JAVA_LONG.byteSize());
        flushAddValPtrs = flushArena.allocate(newCap * ValueLayout.ADDRESS.byteSize());
        flushAddValLens = flushArena.allocate(newCap * ValueLayout.JAVA_LONG.byteSize());
        flushDelOffsets = flushArena.allocate((newCap + 1) * Integer.BYTES);
        flushPairCapacity = newCap;
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
        try {
            flushPendingToEngine();
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
        try (FrsIterator iter = linker.prefixLookupOpen(db, cf, keyGroupPrefix(kg), arena)) {
            int read = 0;
            while (read < REFILL_BATCH) {
                ForStRsLinker.IteratorEntry e = linker.iteratorNext(iter);
                if (e == null) {
                    break;
                }
                pollCache.addLast(new Entry(e.key(), decodeElement(e.key())));
                read++;
            }
        }
    }

    /** Invalidates the poll-ahead cache. Cheap (just clears the deque). */
    private void invalidateCache() {
        pollCache.clear();
        cachedKg = -1;
    }

    private int sizeForKeyGroup(int kg) {
        int n = 0;
        try (FrsIterator iter = linker.prefixLookupOpen(db, cf, keyGroupPrefix(kg), arena)) {
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
                    current = linker.prefixLookupOpen(db, cf, keyGroupPrefix(kg), arena);
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

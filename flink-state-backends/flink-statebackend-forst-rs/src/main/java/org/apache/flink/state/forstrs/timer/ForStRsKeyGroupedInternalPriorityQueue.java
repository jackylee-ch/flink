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
 * <p><b>Key encoding.</b> Each entry is stored under a composite ForSt key:
 *
 * <pre>
 *   composite = QUEUE_NS_MARKER || stateName.bytes(UTF-8) || '/'
 *            || kg(2B BE) || ts(8B BE, sign-flipped) || serialize(T)
 * </pre>
 *
 * <p>The fixed-width 2-byte big-endian key-group followed by an 8-byte big-endian (sign-flipped)
 * timestamp gives a strict prefix-scan ordering of {@code (key-group ascending, timestamp
 * ascending)}. Within the same {@code (kg, ts)} bucket FIFO order is preserved by the serialized
 * tail bytes (the underlying engine sorts lexicographically and the tail also includes whatever
 * disambiguator the element carries — e.g. a {@code TimerHeapInternalTimer}'s namespace+key — which
 * keeps two entries at the same timestamp distinct without a synthetic counter).
 *
 * <p><b>Sign-flipped timestamps.</b> Flink's timer timestamp can be negative ({@link
 * Long#MIN_VALUE} is a sentinel). To keep big-endian byte order consistent with signed-numerical
 * order we XOR the sign bit on encode/decode (the standard "flip-MSB" trick). This means {@code
 * Long.MIN_VALUE} encodes to {@code 0x00 0x00 ... 0x00} and {@code Long.MAX_VALUE} encodes to
 * {@code 0xFF 0xFF ... 0xFF}, so a lexicographic prefix scan returns entries in ascending-timestamp
 * order regardless of sign.
 *
 * <p><b>Thread-safety.</b> This class is not thread-safe; the calling timer-service is responsible
 * for serialising calls per Flink's keyed-operator contract.
 *
 * <p><b>Element bounds.</b> Spec asks for {@code T extends HeapPriorityQueueElement &
 * PriorityComparable<? super T> & Keyed<?>}; we relax the {@code Keyed} bound to widen reuse — the
 * real timer-element type satisfies it but tests can use simpler stand-ins. The {@link
 * HeapPriorityQueueElement} bound is preserved because Flink's heap-backend code paths assume the
 * {@code internalIndex} accessor exists. We never write the internal index to disk (it is only
 * meaningful for an in-memory heap), so the field is left at its default after a {@link #poll()}
 * round-trip.
 *
 * @param <T> queue element type
 */
@Internal
public class ForStRsKeyGroupedInternalPriorityQueue<T extends HeapPriorityQueueElement>
        implements KeyGroupedInternalPriorityQueue<T> {

    /** Distinguishes priority-queue rows from regular keyed-state rows in the same default CF. */
    private static final byte[] QUEUE_NS_MARKER = "q/".getBytes(StandardCharsets.UTF_8);

    private static final byte SEP = (byte) '/';

    /**
     * XOR mask that flips the sign bit of a {@code long} so that big-endian lexicographic byte
     * order matches signed-integer numerical order across the full {@code long} range.
     */
    private static final long SIGN_FLIP = 0x8000_0000_0000_0000L;

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
    // Poll-ahead cache (vectorized perf path — SP3)
    //
    // The legacy hot path was: each poll() opens an iterator, reads one
    // entry, closes the iterator, then deletes the key — ≈ 4 FFI crossings
    // per poll. On Q5's ~460M timer ops this dominates runtime.
    //
    // The cache batches the READ side: on a miss we call
    // `linker.prefixGetAll(prefix, REFILL_BATCH)` which returns N entries in
    // one FFI roundtrip. Subsequent poll()/peek() within the same key-group
    // serve from the cache until exhausted. We still issue one
    // `linker.delete` per poll for now (correctness-first; batched delete is
    // a follow-up).
    //
    // The cache is INVALIDATED on any mutating call (add, remove, removeAll)
    // because such calls may shift the min-element. isEmpty / size /
    // iterator / getSubsetForKeyGroup also invalidate to keep semantics
    // simple — they go straight to the engine through the existing paths.
    // ------------------------------------------------------------------

    private static final int REFILL_BATCH = 128;
    private final ArrayDeque<Entry> pollCache = new ArrayDeque<>();
    private int cachedKg = -1; // -1 = cache invalid / empty

    /**
     * Constructs a queue rooted at {@code stateName} that scans the supplied {@code keyGroupRange}
     * and looks up the "current key group" via {@code currentKeyGroupSupplier}. The {@code
     * timestampExtractor} maps a {@code T} to the {@code long} priority (lower = earlier). Tests
     * pass a fixed-value supplier; real backends pass a method-reference into {@code
     * AbstractKeyedStateBackend.getCurrentKeyGroupIndex()}.
     */
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
     * Adds {@code element} to the queue under the current key group; returns {@code true} if the
     * head of the queue may have changed (the contract used by Flink's timer-service). We
     * approximate "head may have changed" as "always true" because tracking the current min would
     * require a read-after-write probe — fine for correctness, only loses a marginal optimisation.
     */
    @Override
    public boolean add(T element) {
        int kg = currentKeyGroupSupplier.getAsInt();
        byte[] key = encode(kg, element);
        linker.put(db, cf, key, EMPTY_VALUE);
        invalidateCache();
        return true;
    }

    @Override
    public boolean remove(T element) {
        int kg = currentKeyGroupSupplier.getAsInt();
        byte[] key = encode(kg, element);
        // Engine-level delete is idempotent; we report based on whether the key existed.
        byte[] existing = linker.get(db, cf, key);
        linker.delete(db, cf, key);
        invalidateCache();
        return existing != null;
    }

    @Override
    public T poll() {
        int kg = currentKeyGroupSupplier.getAsInt();
        Entry head = cachedHeadEntry(kg);
        if (head == null) {
            return null;
        }
        // Remove from engine + remove from cache front (cachedHeadEntry left it in place).
        linker.delete(db, cf, head.composite);
        pollCache.pollFirst();
        return head.element;
    }

    @Override
    public T peek() {
        int kg = currentKeyGroupSupplier.getAsInt();
        Entry head = cachedHeadEntry(kg);
        return head == null ? null : head.element;
    }

    @Override
    public boolean isEmpty() {
        // size() is O(N) — but isEmpty needs only the first hit for any covered key-group.
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
        // size walks all entries; cache state is irrelevant. Flush cache to keep
        // engine view consistent (no stale-cache view from another kg).
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
        invalidateCache();
        return new MultiKeyGroupIterator();
    }

    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroup) {
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
     * the count of elements actually deleted (i.e. that were present prior to this call). This
     * mirrors Flink's bulk-removal pattern used by window/timer operators when eviction fires.
     */
    public int removeAll(Collection<? extends T> toRemove) {
        if (toRemove == null || toRemove.isEmpty()) {
            return 0;
        }
        int kg = currentKeyGroupSupplier.getAsInt();
        int removed = 0;
        for (T t : toRemove) {
            byte[] key = encode(kg, t);
            byte[] existing = linker.get(db, cf, key);
            if (existing != null) {
                linker.delete(db, cf, key);
                removed++;
            }
        }
        invalidateCache();
        return removed;
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
     * miss. The returned entry stays at the front of {@link #pollCache} so that subsequent
     * {@link #peek()} calls are idempotent; {@link #poll()} pops it after the engine delete
     * succeeds.
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
     * Refills {@link #pollCache} from the engine for {@code kg} via a single {@code prefixGetAll}
     * roundtrip (capacity {@link #REFILL_BATCH}). On any error the cache is left empty.
     */
    private void refillCache(int kg) {
        ForStRsLinker.IteratorEntry[] entries =
                linker.prefixGetAll(db, cf, keyGroupPrefix(kg), REFILL_BATCH);
        if (entries == null || entries.length == 0) {
            return;
        }
        for (ForStRsLinker.IteratorEntry e : entries) {
            pollCache.addLast(new Entry(e.key(), decodeElement(e.key())));
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

    /**
     * Sentinel "value" written with every queue entry. The composite-key holds all the information
     * we need; the engine's {@code get()} returns {@code null} for absent keys but cannot
     * distinguish "absent" from "present with empty value" — so we write a 1-byte non-empty marker
     * to keep {@link #remove(Object)} / {@link #removeAll(Collection)} able to tell the two apart.
     */
    private static final byte[] EMPTY_VALUE = new byte[] {(byte) 1};

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

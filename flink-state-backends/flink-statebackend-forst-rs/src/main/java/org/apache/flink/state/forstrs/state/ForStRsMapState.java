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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;
import org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView;
import org.apache.flink.state.forstrs.v1sync.MemorySegmentDataOutputView;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Minimal {@link MapState} implementation backed by ForSt-RS via the {@link ForStRsLinker} FFM
 * bridge.
 *
 * <p>Two construction modes are supported:
 *
 * <ol>
 *   <li><b>Static byte[] prefix (legacy)</b>: composite ForSt key is {@code keyPrefix ||
 *       serialize(UK)}. Prefix-scans use {@code keyPrefix} verbatim.
 *   <li><b>Spec §6 kg-prefixed mode</b>: composite ForSt key is built per call via the supplied
 *       {@code compositeKeyComputer} (which the keyed-state backend wires to {@code
 *       ForStRsKeyGroupedSerializer.encodeForMap(currentKg, currentKey, stateName, ukSer, uk)}).
 *       Prefix-scans use the per-state-prefix returned by {@code prefixComputer} (typically {@code
 *       encodeForState(currentKg, currentKey, stateName)} — i.e. the kg+K+state portion that every
 *       map entry shares as a byte prefix).
 * </ol>
 *
 * @param <UK> user key type
 * @param <UV> user value type
 */
@Internal
public class ForStRsMapState<UK, UV> implements MapState<UK, UV> {

    private static final long serialVersionUID = 1L;

    /** Initial buffer size for key/value serialization (grows on demand). */
    private static final int DEFAULT_OUTPUT_BUFFER = 64;

    private static final int MAP_WRITE_BUFFER_THRESHOLD = 524288;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final byte[] keyPrefix;
    private final Supplier<byte[]> prefixComputer;
    private final Function<UK, byte[]> compositeKeyComputer;
    private final TypeSerializer<UK> keySerializer;
    private final TypeSerializer<UV> valueSerializer;

    private final DataOutputSerializer keyOutBuffer;
    private final DataOutputSerializer valueOutBuffer;
    private final DataInputDeserializer inputBuffer;

    // ------------------------------------------------------------------
    // Off-heap (Arrow) mode fields (1c.1). All null in legacy modes.
    // ------------------------------------------------------------------

    private final java.util.function.Supplier<MemorySegment> scratchArenaSupplier;
    private final org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer<Object>
            kgSerializerOffheap;
    private final byte[] stateNameBytesOffheap;
    private final java.util.function.IntSupplier keyGroupSupplier;
    private final java.util.function.Supplier<Object> keySupplier;
    private final ArrowBinaryBuffer statebuf;
    private final ArrowBinaryBufferAutoTuner tuner;
    private final MemorySegmentDataInputView offheapInputView;
    private final MemorySegmentDataOutputView offheapOutputView;

    /**
     * SP6 Phase 6.3 — off-heap value staging.
     *
     * <p>The on-heap {@code Map<ByteArrayKey, byte[]>} writeCache used to allocate two byte[]s per
     * put (one for the value payload returned by {@code DataOutputSerializer.getCopyOfBuffer()} and
     * one for the composite key). On Nexmark Q3 with ~500K puts per slot the value-payload alloc
     * was the dominant heap pressure source.
     *
     * <p>The new path: per-put, the value is serialized DIRECTLY into a reusable off-heap {@code
     * valueStaging} segment via {@link MemorySegmentDataOutputView}. The writeCache stores a tiny
     * {@link OffHeapSlice} record (24 bytes total) holding the byte offset + length into the
     * staging segment instead of the byte[] payload. Reads from the writeCache deserialize directly
     * off-heap via {@link MemorySegmentDataInputView}.
     *
     * <p>The composite-key byte[] is still allocated (for {@link ByteArrayKey} HashMap lookup);
     * eliminating it would require a primitive-keyed hashtable + off-heap-aware equals. Deferred to
     * a follow-up cut.
     */
    private final Map<ByteArrayKey, OffHeapSlice> writeCache = new HashMap<>();

    private final Map<ByteArrayKey, byte[]> readCache = new HashMap<>(256);
    private int writeCacheCount = 0;

    /** Lazily-allocated. Released on close(). */
    private Arena offHeapArena;

    private MemorySegment valueStaging;
    private int valueStagingPos;
    private int valueStagingCap;
    private MemorySegmentDataOutputView valueOutputView;
    private MemorySegmentDataInputView valueInputView;

    /** Initial value-staging size; grows on demand (with bulk copy preserving prior offsets). */
    private static final int VALUE_STAGING_INITIAL = 64 * 1024;

    /** Tiny record holding an off-heap slice (offset, length) into {@link #valueStaging}. */
    static final class OffHeapSlice {
        final int offset;
        final int length;

        OffHeapSlice(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }

    static final class ByteArrayKey {
        final byte[] bytes;
        private final int hash;

        ByteArrayKey(byte[] bytes) {
            this.bytes = bytes;
            this.hash = java.util.Arrays.hashCode(bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ByteArrayKey k && java.util.Arrays.equals(bytes, k.bytes);
        }
    }

    /** Legacy byte[]-prefix constructor. */
    public ForStRsMapState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            byte[] keyPrefix,
            TypeSerializer<UK> keySerializer,
            TypeSerializer<UV> valueSerializer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = keyPrefix.clone();
        this.prefixComputer = null;
        this.compositeKeyComputer = null;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.valueOutBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
        this.scratchArenaSupplier = null;
        this.kgSerializerOffheap = null;
        this.stateNameBytesOffheap = null;
        this.keyGroupSupplier = null;
        this.keySupplier = null;
        this.statebuf = null;
        this.tuner = null;
        this.offheapInputView = null;
        this.offheapOutputView = null;
    }

    /**
     * Spec §6 kg-prefixed constructor. {@code prefixComputer} returns the per-state byte prefix
     * (kg+K+stateName/) shared by every map entry; {@code compositeKeyComputer} maps a user key to
     * the full composite ForSt key (the prefix bytes followed by serialize(UK)).
     */
    public ForStRsMapState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            TypeSerializer<UK> keySerializer,
            TypeSerializer<UV> valueSerializer,
            Supplier<byte[]> prefixComputer,
            Function<UK, byte[]> compositeKeyComputer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = null;
        this.prefixComputer = prefixComputer;
        this.compositeKeyComputer = compositeKeyComputer;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.valueOutBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
        this.scratchArenaSupplier = null;
        this.kgSerializerOffheap = null;
        this.stateNameBytesOffheap = null;
        this.keyGroupSupplier = null;
        this.keySupplier = null;
        this.statebuf = null;
        this.tuner = null;
        this.offheapInputView = null;
        this.offheapOutputView = null;
    }

    /**
     * Off-heap (Arrow) mode constructor (1c.1) — zero byte[] allocation on the hot path for
     * {@link #get(Object) get} / {@link #put(Object, Object) put} / {@link #remove(Object)
     * remove} / {@link #contains(Object) contains}.
     *
     * <p>Composite keys (kg+K+stateName+SEP+mapKey) are encoded into a per-thread scratch
     * {@link MemorySegment} per call via {@link
     * org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer#encodeForMapOffheap};
     * values are stored in {@code statebuf}'s off-heap Arrow regions; native fall-through uses
     * {@link ForStRsLinker#getPinnedSegment} / {@link ForStRsLinker#deleteSegment} with the same
     * scratch segment.
     *
     * <p>Iterators ({@link #entries}, {@link #keys}, {@link #values}, {@link #isEmpty}) and
     * {@link #clear()} use an iter-no-flush merge: they walk live statebuf rows for entries
     * whose composite-key begins with this map's prefix (statebuf takes precedence — newer
     * writes), then walk the engine for the remaining rows. This avoids the per-iter
     * batchPut stall that regressed Q19 by ~60% in an earlier 1c.1 attempt.
     */
    @SuppressWarnings("unchecked")
    public ForStRsMapState(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            TypeSerializer<UK> keySerializer,
            TypeSerializer<UV> valueSerializer,
            java.util.function.Supplier<MemorySegment> scratchArenaSupplier,
            org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer<?> kgSerializer,
            String stateName,
            java.util.function.IntSupplier keyGroupSupplier,
            java.util.function.Supplier<Object> keySupplier,
            ArrowBinaryBuffer statebuf,
            ArrowBinaryBufferAutoTuner tuner,
            Supplier<byte[]> prefixComputer) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyPrefix = null;
        // prefixComputer is still needed by iterators/clear/isEmpty (legacy paths) so we keep
        // it even in off-heap mode. The compositeKeyComputer is null because the off-heap path
        // builds the composite key directly into the scratch MemorySegment.
        this.prefixComputer = prefixComputer;
        this.compositeKeyComputer = null;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.valueOutBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
        this.scratchArenaSupplier = scratchArenaSupplier;
        this.kgSerializerOffheap =
                (org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer<Object>)
                        kgSerializer;
        this.stateNameBytesOffheap =
                stateName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        this.keyGroupSupplier = keyGroupSupplier;
        this.keySupplier = keySupplier;
        this.statebuf = statebuf;
        this.tuner = tuner;
        this.offheapInputView = new MemorySegmentDataInputView();
        this.offheapOutputView = new MemorySegmentDataOutputView();
    }

    @Override
    public UV get(UK key) throws IOException {
        if (statebuf != null) {
            // 1c.1 off-heap mode — ZERO byte[] allocation on the hot path.
            MemorySegment scratch = scratchArenaSupplier.get();
            long encoded =
                    kgSerializerOffheap.encodeForMapOffheap(
                            keyGroupSupplier.getAsInt(),
                            keySupplier.get(),
                            stateNameBytesOffheap,
                            keySerializer,
                            key,
                            scratch,
                            0L);
            int keyOff = (int) (encoded >>> 32);
            int keyLen = (int) (encoded & 0xFFFFFFFFL);
            int row = statebuf.find(scratch, keyOff, keyLen);
            tuner.observeRead(row >= 0, statebuf.size(), statebuf.capacity());
            if (row >= 0) {
                offheapInputView.rewind(
                        statebuf.valueDataSegment(),
                        statebuf.valueOffsetOf(row),
                        statebuf.valueLengthOf(row));
                return valueSerializer.deserialize(offheapInputView);
            }
            // Miss → native getPinnedSegment, writing result into scratch after the key.
            int valOff = keyOff + keyLen;
            int valMax = (int) (scratch.byteSize() - valOff);
            int valLen =
                    linker.getPinnedSegment(
                            db, cf, scratch, keyOff, keyLen, scratch, valOff, valMax);
            if (valLen < 0) {
                return null;
            }
            offheapInputView.rewind(scratch, valOff, valLen);
            return valueSerializer.deserialize(offheapInputView);
        }
        byte[] compositeKey = composite(key);
        ByteArrayKey cacheKey = new ByteArrayKey(compositeKey);
        OffHeapSlice cached = writeCache.get(cacheKey);
        if (cached != null) {
            // Off-heap fast path: deserialize directly from valueStaging segment.
            valueInputView.rewind(valueStaging, cached.offset, cached.length);
            return valueSerializer.deserialize(valueInputView);
        }
        byte[] heapCached = readCache.get(cacheKey);
        if (heapCached != null) {
            inputBuffer.setBuffer(heapCached);
            return valueSerializer.deserialize(inputBuffer);
        }
        byte[] raw = linker.getFast(db, cf, compositeKey);
        if (raw == null) {
            return null;
        }
        readCache.put(cacheKey, raw);
        inputBuffer.setBuffer(raw);
        return valueSerializer.deserialize(inputBuffer);
    }

    @Override
    public void put(UK key, UV value) throws IOException {
        if (statebuf != null) {
            // 1c.1 off-heap mode — ZERO byte[] allocation on the hot path.
            MemorySegment scratch = scratchArenaSupplier.get();
            long encoded =
                    kgSerializerOffheap.encodeForMapOffheap(
                            keyGroupSupplier.getAsInt(),
                            keySupplier.get(),
                            stateNameBytesOffheap,
                            keySerializer,
                            key,
                            scratch,
                            0L);
            int keyOff = (int) (encoded >>> 32);
            int keyLen = (int) (encoded & 0xFFFFFFFFL);
            int valOff = keyOff + keyLen;
            offheapOutputView.reset(scratch, valOff);
            valueSerializer.serialize(value, offheapOutputView);
            int valLen = offheapOutputView.position() - valOff;
            // Drain to engine before insert if the buffer is at capacity (forced) or has
            // reached the high-water mark (opportunistic). Same pattern as 1b.3 ValueState.
            if (statebuf.needsFlush() || statebuf.shouldAutoFlush()) {
                statebuf.flushTo(linker, db, cf);
            }
            int row = statebuf.insert(scratch, keyOff, keyLen, scratch, valOff, valLen);
            if (row == ArrowBinaryBuffer.INSERT_NEEDS_FLUSH) {
                // AutoTuner refused to grow (small-WS workload). Drain + retry.
                statebuf.flushTo(linker, db, cf);
                statebuf.insert(scratch, keyOff, keyLen, scratch, valOff, valLen);
            }
            return;
        }
        ensureOffHeapStaging();
        // Serialize value DIRECTLY into the off-heap staging region. No byte[] payload alloc
        // (vs the prior `valueOutBuffer.getCopyOfBuffer()` path which alloc'd one byte[] per put).
        int startOffset = valueStagingPos;
        valueOutputView.reset(valueStaging, startOffset);
        try {
            valueSerializer.serialize(value, valueOutputView);
        } catch (IOException overflow) {
            // Off-heap staging too small. Grow + retry once (preserves offsets via bulk copy).
            growValueStaging();
            valueOutputView.reset(valueStaging, startOffset);
            valueSerializer.serialize(value, valueOutputView);
        }
        int len = valueOutputView.position() - startOffset;
        valueStagingPos += len;

        byte[] compositeKey = composite(key);
        writeCache.put(new ByteArrayKey(compositeKey), new OffHeapSlice(startOffset, len));
        writeCacheCount++;
        if (writeCacheCount >= MAP_WRITE_BUFFER_THRESHOLD) {
            flushMapWriteCache();
        }
    }

    private void ensureOffHeapStaging() {
        // A-C4R5-H2 + D-R5-H3: consult stateClosed BEFORE allocating. Pre-fix
        // a concurrent close() racing with mailbox put() could observe
        // offHeapArena=null after close set stateClosed=true, re-allocate a
        // fresh shared arena, and end up with both stateClosed=true AND a
        // non-null offHeapArena — close() is idempotent and won't release it,
        // so the freshly-allocated arena leaks for the JVM lifetime. Fail
        // loud instead so the offending caller surfaces rather than silently
        // leaking. The check + assignment are NOT atomic on their own; the
        // backend's lifecycle invariant + the volatile semantics on
        // stateClosed below give us the needed happens-before edge.
        if (stateClosed) {
            throw new IllegalStateException("ForStRsMapState already closed");
        }
        if (offHeapArena == null) {
            // D-R4-H1: SHARED arena (not confined). Reverted from the cycle-3 ofConfined
            // attempt because the per-instance offHeapArena outlives the call that creates
            // it — close() runs from {@link
            // org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend#close()} which
            // Flink may invoke from a thread OTHER than the keyed-state mailbox (Task
            // canceler / disposer / executor service). Arena.ofConfined.close() throws
            // {@link WrongThreadException} from a non-owner thread; the previous code's
            // best-effort `catch (Throwable ignore)` then SILENTLY LEAKED the entire
            // valueStaging segment (multi-MB after grows) for the JVM lifetime — a real
            // native-memory leak on every job cancel/recovery. ofShared's close-handshake
            // is paid once per state-instance lifetime — irrelevant compared to the put/get
            // hot path.
            // D-R4-H3: assign `offHeapArena` LAST so a partial init (allocate throw,
            // Thread.interrupt between Arena.ofShared() and allocate) doesn't leave the
            // field non-null pointing at an arena with no backing segment.
            Arena tmp = Arena.ofShared();
            MemorySegment seg;
            try {
                seg = tmp.allocate(VALUE_STAGING_INITIAL);
            } catch (Throwable t) {
                tmp.close();
                throw t;
            }
            valueStaging = seg;
            valueStagingCap = VALUE_STAGING_INITIAL;
            valueOutputView = new MemorySegmentDataOutputView();
            valueInputView = new MemorySegmentDataInputView();
            offHeapArena = tmp;
        }
    }

    private void growValueStaging() {
        // D-R3-H2 + D-R4-H2: rotate the arena so the prior (now-orphaned) segment is
        // actually freed. Pre-fix every grow leaked the previous allocation until
        // close() (which didn't even exist). Lifecycle:
        //   1) allocate a fresh arena + segment at the new cap (try/catch so a
        //      throw from allocate/copy closes the new arena instead of leaking it
        //      — D-R4-H2)
        //   2) bulk-copy live bytes (offsets are stable — OffHeapSlice holds
        //      (offset, length), not segment refs, so all in-flight slices remain
        //      valid against the new valueStaging)
        //   3) reassign valueStaging, then close the old arena
        // ofShared mirrors ensureOffHeapStaging (see D-R4-H1 reasoning).
        int newCap = Math.max(valueStagingCap * 2, valueStagingCap + VALUE_STAGING_INITIAL);
        Arena newArena = Arena.ofShared();
        MemorySegment grown;
        try {
            grown = newArena.allocate(newCap);
            MemorySegment.copy(valueStaging, 0L, grown, 0L, valueStagingPos);
        } catch (Throwable t) {
            newArena.close();
            throw t;
        }
        Arena oldArena = offHeapArena;
        valueStaging = grown;
        valueStagingCap = newCap;
        offHeapArena = newArena;
        oldArena.close();
    }

    // D-R4-H4 + D-R5-H3 + A-C4R5-H3: track explicit closed state so post-close put/get
    // from a stale state reference fails loud instead of NPE'ing on valueStaging=null
    // or silently re-allocating a new arena that's never cleaned up. Volatile so a
    // close()-thread store happens-before a mailbox-thread ensureOffHeapStaging read
    // — without it the JMM allows the put-path to never observe the close.
    private volatile boolean stateClosed = false;

    /**
     * D-R3-H1: release the off-heap arena. Invoked from {@link
     * org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend#close()} when the backend
     * disposes each registered MapState. Idempotent.
     */
    public void close() {
        if (stateClosed) {
            return;
        }
        stateClosed = true;
        if (offHeapArena != null) {
            try {
                offHeapArena.close();
            } catch (Throwable ignore) {
                // best-effort — already-closed or pinned-segment failures are
                // dispatcher-side issues; backend close() must continue.
            }
            offHeapArena = null;
            valueStaging = null;
            valueStagingCap = 0;
            valueStagingPos = 0;
        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return;
        }
        for (Map.Entry<UK, UV> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void remove(UK key) {
        if (statebuf != null) {
            // 1c.1 off-heap mode — drop from buffer + issue native delete.
            MemorySegment scratch = scratchArenaSupplier.get();
            long encoded =
                    kgSerializerOffheap.encodeForMapOffheap(
                            keyGroupSupplier.getAsInt(),
                            keySupplier.get(),
                            stateNameBytesOffheap,
                            keySerializer,
                            key,
                            scratch,
                            0L);
            int keyOff = (int) (encoded >>> 32);
            int keyLen = (int) (encoded & 0xFFFFFFFFL);
            statebuf.remove(scratch, keyOff, keyLen);
            linker.deleteSegment(db, cf, scratch, keyOff, keyLen);
            return;
        }
        byte[] compositeKey = composite(key);
        ByteArrayKey cacheKey = new ByteArrayKey(compositeKey);
        writeCache.remove(cacheKey);
        readCache.remove(cacheKey);
        // R0C-NEW-H1 Tier-2: segment FFI surface.
        linker.deleteSegment(
                db, cf, MemorySegment.ofArray(compositeKey), 0L, compositeKey.length);
    }

    @Override
    public boolean contains(UK key) throws IOException {
        if (statebuf != null) {
            // 1c.1 off-heap mode — buffer-first, then native existence probe.
            MemorySegment scratch = scratchArenaSupplier.get();
            long encoded =
                    kgSerializerOffheap.encodeForMapOffheap(
                            keyGroupSupplier.getAsInt(),
                            keySupplier.get(),
                            stateNameBytesOffheap,
                            keySerializer,
                            key,
                            scratch,
                            0L);
            int keyOff = (int) (encoded >>> 32);
            int keyLen = (int) (encoded & 0xFFFFFFFFL);
            int row = statebuf.find(scratch, keyOff, keyLen);
            tuner.observeRead(row >= 0, statebuf.size(), statebuf.capacity());
            if (row >= 0) {
                return true;
            }
            int valOff = keyOff + keyLen;
            int valMax = (int) (scratch.byteSize() - valOff);
            return linker.getPinnedSegment(db, cf, scratch, keyOff, keyLen, scratch, valOff, valMax)
                    >= 0;
        }
        byte[] compositeKey = composite(key);
        ByteArrayKey cacheKey = new ByteArrayKey(compositeKey);
        if (writeCache.containsKey(cacheKey)) {
            return true;
        }
        if (readCache.containsKey(cacheKey)) {
            return true;
        }
        return linker.getIntoBuf(db, cf, compositeKey) != null;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws IOException {
        List<Map.Entry<UK, UV>> out = new ArrayList<>();
        forEachEntry(
                (uk, uv) -> out.add(new AbstractMap.SimpleImmutableEntry<>(uk, uv)),
                /* loadValues= */ true);
        return out;
    }

    @Override
    public Iterable<UK> keys() throws IOException {
        List<UK> out = new ArrayList<>();
        forEachEntry((uk, ignored) -> out.add(uk), /* loadValues= */ false);
        return out;
    }

    @Override
    public Iterable<UV> values() throws IOException {
        List<UV> out = new ArrayList<>();
        forEachEntry((ignored, uv) -> out.add(uv), /* loadValues= */ true);
        return out;
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws IOException {
        return entries().iterator();
    }

    @Override
    public boolean isEmpty() throws IOException {
        if (statebuf != null) {
            // 1c.1 off-heap mode — check statebuf for any row matching this map's prefix WITHOUT
            // pre-flushing. If found we're done; otherwise consult engine.
            byte[] prefix = currentPrefix();
            if (statebufHasPrefix(prefix)) {
                return false;
            }
            // D-R3-H3: confined (operator-thread) arena. Shared arenas trigger
            // a global safepoint handshake on close — measurably slower and
            // unnecessary for a single-threaded prefix-iter lifetime.
            try (Arena arena = Arena.ofConfined();
                    FrsIterator iter = linker.prefixLookupOpen(db, cf, prefix, arena)) {
                return linker.iteratorNext(iter) == null;
            }
        }
        if (!writeCache.isEmpty()) {
            byte[] prefix = currentPrefix();
            for (ByteArrayKey k : writeCache.keySet()) {
                if (startsWith(k.bytes, prefix)) {
                    return false;
                }
            }
        }
        flushMapWriteCache();
        // D-R3-H3: confined arena (see isEmpty's 1c.1 site).
        try (Arena arena = Arena.ofConfined();
                FrsIterator iter = linker.prefixLookupOpen(db, cf, currentPrefix(), arena)) {
            return linker.iteratorNext(iter) == null;
        }
    }

    @Override
    public void clear() {
        if (statebuf != null) {
            // 1c.1 off-heap mode — tombstone all statebuf rows matching this map's prefix, then
            // delete any engine-resident keys via the legacy iterator+delete loop. No flush of
            // pending writes — the tombstones cover them.
            byte[] prefix = currentPrefix();
            int[] liveRows = statebuf.liveRows();
            MemorySegment kd = statebuf.keyDataSegment();
            for (int row : liveRows) {
                int kOff = statebuf.keyOffsetOf(row);
                int kLen = statebuf.keyLengthOf(row);
                if (segmentStartsWith(kd, kOff, kLen, prefix)) {
                    statebuf.tombstoneRow(row);
                }
            }
            // FRS-V1-VEC (2026-06-01): enumerate engine-resident keys via the chunked vectorized
            // drain (N keys per FFM crossing) and delete them in a SINGLE vectorizedBatchDelete
            // crossing — replaces the per-entry iteratorNext loop + per-key deleteSegment loop
            // (2N crossings) with O(N/chunk) read crossings + 1 batched delete.
            List<byte[]> compositeKeys = new ArrayList<>();
            try {
                forEachEngineEntryVectorized(prefix, false, (k, v) -> compositeKeys.add(k));
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to enumerate MapState engine keys during clear()", e);
            }
            batchDeleteCompositeKeys(compositeKeys);
            return;
        }
        flushMapWriteCache();
        byte[] prefix = currentPrefix();
        writeCache.keySet().removeIf(k -> startsWith(k.bytes, prefix));
        readCache.keySet().removeIf(k -> startsWith(k.bytes, prefix));
        // FRS-V1-VEC: chunked vectorized enumerate + single batched delete (see off-heap branch).
        List<byte[]> compositeKeys = new ArrayList<>();
        try {
            forEachEngineEntryVectorized(prefix, false, (k, v) -> compositeKeys.add(k));
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to enumerate MapState engine keys during clear()", e);
        }
        batchDeleteCompositeKeys(compositeKeys);
    }

    /**
     * Returns true if any live statebuf row's key begins with {@code prefix}. O(liveRows).
     */
    private boolean statebufHasPrefix(byte[] prefix) {
        if (statebuf == null || statebuf.size() == 0) {
            return false;
        }
        int[] liveRows = statebuf.liveRows();
        MemorySegment kd = statebuf.keyDataSegment();
        for (int row : liveRows) {
            int kOff = statebuf.keyOffsetOf(row);
            int kLen = statebuf.keyLengthOf(row);
            if (segmentStartsWith(kd, kOff, kLen, prefix)) {
                return true;
            }
        }
        return false;
    }

    private static boolean segmentStartsWith(
            MemorySegment seg, int segOff, int segLen, byte[] prefix) {
        if (segLen < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (seg.get(ValueLayout.JAVA_BYTE, (long) (segOff + i)) != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private void forEachEntry(EntryVisitor<UK, UV> visitor, boolean loadValues) throws IOException {
        if (statebuf != null) {
            // 1c.1 off-heap iter-no-flush: merge statebuf with engine WITHOUT pre-flushing.
            //
            //   1) Walk statebuf — emit rows whose composite-key begins with this map's prefix.
            //      Record the (mapKey bytes) in a Set so engine rows for the same userKey are
            //      shadowed by the (newer) statebuf entry.
            //   2) Walk engine via prefixLookupOpen — emit entries whose mapKey is NOT in seen.
            //
            // Avoids the per-iter batchPut stall observed in Q19 (60% regression on first 1c.1).
            byte[] prefix = currentPrefix();
            Set<ByteArrayKey> seenMapKeys = new HashSet<>();
            int[] liveRows = statebuf.liveRows();
            MemorySegment kd = statebuf.keyDataSegment();
            MemorySegment vd = statebuf.valueDataSegment();
            for (int row : liveRows) {
                int kOff = statebuf.keyOffsetOf(row);
                int kLen = statebuf.keyLengthOf(row);
                if (!segmentStartsWith(kd, kOff, kLen, prefix)) {
                    continue;
                }
                int mapKeyOff = kOff + prefix.length;
                int mapKeyLen = kLen - prefix.length;
                // Materialize mapKey bytes for the dedup set + UK deserialization. This is a
                // small alloc (mapKeyLen bytes per row) but is bounded by statebuf size and
                // happens only during iter — not on the hot put/get path.
                byte[] mapKeyBytes = new byte[mapKeyLen];
                MemorySegment.copy(
                        kd, ValueLayout.JAVA_BYTE, mapKeyOff, mapKeyBytes, 0, mapKeyLen);
                seenMapKeys.add(new ByteArrayKey(mapKeyBytes));
                inputBuffer.setBuffer(mapKeyBytes, 0, mapKeyLen);
                UK uk = keySerializer.deserialize(inputBuffer);
                UV uv = null;
                if (loadValues) {
                    int vOff = statebuf.valueOffsetOf(row);
                    int vLen = statebuf.valueLengthOf(row);
                    byte[] vBytes = new byte[vLen];
                    MemorySegment.copy(vd, ValueLayout.JAVA_BYTE, vOff, vBytes, 0, vLen);
                    inputBuffer.setBuffer(vBytes);
                    uv = valueSerializer.deserialize(inputBuffer);
                }
                visitor.accept(uk, uv);
            }
            // FRS-V1-VEC: chunked/vectorized engine drain (was a per-entry iteratorNext
            // loop), with the statebuf-shadow dedup applied per row.
            forEachEngineEntryVectorized(
                    prefix,
                    loadValues,
                    (composite, value) -> {
                        if (composite.length < prefix.length) {
                            throw new IOException(
                                    "Encountered composite key shorter than prefix during MapState scan");
                        }
                        int mapKeyLen = composite.length - prefix.length;
                        byte[] mapKeyBytes = new byte[mapKeyLen];
                        System.arraycopy(composite, prefix.length, mapKeyBytes, 0, mapKeyLen);
                        if (seenMapKeys.contains(new ByteArrayKey(mapKeyBytes))) {
                            return; // shadowed by a newer statebuf entry
                        }
                        inputBuffer.setBuffer(mapKeyBytes, 0, mapKeyLen);
                        UK uk = keySerializer.deserialize(inputBuffer);
                        UV uv = null;
                        if (loadValues) {
                            inputBuffer.setBuffer(value);
                            uv = valueSerializer.deserialize(inputBuffer);
                        }
                        visitor.accept(uk, uv);
                    });
            return;
        }
        flushMapWriteCache();
        byte[] prefix = currentPrefix();
        // FRS-V1-VEC: chunked/vectorized engine drain (was a per-entry iteratorNext loop).
        forEachEngineEntryVectorized(
                prefix,
                loadValues,
                (composite, value) -> {
                    if (composite.length < prefix.length) {
                        throw new IOException(
                                "Encountered composite key shorter than prefix during MapState scan");
                    }
                    inputBuffer.setBuffer(
                            composite, prefix.length, composite.length - prefix.length);
                    UK uk = keySerializer.deserialize(inputBuffer);
                    UV uv = null;
                    if (loadValues) {
                        inputBuffer.setBuffer(value);
                        uv = valueSerializer.deserialize(inputBuffer);
                    }
                    visitor.accept(uk, uv);
                });
    }

    /**
     * FRS-V1-VEC (2026-06-01): VECTORIZED engine prefix scan for the V1-sync MapState
     * iteration path. Replaces the per-entry {@code linker.iteratorNext} loop (ONE FFM
     * crossing + ONE heap byte[] alloc per entry) with the chunked
     * {@code frsVecIterPrefixOpen/Next} drain that the V2 path already uses: each FFM
     * crossing returns up to a 64 KiB chunk of rows — layout per row
     * {@code [u32 klen LE][u32 vlen LE][key bytes][value bytes]} — cutting crossings
     * from O(entries) to O(entries / chunk). The Flink operator still invokes the
     * iteration once per record (the sync operator model is immutable), but the
     * per-ENTRY FFM crossings WITHIN one scan are now batched/vectorized. Row bytes are
     * copied out of {@code chunkBuf} before the next {@code _next} overwrites it, so no
     * snapshot arena is needed. {@code rowVisitor} receives (compositeKeyBytes,
     * valueBytes-or-null).
     */
    @FunctionalInterface
    private interface RawRowConsumer {
        void accept(byte[] compositeKey, byte[] value) throws IOException;
    }

    private void forEachEngineEntryVectorized(
            byte[] prefix, boolean loadValues, RawRowConsumer rowVisitor) throws IOException {
        final int CHUNK_CAP = 64 * 1024;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment chunkBuf = arena.allocate(CHUNK_CAP);
            MemorySegment prefixSeg = arena.allocate(Math.max(1, prefix.length));
            if (prefix.length > 0) {
                MemorySegment.copy(prefix, 0, prefixSeg, ValueLayout.JAVA_BYTE, 0, prefix.length);
            }
            MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment outRowCount = arena.allocate(ValueLayout.JAVA_INT);
            MemorySegment outBytesUsed = arena.allocate(ValueLayout.JAVA_INT);
            int rc =
                    linker.frsVecIterPrefixOpen(
                            db.handle(),
                            cf.handle(),
                            prefixSeg,
                            prefix.length,
                            chunkBuf,
                            CHUNK_CAP,
                            outHandle,
                            outRowCount,
                            outBytesUsed);
            if (rc != 0) {
                throw new IOException("frs_vec_iter_prefix_open rc=" + rc);
            }
            long handle = outHandle.get(ValueLayout.JAVA_LONG, 0);
            try {
                int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                while (true) {
                    int off = 0;
                    for (int i = 0; i < rowCount; i++) {
                        int klen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
                        off += 4;
                        int vlen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
                        off += 4;
                        byte[] k = new byte[klen];
                        MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, off, k, 0, klen);
                        off += klen;
                        byte[] v = null;
                        if (loadValues) {
                            v = new byte[vlen];
                            MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, off, v, 0, vlen);
                        }
                        off += vlen;
                        rowVisitor.accept(k, v);
                    }
                    if (rowCount == 0) {
                        break;
                    }
                    rc =
                            linker.frsVecIterPrefixNext(
                                    handle, chunkBuf, CHUNK_CAP, outRowCount, outBytesUsed);
                    if (rc != 0) {
                        throw new IOException("frs_vec_iter_prefix_next rc=" + rc);
                    }
                    rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                }
            } finally {
                linker.frsVecIterPrefixClose(handle);
            }
        }
    }

    /**
     * Returns the per-state byte prefix shared by every map entry. In legacy mode this is the
     * construction-time {@code keyPrefix}; in kg-mode this is invoked dynamically.
     */
    private byte[] currentPrefix() {
        if (prefixComputer != null) {
            return prefixComputer.get();
        }
        return keyPrefix;
    }

    /**
     * Returns the composite ForSt key for {@code userKey}: in legacy mode {@code keyPrefix ||
     * serialize(userKey)}; in kg-mode {@code compositeKeyComputer.apply(userKey)} (which the
     * keyed-state backend wires to {@code ForStRsKeyGroupedSerializer.encodeForMap}).
     */
    private byte[] composite(UK userKey) {
        if (compositeKeyComputer != null) {
            return compositeKeyComputer.apply(userKey);
        }
        keyOutBuffer.clear();
        try {
            keySerializer.serialize(userKey, keyOutBuffer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize MapState user key", e);
        }
        // PR-E4: copy straight from the serializer's shared buffer into the composite key
        // — saves the intermediate byte[] that getCopyOfBuffer() would allocate. The final
        // {@code full} array still needs to be owned (it's stashed in the writeCache).
        int keyLen = keyOutBuffer.length();
        byte[] full = new byte[keyPrefix.length + keyLen];
        System.arraycopy(keyPrefix, 0, full, 0, keyPrefix.length);
        System.arraycopy(keyOutBuffer.getSharedBuffer(), 0, full, keyPrefix.length, keyLen);
        return full;
    }

    @FunctionalInterface
    private interface EntryVisitor<UK, UV> {
        void accept(UK uk, UV uv) throws IOException;
    }

    /** Returns an immutable empty iterable; used for documentation parity. */
    @SuppressWarnings("unused")
    private static <X> Iterable<X> emptyIterable() {
        return Collections.emptyList();
    }

    private static boolean startsWith(byte[] data, byte[] prefix) {
        if (data.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (data[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private void flushMapWriteCache() {
        // 1c.1: in off-heap mode, drain the per-instance ArrowBinaryBuffer to the engine. The
        // legacy writeCache is always empty in off-heap mode so the rest of this method is a no-op.
        if (statebuf != null && statebuf.size() > 0) {
            statebuf.flushTo(linker, db, cf);
        }
        if (writeCache.isEmpty()) {
            return;
        }
        int count = writeCache.size();
        // R0C-NEW-H2 (Tier 6): direct off-heap Arrow-style staging + vectorizedBatchPut.
        // Pre-fix built `byte[][] keys`/`byte[][] values` and called legacy
        // `linker.batchPut(byte[][])` which internally did per-row `Arena.allocate`
        // + `MemorySegment.copy` + pointer-array sets. Post-fix: ONE off-heap
        // contiguous stage + one FFM call, zero per-row pointer materialization.
        // Values are already in `valueStaging` at sparse `slice.offset` positions
        // — compacted into the contiguous `valData` segment via off-heap → off-heap
        // copy. Keys live in heap `ByteArrayKey.bytes` (intrinsic source) and copy
        // heap → off-heap once into `keyData`.
        long totalKeyBytes = 0L;
        long totalValBytes = 0L;
        for (Map.Entry<ByteArrayKey, OffHeapSlice> entry : writeCache.entrySet()) {
            totalKeyBytes += entry.getKey().bytes.length;
            totalValBytes += entry.getValue().length;
        }
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keyOffsets =
                    local.allocate((long) (count + 1) * Integer.BYTES, 4L);
            MemorySegment keyData = local.allocate(Math.max(1L, totalKeyBytes));
            MemorySegment valOffsets =
                    local.allocate((long) (count + 1) * Integer.BYTES, 4L);
            MemorySegment valData = local.allocate(Math.max(1L, totalValBytes));
            keyOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
            valOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
            long kPos = 0L;
            long vPos = 0L;
            int idx = 0;
            for (Map.Entry<ByteArrayKey, OffHeapSlice> entry : writeCache.entrySet()) {
                byte[] kBytes = entry.getKey().bytes;
                OffHeapSlice slice = entry.getValue();
                if (kBytes.length > 0) {
                    MemorySegment.copy(
                            kBytes, 0, keyData, ValueLayout.JAVA_BYTE, kPos, kBytes.length);
                }
                kPos += kBytes.length;
                // valueStaging → valData direct off-heap copy (compaction).
                MemorySegment.copy(valueStaging, slice.offset, valData, vPos, slice.length);
                vPos += slice.length;
                idx++;
                keyOffsets.set(
                        ValueLayout.JAVA_INT, (long) idx * Integer.BYTES, (int) kPos);
                valOffsets.set(
                        ValueLayout.JAVA_INT, (long) idx * Integer.BYTES, (int) vPos);
            }
            linker.vectorizedBatchPut(
                    db, cf, keyOffsets, keyData, valOffsets, valData, count);
        }
        writeCache.clear();
        writeCacheCount = 0;
        // Reset off-heap staging cursor for the next batch — writeCache offsets are all gone.
        valueStagingPos = 0;
    }

    /**
     * FRS-V1-VEC (2026-06-01): delete a batch of engine-resident composite keys in a SINGLE FFM
     * crossing via {@link ForStRsLinker#vectorizedBatchDelete}, instead of the prior per-key
     * {@code linker.delete(...)} / {@code deleteSegment(...)} loop (one crossing per key). Keys are
     * staged once into a contiguous off-heap Arrow-style {@code [keyOffsets][keyData]} layout —
     * mirrors {@link #flushMapWriteCache}'s {@code vectorizedBatchPut} staging — so the whole
     * delete set crosses the boundary once. No-op on an empty list.
     */
    private void batchDeleteCompositeKeys(List<byte[]> keys) {
        int count = keys.size();
        if (count == 0) {
            return;
        }
        long totalKeyBytes = 0L;
        for (byte[] k : keys) {
            totalKeyBytes += k.length;
        }
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keyOffsets = local.allocate((long) (count + 1) * Integer.BYTES, 4L);
            MemorySegment keyData = local.allocate(Math.max(1L, totalKeyBytes));
            keyOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
            long kPos = 0L;
            int idx = 0;
            for (byte[] k : keys) {
                if (k.length > 0) {
                    MemorySegment.copy(k, 0, keyData, ValueLayout.JAVA_BYTE, kPos, k.length);
                }
                kPos += k.length;
                idx++;
                keyOffsets.set(ValueLayout.JAVA_INT, (long) idx * Integer.BYTES, (int) kPos);
            }
            linker.vectorizedBatchDelete(db, cf, keyOffsets, keyData, count);
        }
    }

    public void flush() {
        flushMapWriteCache();
        readCache.clear();
    }

    /**
     * 1c.1: drains the off-heap buffer to the engine and clears it. Called by the owning backend
     * on snapshot/close. No-op in legacy modes (where {@code statebuf} is null).
     */
    public void flushStateBuffer() {
        if (statebuf != null && statebuf.size() > 0) {
            statebuf.flushTo(linker, db, cf);
        }
    }
}

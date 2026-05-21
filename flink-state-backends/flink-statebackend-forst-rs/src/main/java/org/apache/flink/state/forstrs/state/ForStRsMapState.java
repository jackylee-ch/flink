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
        if (offHeapArena == null) {
            offHeapArena = Arena.ofShared();
            valueStagingCap = VALUE_STAGING_INITIAL;
            valueStaging = offHeapArena.allocate(valueStagingCap);
            valueOutputView = new MemorySegmentDataOutputView();
            valueInputView = new MemorySegmentDataInputView();
        }
    }

    private void growValueStaging() {
        int newCap = Math.max(valueStagingCap * 2, valueStagingCap + VALUE_STAGING_INITIAL);
        MemorySegment grown = offHeapArena.allocate(newCap);
        MemorySegment.copy(valueStaging, 0L, grown, 0L, valueStagingPos);
        valueStaging = grown;
        valueStagingCap = newCap;
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
        linker.delete(db, cf, compositeKey);
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
            try (Arena arena = Arena.ofShared();
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
        try (Arena arena = Arena.ofShared();
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
            List<byte[]> compositeKeys = new ArrayList<>();
            try (Arena arena = Arena.ofShared();
                    FrsIterator iter = linker.prefixLookupOpen(db, cf, prefix, arena)) {
                ForStRsLinker.IteratorEntry entry;
                while ((entry = linker.iteratorNext(iter)) != null) {
                    compositeKeys.add(entry.key());
                }
            }
            for (byte[] k : compositeKeys) {
                linker.delete(db, cf, k);
            }
            return;
        }
        flushMapWriteCache();
        byte[] prefix = currentPrefix();
        writeCache.keySet().removeIf(k -> startsWith(k.bytes, prefix));
        readCache.keySet().removeIf(k -> startsWith(k.bytes, prefix));
        List<byte[]> compositeKeys = new ArrayList<>();
        try (Arena arena = Arena.ofShared();
                FrsIterator iter = linker.prefixLookupOpen(db, cf, prefix, arena)) {
            ForStRsLinker.IteratorEntry entry;
            while ((entry = linker.iteratorNext(iter)) != null) {
                compositeKeys.add(entry.key());
            }
        }
        for (byte[] k : compositeKeys) {
            linker.delete(db, cf, k);
        }
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
            try (Arena arena = Arena.ofShared();
                    FrsIterator iter = linker.prefixLookupOpen(db, cf, prefix, arena)) {
                ForStRsLinker.IteratorEntry entry;
                while ((entry = linker.iteratorNext(iter)) != null) {
                    byte[] composite = entry.key();
                    if (composite.length < prefix.length) {
                        throw new IOException(
                                "Encountered composite key shorter than prefix during MapState scan");
                    }
                    int mapKeyLen = composite.length - prefix.length;
                    byte[] mapKeyBytes = new byte[mapKeyLen];
                    System.arraycopy(composite, prefix.length, mapKeyBytes, 0, mapKeyLen);
                    if (seenMapKeys.contains(new ByteArrayKey(mapKeyBytes))) {
                        continue;
                    }
                    inputBuffer.setBuffer(mapKeyBytes, 0, mapKeyLen);
                    UK uk = keySerializer.deserialize(inputBuffer);
                    UV uv = null;
                    if (loadValues) {
                        inputBuffer.setBuffer(entry.value());
                        uv = valueSerializer.deserialize(inputBuffer);
                    }
                    visitor.accept(uk, uv);
                }
            }
            return;
        }
        flushMapWriteCache();
        byte[] prefix = currentPrefix();
        try (Arena arena = Arena.ofShared();
                FrsIterator iter = linker.prefixLookupOpen(db, cf, prefix, arena)) {
            ForStRsLinker.IteratorEntry entry;
            while ((entry = linker.iteratorNext(iter)) != null) {
                byte[] composite = entry.key();
                if (composite.length < prefix.length) {
                    throw new IOException(
                            "Encountered composite key shorter than prefix during MapState scan");
                }
                inputBuffer.setBuffer(composite, prefix.length, composite.length - prefix.length);
                UK uk = keySerializer.deserialize(inputBuffer);
                UV uv = null;
                if (loadValues) {
                    inputBuffer.setBuffer(entry.value());
                    uv = valueSerializer.deserialize(inputBuffer);
                }
                visitor.accept(uk, uv);
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
        byte[] keyBytes = keyOutBuffer.getCopyOfBuffer();
        byte[] full = new byte[keyPrefix.length + keyBytes.length];
        System.arraycopy(keyPrefix, 0, full, 0, keyPrefix.length);
        System.arraycopy(keyBytes, 0, full, keyPrefix.length, keyBytes.length);
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
        byte[][] keys = new byte[count][];
        byte[][] values = new byte[count][];
        int i = 0;
        for (Map.Entry<ByteArrayKey, OffHeapSlice> entry : writeCache.entrySet()) {
            keys[i] = entry.getKey().bytes;
            // Extract from off-heap staging into a heap byte[] for the legacy batchPut FFI.
            // Future optimization (SP6 Phase 6.3.2): stage keys+values directly into a
            // ColumnarBatchBuffer pair and call linker.vectorizedBatchPut to avoid this copy.
            OffHeapSlice slice = entry.getValue();
            byte[] v = new byte[slice.length];
            MemorySegment.copy(
                    valueStaging,
                    java.lang.foreign.ValueLayout.JAVA_BYTE,
                    slice.offset,
                    v,
                    0,
                    slice.length);
            values[i] = v;
            i++;
        }
        linker.batchPut(db, cf, keys, values);
        writeCache.clear();
        writeCacheCount = 0;
        // Reset off-heap staging cursor for the next batch — writeCache offsets are all gone.
        valueStagingPos = 0;
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

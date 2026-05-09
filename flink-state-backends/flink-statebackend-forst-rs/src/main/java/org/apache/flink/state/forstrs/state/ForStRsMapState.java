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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Minimal {@link MapState} implementation backed by ForSt-RS via the {@link ForStRsLinker} FFM
 * bridge.
 *
 * <p>This is a Phase-D L4 stepping stone: it demonstrates the keyed-map-state bridging pattern
 * end-to-end (composite-key encoding → put/get/delete + prefix-scan enumeration) without yet
 * plugging into Flink's {@code AbstractKeyedStateBackend} composition. A real keyed-state binding
 * would derive the per-record key from the operator's {@code KeyContext} (key + namespace) and
 * concat with the state-id prefix; here we accept a fixed prefix at construction time and treat
 * it as the map's identity prefix. That is sufficient for proof-of-concept enumeration semantics
 * and unit testing the FFM contract, but not for production-quality keyed-state.
 *
 * <p><b>Storage encoding</b>: each map entry is stored under a composite ForSt key
 * <pre>
 *   composite_key = keyPrefix || serialize(UK)
 * </pre>
 * with the entry value being {@code serialize(UV)}. The construction-time {@code keyPrefix} acts
 * as the namespace for the map — every entry shares it and {@link #entries()} / {@link #keys()} /
 * {@link #values()} / {@link #iterator()} / {@link #isEmpty()} / {@link #clear()} are implemented
 * via {@link ForStRsLinker#prefixLookupOpen} bounded to {@code keyPrefix}. To recover the user
 * key on iteration we strip the leading {@code keyPrefix.length} bytes from the composite key
 * before deserializing — this is correct as long as no other state shares the same prefix.
 *
 * <p><b>Concurrency</b>: there is no transactional guarantee across multi-key operations; if the
 * caller mutates the map concurrently with an in-flight iteration the results are unspecified —
 * the same caveat the underlying ForSt-RS iterator exposes.
 *
 * @param <UK> user key type
 * @param <UV> user value type
 */
@Internal
public class ForStRsMapState<UK, UV> implements MapState<UK, UV> {

    private static final long serialVersionUID = 1L;

    /** Initial buffer size for key/value serialization (grows on demand). */
    private static final int DEFAULT_OUTPUT_BUFFER = 64;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final byte[] keyPrefix;
    private final TypeSerializer<UK> keySerializer;
    private final TypeSerializer<UV> valueSerializer;

    private final DataOutputSerializer keyOutBuffer;
    private final DataOutputSerializer valueOutBuffer;
    private final DataInputDeserializer inputBuffer;

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
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyOutBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.valueOutBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
        this.inputBuffer = new DataInputDeserializer();
    }

    @Override
    public UV get(UK key) throws IOException {
        byte[] raw = linker.lookupKv(db, cf, composite(key));
        if (raw == null) {
            return null;
        }
        inputBuffer.setBuffer(raw);
        return valueSerializer.deserialize(inputBuffer);
    }

    @Override
    public void put(UK key, UV value) throws IOException {
        valueOutBuffer.clear();
        valueSerializer.serialize(value, valueOutBuffer);
        byte[] payload = valueOutBuffer.getCopyOfBuffer();
        linker.put(db, cf, composite(key), payload);
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
        linker.delete(db, cf, composite(key));
    }

    @Override
    public boolean contains(UK key) throws IOException {
        return linker.lookupKv(db, cf, composite(key)) != null;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws IOException {
        // Materialize eagerly: the underlying ForSt-RS iterator must be closed before we hand a
        // result back to the caller, and the entries() contract returns an Iterable that may be
        // walked multiple times. A future revision can swap this for a streaming view that
        // owns/releases the iterator on its own lifecycle.
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
        try (Arena arena = Arena.ofShared();
                FrsIterator iter = linker.prefixLookupOpen(db, cf, keyPrefix, arena)) {
            return linker.iteratorNext(iter) == null;
        }
    }

    @Override
    public void clear() {
        // Two-phase: collect composite keys under the prefix, then delete each. Holding the
        // iterator open while issuing deletes is unsafe in general (snapshot vs mutation
        // semantics) so we materialize first.
        List<byte[]> compositeKeys = new ArrayList<>();
        try (Arena arena = Arena.ofShared();
                FrsIterator iter = linker.prefixLookupOpen(db, cf, keyPrefix, arena)) {
            ForStRsLinker.IteratorEntry entry;
            while ((entry = linker.iteratorNext(iter)) != null) {
                compositeKeys.add(entry.key());
            }
        }
        for (byte[] k : compositeKeys) {
            linker.delete(db, cf, k);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * Common iteration helper. Walks every entry whose composite key starts with {@link
     * #keyPrefix}, decodes the user key, optionally decodes the user value, and invokes the
     * visitor. The visitor receives {@code null} for {@code uv} when {@code loadValues} is false,
     * which lets the {@link #keys()} path skip a deserialization round-trip.
     */
    private void forEachEntry(EntryVisitor<UK, UV> visitor, boolean loadValues) throws IOException {
        try (Arena arena = Arena.ofShared();
                FrsIterator iter = linker.prefixLookupOpen(db, cf, keyPrefix, arena)) {
            ForStRsLinker.IteratorEntry entry;
            while ((entry = linker.iteratorNext(iter)) != null) {
                byte[] composite = entry.key();
                if (composite.length < keyPrefix.length) {
                    throw new IOException(
                            "Encountered composite key shorter than prefix during MapState scan");
                }
                inputBuffer.setBuffer(
                        composite, keyPrefix.length, composite.length - keyPrefix.length);
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
     * Returns the composite ForSt key for {@code userKey}: {@code keyPrefix || serialize(userKey)}.
     */
    private byte[] composite(UK userKey) {
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
}

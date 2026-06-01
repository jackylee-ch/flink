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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.runtime.state.v2.ttl.TtlStateFactory.TtlSerializer;
import org.apache.flink.state.forstrs.IteratorEntryView;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Root-cause probe for task #499: MapState iter {@code deserializeUserKey} EOFException under TTL on
 * S3. Builds a {@link ForStRsMapStateV2} whose VALUE serializer is the runtime {@link TtlSerializer}
 * (exactly what {@code TtlStateFactory.createMapState} hands to {@code createStateInternal} on the
 * TTL-delegation path), writes a handful of composite MapState keys directly into a live in-memory
 * engine, then drives a real vectorized prefix-scan over them and decodes the user keys through the
 * same {@code deserializeUserKey(IteratorEntryView, prefixLen)} path the production iterator uses.
 *
 * <p>If the key encoding were TTL-dependent (hypothesis a), the round-trip would throw EOF here.
 */
class MapStateV2TtlIterRoundTripTest {

    private Arena linkerArena;
    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;

    @BeforeEach
    void setUp() {
        linkerArena = Arena.ofShared();
        linker = new ForStRsLinker(linkerArena);
        db = linker.dbOpenMemory(linkerArena);
        cf = linker.dbDefaultCf(db, linkerArena);
    }

    @AfterEach
    void tearDown() {
        cf.close();
        db.close();
        linkerArena.close();
    }

    /** Mirrors ForStRsMapStateV2.serializeKey for a MAP_PUT, VoidNamespace, String UK. */
    private static byte[] compositeKey(long opKey, String stateName, String userKey)
            throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.write("k/".getBytes(StandardCharsets.UTF_8));
        LongSerializer.INSTANCE.serialize(opKey, out);
        out.write("/".getBytes(StandardCharsets.UTF_8));
        out.write(stateName.getBytes(StandardCharsets.UTF_8));
        out.write("/".getBytes(StandardCharsets.UTF_8));
        // VoidNamespace -> skipped (matches serializeKey's !(ns instanceof VoidNamespace) guard).
        StringSerializer.INSTANCE.serialize(userKey, out);
        return out.getCopyOfBuffer();
    }

    /** Iter prefix = composite key WITHOUT the trailing user key. */
    private static byte[] iterPrefix(long opKey, String stateName) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.write("k/".getBytes(StandardCharsets.UTF_8));
        LongSerializer.INSTANCE.serialize(opKey, out);
        out.write("/".getBytes(StandardCharsets.UTF_8));
        out.write(stateName.getBytes(StandardCharsets.UTF_8));
        out.write("/".getBytes(StandardCharsets.UTF_8));
        return out.getCopyOfBuffer();
    }

    private static byte[] ttlValueBytes(String userValue, long ts) throws Exception {
        // CompositeSerializer<TtlValue> layout: [long ts][UV].
        TtlSerializer<String> ttl =
                new TtlSerializer<>(LongSerializer.INSTANCE, StringSerializer.INSTANCE);
        DataOutputSerializer out = new DataOutputSerializer(32);
        ttl.serialize(new TtlValue<>(userValue, ts), out);
        return out.getCopyOfBuffer();
    }

    @Test
    void ttlWrappedMapStateUserKeysRoundTripThroughIter() throws Exception {
        long opKey = 42L;
        String stateName = "myMap";

        // The raw MapStateV2 the TtlStateFactory builds: UK = String, UV = TtlValue<String>
        // via the runtime TtlSerializer. linker/db/cf null -> we drive serializeKey/deserialize
        // directly; the engine round-trip is done via the linker below.
        @SuppressWarnings("unchecked")
        TypeSerializer<TtlValue<String>> ttlSer =
                (TypeSerializer<TtlValue<String>>)
                        (TypeSerializer<?>)
                                new TtlSerializer<>(
                                        LongSerializer.INSTANCE, StringSerializer.INSTANCE);
        ForStRsMapStateV2<Long, VoidNamespace, String, TtlValue<String>> state =
                new ForStRsMapStateV2<>(
                        null,
                        stateName,
                        LongSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        ttlSer);

        Set<String> expectedKeys = new HashSet<>();
        String[] userKeys = {"alpha", "bravo", "charlie", "delta", ""};
        for (int i = 0; i < userKeys.length; i++) {
            byte[] k = compositeKey(opKey, stateName, userKeys[i]);
            byte[] v = ttlValueBytes("val-" + i, 1000L + i);
            linker.put(db, cf, k, v);
            expectedKeys.add(userKeys[i]);
        }

        byte[] prefix = iterPrefix(opKey, stateName);
        int prefixLen = prefix.length;

        // Drive a real vectorized prefix scan, decode each user key through the production path.
        List<String> decoded = new ArrayList<>();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment chunkBuf = arena.allocate(64 * 1024);
            MemorySegment outHandle = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment outRowCount = arena.allocate(ValueLayout.JAVA_INT);
            MemorySegment outBytesUsed = arena.allocate(ValueLayout.JAVA_INT);
            MemorySegment prefixSeg = arena.allocate(prefix.length);
            MemorySegment.copy(prefix, 0, prefixSeg, ValueLayout.JAVA_BYTE, 0, prefix.length);

            int rc =
                    linker.frsVecIterPrefixOpen(
                            db.handle(),
                            cf.handle(),
                            prefixSeg,
                            prefix.length,
                            chunkBuf,
                            64 * 1024,
                            outHandle,
                            outRowCount,
                            outBytesUsed);
            assertEquals(0, rc, "frs_vec_iter_prefix_open rc");
            long handle = outHandle.get(ValueLayout.JAVA_LONG, 0);

            int total = 0;
            int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
            int bytesUsed = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
            decodeChunk(chunkBuf, rowCount, bytesUsed, prefixLen, state, decoded, arena);
            total += rowCount;
            while (true) {
                rc =
                        linker.frsVecIterPrefixNext(
                                handle, chunkBuf, 64 * 1024, outRowCount, outBytesUsed);
                assertEquals(0, rc, "frs_vec_iter_prefix_next rc");
                rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                if (rowCount == 0) {
                    break;
                }
                bytesUsed = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
                decodeChunk(chunkBuf, rowCount, bytesUsed, prefixLen, state, decoded, arena);
                total += rowCount;
            }
            linker.frsVecIterPrefixClose(handle);
            assertEquals(userKeys.length, total, "row count");
        }

        assertEquals(expectedKeys.size(), new HashSet<>(decoded).size(), "distinct decoded keys");
        for (String uk : userKeys) {
            assertTrue(decoded.contains(uk), "user key round-trips: '" + uk + "'");
        }
    }

    private static void decodeChunk(
            MemorySegment chunkBuf,
            int rowCount,
            int bytesUsed,
            int prefixLen,
            ForStRsMapStateV2<Long, VoidNamespace, String, TtlValue<String>> state,
            List<String> out,
            Arena arena) {
        MemorySegment snap = arena.allocate(bytesUsed);
        MemorySegment.copy(chunkBuf, 0, snap, 0, bytesUsed);
        int off = 0;
        for (int i = 0; i < rowCount; i++) {
            int klen = snap.get(ValueLayout.JAVA_INT_UNALIGNED, off);
            off += 4;
            int vlen = snap.get(ValueLayout.JAVA_INT_UNALIGNED, off);
            off += 4;
            int keyOff = off;
            off += klen;
            int valOff = off;
            off += vlen;
            IteratorEntryView view = new IteratorEntryView(snap, keyOff, klen, valOff, vlen);
            String uk = state.deserializeUserKey(view, prefixLen);
            out.add(uk);
        }
    }
}

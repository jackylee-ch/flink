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

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.state.forstrs.cache.MapStateCache;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V2-violation V2 integration test for the slice-based hot path of {@link ForStRsMapStateV2}.
 *
 * <p>Pre-fix, every async MapState op allocated a fresh {@code byte[]} via {@link
 * DataOutputSerializer#getCopyOfBuffer()} for the composite key. Post-fix, the hot path writes the
 * composite into the per-state shared {@code keyOut} buffer and passes a (buf, off, len) slice to
 * the cache + off-heap buffer APIs. This test pins three properties of that slice path:
 *
 * <ol>
 *   <li><b>Correctness under buffer reuse</b>: 10K interleaved put/get/contains/remove operations
 *       using a SHARED {@code DataOutputSerializer} (the same backing byte[] the production hot
 *       path reuses across rows) produce identical results to a heap-{@code HashMap} oracle.
 *   <li><b>Snapshot drain durability</b>: after the operations, calling {@link
 *       MapStateArrowBuffer#flushTo} (the snapshot pre-hook contract) lands every PUT in the
 *       engine + propagates every tombstone as a native delete. This catches a class of bug where
 *       a slice-based PUT might write the WRONG key bytes if the shared buffer was clobbered
 *       between staging and drain — the previous BulkFlushHandler refactor exhibited exactly this
 *       kind of cross-row corruption under load.
 *   <li><b>Cache + buffer + engine consistency post-drain</b>: after the drain, the cache holds
 *       authoritative values for keys that were put; the buffer is empty; the engine has each
 *       PUT'd key with the value the oracle predicted.
 * </ol>
 *
 * <p>This test does NOT spin up a full {@link
 * org.apache.flink.runtime.asyncprocessing.AsyncExecutionController} — the AEC's construction
 * surface (mailbox + executor) is impractical in a unit test, and the production hot path's only
 * direct interactions with the AEC are {@code getCurrentContext().getKey() / getNamespace()},
 * both of which are exercised by {@code MapStateV2TtlIterRoundTripTest} and Nexmark. Instead it
 * binds the contract at the slice boundary that the new {@link MapStateCache#put(byte[], int,
 * int, Object)} / {@link MapStateArrowBuffer#putShared} APIs expose — the actual API surface the
 * refactor introduces.
 */
class MapStateV2SliceHotPathIntegrationTest {

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

    /**
     * Writes a composite key into the shared {@link DataOutputSerializer} mirroring the layout
     * produced by {@code ForStRsMapStateV2.serializeMapEntryKeyShared}:
     * {@code KEY_PREFIX(=k/) || operatorKey || / || stateName || / || namespace || userKey}.
     *
     * <p>Returns the byte length the caller should read from {@code out.getSharedBuffer()}.
     */
    private static int writeComposite(
            DataOutputSerializer out,
            long operatorKey,
            byte[] stateNameBytes,
            int namespace,
            long userKey)
            throws Exception {
        out.clear();
        out.writeByte('k');
        out.writeByte('/');
        out.writeLong(operatorKey);
        out.writeByte('/');
        out.write(stateNameBytes);
        out.writeByte('/');
        out.writeInt(namespace);
        out.writeLong(userKey);
        return out.length();
    }

    private static byte[] valueBytes(long v) {
        return new byte[] {
            (byte) (v >>> 56), (byte) (v >>> 48), (byte) (v >>> 40), (byte) (v >>> 32),
            (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v
        };
    }

    @Test
    void interleavedSliceOpsAgreeWithHeapOracleAndPersistAcrossDrain() throws Exception {
        // -- Shared per-state buffers (mirror MapStateV2.keyOut + valueOut layout) --
        DataOutputSerializer keyOut = new DataOutputSerializer(128);
        byte[] stateName = "myMap".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        MapStateCache<Long> cache = new MapStateCache<>();
        MapStateArrowBuffer buffer = new MapStateArrowBuffer();
        Map<Long, Long> oracle = new HashMap<>(); // userKey → value (null = removed)

        try {
            final long operatorKey = 7L;
            final int namespace = 99;

            // -- 10K interleaved ops --
            // The seed is fixed for reproducibility; mix in tombstones + revisits so the cache and
            // buffer paths both fire repeatedly on the same composite key (the shared buffer is
            // reused across every op).
            Random rng = new Random(1234567L);
            final int ops = 10_000;
            for (int i = 0; i < ops; i++) {
                long userKey = rng.nextInt(256); // small key space → many revisits
                int op = rng.nextInt(4); // 0=put, 1=get, 2=remove, 3=contains
                int keyLen = writeComposite(keyOut, operatorKey, stateName, namespace, userKey);
                byte[] keyBuf = keyOut.getSharedBuffer();
                switch (op) {
                    case 0: // PUT
                    {
                        long value = rng.nextLong();
                        cache.put(keyBuf, 0, keyLen, value);
                        byte[] valBytes = valueBytes(value);
                        buffer.putShared(
                                keyBuf,
                                0,
                                keyLen,
                                valBytes,
                                0,
                                valBytes.length,
                                linker,
                                db,
                                cf);
                        oracle.put(userKey, value);
                        break;
                    }
                    case 1: // GET (cache + buffer probe, no engine round-trip in this test)
                    {
                        MapStateCache.Lookup<Long> hit = cache.lookup(keyBuf, 0, keyLen);
                        if (hit != null && hit.cached()) {
                            assertEquals(
                                    oracle.get(userKey),
                                    hit.value(),
                                    "cache hit must equal oracle for key " + userKey);
                            break;
                        }
                        MapStateArrowBuffer.Lookup bufHit = buffer.lookup(keyBuf, 0, keyLen);
                        if (bufHit.cached) {
                            if (bufHit.tombstone) {
                                assertNull(
                                        oracle.get(userKey),
                                        "buffer tombstone must align with oracle null for key "
                                                + userKey);
                            } else {
                                // Buffer hit on a live row — value bytes are off-heap. Just
                                // assert the oracle agrees (we don't decode here; the value path
                                // is exercised by MapStateV2ArrowBufferParityTest).
                                assertNotNull(
                                        oracle.get(userKey),
                                        "buffer live hit must align with oracle non-null for key "
                                                + userKey);
                            }
                        }
                        break;
                    }
                    case 2: // REMOVE
                    {
                        cache.remove(keyBuf, 0, keyLen);
                        buffer.remove(keyBuf, 0, keyLen, linker, db, cf);
                        oracle.put(userKey, null);
                        break;
                    }
                    case 3: // CONTAINS (cache + buffer probe)
                    {
                        MapStateCache.Lookup<Long> hit = cache.lookup(keyBuf, 0, keyLen);
                        if (hit != null && hit.cached()) {
                            assertEquals(
                                    oracle.get(userKey) != null,
                                    hit.value() != null,
                                    "cache contains must equal oracle for key " + userKey);
                            break;
                        }
                        MapStateArrowBuffer.Lookup bufHit = buffer.lookup(keyBuf, 0, keyLen);
                        if (bufHit.cached) {
                            assertEquals(
                                    oracle.get(userKey) != null,
                                    !bufHit.tombstone,
                                    "buffer contains must equal oracle for key " + userKey);
                        }
                        break;
                    }
                    default:
                        throw new AssertionError(op);
                }
            }

            // -- Snapshot drain --
            // Mirrors what ForStRsAsyncKeyedStateBackend.snapshot() does via its Trace E
            // pre-hook: flushOffHeapBuffer for every registered MapStateV2. After this, the
            // engine must hold every still-live PUT and have applied every tombstone as a
            // native delete.
            buffer.flushTo(linker, db, cf);

            // -- Post-drain invariants --
            assertEquals(0, buffer.underlying().size(), "buffer empty after flushTo");
            assertEquals(0, buffer.tombstoneCount(), "tombstones drained after flushTo");

            // Verify EVERY oracle-tracked key against the engine: live keys must round-trip
            // their value bytes; removed keys must return null.
            int verifiedLive = 0;
            int verifiedRemoved = 0;
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                long userKey = e.getKey();
                Long expected = e.getValue();
                int keyLen = writeComposite(keyOut, operatorKey, stateName, namespace, userKey);
                byte[] probe = java.util.Arrays.copyOf(keyOut.getSharedBuffer(), keyLen);
                byte[] got = linker.getFast(db, cf, probe);
                if (expected == null) {
                    assertNull(
                            got,
                            "engine must return null for removed key " + userKey + " post-drain");
                    verifiedRemoved++;
                } else {
                    assertNotNull(got, "engine must hold value for key " + userKey + " post-drain");
                    assertArrayEquals(
                            valueBytes(expected),
                            got,
                            "engine must round-trip value bytes for key " + userKey);
                    verifiedLive++;
                }
            }
            // Sanity: both branches exercised given the workload mix.
            assertTrue(verifiedLive > 0, "expected at least one live key after 10K ops");
            assertTrue(verifiedRemoved > 0, "expected at least one removed key after 10K ops");
        } finally {
            cache.close();
            buffer.close();
        }
    }

    /**
     * Pins the byte-format invariant: a composite key produced by writing into a shared
     * {@link DataOutputSerializer} via {@code writeComposite} and consumed by the slice variants
     * is byte-identical to the same key produced by taking a {@link
     * DataOutputSerializer#getCopyOfBuffer() copy} and passing the resulting array. If the slice
     * variants ever read past {@code keyLen} or mis-interpret the offset, this assertion fires.
     */
    @Test
    void sliceVariantsReadIdenticalBytesAsCopyOfBufferVariants() throws Exception {
        DataOutputSerializer keyOut = new DataOutputSerializer(128);
        byte[] stateName = "myMap".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        MapStateCache<String> cache = new MapStateCache<>();
        MapStateArrowBuffer buffer = new MapStateArrowBuffer();
        try {
            // Stage some entries using the slice variant.
            for (long uk = 0; uk < 32; uk++) {
                int keyLen = writeComposite(keyOut, 1L, stateName, 0, uk);
                cache.put(keyOut.getSharedBuffer(), 0, keyLen, "v" + uk);
                buffer.putShared(
                        keyOut.getSharedBuffer(),
                        0,
                        keyLen,
                        valueBytes(uk),
                        0,
                        8,
                        linker,
                        db,
                        cf);
            }
            // Probe using the full-array path (constructs a fresh copy each call, mirroring the
            // legacy serializeMapEntryKey path).
            for (long uk = 0; uk < 32; uk++) {
                int keyLen = writeComposite(keyOut, 1L, stateName, 0, uk);
                byte[] copy = java.util.Arrays.copyOf(keyOut.getSharedBuffer(), keyLen);
                MapStateCache.Lookup<String> hit = cache.lookup(copy);
                assertNotNull(hit, "full-array lookup must hit slice-staged entry uk=" + uk);
                assertEquals("v" + uk, hit.value());

                MapStateArrowBuffer.Lookup bufHit = buffer.lookup(copy);
                assertTrue(bufHit.cached, "buffer full-array lookup must hit slice-staged uk=" + uk);
                assertFalse(bufHit.tombstone);
            }
            // And vice-versa: stage via full-array on a fresh state, probe via slice.
            MapStateCache<String> cache2 = new MapStateCache<>();
            try {
                for (long uk = 0; uk < 32; uk++) {
                    int keyLen = writeComposite(keyOut, 2L, stateName, 0, uk);
                    byte[] copy = java.util.Arrays.copyOf(keyOut.getSharedBuffer(), keyLen);
                    cache2.put(copy, "w" + uk);
                }
                for (long uk = 0; uk < 32; uk++) {
                    int keyLen = writeComposite(keyOut, 2L, stateName, 0, uk);
                    MapStateCache.Lookup<String> hit =
                            cache2.lookup(keyOut.getSharedBuffer(), 0, keyLen);
                    assertNotNull(hit, "slice lookup must hit full-array-staged entry uk=" + uk);
                    assertEquals("w" + uk, hit.value());
                }
            } finally {
                cache2.close();
            }
        } finally {
            cache.close();
            buffer.close();
        }
    }

    /**
     * Catches the BulkFlushHandler-style cross-row corruption regression: write the composite key
     * for userKey=A, capture the slice (off=0, len=N), then IMMEDIATELY write the composite key
     * for userKey=B into the SAME buffer (overwriting A's bytes). The slice handed to {@code
     * cache.put(buf, 0, N, ...)} must have been consumed synchronously — i.e. the cache must have
     * stored A's value under A's key bytes, not B's bytes that now occupy the buffer.
     *
     * <p>If the slice variant's {@code appendKey} ever lazily holds a reference to the source
     * byte[] instead of memcpy'ing into off-heap, this test fails because A's stored key now
     * reads as B's bytes after the overwrite.
     */
    @Test
    void slicePathConsumesSourceBufferSynchronouslySoConcurrentOverwriteIsSafe() throws Exception {
        DataOutputSerializer keyOut = new DataOutputSerializer(128);
        byte[] stateName = "myMap".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        MapStateCache<String> cache = new MapStateCache<>();
        try {
            // Stage userKey=10 via slice, then OVERWRITE the shared buffer with userKey=20.
            int len10 = writeComposite(keyOut, 1L, stateName, 0, 10L);
            cache.put(keyOut.getSharedBuffer(), 0, len10, "ten");

            // OVERWRITE the buffer with a fresh composite that shares NO bytes past the prefix.
            int len20 = writeComposite(keyOut, 1L, stateName, 0, 20L);
            cache.put(keyOut.getSharedBuffer(), 0, len20, "twenty");

            // Now probe BOTH original keys via fresh copies. If the slice variant held a lazy
            // reference to the source byte[] instead of copying eagerly, the userKey=10 row's
            // stored bytes would now be userKey=20's bytes — i.e. the lookup for the userKey=10
            // composite would MISS, and the lookup for the userKey=20 composite would return
            // "ten" or "twenty" depending on hash collisions. We assert the eager-copy contract.
            int probeLen10 = writeComposite(keyOut, 1L, stateName, 0, 10L);
            byte[] probe10 = java.util.Arrays.copyOf(keyOut.getSharedBuffer(), probeLen10);
            MapStateCache.Lookup<String> hit10 = cache.lookup(probe10);
            assertNotNull(hit10, "userKey=10 must still be retrievable post-overwrite");
            assertEquals(
                    "ten",
                    hit10.value(),
                    "userKey=10 must hold 'ten' even though the source byte[] was overwritten by"
                            + " the userKey=20 put");

            int probeLen20 = writeComposite(keyOut, 1L, stateName, 0, 20L);
            byte[] probe20 = java.util.Arrays.copyOf(keyOut.getSharedBuffer(), probeLen20);
            MapStateCache.Lookup<String> hit20 = cache.lookup(probe20);
            assertNotNull(hit20);
            assertEquals("twenty", hit20.value());
        } finally {
            cache.close();
        }
    }
}

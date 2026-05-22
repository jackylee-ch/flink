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

import org.apache.flink.state.forstrs.cache.MapStateCache;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * PR-A6 (S1-11 / E2-HIGH-2): verifies that after a {@code MapState.asyncClear()} for one
 * (operatorKey, namespace), the per-instance LRU {@link MapStateCache} and off-heap
 * {@link MapStateArrowBuffer} are invalidated for that prefix, and a subsequent read no longer
 * returns the pre-clear cached value.
 *
 * <p>The end-to-end framework path (StateExecutionController → ForStRsStateRequestClassifier →
 * VectorizedExecutor → ForStRsMapStateV2.buildDBPutRequest) is not exercisable in a unit test
 * without a full AEC harness; the existing {@link MapStateV2DispatchTest} suite is intentionally
 * structural for that reason. This test therefore exercises the two cache surfaces directly via
 * the new {@code clearForPrefix} hooks that {@code buildDBPutRequest} wires the CLEAR request to.
 *
 * <p>Together with {@code MapStateCacheTest#clearForPrefix*} (unit-level cache assertions) this
 * gives full coverage of the fix without requiring a live AsyncExecutionController.
 */
class MapStateV2CacheClearOnNamespaceTest {

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
     * Builds a synthetic composite key in the same layout that {@code
     * ForStRsMapStateV2.serializeMapEntryKey} produces:
     * {@code [KEY_PREFIX][operatorKey][/][stateName][/][namespace][userKey]}.
     */
    private static byte[] composite(int operatorKey, int namespace, int userKey) {
        byte[] out = new byte[18];
        out[0] = 'k';
        out[1] = '/';
        // operatorKey, 4 bytes
        out[2] = (byte) (operatorKey >>> 24);
        out[3] = (byte) (operatorKey >>> 16);
        out[4] = (byte) (operatorKey >>> 8);
        out[5] = (byte) operatorKey;
        out[6] = '/';
        out[7] = 'm';
        out[8] = '/';
        // namespace, 4 bytes
        out[9] = (byte) (namespace >>> 24);
        out[10] = (byte) (namespace >>> 16);
        out[11] = (byte) (namespace >>> 8);
        out[12] = (byte) namespace;
        // userKey, 5 bytes (avoid zero-byte collisions)
        out[13] = (byte) 0xA0;
        out[14] = (byte) (userKey >>> 24);
        out[15] = (byte) (userKey >>> 16);
        out[16] = (byte) (userKey >>> 8);
        out[17] = (byte) userKey;
        return out;
    }

    /** Iter prefix is everything up to and including the namespace component. */
    private static byte[] iterPrefix(int operatorKey, int namespace) {
        byte[] out = new byte[13];
        out[0] = 'k';
        out[1] = '/';
        out[2] = (byte) (operatorKey >>> 24);
        out[3] = (byte) (operatorKey >>> 16);
        out[4] = (byte) (operatorKey >>> 8);
        out[5] = (byte) operatorKey;
        out[6] = '/';
        out[7] = 'm';
        out[8] = '/';
        out[9] = (byte) (namespace >>> 24);
        out[10] = (byte) (namespace >>> 16);
        out[11] = (byte) (namespace >>> 8);
        out[12] = (byte) namespace;
        return out;
    }

    private static byte[] value(int v) {
        return new byte[] {(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24)};
    }

    // -----------------------------------------------------------------
    // LRU cache: targeted invalidation across namespaces.
    // -----------------------------------------------------------------

    @Test
    void cacheClearForNamespaceLeavesOtherNamespacesIntact() {
        MapStateCache<String> cache = new MapStateCache<>();

        // Populate cache for (operatorKey=1, namespace=10) — three userKeys.
        cache.put(composite(1, 10, 100), "v1-10-100");
        cache.put(composite(1, 10, 101), "v1-10-101");
        cache.put(composite(1, 10, 102), "v1-10-102");
        // Populate cache for (operatorKey=1, namespace=20) — one userKey.
        cache.put(composite(1, 20, 200), "v1-20-200");
        // Populate cache for (operatorKey=2, namespace=10) — same namespace, different operator.
        cache.put(composite(2, 10, 100), "v2-10-100");
        assertEquals(5, cache.size());

        // Simulate the asyncClear hook firing for (operatorKey=1, namespace=10).
        int removed = cache.clearForPrefix(iterPrefix(1, 10));
        assertEquals(3, removed, "must remove only the 3 entries owned by (op=1, ns=10)");
        assertEquals(2, cache.size());

        // Cleared rows must be gone — a subsequent lookup must MISS so the read falls through to
        // the engine (which has correctly applied the CLEAR's prefix-delete).
        assertNull(cache.lookup(composite(1, 10, 100)));
        assertNull(cache.lookup(composite(1, 10, 101)));
        assertNull(cache.lookup(composite(1, 10, 102)));

        // Other (operatorKey, namespace) pairs must still cache hit.
        MapStateCache.Lookup<String> otherNs = cache.lookup(composite(1, 20, 200));
        assertNotNull(otherNs);
        assertEquals("v1-20-200", otherNs.value());
        MapStateCache.Lookup<String> otherOp = cache.lookup(composite(2, 10, 100));
        assertNotNull(otherOp);
        assertEquals("v2-10-100", otherOp.value());
    }

    @Test
    void cacheClearForNamespaceAlsoRemovesTombstones() {
        // After PR-A6, a tombstone written under a cleared namespace must NOT survive — otherwise
        // asyncContains/asyncGet for the same userKey after CLEAR + re-PUT would still see the
        // stale "known missing" verdict.
        MapStateCache<String> cache = new MapStateCache<>();
        cache.remove(composite(1, 10, 100)); // tombstone
        cache.put(composite(1, 10, 101), "live");
        assertEquals(2, cache.size());

        cache.clearForPrefix(iterPrefix(1, 10));
        assertEquals(0, cache.size());
        assertNull(cache.lookup(composite(1, 10, 100)));
        assertNull(cache.lookup(composite(1, 10, 101)));
    }

    // -----------------------------------------------------------------
    // Off-heap arrow buffer: clearForPrefix flushes pending writes + drops in-buffer rows.
    // -----------------------------------------------------------------

    @Test
    void arrowBufferClearForPrefixFlushesPendingWritesAndEmptiesBuffer() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();

        // Stage writes under two namespaces.
        buf.put(composite(1, 10, 100), value(1), linker, db, cf);
        buf.put(composite(1, 10, 101), value(2), linker, db, cf);
        buf.put(composite(1, 20, 200), value(3), linker, db, cf);

        // Pre-clear: nothing in the engine yet (buffer hasn't flushed).
        // (We can't assert "absent" reliably because auto-flush could have fired for a low-cap
        // buffer; but with default capacity 3 rows definitely fit.)

        // Trigger the clearForPrefix hook for (op=1, ns=10). It must:
        //   1) flush pending PUTs to the engine (durability),
        //   2) drop all in-buffer rows + tombstones (so subsequent lookups MISS).
        buf.clearForPrefix(iterPrefix(1, 10), linker, db, cf);

        // Buffer must be empty.
        assertEquals(0, buf.underlying().size(), "buffer must be empty after clearForPrefix");
        assertEquals(0, buf.tombstoneCount(), "tombstone set must be empty after clearForPrefix");

        // The flushed-then-cleared rows ARE in the engine (the engine prefix-delete is the
        // framework's job after this hook returns — not this hook's). Verify durability of all
        // PUTs that were pending at clear time.
        assertNotNull(linker.getFast(db, cf, composite(1, 10, 100)));
        assertNotNull(linker.getFast(db, cf, composite(1, 10, 101)));
        assertNotNull(linker.getFast(db, cf, composite(1, 20, 200)));

        buf.close();
    }

    @Test
    void arrowBufferClearForPrefixFlushesTombstonesAsDeletes() {
        MapStateArrowBuffer buf = new MapStateArrowBuffer();

        // Seed engine with a row, then stage a remove (tombstone in buffer).
        buf.put(composite(1, 10, 100), value(1), linker, db, cf);
        buf.flushTo(linker, db, cf);
        assertNotNull(linker.getFast(db, cf, composite(1, 10, 100)));

        // Stage a tombstone.
        buf.remove(composite(1, 10, 100), linker, db, cf);
        assertEquals(1, buf.tombstoneCount());

        // clearForPrefix must propagate the tombstone as a native delete (via flushTo), then empty
        // the buffer.
        buf.clearForPrefix(iterPrefix(1, 10), linker, db, cf);
        assertEquals(0, buf.tombstoneCount());
        assertNull(
                linker.getFast(db, cf, composite(1, 10, 100)),
                "pending tombstone must propagate as a native delete on clearForPrefix");

        buf.close();
    }

    @Test
    void arrowBufferLookupMissesAfterClearForPrefix() {
        // Central S1-11 correctness assertion at the buffer surface: after the clear hook fires,
        // a buffer lookup for a previously-staged userKey under the cleared namespace must MISS
        // (i.e. fall through to engine), not return a stale staged value.
        MapStateArrowBuffer buf = new MapStateArrowBuffer();
        buf.put(composite(1, 10, 100), value(42), linker, db, cf);
        // Lookup before clear: hit.
        MapStateArrowBuffer.Lookup pre = buf.lookup(composite(1, 10, 100));
        org.junit.jupiter.api.Assertions.assertTrue(pre.cached, "pre-clear lookup must hit");

        buf.clearForPrefix(iterPrefix(1, 10), linker, db, cf);

        // Lookup after clear: miss — the framework will route to the engine on miss.
        MapStateArrowBuffer.Lookup post = buf.lookup(composite(1, 10, 100));
        org.junit.jupiter.api.Assertions.assertFalse(
                post.cached, "post-clear lookup must MISS so the read falls through to the engine");

        buf.close();
    }
}

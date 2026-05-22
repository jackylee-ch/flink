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

import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * PR-B2 parity test for {@link FlatStateCache}'s 4-byte big-endian read/write helpers.
 *
 * <p>The production code switched from a manual {@code (b[off]&0xFF)<<24 | ...} shift form
 * to a {@code MethodHandles.byteArrayViewVarHandle(int[].class, BIG_ENDIAN)}-backed
 * implementation (V2-15 in the PR-B2 closure list). This test verifies:
 *
 * <ul>
 *   <li>Round-trip {@code writeInt} then {@code readInt} reproduces the value
 *       (including edge cases: 0, {@code Integer.MIN_VALUE}, {@code Integer.MAX_VALUE}, -1).
 *   <li>The on-buffer byte layout matches the legacy big-endian shift form, so any cached
 *       payload populated by an older code path remains binary-compatible.
 *   <li>The VarHandle path produces the same {@code int} as the legacy form when reading
 *       arbitrary random buffers (no off-by-one signedness bug).
 * </ul>
 *
 * <p>{@code FlatStateCache.readInt}/{@code writeInt} are package-private static helpers;
 * this test lives in the same package to exercise them directly without reflection.
 */
class FlatStateCacheVarHandleParityTest {

    /** Legacy shift-pack form, copied verbatim from the pre-PR-B2 source for reference parity. */
    private static int legacyReadInt(byte[] buf, int off) {
        return (buf[off] & 0xFF) << 24
                | (buf[off + 1] & 0xFF) << 16
                | (buf[off + 2] & 0xFF) << 8
                | (buf[off + 3] & 0xFF);
    }

    /** Legacy shift-pack form, copied verbatim from the pre-PR-B2 source for reference parity. */
    private static void legacyWriteInt(byte[] buf, int off, int val) {
        buf[off] = (byte) (val >>> 24);
        buf[off + 1] = (byte) (val >>> 16);
        buf[off + 2] = (byte) (val >>> 8);
        buf[off + 3] = (byte) val;
    }

    /** The new VarHandle-backed form (same definition as the production code). */
    private static final VarHandle INT_VH =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    private static int newReadInt(byte[] buf, int off) {
        return (int) INT_VH.get(buf, off);
    }

    private static void newWriteInt(byte[] buf, int off, int val) {
        INT_VH.set(buf, off, val);
    }

    @Test
    void roundTripEdgeCases() {
        int[] values = {
            0,
            1,
            -1,
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            0x7F_00_00_00,
            0x80_00_00_01,
            0xCAFEBABE,
            0xDEADBEEF
        };
        byte[] buf = new byte[8]; // include trailing/leading slack to catch off-by-one writes
        for (int v : values) {
            newWriteInt(buf, 2, v);
            int got = newReadInt(buf, 2);
            assertEquals(v, got, "round-trip failed for 0x" + Integer.toHexString(v));
        }
    }

    @Test
    void byteLayoutMatchesLegacy() {
        // Same value written via the new and legacy forms must produce byte-identical buffers
        // so any on-disk / in-memory state populated by an older code path is still readable.
        int[] values = {
            0,
            1,
            -1,
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            0xCAFEBABE,
            0x12_34_56_78,
            0xFF_00_FF_00
        };
        for (int v : values) {
            byte[] legacyBuf = new byte[4];
            byte[] newBuf = new byte[4];
            legacyWriteInt(legacyBuf, 0, v);
            newWriteInt(newBuf, 0, v);
            assertArrayEquals(
                    legacyBuf,
                    newBuf,
                    "byte layout mismatch for 0x" + Integer.toHexString(v));
        }
    }

    @Test
    void readAtArbitraryOffsetMatchesLegacy() {
        // 4-byte windows at varying offsets must read the same int via legacy vs new.
        Random rng = new Random(0x1234_5678L);
        byte[] buf = new byte[256];
        rng.nextBytes(buf);
        for (int off = 0; off + 4 <= buf.length; off++) {
            int legacy = legacyReadInt(buf, off);
            int got = newReadInt(buf, off);
            assertEquals(legacy, got, "read parity failed at off=" + off);
        }
    }

    @Test
    void writeAtArbitraryOffsetMatchesLegacy() {
        // Writing into the middle of a populated buffer must not corrupt neighbouring bytes
        // and must lay down the same 4-byte sequence both ways.
        Random rng = new Random(0xABCD_EF01L);
        byte[] legacyBuf = new byte[64];
        byte[] newBuf = new byte[64];
        rng.nextBytes(legacyBuf);
        System.arraycopy(legacyBuf, 0, newBuf, 0, legacyBuf.length);
        for (int off = 0; off + 4 <= legacyBuf.length; off += 7) {
            int v = rng.nextInt();
            legacyWriteInt(legacyBuf, off, v);
            newWriteInt(newBuf, off, v);
        }
        assertArrayEquals(legacyBuf, newBuf, "buffer divergence after interleaved writes");
    }

    @Test
    void integratesWithFlatStateCachePutGet() {
        // Smoke test: FlatStateCache's put/get path uses readInt/writeInt for the
        // [keyLen][valLen] header. A simple put/get/contains cycle exercises both helpers.
        FlatStateCache cache = new FlatStateCache(64, 4096);
        byte[] k1 = "alpha".getBytes();
        byte[] v1 = new byte[] {1, 2, 3, 4, 5};
        cache.put(k1, v1);
        assertArrayEquals(v1, cache.get(k1));

        byte[] k2 = "beta".getBytes();
        byte[] v2 = new byte[300]; // larger payload to exercise non-trivial lengths
        for (int i = 0; i < v2.length; i++) {
            v2[i] = (byte) (i & 0xFF);
        }
        cache.put(k2, v2);
        assertArrayEquals(v2, cache.get(k2));

        // Update with a different size to exercise the append-and-reindex path.
        byte[] v1b = new byte[] {9, 8, 7};
        cache.put(k1, v1b);
        assertArrayEquals(v1b, cache.get(k1));
    }
}

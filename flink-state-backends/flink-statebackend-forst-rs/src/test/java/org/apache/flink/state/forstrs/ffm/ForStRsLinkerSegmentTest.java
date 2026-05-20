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

package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers the segment-based FFM signatures on {@link ForStRsLinker}: {@code getPinnedSegment},
 * {@code putSegment}, {@code deleteSegment}. These are the zero-byte[]-allocation entry points
 * used by V1-sync ValueState. Initial implementation routes through the legacy byte[] FFI
 * internally; the test simply verifies functional round-trip parity.
 */
class ForStRsLinkerSegmentTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static MemorySegment segOf(Arena arena, byte[] src) {
        MemorySegment seg = arena.allocate(src.length);
        for (int i = 0; i < src.length; i++) {
            seg.set(ValueLayout.JAVA_BYTE, i, src[i]);
        }
        return seg;
    }

    @Test
    void putAndGetViaSegmentsRoundTrip() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = utf8("segkey-1");
                byte[] value = utf8("segment-value-payload");

                MemorySegment keySeg = segOf(arena, key);
                MemorySegment valSeg = segOf(arena, value);

                linker.putSegment(db, cf, keySeg, 0L, key.length, valSeg, 0L, value.length);

                MemorySegment outSeg = arena.allocate(256);
                int len =
                        linker.getPinnedSegment(
                                db, cf, keySeg, 0L, key.length, outSeg, 0L, 256);

                assertEquals(value.length, len);
                byte[] got = new byte[len];
                MemorySegment.copy(outSeg, ValueLayout.JAVA_BYTE, 0L, got, 0, len);
                assertArrayEquals(value, got);
            }
        }
    }

    @Test
    void getSegmentMissReturnsNegativeOne() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = utf8("nope");
                MemorySegment keySeg = segOf(arena, key);
                MemorySegment outSeg = arena.allocate(128);

                int len =
                        linker.getPinnedSegment(
                                db, cf, keySeg, 0L, key.length, outSeg, 0L, 128);
                assertEquals(-1, len);
            }
        }
    }

    @Test
    void deleteSegmentRemovesKey() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = utf8("doomed");
                byte[] value = utf8("v");

                MemorySegment keySeg = segOf(arena, key);
                MemorySegment valSeg = segOf(arena, value);

                linker.putSegment(db, cf, keySeg, 0L, key.length, valSeg, 0L, value.length);

                MemorySegment outSeg = arena.allocate(64);
                int len1 =
                        linker.getPinnedSegment(
                                db, cf, keySeg, 0L, key.length, outSeg, 0L, 64);
                assertEquals(value.length, len1);

                linker.deleteSegment(db, cf, keySeg, 0L, key.length);

                int len2 =
                        linker.getPinnedSegment(
                                db, cf, keySeg, 0L, key.length, outSeg, 0L, 64);
                assertEquals(-1, len2);
            }
        }
    }
}

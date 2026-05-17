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

package org.apache.flink.state.forstrs.v1sync;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parity test: write a typed sequence through MemorySegmentDataOutputView (off-heap), then read it
 * back through MemorySegmentDataInputView. Cross-check by writing the same sequence through Flink's
 * stock DataOutputSerializer and reading via DataInputDeserializer.
 *
 * <p>This is the key correctness gate for SP6 — every Flink TypeSerializer must round-trip
 * identically through the off-heap views vs the on-heap stock pair.
 */
class MemorySegmentDataViewParityTest {

    @Test
    void roundTripAllPrimitivesAndStrings() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            MemorySegment seg = arena.allocate(4096);
            MemorySegmentDataOutputView out = new MemorySegmentDataOutputView();
            out.reset(seg, 0);

            out.writeBoolean(true);
            out.writeBoolean(false);
            out.writeByte(-128);
            out.writeByte(127);
            out.writeShort(-32768);
            out.writeShort(32767);
            out.writeChar('A');
            out.writeChar('中');
            out.writeInt(Integer.MIN_VALUE);
            out.writeInt(Integer.MAX_VALUE);
            out.writeLong(Long.MIN_VALUE);
            out.writeLong(Long.MAX_VALUE);
            out.writeFloat(3.14159f);
            out.writeDouble(2.7182818284590451);
            out.write(new byte[] {1, 2, 3, 4, 5});
            out.writeUTF("hello world");
            out.writeUTF("流处理"); // multi-byte
            out.writeUTF("");

            int writtenOffHeap = out.position();

            // Same sequence through Flink's stock serializer for cross-check.
            DataOutputSerializer stock = new DataOutputSerializer(4096);
            stock.writeBoolean(true);
            stock.writeBoolean(false);
            stock.writeByte(-128);
            stock.writeByte(127);
            stock.writeShort(-32768);
            stock.writeShort(32767);
            stock.writeChar('A');
            stock.writeChar('中');
            stock.writeInt(Integer.MIN_VALUE);
            stock.writeInt(Integer.MAX_VALUE);
            stock.writeLong(Long.MIN_VALUE);
            stock.writeLong(Long.MAX_VALUE);
            stock.writeFloat(3.14159f);
            stock.writeDouble(2.7182818284590451);
            stock.write(new byte[] {1, 2, 3, 4, 5});
            stock.writeUTF("hello world");
            stock.writeUTF("流处理");
            stock.writeUTF("");

            byte[] stockBytes = stock.getCopyOfBuffer();
            assertEquals(stockBytes.length, writtenOffHeap, "byte-count parity");

            // Byte-level parity.
            byte[] offHeapBytes = new byte[writtenOffHeap];
            for (int i = 0; i < writtenOffHeap; i++) {
                offHeapBytes[i] = seg.get(java.lang.foreign.ValueLayout.JAVA_BYTE, i);
            }
            assertArrayEquals(stockBytes, offHeapBytes, "off-heap bytes match on-heap bytes");

            // Read-back via MemorySegmentDataInputView.
            MemorySegmentDataInputView in = new MemorySegmentDataInputView();
            in.rewind(seg, 0, writtenOffHeap);
            assertTrue(in.readBoolean());
            assertFalse(in.readBoolean());
            assertEquals(-128, in.readByte());
            assertEquals(127, in.readByte());
            assertEquals(-32768, in.readShort());
            assertEquals(32767, in.readShort());
            assertEquals('A', in.readChar());
            assertEquals('中', in.readChar());
            assertEquals(Integer.MIN_VALUE, in.readInt());
            assertEquals(Integer.MAX_VALUE, in.readInt());
            assertEquals(Long.MIN_VALUE, in.readLong());
            assertEquals(Long.MAX_VALUE, in.readLong());
            assertEquals(3.14159f, in.readFloat());
            assertEquals(2.7182818284590451, in.readDouble());
            byte[] buf = new byte[5];
            in.readFully(buf);
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, buf);
            assertEquals("hello world", in.readUTF());
            assertEquals("流处理", in.readUTF());
            assertEquals("", in.readUTF());
            assertEquals(0, in.remaining(), "fully consumed");

            // And confirm the stock pair also round-trips identically.
            DataInputDeserializer stockIn = new DataInputDeserializer(stockBytes);
            assertTrue(stockIn.readBoolean());
            assertFalse(stockIn.readBoolean());
            assertEquals(-128, stockIn.readByte());
            // … parity already proved via byte arrays.
        }
    }

    @Test
    void overflowThrowsIoException() {
        try (Arena arena = Arena.ofShared()) {
            MemorySegment seg = arena.allocate(8);
            MemorySegmentDataOutputView out = new MemorySegmentDataOutputView();
            out.reset(seg, 0);
            try {
                out.writeLong(0L); // fills the segment
                out.writeByte(0); // one byte over
                throw new AssertionError("expected IOException");
            } catch (java.io.IOException e) {
                // expected
            }
        }
    }

    @Test
    void rewindReadsCorrectSlice() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            MemorySegment seg = arena.allocate(64);
            MemorySegmentDataOutputView out = new MemorySegmentDataOutputView();
            out.reset(seg, 10); // start at offset 10
            out.writeInt(0xDEADBEEF);
            out.writeInt(0xCAFEBABE);

            MemorySegmentDataInputView in = new MemorySegmentDataInputView();
            in.rewind(seg, 10, 8);
            assertEquals(0xDEADBEEF, in.readInt());
            assertEquals(0xCAFEBABE, in.readInt());
            assertEquals(0, in.remaining());
        }
    }
}

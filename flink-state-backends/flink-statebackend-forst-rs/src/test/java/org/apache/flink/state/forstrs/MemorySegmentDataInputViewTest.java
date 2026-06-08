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

package org.apache.flink.state.forstrs;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Correctness gate: {@link MemorySegmentDataInputView} must read byte-for-byte identically to
 * Flink's heap {@link DataInputDeserializer} — otherwise the zero-copy iterator decode corrupts
 * join results.
 */
class MemorySegmentDataInputViewTest {

    private static MemorySegment toSeg(Arena arena, byte[] data, int prefixPad) {
        // Place the payload at a non-zero offset to exercise the base-offset slice path.
        MemorySegment seg = arena.allocate(prefixPad + data.length);
        MemorySegment.copy(data, 0, seg, ValueLayout.JAVA_BYTE, prefixPad, data.length);
        return seg;
    }

    @Test
    void primitiveReadsMatchDataInputDeserializer() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(128);
        out.writeBoolean(true);
        out.writeByte(-7);
        out.writeShort(0x1234);
        out.writeChar('Z');
        out.writeInt(0xDEADBEEF);
        out.writeLong(0x0123456789ABCDEFL);
        out.writeFloat(3.14159f);
        out.writeDouble(2.718281828d);
        out.writeUTF("héllo-世界");
        byte[] payload = new byte[] {1, 2, 3, 4, 5};
        out.write(payload);
        byte[] bytes = out.getCopyOfBuffer();

        try (Arena arena = Arena.ofConfined()) {
            int pad = 16;
            MemorySegment seg = toSeg(arena, bytes, pad);
            MemorySegmentDataInputView view =
                    new MemorySegmentDataInputView().reset(seg, pad, bytes.length);

            assertEquals(true, view.readBoolean());
            assertEquals((byte) -7, view.readByte());
            assertEquals((short) 0x1234, view.readShort());
            assertEquals('Z', view.readChar());
            assertEquals(0xDEADBEEF, view.readInt());
            assertEquals(0x0123456789ABCDEFL, view.readLong());
            assertEquals(3.14159f, view.readFloat());
            assertEquals(2.718281828d, view.readDouble());
            assertEquals("héllo-世界", view.readUTF());
            byte[] tail = new byte[payload.length];
            view.readFully(tail);
            for (int i = 0; i < payload.length; i++) {
                assertEquals(payload[i], tail[i]);
            }
        }
    }

    @Test
    void serializerRoundTripMatchesHeapPath() throws Exception {
        StringSerializer ss = StringSerializer.INSTANCE;
        LongSerializer ls = LongSerializer.INSTANCE;

        DataOutputSerializer out = new DataOutputSerializer(64);
        ss.serialize("auction-42::bidder-xyz", out);
        ls.serialize(9_876_543_210L, out);
        byte[] bytes = out.getCopyOfBuffer();

        // Heap reference path.
        DataInputDeserializer heap = new DataInputDeserializer(bytes);
        String heapStr = ss.deserialize(heap);
        long heapLong = ls.deserialize(heap);

        // Zero-copy off-heap path.
        try (Arena arena = Arena.ofConfined()) {
            int pad = 7;
            MemorySegment seg = toSeg(arena, bytes, pad);
            MemorySegmentDataInputView view =
                    new MemorySegmentDataInputView().reset(seg, pad, bytes.length);
            String segStr = ss.deserialize(view);
            long segLong = ls.deserialize(view);

            assertEquals(heapStr, segStr);
            assertEquals(heapLong, segLong);
        }
    }
}

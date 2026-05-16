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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * SP6 — off-heap {@link DataOutputView} backed by a JDK 25 FFM {@link MemorySegment}.
 *
 * <p>Used on the V1 sync hot path: a {@code TypeSerializer.serialize(value, view)} call writes
 * its bytes <b>directly</b> into the off-heap region, with no intermediate {@code byte[]} on
 * the Java heap. The view is rewound via {@link #reset(MemorySegment, int)} per call; the
 * backing segment is owned by the caller (typically the per-state composite-key staging region
 * or the executor's write-buffer payload).
 *
 * <p><b>Endianness.</b> All multi-byte writes are big-endian to match Flink's standard
 * {@code DataOutputSerializer} / {@code DataInputDeserializer} pair. This is the wire format
 * every Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} produces.
 *
 * <p><b>Growth.</b> This view does NOT grow the segment; the caller is responsible for sizing.
 * If a write would exceed the segment, an {@link IOException} is thrown so the caller can
 * grow + retry. This matches the contract of {@code DataOutputSerializer} when its capacity
 * is exhausted, but with explicit ownership of the buffer.
 *
 * <p><b>Pooling.</b> A new instance is cheap (one object + int field) but pools may reuse a
 * single instance across calls. Always call {@link #reset(MemorySegment, int)} before use.
 *
 * <p>Not thread-safe — same single-threaded-per-slot contract as the rest of the V1 sync path.
 */
@Internal
public final class MemorySegmentDataOutputView implements DataOutputView {

    private MemorySegment segment;
    private int position;
    private int limit;

    public MemorySegmentDataOutputView() {}

    /** Reset the view to start writing at {@code offset} in {@code segment}. */
    public void reset(MemorySegment segment, int offset) {
        this.segment = segment;
        this.position = offset;
        this.limit = (int) segment.byteSize();
    }

    /** Bytes written since last {@link #reset}. */
    public int position() {
        return position;
    }

    public MemorySegment segment() {
        return segment;
    }

    private void ensureRoom(int bytes) throws IOException {
        if (position + bytes > limit) {
            throw new IOException(
                    "MemorySegmentDataOutputView overflow: need "
                            + bytes
                            + " bytes at position "
                            + position
                            + ", limit "
                            + limit);
        }
    }

    // -----------------------------------------------------------------
    // DataOutputView extras
    // -----------------------------------------------------------------

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        ensureRoom(numBytes);
        position += numBytes;
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        ensureRoom(numBytes);
        for (int i = 0; i < numBytes; i++) {
            segment.set(ValueLayout.JAVA_BYTE, position++, source.readByte());
        }
    }

    // -----------------------------------------------------------------
    // DataOutput interface
    // -----------------------------------------------------------------

    @Override
    public void write(int b) throws IOException {
        ensureRoom(1);
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return;
        }
        ensureRoom(len);
        MemorySegment.copy(b, off, segment, ValueLayout.JAVA_BYTE, position, len);
        position += len;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) throws IOException {
        write(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        ensureRoom(2);
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 8));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        writeShort(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        ensureRoom(4);
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 24));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 16));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 8));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        ensureRoom(8);
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 56));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 48));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 40));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 32));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 24));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 16));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (v >>> 8));
        segment.set(ValueLayout.JAVA_BYTE, position++, (byte) v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        int n = s.length();
        ensureRoom(n);
        for (int i = 0; i < n; i++) {
            segment.set(ValueLayout.JAVA_BYTE, position++, (byte) s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        int n = s.length();
        ensureRoom(n * 2);
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (c >>> 8));
            segment.set(ValueLayout.JAVA_BYTE, position++, (byte) c);
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        // Modified UTF-8 encoding to match java.io.DataOutputStream.writeUTF.
        int strlen = s.length();
        int utflen = 0;
        for (int i = 0; i < strlen; i++) {
            int c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }
        if (utflen > 65535) {
            throw new IOException("encoded UTF length " + utflen + " > 65535");
        }
        ensureRoom(utflen + 2);
        writeShort(utflen);
        for (int i = 0; i < strlen; i++) {
            int c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                segment.set(ValueLayout.JAVA_BYTE, position++, (byte) c);
            } else if (c > 0x07FF) {
                segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (0xE0 | ((c >> 12) & 0x0F)));
                segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (0x80 | (c & 0x3F)));
            } else {
                segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (0xC0 | ((c >> 6) & 0x1F)));
                segment.set(ValueLayout.JAVA_BYTE, position++, (byte) (0x80 | (c & 0x3F)));
            }
        }
    }
}

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

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * SP6 — off-heap {@link DataInputView} backed by a JDK 25 FFM {@link MemorySegment}.
 *
 * <p>Used on the V1 sync read path: state.get() fills an off-heap result buffer via {@code
 * frs_get_into_buf}, then {@link #rewind(MemorySegment, int, int)} points this view at the
 * populated region and {@code TypeSerializer.deserialize(view)} reads bytes directly off-heap. No
 * intermediate {@code byte[]} on the Java heap.
 *
 * <p>Endianness matches Flink's {@code DataInputDeserializer} (big-endian for multi-byte reads).
 * UTF reads use the same modified-UTF-8 format as {@code java.io.DataInputStream.readUTF}.
 *
 * <p>Not thread-safe.
 */
@Internal
public final class MemorySegmentDataInputView implements DataInputView {

    private MemorySegment segment;
    private int position;
    private int limit;

    public MemorySegmentDataInputView() {}

    /** Rewind to read {@code length} bytes starting at {@code offset} in {@code segment}. */
    public void rewind(MemorySegment segment, int offset, int length) {
        this.segment = segment;
        this.position = offset;
        this.limit = offset + length;
    }

    public int position() {
        return position;
    }

    public int remaining() {
        return limit - position;
    }

    private byte readByteUnsafe() throws EOFException {
        if (position >= limit) {
            throw new EOFException("MemorySegmentDataInputView underflow at position " + position);
        }
        return segment.get(ValueLayout.JAVA_BYTE, position++);
    }

    private int unsignedByte() throws EOFException {
        return readByteUnsafe() & 0xFF;
    }

    // -----------------------------------------------------------------
    // DataInputView extras
    // -----------------------------------------------------------------

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        if (position + numBytes > limit) {
            throw new EOFException(
                    "skipBytesToRead beyond limit: want "
                            + numBytes
                            + " bytes at position "
                            + position
                            + ", limit "
                            + limit);
        }
        position += numBytes;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int available = limit - position;
        if (available <= 0) {
            return -1;
        }
        int actual = Math.min(available, len);
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, position, b, off, actual);
        position += actual;
        return actual;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    // -----------------------------------------------------------------
    // DataInput interface
    // -----------------------------------------------------------------

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        if (position + len > limit) {
            throw new EOFException(
                    "readFully beyond limit: want "
                            + len
                            + " bytes at position "
                            + position
                            + ", limit "
                            + limit);
        }
        if (len > 0) {
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, position, b, off, len);
            position += len;
        }
    }

    @Override
    public int skipBytes(int n) {
        int actual = Math.min(n, limit - position);
        position += actual;
        return actual;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByteUnsafe() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        return readByteUnsafe();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return unsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        int hi = unsignedByte();
        int lo = unsignedByte();
        return (short) ((hi << 8) | lo);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        int hi = unsignedByte();
        int lo = unsignedByte();
        return (hi << 8) | lo;
    }

    @Override
    public char readChar() throws IOException {
        return (char) readUnsignedShort();
    }

    @Override
    public int readInt() throws IOException {
        int b0 = unsignedByte();
        int b1 = unsignedByte();
        int b2 = unsignedByte();
        int b3 = unsignedByte();
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }

    @Override
    public long readLong() throws IOException {
        long b0 = unsignedByte();
        long b1 = unsignedByte();
        long b2 = unsignedByte();
        long b3 = unsignedByte();
        long b4 = unsignedByte();
        long b5 = unsignedByte();
        long b6 = unsignedByte();
        long b7 = unsignedByte();
        return (b0 << 56)
                | (b1 << 48)
                | (b2 << 40)
                | (b3 << 32)
                | (b4 << 24)
                | (b5 << 16)
                | (b6 << 8)
                | b7;
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    @Deprecated
    public String readLine() {
        throw new UnsupportedOperationException(
                "MemorySegmentDataInputView does not implement readLine (deprecated method).");
    }

    @Override
    public String readUTF() throws IOException {
        // Modified UTF-8 — mirrors java.io.DataInputStream.readUTF.
        int utflen = readUnsignedShort();
        if (position + utflen > limit) {
            throw new EOFException(
                    "readUTF beyond limit: want "
                            + utflen
                            + " bytes at position "
                            + position
                            + ", limit "
                            + limit);
        }
        char[] chars = new char[utflen];
        int chararrCount = 0;
        int endPos = position + utflen;
        while (position < endPos) {
            int c = unsignedByte();
            switch (c >> 4) {
                case 0, 1, 2, 3, 4, 5, 6, 7 -> chars[chararrCount++] = (char) c;
                case 12, 13 -> {
                    int c2 = unsignedByte();
                    if ((c2 & 0xC0) != 0x80) {
                        throw new IOException("malformed UTF-8");
                    }
                    chars[chararrCount++] = (char) (((c & 0x1F) << 6) | (c2 & 0x3F));
                }
                case 14 -> {
                    int c2 = unsignedByte();
                    int c3 = unsignedByte();
                    if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) {
                        throw new IOException("malformed UTF-8");
                    }
                    chars[chararrCount++] =
                            (char) (((c & 0x0F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F));
                }
                default -> throw new IOException("malformed UTF-8");
            }
        }
        return new String(chars, 0, chararrCount);
    }
}

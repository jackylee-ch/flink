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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Zero-copy {@link DataInputView} over a slice of an off-heap {@link MemorySegment} (an iter-chunk
 * {@link IteratorEntryView}). Lets a {@code TypeSerializer} deserialize a key/value DIRECTLY from
 * the off-heap chunk, eliminating the per-entry {@code new byte[]} + full off-heap→heap copy that
 * {@link IteratorEntryView#keyBytes()}/{@link IteratorEntryView#valueBytes()} incur on the join
 * iterator hot path (q7/q9/q20). Reads are big-endian, byte-for-byte identical to Flink's
 * {@code DataInputDeserializer} (verified by a deserialize-parity unit test), so deserialized
 * objects are identical.
 *
 * <p>Single-threaded, reused per decode: call {@link #reset(MemorySegment, int, int)} to point it
 * at a new slice. Not thread-safe (the iter drain decodes synchronously on one thread).
 */
@Internal
public final class MemorySegmentDataInputView implements DataInputView {

    private MemorySegment seg;
    private int base; // absolute offset of the slice start within seg
    private int end; // absolute offset one past the slice end
    private int pos; // absolute cursor within seg

    public MemorySegmentDataInputView() {}

    /** Points this view at {@code seg[offset, offset+length)} and rewinds the cursor. */
    public MemorySegmentDataInputView reset(MemorySegment seg, int offset, int length) {
        this.seg = seg;
        this.base = offset;
        this.end = offset + length;
        this.pos = offset;
        return this;
    }

    private void require(int n) throws EOFException {
        if (pos + n > end) {
            throw new EOFException(
                    "MemorySegmentDataInputView underflow: need " + n + " at pos " + (pos - base)
                            + " of " + (end - base));
        }
    }

    private int u8(int absPos) {
        return seg.get(ValueLayout.JAVA_BYTE, absPos) & 0xFF;
    }

    // ---- DataInput ----

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return;
        }
        require(len);
        MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, pos, b, off, len);
        pos += len;
    }

    @Override
    public int skipBytes(int n) {
        int avail = end - pos;
        int skipped = Math.min(n, Math.max(avail, 0));
        pos += skipped;
        return skipped;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        require(1);
        return seg.get(ValueLayout.JAVA_BYTE, pos++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        require(1);
        return u8(pos++);
    }

    @Override
    public short readShort() throws IOException {
        require(2);
        int v = (u8(pos) << 8) | u8(pos + 1);
        pos += 2;
        return (short) v;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        require(2);
        int v = (u8(pos) << 8) | u8(pos + 1);
        pos += 2;
        return v;
    }

    @Override
    public char readChar() throws IOException {
        return (char) readUnsignedShort();
    }

    @Override
    public int readInt() throws IOException {
        require(4);
        int v = (u8(pos) << 24) | (u8(pos + 1) << 16) | (u8(pos + 2) << 8) | u8(pos + 3);
        pos += 4;
        return v;
    }

    @Override
    public long readLong() throws IOException {
        require(8);
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | u8(pos + i);
        }
        pos += 8;
        return v;
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
        throw new UnsupportedOperationException("readLine not supported by MemorySegmentDataInputView");
    }

    /** Modified UTF-8, identical to {@link java.io.DataInputStream#readUTF}. */
    @Override
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];
        readFully(bytearr, 0, utflen);

        int c, char2, char3;
        int count = 0;
        int chararrCount = 0;
        while (count < utflen) {
            c = bytearr[count] & 0xFF;
            if (c > 127) {
                break;
            }
            count++;
            chararr[chararrCount++] = (char) c;
        }
        while (count < utflen) {
            c = bytearr[count] & 0xFF;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    count++;
                    chararr[chararrCount++] = (char) c;
                    break;
                case 12:
                case 13:
                    count += 2;
                    if (count > utflen) {
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    }
                    char2 = bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    }
                    chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    count += 3;
                    if (count > utflen) {
                        throw new UTFDataFormatException("malformed input: partial character at end");
                    }
                    char2 = bytearr[count - 2];
                    char3 = bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException("malformed input around byte " + (count - 1));
                    }
                    chararr[chararrCount++] =
                            (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
                    break;
                default:
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
        return new String(chararr, 0, chararrCount);
    }

    // ---- DataInputView ----

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        require(numBytes);
        pos += numBytes;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        int avail = end - pos;
        if (avail <= 0) {
            return len == 0 ? 0 : -1;
        }
        int n = Math.min(len, avail);
        MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, pos, b, off, n);
        pos += n;
        return n;
    }

    @Override
    public int read(byte[] b) {
        return read(b, 0, b.length);
    }
}

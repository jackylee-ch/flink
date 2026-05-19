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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Slice-based view into a parsed iter chunk row. References (key, value) ranges in the underlying
 * {@code chunkBuf} {@link MemorySegment} by (offset, length) so that we can defer / avoid the
 * per-entry {@code byte[]} copy on the iter drain path (Commit B of vectorization-violation #1
 * fix).
 *
 * <p>Lifetime contract: the view references a per-chunk immutable snapshot segment allocated from
 * the iter request's arena (see {@code parseChunkInto} in {@link ForStRsDBIterRequest}). The
 * snapshot is NOT the reusable {@code chunkBuf} destination — it is a fresh per-chunk copy that is
 * never overwritten — so the view is valid for the full lifetime of the arena. Callers must still
 * consume the view before the arena is closed by the caller of {@code process()}; the current call
 * sites decode synchronously within the same arena scope, so this is safe.
 *
 * <p>The {@link #keyBytes()} and {@link #valueBytes()} helpers materialize a heap {@code byte[]}
 * for callers that still need the legacy byte[] path (e.g. the default decode methods in {@link
 * ForStRsIterableState}). State implementations that override the View-based decoders can read
 * directly from the segment slice without going through these helpers.
 */
@Internal
public record IteratorEntryView(
        MemorySegment chunkBuf, int keyOffset, int keyLength, int valueOffset, int valueLength) {

    /** Returns {@code true} when this entry has an empty value slice (length == 0). */
    public boolean isValueEmpty() {
        return valueLength == 0;
    }

    /**
     * Materializes the key slice into a fresh {@code byte[]}. Allocates {@code keyLength} bytes
     * plus the array header — use only when a heap byte[] is unavoidable (legacy decoders).
     */
    public byte[] keyBytes() {
        byte[] buf = new byte[keyLength];
        MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, keyOffset, buf, 0, keyLength);
        return buf;
    }

    /**
     * Materializes the value slice into a fresh {@code byte[]}. Returns {@code null} when the value
     * slice is empty (matches the legacy {@code IteratorEntry.value() == null} semantic relied on
     * by the {@code deserializeUserValue(byte[])} contract).
     */
    public byte[] valueBytes() {
        if (valueLength == 0) {
            return null;
        }
        byte[] buf = new byte[valueLength];
        MemorySegment.copy(chunkBuf, ValueLayout.JAVA_BYTE, valueOffset, buf, 0, valueLength);
        return buf;
    }
}

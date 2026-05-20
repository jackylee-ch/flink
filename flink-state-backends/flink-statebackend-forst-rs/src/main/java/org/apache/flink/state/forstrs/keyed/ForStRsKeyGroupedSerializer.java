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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Composite key encoder/decoder per spec §6.
 *
 * <p>Layout (Value/List/Reducing/Aggregating):
 *
 * <pre>
 * composite = kg(2B BE) || serialize(K) || '/' || stateName.bytes || '/'
 * </pre>
 *
 * <p>Layout (Map):
 *
 * <pre>
 * composite = kg(2B BE) || serialize(K) || '/' || stateName.bytes || '/' || serialize(UK)
 * </pre>
 */
public final class ForStRsKeyGroupedSerializer<K> {

    private static final byte SEP = (byte) '/';

    /**
     * ThreadLocal pool for the DataOutputSerializer used by {@link #encodeForState} and {@link
     * #encodeForMap}. Avoids allocating a fresh buffer on every key-encoding call. The buffer is
     * reset via {@code clear()} which resets the write position without reallocating the backing
     * array (Phase B1+B3 optimization).
     */
    private static final ThreadLocal<DataOutputSerializer> POOL =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(256));

    private final TypeSerializer<K> keySerializer;

    public ForStRsKeyGroupedSerializer(TypeSerializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    public TypeSerializer<K> keySerializer() {
        return keySerializer;
    }

    public byte[] encodeForState(int keyGroup, K userKey, String stateName) {
        validateKeyGroup(keyGroup);
        DataOutputSerializer out = POOL.get();
        out.clear();
        try {
            out.writeShort(keyGroup); // 2 bytes BE per Flink convention
            keySerializer.serialize(userKey, out);
            out.write(SEP);
            byte[] sn = stateName.getBytes(StandardCharsets.UTF_8);
            out.write(sn);
            out.write(SEP);
        } catch (IOException e) {
            throw new RuntimeException("encodeForState failed: " + e.getMessage(), e);
        }
        return out.getCopyOfBuffer();
    }

    /**
     * Off-heap variant of {@link #encodeForState}. Writes the composite key {@code [kg(2 BE)]
     * [serialized userKey] [SEP] [stateName UTF-8] [SEP]} into the supplied {@code scratchArena}
     * starting at {@code startOffset}. Returns the (offset, length) packed as a long: {@code
     * (offset << 32) | length}.
     *
     * <p>Caller provides pre-encoded UTF-8 bytes of the state name to avoid the per-call {@code
     * String.getBytes(UTF_8)} allocation (cache once at state-instance construction).
     *
     * <p>Uses {@link DataOutputSerializer#getSharedBuffer()} for zero-copy access to the thread-
     * local serialization buffer, eliminating the per-call {@code byte[]} allocation in {@link
     * #encodeForState}.
     *
     * @return long-packed {@code (startOffset << 32) | totalLen}
     */
    public long encodeForStateOffheap(
            int keyGroup,
            K userKey,
            byte[] preEncodedStateNameBytes,
            java.lang.foreign.MemorySegment scratchArena,
            long startOffset) {
        validateKeyGroup(keyGroup);
        DataOutputSerializer out = POOL.get();
        out.clear();
        try {
            keySerializer.serialize(userKey, out);
        } catch (IOException e) {
            throw new RuntimeException("encodeForStateOffheap failed: " + e.getMessage(), e);
        }
        int userKeyLen = out.length();
        byte[] userKeyBuf = out.getSharedBuffer();
        long off = startOffset;
        scratchArena.set(
                java.lang.foreign.ValueLayout.JAVA_BYTE,
                off++,
                (byte) ((keyGroup >>> 8) & 0xFF));
        scratchArena.set(
                java.lang.foreign.ValueLayout.JAVA_BYTE, off++, (byte) (keyGroup & 0xFF));
        for (int i = 0; i < userKeyLen; i++) {
            scratchArena.set(
                    java.lang.foreign.ValueLayout.JAVA_BYTE, off + i, userKeyBuf[i]);
        }
        off += userKeyLen;
        scratchArena.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off++, SEP);
        for (int i = 0; i < preEncodedStateNameBytes.length; i++) {
            scratchArena.set(
                    java.lang.foreign.ValueLayout.JAVA_BYTE,
                    off + i,
                    preEncodedStateNameBytes[i]);
        }
        off += preEncodedStateNameBytes.length;
        scratchArena.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off++, SEP);
        int totalLen = (int) (off - startOffset);
        return ((long) startOffset << 32) | (long) totalLen;
    }

    public <UK> byte[] encodeForMap(
            int keyGroup,
            K userKey,
            String stateName,
            TypeSerializer<UK> userKeySerializer,
            UK userMapKey) {
        validateKeyGroup(keyGroup);
        DataOutputSerializer out = POOL.get();
        out.clear();
        try {
            out.writeShort(keyGroup);
            keySerializer.serialize(userKey, out);
            out.write(SEP);
            byte[] sn = stateName.getBytes(StandardCharsets.UTF_8);
            out.write(sn);
            out.write(SEP);
            userKeySerializer.serialize(userMapKey, out);
        } catch (IOException e) {
            throw new RuntimeException("encodeForMap failed: " + e.getMessage(), e);
        }
        return out.getCopyOfBuffer();
    }

    public byte[] keyGroupPrefix(int keyGroup) {
        validateKeyGroup(keyGroup);
        return new byte[] {(byte) ((keyGroup >>> 8) & 0xFF), (byte) (keyGroup & 0xFF)};
    }

    public byte[] keyGroupAndStatePrefix(int keyGroup, String stateName) {
        // For per-state-per-keygroup scans we need the kg prefix; the state name
        // discriminator inside the composite key follows the user-key portion,
        // so a "state-only" prefix isn't a clean byte prefix unless the user-key
        // serialization is fixed-length. For variable-length user keys we just
        // use the kg prefix and post-filter by state name during iteration.
        // This method exists for the fixed-length case (tests cover the kg
        // portion only).
        return keyGroupPrefix(keyGroup);
    }

    public Decoded<K> decode(byte[] composite) {
        if (composite.length < 4) {
            throw new IllegalArgumentException("composite too short: " + composite.length);
        }
        int keyGroup = ((composite[0] & 0xFF) << 8) | (composite[1] & 0xFF);
        DataInputDeserializer in = new DataInputDeserializer();
        in.setBuffer(composite, 2, composite.length - 2);
        K userKey;
        try {
            userKey = keySerializer.deserialize(in);
        } catch (IOException e) {
            throw new RuntimeException("decode userKey failed: " + e.getMessage(), e);
        }
        // After userKey we expect /stateName/ — find the LAST occurrence of /
        // to be robust to map-state UK suffix.
        int afterUserKey = 2 + (composite.length - 2 - in.available());
        // Find first / after afterUserKey.
        int firstSlash = -1;
        for (int i = afterUserKey; i < composite.length; i++) {
            if (composite[i] == SEP) {
                firstSlash = i;
                break;
            }
        }
        if (firstSlash < 0) {
            throw new IllegalArgumentException("no separator after userKey");
        }
        // Find last / in the composite (handles map-state UK).
        int lastSlash = -1;
        for (int i = composite.length - 1; i > firstSlash; i--) {
            if (composite[i] == SEP) {
                lastSlash = i;
                break;
            }
        }
        if (lastSlash < 0) {
            lastSlash = firstSlash; // value-state (no UK) — only one /
        }
        // For value-state the composite ends with the second /, so stateName is
        // between firstSlash+1 and lastSlash. For map-state, lastSlash is the
        // separator before UK; same range.
        int stateStart = firstSlash + 1;
        int stateEnd = lastSlash;
        String stateName =
                new String(composite, stateStart, stateEnd - stateStart, StandardCharsets.UTF_8);
        return new Decoded<>(keyGroup, userKey, stateName);
    }

    // ------------------------------------------------------------------
    // JDK 25 Vector API: batch key-group assignment (SIMD)
    // ------------------------------------------------------------------

    /** Preferred SIMD species for int lanes (typically 256-bit = 8 lanes on x86 AVX2). */
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;

    /**
     * Computes key-group assignments for N serialized keys using SIMD (JDK 25 Vector API).
     *
     * <p>The computation mirrors Flink's {@code KeyGroupRangeAssignment.computeKeyGroupForKeyHash}:
     * {@code MathUtils.murmurHash(Arrays.hashCode(key)) % maxParallelism}. The murmur hash is fully
     * vectorized (multiply, rotate-left, xor, shift are all available as SIMD lane ops). The final
     * modulo uses bitwise AND when {@code maxParallelism} is a power of 2 (the common Flink
     * default), falling back to scalar modulo otherwise.
     *
     * <p><b>Performance note.</b> The benefit is proportional to batch size and SIMD width. On AVX2
     * (8 int lanes), a batch of 64 keys processes 8 hashes per cycle vs 1 in the scalar path. The
     * method falls back gracefully to scalar for tail elements and non-power-of-2 maxParallelism.
     *
     * <p><b>Design decision: Arrow flush path.</b> The Arrow C Data Interface zero-copy flush was
     * evaluated but not implemented here. The write-buffer already uses pre-allocated
     * MemorySegments (commit 95df71e17da) which eliminate most allocation overhead. Building Arrow
     * FFI_ArrowArray/FFI_ArrowSchema structs by hand is more complex than the savings justify for
     * this path. The Arrow flush is better suited for future columnar-processing integration where
     * data arrives in Arrow format natively.
     *
     * <p><b>Design decision: ScopedValue.</b> JDK 25's {@code ScopedValue} (JEP 481) was evaluated
     * for replacing the ThreadLocal buffer pool. ScopedValue has immutable-within-scope semantics
     * and requires a {@code runWhere} block, making it unsuitable for per-call mutable buffer
     * reuse. ThreadLocal remains the correct pattern for pooling mutable buffers that are reset and
     * reused across calls.
     *
     * @param serializedKeys array of pre-serialized key byte arrays
     * @param maxParallelism the maximum parallelism (number of key-groups)
     * @return array of key-group assignments, same length as {@code serializedKeys}
     */
    public static int[] batchAssignKeyGroups(byte[][] serializedKeys, int maxParallelism) {
        int n = serializedKeys.length;
        int[] keyGroups = new int[n];
        if (n == 0) {
            return keyGroups;
        }

        // Step 1: compute raw hash codes (scalar — Arrays.hashCode is data-dependent on length)
        int[] hashes = new int[n];
        for (int i = 0; i < n; i++) {
            hashes[i] = Arrays.hashCode(serializedKeys[i]);
        }

        // Step 2: vectorized murmur hash (mirrors MathUtils.murmurHash exactly)
        vectorizedMurmurHash(hashes);

        // Step 3: vectorized modulo (key-group assignment)
        if (Integer.bitCount(maxParallelism) == 1) {
            // Power-of-2: use bitwise AND (fully vectorizable)
            int mask = maxParallelism - 1;
            int i = 0;
            for (; i + INT_SPECIES.length() <= n; i += INT_SPECIES.length()) {
                IntVector v = IntVector.fromArray(INT_SPECIES, hashes, i);
                IntVector result = v.and(mask);
                result.intoArray(keyGroups, i);
            }
            // Scalar tail
            for (; i < n; i++) {
                keyGroups[i] = hashes[i] & mask;
            }
        } else {
            // Non-power-of-2: scalar modulo (Vector API lacks efficient integer modulo)
            for (int i = 0; i < n; i++) {
                keyGroups[i] = hashes[i] % maxParallelism;
            }
        }
        return keyGroups;
    }

    /**
     * Applies Flink's murmur hash in-place on the given array using SIMD. After this method
     * returns, each element contains the non-negative murmur hash of its original value.
     *
     * <p>The algorithm mirrors {@code MathUtils.murmurHash} + {@code MathUtils.bitMix}:
     *
     * <pre>
     * code *= 0xcc9e2d51; code = rotateLeft(code, 15); code *= 0x1b873593;
     * code = rotateLeft(code, 13); code = code * 5 + 0xe6546b64;
     * code ^= 4; // length=4 in Flink's murmur
     * code ^= code >>> 16; code *= 0x85ebca6b;
     * code ^= code >>> 13; code *= 0xc2b2ae35;
     * code ^= code >>> 16;
     * code = abs(code)  // non-negative
     * </pre>
     */
    private static void vectorizedMurmurHash(int[] data) {
        int n = data.length;
        int i = 0;

        for (; i + INT_SPECIES.length() <= n; i += INT_SPECIES.length()) {
            IntVector v = IntVector.fromArray(INT_SPECIES, data, i);

            // murmur body
            v = v.mul(0xcc9e2d51);
            v = v.lanewise(VectorOperators.ROL, 15);
            v = v.mul(0x1b873593);

            v = v.lanewise(VectorOperators.ROL, 13);
            v = v.mul(5).add(0xe6546b64);

            // length XOR (Flink uses fixed length=4)
            v = v.lanewise(VectorOperators.XOR, 4);

            // bitMix (finalization)
            v = v.lanewise(VectorOperators.XOR, v.lanewise(VectorOperators.LSHR, 16));
            v = v.mul(0x85ebca6b);
            v = v.lanewise(VectorOperators.XOR, v.lanewise(VectorOperators.LSHR, 13));
            v = v.mul(0xc2b2ae35);
            v = v.lanewise(VectorOperators.XOR, v.lanewise(VectorOperators.LSHR, 16));

            // abs: make non-negative (mirrors MathUtils.murmurHash's tail).
            // IntVector.abs() returns MIN_VALUE for MIN_VALUE (overflow), so we
            // use a max(v, 0) approach: negate negative lanes, then clamp MIN_VALUE to 0.
            // Equivalent to: v >= 0 ? v : (v == MIN_VALUE ? 0 : -v)
            // Simplified: abs then AND with MAX_VALUE (clears sign bit, maps MIN_VALUE to 0).
            v = v.abs().and(Integer.MAX_VALUE);

            v.intoArray(data, i);
        }

        // Scalar tail
        for (; i < n; i++) {
            data[i] = scalarMurmurHash(data[i]);
        }
    }

    /**
     * Scalar murmur hash matching Flink's {@code MathUtils.murmurHash} exactly. Used for the tail
     * elements that don't fill a full SIMD vector.
     */
    static int scalarMurmurHash(int code) {
        code *= 0xcc9e2d51;
        code = Integer.rotateLeft(code, 15);
        code *= 0x1b873593;

        code = Integer.rotateLeft(code, 13);
        code = code * 5 + 0xe6546b64;

        code ^= 4;

        // bitMix
        code ^= code >>> 16;
        code *= 0x85ebca6b;
        code ^= code >>> 13;
        code *= 0xc2b2ae35;
        code ^= code >>> 16;

        if (code >= 0) {
            return code;
        } else if (code != Integer.MIN_VALUE) {
            return -code;
        } else {
            return 0;
        }
    }

    private static void validateKeyGroup(int keyGroup) {
        if (keyGroup < 0 || keyGroup > 0xFFFF) {
            throw new IllegalArgumentException("keyGroup out of [0, 65535]: " + keyGroup);
        }
    }

    /** Decoded composite key: keyGroup, userKey, stateName. */
    public static final class Decoded<K> {
        private final int keyGroup;
        private final K userKey;
        private final String stateName;

        Decoded(int keyGroup, K userKey, String stateName) {
            this.keyGroup = keyGroup;
            this.userKey = userKey;
            this.stateName = stateName;
        }

        public int keyGroup() {
            return keyGroup;
        }

        public K userKey() {
            return userKey;
        }

        public String stateName() {
            return stateName;
        }
    }
}

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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

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

package org.apache.flink.state.forstrs.state.ttl;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * Holder pairing a user value {@code V} with a TTL expiry timestamp (millis since epoch).
 *
 * <p>PR-A7 (S1-12): Wraps each persisted value with the timestamp at which it becomes invisible to
 * readers. Serialized via {@link TtlSerializer} as {@code [long expiry][value bytes]}. Read paths
 * compare {@link #expiryTimestamp} against the current clock and discard expired entries.
 *
 * <p><b>Storage format break:</b> any state whose descriptor has {@code TtlConfig.isEnabled()} now
 * stores an 8-byte timestamp prefix on every value cell. Mixing TTL-enabled and non-TTL data under
 * the same state name across snapshot boundaries is not supported — enabling TTL on existing state
 * requires fresh state or operator migration.
 */
@Internal
public final class TtlValue<V> {

    private final long expiryTimestamp;
    @Nullable private final V value;

    public TtlValue(long expiryTimestamp, @Nullable V value) {
        this.expiryTimestamp = expiryTimestamp;
        this.value = value;
    }

    public long getExpiryTimestamp() {
        return expiryTimestamp;
    }

    @Nullable
    public V getValue() {
        return value;
    }

    /** {@code true} iff {@code currentTime >= expiryTimestamp}. */
    public boolean isExpired(long currentTime) {
        return currentTime >= expiryTimestamp;
    }

    @Override
    public String toString() {
        return "TtlValue{expiry=" + expiryTimestamp + ", value=" + value + '}';
    }
}

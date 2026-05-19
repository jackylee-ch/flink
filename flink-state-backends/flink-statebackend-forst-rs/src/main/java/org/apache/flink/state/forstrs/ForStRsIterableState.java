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
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;

/**
 * Interface for state types that support iteration (prefix scan). Provides deserialization of user
 * keys and values from raw iterator entries.
 */
@Internal
public interface ForStRsIterableState<K, N, UK, UV> {

    byte[] getIterPrefix(StateRequest<K, N, ?, ?> request);

    UK deserializeUserKey(byte[] rawKey, int userKeyOffset);

    UV deserializeUserValue(byte[] rawValue);

    /**
     * Slice-based user-key decode (Commit B). Default implementation materializes the slice via
     * {@link IteratorEntryView#keyBytes()} and delegates to the legacy byte[] decoder, preserving
     * backwards compatibility for state implementations that have not yet been updated.
     * Implementations may override to avoid the {@code byte[]} materialization.
     */
    default UK deserializeUserKey(IteratorEntryView view, int userKeyPrefixOffset) {
        return deserializeUserKey(view.keyBytes(), userKeyPrefixOffset);
    }

    /**
     * Slice-based user-value decode (Commit B). Default implementation materializes the slice via
     * {@link IteratorEntryView#valueBytes()} (which returns {@code null} for empty values) and
     * delegates to the legacy byte[] decoder. Implementations may override to avoid the {@code
     * byte[]} materialization.
     */
    default UV deserializeUserValue(IteratorEntryView view) {
        return deserializeUserValue(view.valueBytes());
    }

    StateRequestHandler getStateRequestHandler();

    State asState();
}

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
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

/**
 * Encapsulates a single get request for batch execution. Holds the serialized key and a reference
 * to the original StateRequest so the result can be completed.
 */
@Internal
public class ForStRsDBGetRequest<K, N, V> {

    private final byte[] serializedKey;
    private final StateRequest<K, N, ?, V> request;
    private final ForStRsInnerTable<K, N, ?> table;

    @SuppressWarnings("unchecked")
    public ForStRsDBGetRequest(
            byte[] serializedKey,
            StateRequest<K, N, ?, ?> request,
            ForStRsInnerTable<K, N, ?> table) {
        this.serializedKey = serializedKey;
        this.request = (StateRequest<K, N, ?, V>) request;
        this.table = table;
    }

    public byte[] getSerializedKey() {
        return serializedKey;
    }

    public StateRequest<K, N, ?, V> getRequest() {
        return request;
    }

    @SuppressWarnings("unchecked")
    public void complete(byte[] rawValue) {
        Object result;
        if (request.getRequestType() == StateRequestType.MAP_CONTAINS) {
            result = rawValue != null;
        } else {
            result = rawValue == null ? null : table.deserializeValue(rawValue);
        }
        ((org.apache.flink.core.asyncprocessing.InternalAsyncFuture<Object>) request.getFuture())
                .complete(result);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.forstdb;

/**
 * Mirrors the {@code org.forstdb.RocksDBException} class that the JNI shim
 * throws via {@code env.throw_new(...)}. We declare it as a {@link
 * RuntimeException} subclass so call-sites do not need to mark themselves
 * {@code throws} — handy for a JMH benchmark where checked-exception ceremony
 * would clutter the hot path.
 */
public class RocksDBException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RocksDBException(String msg) {
        super(msg);
    }
}

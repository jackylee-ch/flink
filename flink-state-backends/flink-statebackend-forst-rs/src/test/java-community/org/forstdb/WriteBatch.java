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
 * Minimal mirror for the community {@code org.forstdb.WriteBatch} JNI surface.
 *
 * <p>JNI symbols matched (community {@code forstjni-0.1.8.dylib}):
 *
 * <ul>
 *   <li>{@code _Java_org_forstdb_WriteBatch_newWriteBatch__I}
 *   <li>{@code _Java_org_forstdb_WriteBatch_put__J_3BI_3BI}
 *   <li>{@code _Java_org_forstdb_WriteBatch_clear0}
 *   <li>{@code _Java_org_forstdb_WriteBatch_disposeInternal}
 * </ul>
 *
 * <p>Long-form mangling for {@code put} is forced by declaring two overloads
 * (the with-CF variant is declared but never called) — see the matching trick
 * in {@link RocksDB} for the rationale.
 */
public final class WriteBatch {

    private WriteBatch() {}

    public static native long newWriteBatch(int reservedBytes);

    /** Maps to {@code _Java_org_forstdb_WriteBatch_put__J_3BI_3BI}. */
    public static native void put(long handle, byte[] key, int keyLen, byte[] val, int valLen);

    /** Declared (but unused) so the JNI mangler picks the long-form symbol for the call above. */
    public static native void put(
            long handle, byte[] key, int keyLen, byte[] val, int valLen, long cfHandle);

    public static native void clear0(long handle);

    public static native void disposeInternal(long handle);
}

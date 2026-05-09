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
 * Minimal mirror for the community {@code org.forstdb.WriteOptions} JNI surface.
 * The bench only needs to construct/dispose a default-options handle so it can
 * call {@link RocksDB#write0(long, long, long)} on the {@link WriteBatch} side.
 *
 * <p>JNI symbols matched: {@code _Java_org_forstdb_WriteOptions_newWriteOptions}
 * (no args) and {@code _Java_org_forstdb_WriteOptions_disposeInternal(J)V}.
 */
public final class WriteOptions {

    private WriteOptions() {}

    public static native long newWriteOptions();

    public static native void disposeInternal(long handle);
}

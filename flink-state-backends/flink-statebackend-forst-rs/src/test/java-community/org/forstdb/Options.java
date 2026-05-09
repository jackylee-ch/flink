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
 * Minimal mirror of the upstream community {@code Options} class for benching.
 *
 * <p>NOTE: the community cdylib exports the LONG-form mangled symbols
 * {@code _Java_org_forstdb_Options_newOptions__} and
 * {@code _Java_org_forstdb_Options_newOptions__JJ}. JNI's symbol-lookup rule
 * is "try short form first; if not present AND the Java method is overloaded,
 * try long form". So we declare BOTH overloads here even though we only call
 * the no-arg one — the second declaration forces JNI to use the long form,
 * which is what the community lib exports.
 */
public class Options {

    static {
        // Mirror RocksDB's loader so that Options can be invoked even before
        // any RocksDB class-load triggers the cdylib load.
        String p = System.getProperty("org.forstdb.libpath");
        if (p == null || p.isEmpty()) {
            System.loadLibrary("forstjni");
        } else {
            System.load(p);
        }
    }

    private Options() {}

    /** Maps to {@code _Java_org_forstdb_Options_newOptions__}. */
    public static native long newOptions();

    /**
     * Maps to {@code _Java_org_forstdb_Options_newOptions__JJ}. Declared
     * solely to force long-form mangling on {@link #newOptions()}; never
     * invoked from the bench.
     */
    public static native long newOptions(long dboptionsHandle, long cfoptionsHandle);

    public static native void setCreateIfMissing(long handle, boolean flag);

    public static native void disposeInternal(long handle);
}

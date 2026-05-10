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

/**
 * Public API for the ForSt-RS state backend.
 *
 * <p>Entry point: {@link org.apache.flink.state.forstrs.ForStRsStateBackend}. Configuration: {@link
 * org.apache.flink.state.forstrs.ForStRsOptions}. SPI factory: {@link
 * org.apache.flink.state.forstrs.ForStRsStateBackendFactory}.
 *
 * <h2>Native bridge boundary (FFM-only in production)</h2>
 *
 * <p>All production code in {@code src/main/java} reaches the Rust engine
 * ({@code libforst_rs_ffi.{dylib,so,dll}}) <strong>exclusively</strong> through the JDK 25
 * Foreign Function &amp; Memory API: {@link java.lang.foreign.Linker#nativeLinker()} and
 * {@link java.lang.invoke.MethodHandle#invokeExact} via {@link
 * org.apache.flink.state.forstrs.ffm.ForStRsLinker}. There are <em>no</em> {@code native}
 * method declarations and no JNI bindings on the production classpath.
 *
 * <p>Library load uses {@link java.lang.System#loadLibrary} only as a symbol-lookup seed for
 * {@link java.lang.foreign.SymbolLookup#loaderLookup()} (the OS-loader fallback when the
 * {@code forstrs.native.libpath} system property is unset). It binds <strong>no</strong>
 * Java {@code native} methods. The preferred path is {@link
 * java.lang.foreign.SymbolLookup#libraryLookup(java.nio.file.Path, java.lang.foreign.Arena)}
 * driven by the {@code forstrs.native.libpath} system property.
 *
 * <h2>Test/JMH bench boundary (JNI permitted, isolated)</h2>
 *
 * <p>The benchmark harness compares three engines for parity:
 *
 * <ul>
 *   <li>{@code src/test/java/.../ForStRsFfmBenchmark.java} — this module's FFM bridge
 *   <li>{@code src/test/java-community/} — community ForSt JNI shim ({@code org.forstdb.*})
 *   <li>{@code src/test/java-rocksdb/} — canonical {@code org.rocksdb:rocksdbjni}
 * </ul>
 *
 * <p>The JNI-using sources live exclusively under {@code src/test/} sourcesets (community,
 * rocksdb) and the {@code rocksdbjni} dependency is declared with test scope in {@code
 * pom.xml}. Neither leaks into the shaded production jar.
 *
 * <h2>Relationship to ForSt {@code compat-jni}</h2>
 *
 * <p>The companion {@code ForSt} repository's {@code compat-jni} feature exposes a JNI
 * surface intended as a drop-in replacement for the community {@code
 * flink-statebackend-forst} module (RocksDB-style {@code org.forstdb.RocksDB} API). That
 * compat shim is a <strong>separate</strong> integration path and is <strong>not</strong>
 * consumed by this {@code flink-statebackend-forst-rs} module. This module talks to
 * {@code libforst_rs_ffi} via FFM directly and bypasses {@code compat-jni} entirely.
 */
package org.apache.flink.state.forstrs;

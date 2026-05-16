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

package org.apache.flink.state.forstrs.ffm;

/**
 * V1 ABI version contract. EXPECTED_ABI_VERSION is checked at backend
 * init against the value returned by frs_abi_version() in the loaded
 * native lib. Mismatch indicates rolling-deploy version skew — Java jar
 * and libforst_rs_ffi.{dylib,so} are out of step.
 *
 * <p>Bump EXPECTED_ABI_VERSION whenever any FFI layout changes (struct
 * field add/remove, enum variant add/remove, function signature change).
 * The Rust side maintains FRS_ABI_VERSION in lockstep.
 */
public final class FrsAbi {
    public static final int EXPECTED_ABI_VERSION = 1;
    private FrsAbi() {}
}

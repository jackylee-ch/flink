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
 * Java-side mirror of Rust's FrsErrorCode (in libforst_rs_ffi). Numeric values MUST match the Rust
 * enum exactly — see spec §4.
 */
public enum FrsErrorCode {
    OK(0),
    NOT_FOUND(1),
    KEY_TOO_LARGE(100),
    VALUE_TOO_LARGE(101),
    BATCH_HEADER_MALFORMED(110),
    ITER_EXPIRED(200),
    ITER_CURSOR_INVALID(201),
    ENGINE_IO(300),
    ENGINE_CORRUPTED(301),
    ENGINE_OOM(302),
    ENGINE_DISK_FULL(303),
    PANIC_CAUGHT(900),
    UNKNOWN(999);

    private final int code;

    FrsErrorCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Convert from native u32; unknown codes map to UNKNOWN (fail-process). */
    public static FrsErrorCode fromU32(int v) {
        for (FrsErrorCode e : values()) {
            if (e.code == v) {
                return e;
            }
        }
        return UNKNOWN;
    }

    /** Fail-row class: one row affected, other rows in the batch resolve normally. */
    public boolean isFailRow() {
        return code >= 1 && code < 300;
    }

    /** Fail-batch class: whole batch failed, recover via Flink replay. */
    public boolean isFailBatch() {
        return code >= 300 && code < 900;
    }

    /** Fail-process class: engine state suspect, escalate via FatalErrorHandler. */
    public boolean isFailProcess() {
        return code >= 900;
    }
}

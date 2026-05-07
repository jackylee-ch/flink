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

/**
 * Mirrors the FRS_STATUS_* int codes from
 * {@code crates/forst-rs-ffi/src/lib.rs} (lines 60–95).
 * Stable ABI: ordinal does not matter; the int code does.
 */
public enum FrsStatus {
    OK(0),
    ERROR(1),
    NULL_ARG(2),
    NOT_FOUND(3),
    INVALID_ARGUMENT(4),
    PANIC(5),
    POISONED(6),
    IO(7),
    CORRUPTION(8),
    NOT_SUPPORTED(9),
    ABORTED(10),
    BUSY(11),
    TIMED_OUT(12),
    EXPIRED(13),
    INCOMPLETE(14);

    private final int code;

    FrsStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static FrsStatus fromCode(int code) {
        for (FrsStatus s : values()) {
            if (s.code == code) {
                return s;
            }
        }
        throw new IllegalArgumentException("unknown FRS status code: " + code);
    }
}

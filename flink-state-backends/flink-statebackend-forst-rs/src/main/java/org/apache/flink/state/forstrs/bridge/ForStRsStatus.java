/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.state.forstrs.bridge;

/** Status codes returned by every {@code frs_*} Rust ABI entry point. */
public final class ForStRsStatus {
    /** Operation completed successfully. */
    public static final int OK = 0;
    /** Generic error. */
    public static final int ERROR = 1;
    /** A required pointer argument was NULL. */
    public static final int NULL_ARG = 2;
    /** The requested item was not found. */
    public static final int NOT_FOUND = 3;
    /** Input was invalid. */
    public static final int INVALID_ARGUMENT = 4;
    /** A Rust panic was caught at the FFI boundary. */
    public static final int PANIC = 5;
    /** The engine's internal state was poisoned by a previous panic. */
    public static final int POISONED = 6;

    private ForStRsStatus() {}

    /** Human-readable description for logs / exceptions. */
    public static String name(int code) {
        return switch (code) {
            case OK -> "OK";
            case ERROR -> "ERROR";
            case NULL_ARG -> "NULL_ARG";
            case NOT_FOUND -> "NOT_FOUND";
            case INVALID_ARGUMENT -> "INVALID_ARGUMENT";
            case PANIC -> "PANIC";
            case POISONED -> "POISONED";
            default -> "UNKNOWN(" + code + ")";
        };
    }
}

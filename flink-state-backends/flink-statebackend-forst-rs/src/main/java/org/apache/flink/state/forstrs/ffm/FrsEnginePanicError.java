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
 * Fatal Forst-RS engine error — used for fail-process codes (PANIC_CAUGHT, UNKNOWN) per umbrella
 * spec §4. Subclasses {@link Error}, not RuntimeException, so it bypasses operator-level
 * try/catch and propagates straight to Flink's FatalErrorHandler.
 *
 * <p>When raised, the engine state in this process is suspect (catch_unwind doesn't guarantee Rust
 * internal invariants survive a panic). The TaskExecutor is restarted; Flink reschedules the
 * operator on a different or freshly-started TM with a fresh engine process.
 */
public final class FrsEnginePanicError extends Error {

    private static final long serialVersionUID = 1L;

    private final FrsErrorCode code;

    public FrsEnginePanicError(FrsErrorCode code, String detail) {
        super(
                "Forst-RS engine fatal: "
                        + code
                        + " — "
                        + (detail == null ? "" : detail));
        this.code = code;
    }

    public FrsErrorCode code() {
        return code;
    }
}

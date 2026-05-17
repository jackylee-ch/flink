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
 * Thrown at backend init when the loaded libforst_rs_ffi reports an ABI version different from the
 * Java side's compile-time EXPECTED_ABI_VERSION.
 *
 * <p>Surfaces as a startup-time RuntimeException so the backend fails fast before any state op
 * runs. In production, this is fatal at the TaskExecutor level via FatalErrorHandler (wired in P4).
 */
public class FrsAbiMismatchException extends RuntimeException {
    private final int actualVersion;
    private final int expectedVersion;

    public FrsAbiMismatchException(int actual, int expected) {
        super(
                "Forst-RS native ABI mismatch: native lib reports version "
                        + actual
                        + " but Java side expects version "
                        + expected
                        + ". Verify libforst_rs_ffi matches the deployed Java jar.");
        this.actualVersion = actual;
        this.expectedVersion = expected;
    }

    public int getActualVersion() {
        return actualVersion;
    }

    public int getExpectedVersion() {
        return expectedVersion;
    }
}

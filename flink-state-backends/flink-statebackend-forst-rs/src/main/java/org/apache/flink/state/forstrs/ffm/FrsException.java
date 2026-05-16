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
 * Single typed exception for all frs_vec_* FFI returns that aren't OK.
 * Callers switch on `code()` rather than message text. Subclasses (e.g.
 * FrsIteratorExpiredException) provide caller-friendly type names but
 * carry the same payload.
 *
 * <p>See umbrella spec §4 for the error contract.
 */
public class FrsException extends RuntimeException {
    private final FrsErrorCode code;
    private final int rowIndex;
    private final byte[] detail;

    public FrsException(FrsErrorCode code, int rowIndex, byte[] detail) {
        super(
            "FrsException code=" + code + " row=" + rowIndex
                + " detail=" + (detail == null ? 0 : detail.length) + "B");
        this.code = code;
        this.rowIndex = rowIndex;
        this.detail = detail == null ? new byte[0] : detail.clone();
    }

    public FrsErrorCode code() {
        return code;
    }

    public int rowIndex() {
        return rowIndex;
    }

    public byte[] detail() {
        return detail.clone();
    }
}

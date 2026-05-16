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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FrsErrorTypesTest {

    @Test
    void errorCodeFromU32() {
        assertEquals(FrsErrorCode.OK, FrsErrorCode.fromU32(0));
        assertEquals(FrsErrorCode.NOT_FOUND, FrsErrorCode.fromU32(1));
        assertEquals(FrsErrorCode.KEY_TOO_LARGE, FrsErrorCode.fromU32(100));
        assertEquals(FrsErrorCode.ENGINE_IO, FrsErrorCode.fromU32(300));
        assertEquals(FrsErrorCode.PANIC_CAUGHT, FrsErrorCode.fromU32(900));
        assertEquals(FrsErrorCode.UNKNOWN, FrsErrorCode.fromU32(99999));
    }

    @Test
    void errorCodeClassification() {
        assertTrue(FrsErrorCode.KEY_TOO_LARGE.isFailRow());
        assertFalse(FrsErrorCode.KEY_TOO_LARGE.isFailBatch());
        assertFalse(FrsErrorCode.KEY_TOO_LARGE.isFailProcess());

        assertTrue(FrsErrorCode.ENGINE_IO.isFailBatch());
        assertFalse(FrsErrorCode.ENGINE_IO.isFailRow());
        assertFalse(FrsErrorCode.ENGINE_IO.isFailProcess());

        assertTrue(FrsErrorCode.PANIC_CAUGHT.isFailProcess());
        assertTrue(FrsErrorCode.UNKNOWN.isFailProcess());

        assertFalse(FrsErrorCode.OK.isFailRow());
        assertFalse(FrsErrorCode.OK.isFailBatch());
        assertFalse(FrsErrorCode.OK.isFailProcess());
    }

    @Test
    void frsExceptionCarriesCodeRowAndDetail() {
        byte[] detail = new byte[] {1, 2, 3};
        FrsException e = new FrsException(FrsErrorCode.KEY_TOO_LARGE, 7, detail);
        assertEquals(FrsErrorCode.KEY_TOO_LARGE, e.code());
        assertEquals(7, e.rowIndex());
        assertArrayEquals(detail, e.detail());
        assertTrue(e.getMessage().contains("KEY_TOO_LARGE"));
        assertTrue(e.getMessage().contains("row=7"));
    }

    @Test
    void iteratorExpiredIsSubclassWithCorrectCode() {
        FrsIteratorExpiredException ex = new FrsIteratorExpiredException(42);
        assertTrue(ex instanceof FrsException);
        assertEquals(FrsErrorCode.ITER_EXPIRED, ex.code());
        assertEquals(42, ex.rowIndex());
    }
}

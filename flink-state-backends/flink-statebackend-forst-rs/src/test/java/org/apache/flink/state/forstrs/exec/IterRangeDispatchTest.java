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

package org.apache.flink.state.forstrs.exec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Structural tests for the P9 ITER_RANGE dispatch implementation. */
class IterRangeDispatchTest {

    @Test
    void executorHasDispatchIterRangeMethod() {
        boolean found = false;
        for (var m : org.apache.flink.state.forstrs.VectorizedExecutor.class.getDeclaredMethods()) {
            if (m.getName().equals("dispatchIterRange")) {
                found = true;
                break;
            }
        }
        assertTrue(found, "VectorizedExecutor.dispatchIterRange must exist (P9 stub replacement)");
    }

    @Test
    void iterRangeRequestExists() {
        assertNotNull(org.apache.flink.state.forstrs.IterRangeRequest.class);
    }

    @Test
    void iterRangeRequestHasIterFirstChunkRecord() throws NoSuchMethodException {
        // Verify IterRangeRequest.IterFirstChunk record exists with expected accessors.
        Class<?> innerClass = null;
        for (Class<?> c :
                org.apache.flink.state.forstrs.IterRangeRequest.class.getDeclaredClasses()) {
            if (c.getSimpleName().equals("IterFirstChunk")) {
                innerClass = c;
                break;
            }
        }
        assertNotNull(innerClass, "IterRangeRequest.IterFirstChunk record must exist");
        assertNotNull(innerClass.getDeclaredMethod("handle"));
        assertNotNull(innerClass.getDeclaredMethod("firstChunkRows"));
    }

    @Test
    void iterRangeBatchBufferIsCompatibleWithDispatch() {
        // Verify IterRangeBatchBuffer has the APIs dispatchIterRange relies on.
        var buf = new org.apache.flink.state.forstrs.IterRangeBatchBuffer();
        assertEquals(0, buf.count());
        assertTrue(buf.isEmpty());
        assertNotNull(buf.loSlices());
        assertNotNull(buf.hiSlices());
        assertNotNull(buf.chunkBufSlices());
        assertNotNull(buf.futures());
    }
}

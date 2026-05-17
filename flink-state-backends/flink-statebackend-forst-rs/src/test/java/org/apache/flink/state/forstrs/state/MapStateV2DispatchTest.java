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

package org.apache.flink.state.forstrs.state;

import org.apache.flink.state.forstrs.IterPrefixRequest;
import org.apache.flink.state.forstrs.VectorizedExecutor;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P5 verification tests for {@link ForStRsMapStateV2} and {@link ForStRsValueStateV2} shape.
 *
 * <p>These are structural/reflective tests that verify the classes exist in the expected package
 * and expose the required methods. Full end-to-end integration tests against a running engine are
 * deferred to P11 (integration test suite).
 */
class MapStateV2DispatchTest {

    @Test
    void mapStateV2ClassExists() {
        assertNotNull(ForStRsMapStateV2.class, "ForStRsMapStateV2 must exist in the state package");
    }

    @Test
    void valueStateV2ClassExists() {
        assertNotNull(
                ForStRsValueStateV2.class, "ForStRsValueStateV2 must exist in the state package");
    }

    @Test
    void mapStateV2HasGetIterPrefixMethod() {
        boolean found = false;
        for (Method m : ForStRsMapStateV2.class.getMethods()) {
            if (m.getName().equals("getIterPrefix")) {
                found = true;
                break;
            }
        }
        // ForStRsIterableState.getIterPrefix must be implemented — it is used by
        // VectorizedClassifier.buildIterRequest() to construct ForStRsDBIterRequest.
        assertTrue(
                found,
                "ForStRsMapStateV2 must implement getIterPrefix() from ForStRsIterableState");
    }

    @Test
    void mapStateV2HasDeserializeUserKeyMethod() {
        boolean found = false;
        for (Method m : ForStRsMapStateV2.class.getMethods()) {
            if (m.getName().equals("deserializeUserKey")) {
                found = true;
                break;
            }
        }
        assertTrue(
                found,
                "ForStRsMapStateV2 must implement deserializeUserKey() from ForStRsIterableState");
    }

    @Test
    void mapStateV2HasSerializeKeyIntoMethod() {
        boolean found = false;
        for (Method m : ForStRsMapStateV2.class.getMethods()) {
            if (m.getName().equals("serializeKeyInto")) {
                found = true;
                break;
            }
        }
        assertTrue(
                found,
                "ForStRsMapStateV2 must implement serializeKeyInto() from ForStRsInnerTable "
                        + "(off-heap vectorized path)");
    }

    @Test
    void valueStateV2HasSerializeKeyIntoMethod() {
        boolean found = false;
        for (Method m : ForStRsValueStateV2.class.getMethods()) {
            if (m.getName().equals("serializeKeyInto")) {
                found = true;
                break;
            }
        }
        assertTrue(
                found,
                "ForStRsValueStateV2 must implement serializeKeyInto() (SP6 off-heap staging)");
    }

    @Test
    void vectorizedExecutorHasSetSlotScopeMethod() throws Exception {
        // Verify the setter added in P5 is present — required for ITER_PREFIX dispatch.
        Method m =
                VectorizedExecutor.class.getMethod(
                        "setSlotScope", org.apache.flink.state.forstrs.exec.SlotArenaScope.class);
        assertNotNull(m, "VectorizedExecutor must expose setSlotScope(SlotArenaScope)");
    }

    @Test
    void vectorizedExecutorHasDispatchIterPrefixMethod() throws Exception {
        // Verify the real (non-stub) dispatchIterPrefix signature is present.
        Method m =
                VectorizedExecutor.class.getDeclaredMethod(
                        "dispatchIterPrefix",
                        org.apache.flink.state.forstrs.IterPrefixBatchBuffer.class);
        assertNotNull(
                m, "VectorizedExecutor must declare dispatchIterPrefix(IterPrefixBatchBuffer)");
    }

    @Test
    void iterPrefixRequestCarriesIterFirstChunk() {
        // Structural check: IterFirstChunk record must carry handle + firstChunkRows.
        IterPrefixRequest req =
                new IterPrefixRequest(
                        "testState",
                        java.lang.foreign.MemorySegment.NULL,
                        java.lang.foreign.MemorySegment.NULL);
        assertNotNull(req.future(), "IterPrefixRequest must expose a CompletableFuture");
        // Verify IterFirstChunk record fields exist via reflection.
        boolean handleField = false;
        boolean rowsField = false;
        for (Method m : IterPrefixRequest.IterFirstChunk.class.getMethods()) {
            if (m.getName().equals("handle")) {
                handleField = true;
            }
            if (m.getName().equals("firstChunkRows")) {
                rowsField = true;
            }
        }
        assertTrue(handleField, "IterFirstChunk must have handle() accessor");
        assertTrue(rowsField, "IterFirstChunk must have firstChunkRows() accessor");
    }
}

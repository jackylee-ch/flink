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

import org.apache.flink.state.forstrs.AppendMergeRequest;
import org.apache.flink.state.forstrs.DeleteRequest;
import org.apache.flink.state.forstrs.ForStRsDBGetRequest;
import org.apache.flink.state.forstrs.ForStRsDBIterRequest;
import org.apache.flink.state.forstrs.ForStRsDBPutRequest;
import org.apache.flink.state.forstrs.IterPrefixRequest;
import org.apache.flink.state.forstrs.IterRangeRequest;
import org.apache.flink.state.forstrs.VectorizedStateRequest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the sealed {@link VectorizedStateRequest} hierarchy (P2.4 v2, spec §1).
 *
 * <p><b>Package placement note:</b> the sealed interface lives in {@code
 * org.apache.flink.state.forstrs} (not {@code .exec}) because Java sealed interfaces in the
 * unnamed module require all permitted subtypes to be in the same package (JEP 409). The three
 * pre-existing concrete classes are in the parent package, so the interface must live there too.
 * This test class remains in {@code .exec} as specified; it imports the interface from the parent
 * package.
 */
class VectorizedStateRequestTest {

    @Test
    void allSixKindsExist() {
        VectorizedStateRequest.Kind[] kinds = VectorizedStateRequest.Kind.values();
        assertEquals(6, kinds.length, "spec §1 V1 dispatch table has exactly 6 kinds");
    }

    @Test
    void existingConcreteClassesImplementSealedInterface() {
        // After the refactor, the three pre-existing classes must be subtypes.
        assertTrue(
                VectorizedStateRequest.class.isAssignableFrom(ForStRsDBGetRequest.class),
                "ForStRsDBGetRequest must implement VectorizedStateRequest");
        assertTrue(
                VectorizedStateRequest.class.isAssignableFrom(ForStRsDBPutRequest.class),
                "ForStRsDBPutRequest must implement VectorizedStateRequest");
        assertTrue(
                VectorizedStateRequest.class.isAssignableFrom(ForStRsDBIterRequest.class),
                "ForStRsDBIterRequest must implement VectorizedStateRequest");
    }

    @Test
    void newRequestSubtypesExist() {
        assertTrue(
                VectorizedStateRequest.class.isAssignableFrom(DeleteRequest.class),
                "DeleteRequest must implement VectorizedStateRequest");
        assertTrue(
                VectorizedStateRequest.class.isAssignableFrom(AppendMergeRequest.class),
                "AppendMergeRequest must implement VectorizedStateRequest");
        assertTrue(
                VectorizedStateRequest.class.isAssignableFrom(IterPrefixRequest.class),
                "IterPrefixRequest must implement VectorizedStateRequest");
        assertTrue(
                VectorizedStateRequest.class.isAssignableFrom(IterRangeRequest.class),
                "IterRangeRequest must implement VectorizedStateRequest");
    }

    @Test
    void newRequestSubtypesReturnCorrectKind() {
        // Verify the new subtypes' kind() methods return the expected discriminants.
        DeleteRequest del = new DeleteRequest("state1", java.lang.foreign.MemorySegment.NULL);
        assertEquals(VectorizedStateRequest.Kind.DELETE, del.kind());
        assertEquals("state1", del.stateName());

        AppendMergeRequest merge =
                new AppendMergeRequest(
                        "listState",
                        java.lang.foreign.MemorySegment.NULL,
                        new java.lang.foreign.MemorySegment[0]);
        assertEquals(VectorizedStateRequest.Kind.APPEND_MERGE, merge.kind());

        IterPrefixRequest prefix =
                new IterPrefixRequest(
                        "mapState",
                        java.lang.foreign.MemorySegment.NULL,
                        java.lang.foreign.MemorySegment.NULL);
        assertEquals(VectorizedStateRequest.Kind.ITER_PREFIX, prefix.kind());

        IterRangeRequest range =
                new IterRangeRequest(
                        "mapState",
                        java.lang.foreign.MemorySegment.NULL,
                        java.lang.foreign.MemorySegment.NULL,
                        java.lang.foreign.MemorySegment.NULL);
        assertEquals(VectorizedStateRequest.Kind.ITER_RANGE, range.kind());
    }
}

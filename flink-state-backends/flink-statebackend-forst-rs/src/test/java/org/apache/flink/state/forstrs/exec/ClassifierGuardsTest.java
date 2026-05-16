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
import org.apache.flink.state.forstrs.ColumnarBatchBuffer;
import org.apache.flink.state.forstrs.VectorizedClassifier;
import org.apache.flink.state.forstrs.VectorizedStateRequest;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the classifier enforces spec §1 §a: APPEND_MERGE is ListState-only.
 * Reducing/Aggregating state must use the RMW cache path (GET + combine + PUT),
 * not append-merge.
 */
class ClassifierGuardsTest {

    @Test
    void appendMergeAcceptedForListStateNames() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment k = arena.allocate(16, 8);
            MemorySegment v = arena.allocate(32, 8);
            // Build a request — should not throw at construction
            AppendMergeRequest req = new AppendMergeRequest(
                "myListState", k, new MemorySegment[]{v});
            assertEquals(VectorizedStateRequest.Kind.APPEND_MERGE, req.kind());

            // Build a classifier, register the list-state name, and verify submission succeeds.
            VectorizedClassifier classifier = new VectorizedClassifier(
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena));
            classifier.initNewKindBuffers(arena);
            classifier.registerListState("myListState");

            // Submission must not throw.
            classifier.submitVectorized(req);
            assertEquals(1, classifier.appendMergeBuffer().count(),
                    "AppendMerge buffer should have one entry after submit");
        }
    }

    @Test
    void appendMergeRejectedForNonListStateName() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment k = arena.allocate(16, 8);
            AppendMergeRequest req = new AppendMergeRequest(
                "reducingState", k, new MemorySegment[0]);

            VectorizedClassifier classifier = new VectorizedClassifier(
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena));
            classifier.initNewKindBuffers(arena);
            // "reducingState" is NOT registered as a list state — guard must fire.

            IllegalArgumentException ex = assertThrows(
                    IllegalArgumentException.class,
                    () -> classifier.submitVectorized(req),
                    "Classifier must reject APPEND_MERGE for non-list state names");
            assertTrue(ex.getMessage().contains("ListState-only"),
                    "Exception message must mention ListState-only restriction");
        }
    }

    @Test
    void appendMergeNameContainsListHint() {
        // The classifier guard (§1 §a) uses a configurable registry populated when the state
        // primitive is created. This test verifies that the AppendMergeRequest carries its
        // stateName, and that the classifier's registry lookup honours it correctly.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment k = arena.allocate(16, 8);
            AppendMergeRequest req = new AppendMergeRequest(
                "auctionsByCategory", k, new MemorySegment[0]);
            assertEquals("auctionsByCategory", req.stateName());

            // Registry-based approach: isListStateName returns false before registration,
            // true after.
            VectorizedClassifier classifier = new VectorizedClassifier(
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena),
                    new ColumnarBatchBuffer(arena));
            classifier.initNewKindBuffers(arena);

            // Before registration: not a list state.
            assertTrue(!classifier.isListStateName("auctionsByCategory"),
                    "Unregistered name must not be recognised as list state");

            // After registration: is a list state.
            classifier.registerListState("auctionsByCategory");
            assertTrue(classifier.isListStateName("auctionsByCategory"),
                    "Registered name must be recognised as list state");

            // After unregistration: no longer a list state.
            classifier.unregisterListState("auctionsByCategory");
            assertTrue(!classifier.isListStateName("auctionsByCategory"),
                    "Unregistered name must not be recognised as list state after removal");
        }
    }
}

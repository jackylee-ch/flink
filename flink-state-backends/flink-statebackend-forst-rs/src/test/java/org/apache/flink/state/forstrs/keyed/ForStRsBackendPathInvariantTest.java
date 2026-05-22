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

package org.apache.flink.state.forstrs.keyed;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * E7-H3 regression: the keyed-backend path invariant must fail loudly when one operator id is
 * observed under BOTH the V1-sync and async paths within the same JVM. Each backend constructs
 * its own private {@code StateSerializerRegistry} — cross-path use would silently bypass
 * schema-drift detection. The invariant is the lower-effort defensive fix.
 */
class ForStRsBackendPathInvariantTest {

    @BeforeEach
    void resetObserver() {
        ForStRsBackendPathInvariant.resetForTests();
    }

    @Test
    void recordingSamePathTwiceIsIdempotent() {
        ForStRsBackendPathInvariant.recordBackendPath(
                "op-A", ForStRsBackendPathInvariant.Path.SYNC_V1);
        // Re-recording the same path under the same id is a no-op (rescaling restart case).
        assertDoesNotThrow(
                () ->
                        ForStRsBackendPathInvariant.recordBackendPath(
                                "op-A", ForStRsBackendPathInvariant.Path.SYNC_V1));
        assertEquals(
                ForStRsBackendPathInvariant.Path.SYNC_V1,
                ForStRsBackendPathInvariant.observedPathForTests("op-A"));
    }

    @Test
    void crossPathOnSameOperatorThrows() {
        ForStRsBackendPathInvariant.recordBackendPath(
                "op-B", ForStRsBackendPathInvariant.Path.SYNC_V1);
        IllegalStateException ex =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                ForStRsBackendPathInvariant.recordBackendPath(
                                        "op-B",
                                        ForStRsBackendPathInvariant.Path.ASYNC_V2));
        // Diagnostic must name both paths so the operator can locate the wiring bug fast.
        String msg = ex.getMessage();
        org.junit.jupiter.api.Assertions.assertTrue(msg.contains("op-B"), msg);
        org.junit.jupiter.api.Assertions.assertTrue(msg.contains("SYNC_V1"), msg);
        org.junit.jupiter.api.Assertions.assertTrue(msg.contains("ASYNC_V2"), msg);
    }

    @Test
    void differentOperatorsMayUseDifferentPaths() {
        // op-C goes V1, op-D goes async — independent JobVertices in the same TaskManager.
        ForStRsBackendPathInvariant.recordBackendPath(
                "op-C", ForStRsBackendPathInvariant.Path.SYNC_V1);
        assertDoesNotThrow(
                () ->
                        ForStRsBackendPathInvariant.recordBackendPath(
                                "op-D", ForStRsBackendPathInvariant.Path.ASYNC_V2));
        assertEquals(
                ForStRsBackendPathInvariant.Path.SYNC_V1,
                ForStRsBackendPathInvariant.observedPathForTests("op-C"));
        assertEquals(
                ForStRsBackendPathInvariant.Path.ASYNC_V2,
                ForStRsBackendPathInvariant.observedPathForTests("op-D"));
    }

    @Test
    void nullOrEmptyIdentifierIsTolerated() {
        // Non-runtime callers (tests, dry-run construction) may not have an operator id.
        assertDoesNotThrow(
                () ->
                        ForStRsBackendPathInvariant.recordBackendPath(
                                null, ForStRsBackendPathInvariant.Path.SYNC_V1));
        assertDoesNotThrow(
                () ->
                        ForStRsBackendPathInvariant.recordBackendPath(
                                "", ForStRsBackendPathInvariant.Path.ASYNC_V2));
    }
}

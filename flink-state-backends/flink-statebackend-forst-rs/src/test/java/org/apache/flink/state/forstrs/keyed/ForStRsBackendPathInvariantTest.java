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

import org.apache.flink.api.common.JobID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    /**
     * E8-H4 regression: keying by {@code (JobID, operatorIdentifier)} permits the same operator
     * identifier to appear in two DIFFERENT jobs under different paths. The pre-fix single-arg
     * map would falsely block the second job; the fixed composite key isolates the two
     * lifecycles.
     */
    @Test
    void differentJobsMayUseDifferentPathsForSameOperatorId() {
        JobID jobA = new JobID();
        JobID jobB = new JobID();
        ForStRsBackendPathInvariant.recordBackendPath(
                jobA, "op-shared", ForStRsBackendPathInvariant.Path.SYNC_V1);
        // Job B can wire the same operator id under a DIFFERENT path without a false-positive
        // cross-path block — the two jobs are independent lifecycles.
        assertDoesNotThrow(
                () ->
                        ForStRsBackendPathInvariant.recordBackendPath(
                                jobB,
                                "op-shared",
                                ForStRsBackendPathInvariant.Path.ASYNC_V2));
        assertEquals(
                ForStRsBackendPathInvariant.Path.SYNC_V1,
                ForStRsBackendPathInvariant.observedPathForTests(jobA, "op-shared"));
        assertEquals(
                ForStRsBackendPathInvariant.Path.ASYNC_V2,
                ForStRsBackendPathInvariant.observedPathForTests(jobB, "op-shared"));
    }

    /**
     * E8-H4 regression: within a single job, the cross-path invariant still fires. Even with the
     * composite key, a second backend on the same {@code (jobID, operatorIdentifier)} pair under
     * a different path must throw — the underlying schema-drift-bypass risk is unchanged.
     */
    @Test
    void crossPathWithinSameJobStillThrows() {
        JobID job = new JobID();
        ForStRsBackendPathInvariant.recordBackendPath(
                job, "op-X", ForStRsBackendPathInvariant.Path.SYNC_V1);
        IllegalStateException ex =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                ForStRsBackendPathInvariant.recordBackendPath(
                                        job,
                                        "op-X",
                                        ForStRsBackendPathInvariant.Path.ASYNC_V2));
        String msg = ex.getMessage();
        org.junit.jupiter.api.Assertions.assertTrue(msg.contains("op-X"), msg);
        org.junit.jupiter.api.Assertions.assertTrue(msg.contains("SYNC_V1"), msg);
        org.junit.jupiter.api.Assertions.assertTrue(msg.contains("ASYNC_V2"), msg);
    }

    /**
     * E8-H4 regression: dispose-time removal of the path slot lets a job redeploy on the same
     * {@code (jobID, operatorIdentifier)} re-register WITH A DIFFERENT PATH. The pre-fix static
     * map had no remove hook, so a V1→V2 toggle on redeploy would crash with a misleading
     * "previously observed" error even though the prior backend was disposed.
     */
    @Test
    void removeBackendPathPermitsRedeployUnderDifferentPath() {
        JobID job = new JobID();
        ForStRsBackendPathInvariant.recordBackendPath(
                job, "op-redeploy", ForStRsBackendPathInvariant.Path.SYNC_V1);
        assertEquals(
                ForStRsBackendPathInvariant.Path.SYNC_V1,
                ForStRsBackendPathInvariant.observedPathForTests(job, "op-redeploy"));
        // Backend.dispose() releases the slot.
        ForStRsBackendPathInvariant.removeBackendPath(job, "op-redeploy");
        assertNull(ForStRsBackendPathInvariant.observedPathForTests(job, "op-redeploy"));
        // Redeploy under V2 — must not throw.
        assertDoesNotThrow(
                () ->
                        ForStRsBackendPathInvariant.recordBackendPath(
                                job,
                                "op-redeploy",
                                ForStRsBackendPathInvariant.Path.ASYNC_V2));
        assertEquals(
                ForStRsBackendPathInvariant.Path.ASYNC_V2,
                ForStRsBackendPathInvariant.observedPathForTests(job, "op-redeploy"));
    }

    /**
     * Legacy single-arg path remains backwards compatible: tests / non-runtime callers that
     * never had a JobID see the same observation behaviour as before E8-H4 — they implicitly
     * key the slot under a synthetic {@code null} job id that cannot collide with a runtime
     * (real JobID) record.
     */
    @Test
    void legacySingleArgPathDoesNotCollideWithRuntimeKeyedEntries() {
        JobID job = new JobID();
        // Runtime path records under (job, "op-shared")
        ForStRsBackendPathInvariant.recordBackendPath(
                job, "op-shared", ForStRsBackendPathInvariant.Path.ASYNC_V2);
        // Legacy path records under (null, "op-shared") — different key, no collision.
        assertDoesNotThrow(
                () ->
                        ForStRsBackendPathInvariant.recordBackendPath(
                                "op-shared", ForStRsBackendPathInvariant.Path.SYNC_V1));
        assertEquals(
                ForStRsBackendPathInvariant.Path.ASYNC_V2,
                ForStRsBackendPathInvariant.observedPathForTests(job, "op-shared"));
        assertEquals(
                ForStRsBackendPathInvariant.Path.SYNC_V1,
                ForStRsBackendPathInvariant.observedPathForTests("op-shared"));
    }
}

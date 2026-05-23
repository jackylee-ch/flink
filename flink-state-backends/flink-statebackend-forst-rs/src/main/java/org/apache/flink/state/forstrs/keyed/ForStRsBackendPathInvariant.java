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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * E7-H3 invariant: within a single Flink job, an operator (identified by {@code
 * operatorIdentifier}) must pick exactly ONE ForSt-RS keyed-backend path — either the V1-sync
 * path ({@link ForStRsAbstractKeyedStateBackend}) or the async path
 * ({@link ForStRsAsyncKeyedStateBackend}), but not both.
 *
 * <p>Each backend constructs its own private {@link
 * org.apache.flink.state.forstrs.state.StateSerializerRegistry}. Cross-path use against the same
 * operator within one job would create two distinct registries within the same logical scope,
 * opening a schema-drift detection bypass: a state registered on one path would not be visible
 * to the other for {@code verifyOrRegister}.
 *
 * <h3>E8-H4 fix: scope the map by (JobID, operatorIdentifier)</h3>
 *
 * <p>The pre-fix implementation keyed the static observation map by {@code operatorIdentifier}
 * alone, with no remove hook on backend dispose. That produced false-positive blocks across
 * unrelated job lifecycles in a long-running TaskManager:
 *
 * <ul>
 *   <li><b>Job redeploy toggling V1/V2:</b> a user that redeploys the same job ID with a
 *       different backend selection (e.g. switching the {@code state.backend.forstrs.async}
 *       knob) would see the same {@code operatorIdentifier} fail with a misleading "previously
 *       observed path was X" message — even though the prior backend instance had already been
 *       disposed and was no longer holding any registry.
 *   <li><b>Distinct jobs with colliding operator IDs:</b> two unrelated jobs that happen to
 *       assign the same operator identifier (rare in practice but legal — operator IDs are
 *       hash-derived and not globally unique) would block each other.
 * </ul>
 *
 * <p>The fixed key uses {@code (JobID, operatorIdentifier)} so the invariant binds to a single
 * job lifecycle. Backends MUST also call {@link #removeBackendPath(JobID, String)} from their
 * {@code dispose()} so a clean shutdown frees the slot — a subsequent restart of the same job
 * id can re-register without a false positive.
 *
 * <p>Scope: JVM-process-wide. A TaskManager hosts every task subtask in one JVM, so the
 * {@code (JobID, operatorIdentifier)} composite key is sufficient to detect cross-path wiring
 * within one running Flink job. Backends MUST call {@link #recordBackendPath} from their
 * factory site and {@link #removeBackendPath} from {@code dispose()}.
 *
 * <p>Legacy single-arg overload {@link #recordBackendPath(String, Path)} remains for tests and
 * non-runtime callers; it uses a synthetic null {@link JobID} so it does not interfere with
 * job-keyed entries from the runtime path.
 */
@Internal
public final class ForStRsBackendPathInvariant {

    /** Which keyed-backend path observed itself for a given operator. */
    public enum Path {
        /** V1-sync path — {@link ForStRsAbstractKeyedStateBackend}. */
        SYNC_V1,
        /** Async path — {@link ForStRsAsyncKeyedStateBackend}. */
        ASYNC_V2
    }

    /**
     * Composite key {@code (jobID, operatorIdentifier)}. A {@code null} {@code jobID} is used by
     * the legacy single-arg overload for test / non-runtime callers — it never collides with a
     * real job-keyed entry.
     */
    private static final class Key {
        private final JobID jobID;
        private final String operatorIdentifier;

        Key(JobID jobID, String operatorIdentifier) {
            this.jobID = jobID;
            this.operatorIdentifier = operatorIdentifier;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Key)) {
                return false;
            }
            Key k = (Key) o;
            return Objects.equals(jobID, k.jobID)
                    && Objects.equals(operatorIdentifier, k.operatorIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobID, operatorIdentifier);
        }

        @Override
        public String toString() {
            return "(" + jobID + ", " + operatorIdentifier + ")";
        }
    }

    /**
     * (JobID, operatorIdentifier) → observed path. Used to fail loudly if a second backend
     * instantiates on the same operator id within the SAME job under a different {@link Path}.
     * Removed on backend dispose so a subsequent job lifecycle can re-record without a false
     * positive.
     */
    private static final ConcurrentHashMap<Key, Path> OBSERVED = new ConcurrentHashMap<>();

    private ForStRsBackendPathInvariant() {}

    /**
     * Register the calling backend's path. Throws {@link IllegalStateException} if the same
     * {@code (jobID, operatorIdentifier)} has previously been recorded under a DIFFERENT
     * {@link Path} — which would indicate a cross-path wiring bug that would otherwise silently
     * bypass {@link
     * org.apache.flink.state.forstrs.state.StateSerializerRegistry#verifyOrRegister} schema-drift
     * checks (each backend constructs its own registry instance).
     *
     * <p>Re-registering the SAME path (e.g., on rescaling restart of the same backend) is a
     * no-op.
     *
     * <p>The {@code jobID} is the Flink runtime's per-job identifier — pulled from
     * {@code KeyedStateBackendParameters.getJobID()} at the factory site. Different jobs cannot
     * collide even if their operator identifiers happen to be equal.
     */
    public static void recordBackendPath(JobID jobID, String operatorIdentifier, Path path) {
        if (operatorIdentifier == null || operatorIdentifier.isEmpty()) {
            // No identifier — cannot enforce; skip rather than throw (tests / non-runtime use).
            return;
        }
        Key key = new Key(jobID, operatorIdentifier);
        Path prior = OBSERVED.putIfAbsent(key, path);
        if (prior != null && prior != path) {
            throw new IllegalStateException(
                    "ForSt-RS keyed-backend path invariant violated for "
                            + key
                            + ": previously observed path was "
                            + prior
                            + ", now attempting to wire "
                            + path
                            + ". Both V1-sync and async backends construct their own"
                            + " StateSerializerRegistry — cross-path use within a single job"
                            + " would silently bypass schema-drift detection. Pick exactly one"
                            + " keyed-backend path per operator within a job, or hoist the"
                            + " registry into a shared per-task scope.");
        }
    }

    /**
     * Legacy single-arg overload — uses a synthetic {@code null} job id, so it cannot collide
     * with runtime-path records keyed by a real {@link JobID}. Kept for tests and non-runtime
     * call sites that do not have a job id at hand.
     */
    public static void recordBackendPath(String operatorIdentifier, Path path) {
        recordBackendPath(null, operatorIdentifier, path);
    }

    /**
     * Release the slot for {@code (jobID, operatorIdentifier)} so a subsequent backend on the
     * same key can register without a false-positive cross-path violation. Called from each
     * backend's {@code dispose()}. Idempotent — silently no-ops if the slot is absent (already
     * removed by a prior dispose / never recorded due to null id).
     */
    public static void removeBackendPath(JobID jobID, String operatorIdentifier) {
        if (operatorIdentifier == null || operatorIdentifier.isEmpty()) {
            return;
        }
        OBSERVED.remove(new Key(jobID, operatorIdentifier));
    }

    /**
     * Legacy single-arg remove — pairs with the legacy single-arg {@link #recordBackendPath}
     * overload (synthetic {@code null} job id).
     */
    public static void removeBackendPath(String operatorIdentifier) {
        removeBackendPath(null, operatorIdentifier);
    }

    /**
     * Test-only: clear all observed paths. Real Flink jobs run one operator-id once per JVM, but
     * unit tests reuse operator ids across test methods.
     */
    public static void resetForTests() {
        OBSERVED.clear();
    }

    /**
     * Test-only accessor — returns the path currently recorded for
     * {@code (jobID, operatorIdentifier)}, or {@code null}.
     */
    public static Path observedPathForTests(JobID jobID, String operatorIdentifier) {
        return OBSERVED.get(new Key(jobID, operatorIdentifier));
    }

    /** Legacy single-arg observation accessor — pairs with the legacy record/remove overloads. */
    public static Path observedPathForTests(String operatorIdentifier) {
        return observedPathForTests(null, operatorIdentifier);
    }
}

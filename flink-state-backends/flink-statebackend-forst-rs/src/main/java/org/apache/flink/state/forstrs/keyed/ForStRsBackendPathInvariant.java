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

import java.util.concurrent.ConcurrentHashMap;

/**
 * E7-H3: invariant assertion that for any one Flink operator (identified by {@code
 * operatorIdentifier}) only ONE ForSt-RS keyed-backend path is wired — either the V1-sync path
 * ({@link ForStRsAbstractKeyedStateBackend}) or the async path ({@link
 * ForStRsAsyncKeyedStateBackend}), but not both.
 *
 * <p>Each backend constructs its own private {@link
 * org.apache.flink.state.forstrs.state.StateSerializerRegistry}. Cross-path use against the same
 * operator would create two distinct registries within the same logical scope, opening a
 * schema-drift detection bypass: a state registered on one path would not be visible to the other
 * for {@code verifyOrRegister}. The lower-effort defensive fix (chosen here over hoisting the
 * registry into a shared per-task scope) is to fail loudly at backend creation if both observers
 * fire for the same operator id.
 *
 * <p>Scope: JVM-process-wide. A TaskManager hosts every task subtask in one JVM, so the
 * {@code operatorIdentifier} key is sufficient to detect cross-path wiring within the running
 * Flink job. Backends MUST call {@link #recordBackendPath} from their factory site.
 *
 * <p>If the V20 ListState V2 dual-class path ever genuinely requires both backends to coexist on
 * the same operator, this assertion will fire and force a deliberate hoist of the registry into
 * shared scope (the structural fix) rather than a silent bypass.
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
     * Operator-id → observed path. Used to fail loudly if a second backend instantiates on the
     * same operator id under a different {@link Path}.
     */
    private static final ConcurrentHashMap<String, Path> OBSERVED = new ConcurrentHashMap<>();

    private ForStRsBackendPathInvariant() {}

    /**
     * Register the calling backend's path. Throws {@link IllegalStateException} if the same
     * {@code operatorIdentifier} has previously been recorded under a DIFFERENT {@link Path} —
     * which would indicate a cross-path wiring bug that would otherwise silently bypass {@link
     * org.apache.flink.state.forstrs.state.StateSerializerRegistry#verifyOrRegister} schema-drift
     * checks (each backend constructs its own registry instance).
     *
     * <p>Re-registering the SAME path (e.g., on rescaling restart of the same backend) is a no-op.
     */
    public static void recordBackendPath(String operatorIdentifier, Path path) {
        if (operatorIdentifier == null || operatorIdentifier.isEmpty()) {
            // No identifier — cannot enforce; skip rather than throw (tests / non-runtime use).
            return;
        }
        Path prior = OBSERVED.putIfAbsent(operatorIdentifier, path);
        if (prior != null && prior != path) {
            throw new IllegalStateException(
                    "ForSt-RS keyed-backend path invariant violated for operator "
                            + operatorIdentifier
                            + ": previously observed path was "
                            + prior
                            + ", now attempting to wire "
                            + path
                            + ". Both V1-sync and async backends construct their own"
                            + " StateSerializerRegistry — cross-path use would silently bypass"
                            + " schema-drift detection. Pick exactly one keyed-backend path per"
                            + " operator, or hoist the registry into a shared per-task scope.");
        }
    }

    /**
     * Test-only: clear all observed paths. Real Flink jobs run one operator-id once per JVM, but
     * unit tests reuse operator ids across test methods.
     */
    public static void resetForTests() {
        OBSERVED.clear();
    }

    /** Test-only accessor — returns the path currently recorded for the given operator, or null. */
    public static Path observedPathForTests(String operatorIdentifier) {
        return OBSERVED.get(operatorIdentifier);
    }
}

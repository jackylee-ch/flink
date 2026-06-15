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

import org.apache.flink.api.common.functions.ReduceFunction;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OPT-N04 (A2 / J2) eligibility oracle test for {@link ForStRsAsyncReducingStateV2#isWrappingI64Add}.
 *
 * <p>A reducing state is merge-routable only when its reducer is a WRAPPING i64 add — byte-equivalent
 * to the engine's {@code NumericAddBeMergeOperator}. This test pins the oracle:
 *
 * <ul>
 *   <li>{@code Long::sum} (descriptor {@code (JJ)J}) → routable.
 *   <li>{@code Math::addExact} → NOT routable (throws on overflow; the operator wraps → divergent
 *       at the i64 rails, so it is excluded to preserve byte-equivalence).
 *   <li>{@code Integer::sum}, custom/anonymous reducers, non-serializable lambdas → NOT routable.
 * </ul>
 *
 * <p>Eligibility additionally requires {@link
 * org.apache.flink.api.common.typeutils.base.LongSerializer} (the on-disk 8-byte BE form); that arm
 * is covered by the byte-identity integration test which constructs real states.
 */
class ReducingStateMergeRoutingEligibilityTest {

    /** Serializable reducers — survive {@code writeReplace} introspection as a SerializedLambda. */
    @FunctionalInterface
    interface SerReduceLong extends ReduceFunction<Long>, java.io.Serializable {}

    @FunctionalInterface
    interface SerReduceInt extends ReduceFunction<Integer>, java.io.Serializable {}

    private static boolean isWrappingI64Add(ReduceFunction<?> reducer) throws Exception {
        Method m =
                ForStRsAsyncReducingStateV2.class.getDeclaredMethod(
                        "isWrappingI64Add", ReduceFunction.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, reducer);
    }

    @Test
    void longSumIsWrappingAddAndRoutable() throws Exception {
        SerReduceLong reducer = Long::sum;
        assertTrue(isWrappingI64Add(reducer), "Long::sum is the canonical wrapping i64 add");
    }

    @Test
    void mathAddExactIsNotRoutable() throws Exception {
        // Math::addExact throws ArithmeticException on overflow whereas NumericAddBeMergeOperator
        // wraps — not byte-equivalent at the rails. MUST be rejected.
        SerReduceLong reducer = Math::addExact;
        assertFalse(
                isWrappingI64Add(reducer),
                "Math::addExact must be rejected (throws on overflow, operator wraps)");
    }

    @Test
    void integerSumIsNotRoutable() throws Exception {
        // Int domain → 4-byte on-disk form; excluded (widening would break the checkpoint format).
        SerReduceInt reducer = Integer::sum;
        assertFalse(isWrappingI64Add(reducer), "Integer::sum is not an (JJ)J long add");
    }

    @Test
    void customLambdaWithSyntheticImplIsNotRoutable() throws Exception {
        // (a,b) -> a + b compiles to a synthetic impl method, not Long::sum — conservatively
        // rejected (the bytecode is not portably inspectable). Falls back to the legacy path.
        SerReduceLong reducer = (a, b) -> a + b;
        assertFalse(
                isWrappingI64Add(reducer),
                "synthetic-impl lambda is not recognized as Long.sum and must be rejected");
    }

    @Test
    void doublingReducerIsNotRoutable() throws Exception {
        SerReduceLong reducer = (a, b) -> a + 2 * b;
        assertFalse(isWrappingI64Add(reducer), "non-additive reducer must be rejected");
    }

    @Test
    void nonSerializableReducerIsNotRoutable() throws Exception {
        // A plain (non-Serializable) lambda has no writeReplace → cannot be introspected → rejected.
        ReduceFunction<Long> reducer = (a, b) -> a + b;
        assertFalse(
                isWrappingI64Add(reducer),
                "non-Serializable reducer cannot be introspected and must be rejected");
    }
}

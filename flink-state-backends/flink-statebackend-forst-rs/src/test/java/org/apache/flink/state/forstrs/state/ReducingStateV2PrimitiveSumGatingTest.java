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
import java.util.function.LongBinaryOperator;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A12-H1 regression: {@link ForStRsAsyncReducingStateV2#tryUnwrapPrimitiveSumReducer} must reject
 * the primitive-sum fast path when the user's reducer signature does not match the value
 * serializer's primitive domain. A {@code Long::sum} reducer paired with an
 * {@link org.apache.flink.api.common.typeutils.base.IntSerializer}-typed state — or
 * {@code Integer::sum} paired with a {@link org.apache.flink.api.common.typeutils.base.LongSerializer}
 * one — would otherwise install a mismatched-domain operator and silently corrupt data at flush
 * (the flush callback truncates with {@code (int) acc} when {@code accIsInteger} is true).
 *
 * <p>The legacy boxing shim would have thrown {@link ClassCastException} on the {@code (Long) out}
 * / {@code (Integer) out} branch in its body; rejecting the fast path here forces the constructor
 * to install that shim, preserving the legacy detection-on-mismatch semantic.
 */
class ReducingStateV2PrimitiveSumGatingTest {

    /** Serializable {@code Long::sum} — survives writeReplace introspection as a SerializedLambda. */
    @FunctionalInterface
    interface SerReduceFunctionLong extends ReduceFunction<Long>, java.io.Serializable {}

    /** Serializable {@code Integer::sum} — same contract for int. */
    @FunctionalInterface
    interface SerReduceFunctionInt extends ReduceFunction<Integer>, java.io.Serializable {}

    private static LongBinaryOperator invokeDetector(ReduceFunction<?> reducer, boolean asInt)
            throws Exception {
        Method m =
                ForStRsAsyncReducingStateV2.class.getDeclaredMethod(
                        "tryUnwrapPrimitiveSumReducer", ReduceFunction.class, boolean.class);
        m.setAccessible(true);
        return (LongBinaryOperator) m.invoke(null, reducer, asInt);
    }

    @Test
    void longSumWithLongDomainReturnsFastPath() throws Exception {
        SerReduceFunctionLong reducer = Long::sum;
        LongBinaryOperator op = invokeDetector(reducer, /* asInt = */ false);
        assertNotNull(op, "Long::sum + long-domain must yield the fast path");
    }

    @Test
    void integerSumWithIntDomainReturnsFastPath() throws Exception {
        SerReduceFunctionInt reducer = Integer::sum;
        LongBinaryOperator op = invokeDetector(reducer, /* asInt = */ true);
        assertNotNull(op, "Integer::sum + int-domain must yield the fast path");
    }

    /**
     * A12-H1: {@code Long::sum} with an int-domain accumulator must NOT install the fast path. The
     * detector returns {@code null}, falling through to the boxing shim — which on the actual
     * dispatch site at runtime would surface a {@link ClassCastException} when reduce(Integer,
     * Integer) is invoked but its result is cast to {@code Long}.
     */
    @Test
    void longSumWithIntDomainRejectsFastPath() throws Exception {
        SerReduceFunctionLong reducer = Long::sum;
        LongBinaryOperator op = invokeDetector(reducer, /* asInt = */ true);
        assertNull(
                op,
                "Long::sum + int-domain must fall through to the boxing shim (legacy CCE behaviour)");
    }

    /**
     * A12-H1: {@code Integer::sum} with a long-domain accumulator must NOT install the fast path
     * (otherwise the cache would silently promote int arithmetic to long and bypass the legacy
     * {@code (Long) out} CCE).
     */
    @Test
    void integerSumWithLongDomainRejectsFastPath() throws Exception {
        SerReduceFunctionInt reducer = Integer::sum;
        LongBinaryOperator op = invokeDetector(reducer, /* asInt = */ false);
        assertNull(
                op,
                "Integer::sum + long-domain must fall through to the boxing shim (legacy CCE behaviour)");
    }
}

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

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FrsIterHandle} that do NOT require a running native engine.
 *
 * <p>Integration tests with real iter open/next/close land in P5, when state primitives create
 * handles end-to-end against a real engine. Here we verify:
 *
 * <ul>
 *   <li>Class compiles and exposes the expected public surface.
 *   <li>{@link FrsIterHandle} implements {@link AutoCloseable}.
 * </ul>
 */
class FrsIterHandleTest {

    @Test
    void classImplementsAutoCloseable() {
        assertTrue(
                AutoCloseable.class.isAssignableFrom(FrsIterHandle.class),
                "FrsIterHandle must implement AutoCloseable");
    }

    @Test
    void expectedPublicMethodsExist() throws Exception {
        Class<?> cls = FrsIterHandle.class;

        // Core iterator methods
        cls.getMethod("next", MemorySegment.class);
        cls.getMethod("close");
        cls.getMethod("forceClose");
        cls.getMethod("requestClose");

        // Accessors used by IterLifetimeWatchdog
        cls.getMethod("handleId");
        cls.getMethod("nativeHandleId");
        cls.getMethod("lastNextNs");
        cls.getMethod("openedAtMs");
        cls.getMethod("closeRequested");
    }

    @Test
    void requestCloseReturnsBooleanFlag() throws Exception {
        // Verify requestClose exists and closeRequested has the right return type.
        var requestClose = FrsIterHandle.class.getMethod("requestClose");
        assertEquals(void.class, requestClose.getReturnType());

        var closeRequested = FrsIterHandle.class.getMethod("closeRequested");
        assertEquals(boolean.class, closeRequested.getReturnType());
    }

    @Test
    void handleIdAndNativeHandleIdReturnLong() throws Exception {
        assertEquals(long.class, FrsIterHandle.class.getMethod("handleId").getReturnType());
        assertEquals(long.class, FrsIterHandle.class.getMethod("nativeHandleId").getReturnType());
        assertEquals(long.class, FrsIterHandle.class.getMethod("lastNextNs").getReturnType());
        assertEquals(long.class, FrsIterHandle.class.getMethod("openedAtMs").getReturnType());
    }

    /**
     * R31-H3: verify {@code isInCall()} is exposed with the expected return type. The watchdog
     * reads this flag to skip handles currently executing a native {@code next()} call.
     */
    @Test
    void isInCallSurfaceExists() throws Exception {
        assertEquals(boolean.class, FrsIterHandle.class.getMethod("isInCall").getReturnType());
    }
}

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

import org.apache.flink.state.forstrs.cache.MapStateCache;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structural tests verifying that {@link ForStRsMapStateV2} integrates the {@link MapStateCache}:
 * the cache field is present, and async methods are overridden (not inherited from
 * {@code AbstractMapState}).
 *
 * <p>Full end-to-end cache-hit verification is exercised by the integration tests / Nexmark benches
 * (Q11/Q12) — this test only asserts the wiring is in place. Direct unit tests of the cache
 * semantics live in {@link org.apache.flink.state.forstrs.cache.MapStateCacheTest}.
 */
class ForStRsMapStateV2CacheTest {

    @Test
    void mapStateV2HasCacheField() throws Exception {
        Field f = ForStRsMapStateV2.class.getDeclaredField("cache");
        assertNotNull(f);
        assertEquals(
                MapStateCache.class,
                f.getType(),
                "ForStRsMapStateV2.cache must be a MapStateCache");
        assertTrue(
                Modifier.isPrivate(f.getModifiers()) && Modifier.isFinal(f.getModifiers()),
                "cache field must be private final");
    }

    @Test
    void asyncGetIsOverriddenInMapStateV2() throws Exception {
        Method m = ForStRsMapStateV2.class.getDeclaredMethod("asyncGet", Object.class);
        assertNotNull(m);
        // Declared directly on ForStRsMapStateV2, not inherited from AbstractMapState.
        assertSame(
                ForStRsMapStateV2.class,
                m.getDeclaringClass(),
                "asyncGet must be overridden on ForStRsMapStateV2 (not inherited)");
        assertFalse(
                Modifier.isStatic(m.getModifiers()),
                "asyncGet override must be an instance method");
    }

    @Test
    void asyncPutIsOverriddenInMapStateV2() throws Exception {
        Method m = ForStRsMapStateV2.class.getDeclaredMethod("asyncPut", Object.class, Object.class);
        assertNotNull(m);
        assertSame(
                ForStRsMapStateV2.class,
                m.getDeclaringClass(),
                "asyncPut must be overridden on ForStRsMapStateV2");
    }

    @Test
    void asyncRemoveIsOverriddenInMapStateV2() throws Exception {
        Method m = ForStRsMapStateV2.class.getDeclaredMethod("asyncRemove", Object.class);
        assertNotNull(m);
        assertSame(
                ForStRsMapStateV2.class,
                m.getDeclaringClass(),
                "asyncRemove must be overridden on ForStRsMapStateV2");
    }

    @Test
    void asyncContainsIsOverriddenInMapStateV2() throws Exception {
        Method m = ForStRsMapStateV2.class.getDeclaredMethod("asyncContains", Object.class);
        assertNotNull(m);
        assertSame(
                ForStRsMapStateV2.class,
                m.getDeclaringClass(),
                "asyncContains must be overridden on ForStRsMapStateV2");
    }

    @Test
    void serializeMapEntryKeyHelperExists() throws Exception {
        // The helper used by the override path — keeps cache-key construction separate from the
        // StateRequest-based serializeKey path so overrides can run before request building.
        Method m =
                ForStRsMapStateV2.class.getDeclaredMethod("serializeMapEntryKey", Object.class);
        assertNotNull(m);
        assertTrue(
                Modifier.isPrivate(m.getModifiers()),
                "serializeMapEntryKey must be private (internal helper)");
    }
}

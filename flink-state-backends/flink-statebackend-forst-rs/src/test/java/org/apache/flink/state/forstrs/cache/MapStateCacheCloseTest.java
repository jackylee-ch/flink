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

package org.apache.flink.state.forstrs.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * D5-H2 lifecycle coverage for {@link MapStateCache}.
 *
 * <p>The cache holds an {@link java.lang.foreign.Arena} with five off-heap segments per instance.
 * Without an explicit {@code close()} hook those segments survived to JVM exit — one perma-leak per
 * V2 MapState. This test asserts the {@link AutoCloseable} contract is wired and idempotent so
 * call sites can safely invoke close on disposal paths that may run twice.
 */
class MapStateCacheCloseTest {

    @Test
    void implementsAutoCloseable() {
        // Confirms the type is wired as AutoCloseable so try-with-resources / disposal chains
        // can rely on the standard interface.
        assertTrue(AutoCloseable.class.isAssignableFrom(MapStateCache.class));
    }

    @Test
    void closeReleasesArenaWithoutThrowing() {
        MapStateCache<String> cache = new MapStateCache<>(16);
        cache.put(new byte[] {1, 2, 3}, "a");
        cache.put(new byte[] {4, 5, 6}, "b");
        assertEquals(2, cache.size());

        assertDoesNotThrow(cache::close);
    }

    @Test
    void closeIsIdempotent() {
        // Disposal paths may be invoked multiple times (backend.dispose / backend.close /
        // operator chain teardown). The second close MUST be a benign no-op so the state class
        // can call it from both lifecycle hooks without guard flags.
        MapStateCache<Integer> cache = new MapStateCache<>(8);
        cache.put(new byte[] {1}, 42);
        cache.close();
        assertDoesNotThrow(cache::close);
    }

    @Test
    void closeOnEmptyCacheIsSafe() {
        // Tests that the close path tolerates a never-used cache instance — e.g. when an
        // operator's MapState was registered but never written.
        MapStateCache<String> cache = new MapStateCache<>();
        assertEquals(0, cache.size());
        assertDoesNotThrow(cache::close);
    }
}

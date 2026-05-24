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
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    /**
     * R40-H1: after {@link MapStateCache#close()} flips the close-gate, every public mutator that
     * touches off-heap arena segments must throw {@link IllegalStateException} with the precise
     * "MapStateCache closed" message rather than letting the FFM access surface an opaque arena-
     * closed error from a late callback. Mirrors the timer queue's R38-H2 / R39-H1 contract.
     */
    @Test
    void mutatorsThrowAfterClose() {
        MapStateCache<String> cache = new MapStateCache<>(16);
        cache.put(new byte[] {1}, "a");
        cache.close();

        byte[] key = new byte[] {1};
        byte[] prefix = new byte[] {1};

        IllegalStateException onLookup =
                assertThrows(IllegalStateException.class, () -> cache.lookup(key));
        assertEquals("MapStateCache closed", onLookup.getMessage());

        IllegalStateException onPut =
                assertThrows(IllegalStateException.class, () -> cache.put(key, "x"));
        assertEquals("MapStateCache closed", onPut.getMessage());

        IllegalStateException onRemove =
                assertThrows(IllegalStateException.class, () -> cache.remove(key));
        assertEquals("MapStateCache closed", onRemove.getMessage());

        IllegalStateException onClear =
                assertThrows(IllegalStateException.class, cache::clear);
        assertEquals("MapStateCache closed", onClear.getMessage());

        IllegalStateException onClearForPrefix =
                assertThrows(IllegalStateException.class, () -> cache.clearForPrefix(prefix));
        assertEquals("MapStateCache closed", onClearForPrefix.getMessage());

        IllegalStateException onSize =
                assertThrows(IllegalStateException.class, cache::size);
        assertEquals("MapStateCache closed", onSize.getMessage());
    }
}

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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for R22-H1 (HIGH): direct dispose() invocation (bypassing close()) must release
 * native db / defaultCf / arena resources.
 *
 * <p>Pre-fix, only {@link ForStRsAsyncKeyedStateBackend#close()} performed the native release. A
 * direct {@link ForStRsAsyncKeyedStateBackend#dispose()} call (the path
 * {@code AbstractStreamOperator#dispose} takes on task cancellation that did not go through
 * close()) tore down the slot arena, watchdog, and cancel-stream registry but left the FrsDb,
 * FrsCfHandle, and backing Arena dangling to process exit.
 *
 * <p>The fix introduces a shared {@code releaseNativeResources()} helper invoked from BOTH
 * close() and dispose(), guarded by an AtomicBoolean CAS so the close()→dispose() chain frees
 * exactly once.
 */
class DisposeReleasesNativeResourcesTest {

    @Test
    void disposeWithoutCloseReleasesNativeResources(@TempDir Path tmp) {
        Path dbDir = tmp.resolve("db");
        Arena arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        FrsDb db = linker.dbOpen(arena, dbDir.toString());
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);

        ForStRsAsyncKeyedStateBackend<Integer> backend =
                new ForStRsAsyncKeyedStateBackend<>(
                        arena,
                        linker,
                        db,
                        cf,
                        IntSerializer.INSTANCE,
                        new KeyGroupRange(0, 0),
                        /* totalKeyGroups= */ 1,
                        /* ownsResources= */ true);

        assertFalse(db.isClosed(), "db must be open before dispose()");
        assertFalse(cf.isClosed(), "cf must be open before dispose()");

        // R22-H1: direct dispose() (no preceding close()) must release native handles.
        backend.dispose();

        assertTrue(db.isClosed(), "db must be closed after dispose() — R22-H1 fix");
        assertTrue(cf.isClosed(), "cf must be closed after dispose() — R22-H1 fix");
    }

    @Test
    void closeThenDisposeIsIdempotent(@TempDir Path tmp) throws Exception {
        Path dbDir = tmp.resolve("db");
        Arena arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        FrsDb db = linker.dbOpen(arena, dbDir.toString());
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);

        ForStRsAsyncKeyedStateBackend<Integer> backend =
                new ForStRsAsyncKeyedStateBackend<>(
                        arena,
                        linker,
                        db,
                        cf,
                        IntSerializer.INSTANCE,
                        new KeyGroupRange(0, 0),
                        /* totalKeyGroups= */ 1,
                        /* ownsResources= */ true);

        // close() internally calls dispose() and reaches releaseNativeResources();
        // a subsequent direct dispose() must not double-close (CAS guard on nativeReleased).
        backend.close();
        assertTrue(db.isClosed(), "db must be closed after close()");
        assertTrue(cf.isClosed(), "cf must be closed after close()");

        // Idempotent — second dispose() observes nativeReleased=true and skips native frees.
        backend.dispose();
        assertTrue(db.isClosed(), "db must remain closed after redundant dispose()");
        assertTrue(cf.isClosed(), "cf must remain closed after redundant dispose()");
    }
}

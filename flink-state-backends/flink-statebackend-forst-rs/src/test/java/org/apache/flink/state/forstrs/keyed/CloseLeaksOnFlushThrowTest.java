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
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R24-H1 (HIGH): {@link ForStRsKeyedStateBackend#close()} previously set {@code closed=true}
 * BEFORE running the flush chain ({@code flushAllOffHeapValueStateBuffers},
 * {@code flushAllMapStates}, {@code flushWriteBuffer}) WITHOUT a try/finally guard around the
 * resource-release block. A throw from any flush leaked the slot arena scope, defaultCf, db, and
 * backing arena because {@code closed=true} blocked any retry via dispose().
 *
 * <p>This test injects a throw into {@code flushWriteBuffer} via a backend subclass and asserts
 * that the native resources (db and cf) are still released — i.e. the try/finally chain in
 * close() ran the resource block despite the flush failure.
 */
class CloseLeaksOnFlushThrowTest {

    @Test
    void closeReleasesNativeResourcesWhenFlushThrows() throws Exception {
        Arena arena = Arena.ofShared();
        ForStRsLinker linker = new ForStRsLinker(arena);
        FrsDb db = linker.dbOpenMemory(arena);
        FrsCfHandle cf = linker.dbDefaultCf(db, arena);

        ForStRsKeyedStateBackend<Integer> backend =
                new ForStRsKeyedStateBackend<>(
                        arena, linker, db, cf, IntSerializer.INSTANCE,
                        /* ownsResources= */ true) {
                    @Override
                    public void flushWriteBuffer() {
                        // R24-H1: simulate an engine-side flush error that pre-fix would tear
                        // down the close() chain partway through, leaking the native handles.
                        throw new RuntimeException("simulated flush failure (R24-H1 regression)");
                    }
                };

        assertFalse(db.isClosed(), "db must be open before close()");
        assertFalse(cf.isClosed(), "cf must be open before close()");

        // R24-H1: close() must propagate the flush throw — the caller needs to know the close
        // observed a flush failure — but it must STILL release the native handles in the
        // finally block. The injected throw is RuntimeException; close() rethrows
        // RuntimeException unchanged (per the rethrow-by-kind block in close()).
        RuntimeException re =
                assertThrows(
                        RuntimeException.class,
                        backend::close,
                        "close() must propagate the flush failure");
        assertTrue(
                re.getMessage() != null && re.getMessage().contains("simulated flush failure"),
                "exception message should mention the simulated flush failure: " + re.getMessage());

        // The critical assertion: despite the flush throw, the finally-block ran the
        // resource-release chain and the native handles are closed (no leak).
        assertTrue(db.isClosed(), "db must be closed after close() — R24-H1 fix");
        assertTrue(cf.isClosed(), "cf must be closed after close() — R24-H1 fix");
    }
}

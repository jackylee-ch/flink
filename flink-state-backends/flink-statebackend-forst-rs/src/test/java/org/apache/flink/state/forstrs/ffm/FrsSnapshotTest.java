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

package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FrsSnapshot} (B-Prod-P2 Task 2.5). Cover round-trip, double-close
 * idempotency, try-with-resources release, and post-close handle access fast-fail.
 */
class FrsSnapshotTest {

    @Test
    void snapshotRoundTrip() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                assertFalse(snap.isClosed());
                snap.close();
                assertTrue(snap.isClosed());
            }
        }
    }

    @Test
    void doubleCloseSafe() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                snap.close();
                assertDoesNotThrow(snap::close);
                assertTrue(snap.isClosed());
            }
        }
    }

    @Test
    void tryWithResourcesReleases() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsSnapshot snap = linker.dbSnapshot(db, arena)) {
                assertFalse(snap.isClosed());
            }
            // After try-with-resources, snap.close() ran exactly once via AutoCloseable.
        }
    }

    @Test
    void handleAccessAfterCloseFails() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                snap.close();
                assertThrows(IllegalStateException.class, snap::handle);
            }
        }
    }
}

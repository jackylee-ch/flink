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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.state.ForStRsMapState;
import org.apache.flink.state.forstrs.state.ForStRsValueState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Snapshot / restore + key-iteration tests for {@link ForStRsKeyedStateBackend}.
 *
 * <p>These exercise the Phase-D L5 simplified snapshot API ( {@link
 * ForStRsKeyedStateBackend#snapshot(Path)} + {@link
 * ForStRsKeyedStateBackend#restoreFromSnapshot(ForStRsLinker, Arena, Path,
 * org.apache.flink.api.common.typeutils.TypeSerializer)} ) and the {@code keys} / {@code
 * applyToAllKeys} surface that's the precursor to Flink's per-key-group iteration.
 */
class ForStRsKeyedStateBackendSnapshotTest {

    /**
     * Constructs an on-disk backend rooted at {@code dbPath} that <i>shares</i> the supplied
     * arena+linker. Closing the returned backend releases the {@link FrsDb}+default-CF but leaves
     * the arena/linker untouched — matching the contract used by {@link
     * ForStRsKeyedStateBackend#restoreFromSnapshot(ForStRsLinker, Arena, Path,
     * org.apache.flink.api.common.typeutils.TypeSerializer)}.
     */
    private static ForStRsKeyedStateBackend<String> openOnDisk(
            ForStRsLinker linker, Arena arena, Path dbPath) {
        FrsDb db = linker.dbOpen(arena, dbPath.toString());
        FrsCfHandle cf;
        try {
            cf = linker.dbDefaultCf(db, arena);
        } catch (RuntimeException e) {
            db.close();
            throw e;
        }
        return new ForStRsKeyedStateBackend<>(
                arena, linker, db, cf, StringSerializer.INSTANCE, /* ownsResources= */ false);
    }

    @Test
    void testSnapshotRestoreValueState(@TempDir Path tmp) throws Exception {
        Path dbDir = tmp.resolve("db");
        Path snapDir = tmp.resolve("snap");

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);

            // Phase 1: open a fresh backend, write under "alice", snapshot, dispose.
            ForStRsKeyedStateBackend<String> writer = openOnDisk(linker, arena, dbDir);
            writer.setCurrentKey("alice");
            ForStRsValueState<String> v = writer.getValueState("secret", StringSerializer.INSTANCE);
            v.update("secret-value");
            assertEquals("secret-value", v.value());

            Path snapshotPath = writer.snapshot(snapDir);
            assertEquals(snapDir, snapshotPath);
            writer.dispose();

            // Phase 2: restore from snapshot and verify the value survived.
            ForStRsKeyedStateBackend<String> restored =
                    ForStRsKeyedStateBackend.restoreFromSnapshot(
                            linker, arena, snapDir, StringSerializer.INSTANCE);
            try {
                restored.setCurrentKey("alice");
                ForStRsValueState<String> rv =
                        restored.getValueState("secret", StringSerializer.INSTANCE);
                assertEquals("secret-value", rv.value(), "value must survive snapshot+restore");
            } finally {
                restored.dispose();
            }
        }
    }

    @Test
    void testSnapshotRestoreMapState(@TempDir Path tmp) throws Exception {
        Path dbDir = tmp.resolve("db");
        Path snapDir = tmp.resolve("snap");

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);

            ForStRsKeyedStateBackend<String> writer = openOnDisk(linker, arena, dbDir);
            writer.setCurrentKey("bob");
            ForStRsMapState<String, Integer> m =
                    writer.getMapState("scores", StringSerializer.INSTANCE, IntSerializer.INSTANCE);
            m.put("python", 1);
            m.put("rust", 2);
            m.put("java", 3);
            assertEquals(Integer.valueOf(1), m.get("python"));

            writer.snapshot(snapDir);
            writer.dispose();

            ForStRsKeyedStateBackend<String> restored =
                    ForStRsKeyedStateBackend.restoreFromSnapshot(
                            linker, arena, snapDir, StringSerializer.INSTANCE);
            try {
                restored.setCurrentKey("bob");
                ForStRsMapState<String, Integer> rm =
                        restored.getMapState(
                                "scores", StringSerializer.INSTANCE, IntSerializer.INSTANCE);
                assertEquals(Integer.valueOf(1), rm.get("python"));
                assertEquals(Integer.valueOf(2), rm.get("rust"));
                assertEquals(Integer.valueOf(3), rm.get("java"));
            } finally {
                restored.dispose();
            }
        }
    }

    @Test
    void testKeysIterator(@TempDir Path tmp) throws Exception {
        Path dbDir = tmp.resolve("db");
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            ForStRsKeyedStateBackend<String> backend = openOnDisk(linker, arena, dbDir);
            try {
                for (String k : new String[] {"alice", "bob", "charlie"}) {
                    backend.setCurrentKey(k);
                    backend.getValueState("counter", IntSerializer.INSTANCE).update(1);
                }

                Iterator<String> iter = backend.keys("counter");
                Set<String> seen = new HashSet<>();
                while (iter.hasNext()) {
                    seen.add(iter.next());
                }
                assertEquals(Set.of("alice", "bob", "charlie"), seen);
            } finally {
                backend.dispose();
            }
        }
    }

    @Test
    void testApplyToAllKeys(@TempDir Path tmp) throws Exception {
        Path dbDir = tmp.resolve("db");
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            ForStRsKeyedStateBackend<String> backend = openOnDisk(linker, arena, dbDir);
            try {
                for (String k : new String[] {"alice", "bob", "charlie"}) {
                    backend.setCurrentKey(k);
                    backend.getValueState("counter", IntSerializer.INSTANCE).update(1);
                }

                List<String> collected = new ArrayList<>();
                backend.applyToAllKeys(
                        "counter",
                        k -> {
                            collected.add(k);
                            return null;
                        });
                assertEquals(
                        3, collected.size(), "applyToAllKeys must visit each key exactly once");
                assertEquals(
                        Set.of("alice", "bob", "charlie"),
                        new HashSet<>(collected),
                        "applyToAllKeys must visit every written key");
                assertNotNull(backend.getCurrentKey(), "applyToAllKeys must restore prior key");
                assertTrue(
                        Set.of("alice", "bob", "charlie").contains(backend.getCurrentKey()),
                        "current key after applyToAllKeys must be one of the written keys");
            } finally {
                backend.dispose();
            }
        }
    }
}

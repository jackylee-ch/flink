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

package org.apache.flink.state.forstrs.migration;

import org.apache.flink.state.forstrs.FrsBackendException;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Acceptance tests for {@link ForStRsStateMigration} (B-Prod-P10, spec §6g).
 *
 * <p>The headline test, {@link #export10kKeysImportRoundTrip}, satisfies the §16 acceptance
 * criterion attached to PR B-Prod-P10: write 10k keys into a source CF, export to disk, import as a
 * brand-new CF in the same engine, read every key back and confirm the value matches. The remaining
 * tests exercise edge cases (empty CF, missing blob, duplicate CF name).
 *
 * <p>Requires the system property {@code forstrs.native.libpath} pointing to {@code
 * libforst_rs_ffi.{dylib,so,dll}}; the module's surefire config sets this from the cdylib built by
 * the cargo job (see {@code pom.xml}).
 */
class ForStRsStateMigrationTest {

    private static final String SOURCE_CF = "migration-source";
    private static final String IMPORTED_CF = "migration-imported";

    /**
     * §16 acceptance criterion: 10k-key round-trip across an export/import boundary on a single
     * shared engine. Mirrors the engine-level Rust test
     * {@code crates/forst-rs-engine/tests/cf_import_export_it.rs::cf_export_then_import_round_trips_10k_keys}
     * but exercises the full Java→FFM→Rust call path.
     */
    @Test
    void export10kKeysImportRoundTrip(@TempDir Path tempDir) {
        Path exportDir = tempDir.resolve("export");

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle src = linker.dbCreateCf(db, arena, SOURCE_CF)) {

                // 1. Write 10k keys into the source CF via the standard FFM
                // put surface — exactly the path Flink uses for normal
                // state writes.
                final int n = 10_000;
                for (int i = 0; i < n; i++) {
                    byte[] key = String.format("k-%08d", i).getBytes(StandardCharsets.UTF_8);
                    byte[] value = String.format("v-%08d", i).getBytes(StandardCharsets.UTF_8);
                    linker.put(db, src, key, value);
                }

                // 2. Export — produces a single self-describing blob.
                Path blobPath = ForStRsStateMigration.exportColumnFamily(db, src, exportDir);
                assertTrue(Files.exists(blobPath), "EXPORT.frsblob must exist after export");
                long blobSize = Files.size(blobPath);
                // 8B magic + 8B name_len + name (16) + 10k * (4 + 11 + 4 + 11)
                // ≈ 32 + 300_000 bytes. Lower-bound check is enough.
                assertTrue(
                        blobSize > 250_000L,
                        () -> "EXPORT.frsblob smaller than expected for 10k keys: " + blobSize);

                // 3. Import as a NEW CF on the same engine.
                try (FrsCfHandle imported =
                        ForStRsStateMigration.createColumnFamilyFromImport(
                                db, arena, IMPORTED_CF, exportDir)) {
                    assertNotNull(imported);

                    // 4. Verify every key is readable from the imported CF
                    // with the original value.
                    for (int i = 0; i < n; i++) {
                        byte[] key = String.format("k-%08d", i).getBytes(StandardCharsets.UTF_8);
                        byte[] expected =
                                String.format("v-%08d", i).getBytes(StandardCharsets.UTF_8);
                        byte[] got = linker.get(db, imported, key);
                        assertNotNull(got, () -> "imported CF missing key idx=" + key.length);
                        assertArrayEquals(expected, got, "value mismatch for key index " + i);
                    }

                    // 5. Sanity: imported CF must NOT return values for
                    // keys absent from the source set.
                    assertNull(
                            linker.get(
                                    db,
                                    imported,
                                    "never-written".getBytes(StandardCharsets.UTF_8)),
                            "imported CF should not return values for unrelated keys");
                }
            }
        } catch (java.io.IOException e) {
            throw new AssertionError("blob filesystem check failed", e);
        }
    }

    /** Empty CF still produces a valid (header-only) blob and an empty importable CF. */
    @Test
    void exportEmptyCfProducesValidImportableBlob(@TempDir Path tempDir) {
        Path exportDir = tempDir.resolve("export-empty");

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle src = linker.dbCreateCf(db, arena, "empty-src")) {
                Path blob = ForStRsStateMigration.exportColumnFamily(db, src, exportDir);
                assertTrue(Files.exists(blob));

                try (FrsCfHandle imported =
                        ForStRsStateMigration.createColumnFamilyFromImport(
                                db, arena, "empty-dst", exportDir)) {
                    // No keys → every read must miss.
                    assertNull(linker.get(db, imported, "any".getBytes(StandardCharsets.UTF_8)));
                }
            }
        }
    }

    /** Importing under an existing CF name must fail rather than silently overwrite. */
    @Test
    void importIntoExistingCfNameFails(@TempDir Path tempDir) {
        Path exportDir = tempDir.resolve("export-dupe");

        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle src = linker.dbCreateCf(db, arena, "dupe-src")) {
                linker.put(db, src, "k".getBytes(StandardCharsets.UTF_8),
                        "v".getBytes(StandardCharsets.UTF_8));
                ForStRsStateMigration.exportColumnFamily(db, src, exportDir);

                // Try to import under the SAME name — must throw.
                FrsBackendException ex =
                        assertThrows(
                                FrsBackendException.class,
                                () ->
                                        ForStRsStateMigration.createColumnFamilyFromImport(
                                                db, arena, "dupe-src", exportDir));
                assertNotNull(ex.getMessage());
            }
        }
    }

    /** Missing blob must surface as a backend exception, not a silent empty CF. */
    @Test
    void importFromMissingBlobFails(@TempDir Path tempDir) {
        // Intentionally do NOT create a blob under tempDir.
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                assertThrows(
                        FrsBackendException.class,
                        () ->
                                ForStRsStateMigration.createColumnFamilyFromImport(
                                        db, arena, "should-not-exist", tempDir));
            }
        }
    }

    /** Null-argument contract: every public entry point must NPE on null. */
    @Test
    void nullArgumentsRejected(@TempDir Path tempDir) {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                assertThrows(
                        NullPointerException.class,
                        () -> ForStRsStateMigration.exportColumnFamily(null, cf, tempDir));
                assertThrows(
                        NullPointerException.class,
                        () -> ForStRsStateMigration.exportColumnFamily(db, null, tempDir));
                assertThrows(
                        NullPointerException.class,
                        () -> ForStRsStateMigration.exportColumnFamily(db, cf, null));
                assertThrows(
                        NullPointerException.class,
                        () ->
                                ForStRsStateMigration.createColumnFamilyFromImport(
                                        null, arena, "x", tempDir));
                assertThrows(
                        NullPointerException.class,
                        () ->
                                ForStRsStateMigration.createColumnFamilyFromImport(
                                        db, null, "x", tempDir));
                assertThrows(
                        NullPointerException.class,
                        () ->
                                ForStRsStateMigration.createColumnFamilyFromImport(
                                        db, arena, null, tempDir));
                assertThrows(
                        NullPointerException.class,
                        () ->
                                ForStRsStateMigration.createColumnFamilyFromImport(
                                        db, arena, "x", null));
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                ForStRsStateMigration.createColumnFamilyFromImport(
                                        db, arena, "", tempDir));
            }
        }
    }

    /** Exposed constant must match the on-disk filename produced by the engine. */
    @Test
    void blobNameConstantMatchesEngine() {
        assertEquals("EXPORT.frsblob", ForStRsStateMigration.EXPORT_BLOB_NAME);
    }
}

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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * State import / export migration API (B-Prod-P10, spec §6g).
 *
 * <p>Cross-job state transfer for ForSt-RS column families. The producer side calls {@link
 * #exportColumnFamily(FrsDb, FrsCfHandle, Path)} to dump every live (key, value) pair in a CF to a
 * single self-describing blob ({@code EXPORT.frsblob}) under an export directory; the directory can
 * then be shipped to another job (S3, NFS, manual scp, etc.). The consumer side calls {@link
 * #createColumnFamilyFromImport(FrsDb, Arena, String, Path)} to reconstitute the CF state under a
 * brand-new CF name.
 *
 * <p>The on-disk blob format (8-byte magic {@code FRSEXP01} + length-prefixed cf-name + repeated
 * length-prefixed key/value entries) is documented in the engine source — {@code
 * crates/forst-rs-engine/src/db.rs::cf_export}. Both endpoints route through the FFM {@link
 * ForStRsLinker}; no JNI is involved.
 *
 * <p><b>Atomicity</b>: the export is "snapshot-at-call-time" — only writes committed before {@link
 * #exportColumnFamily} took the underlying scan are guaranteed to appear in the resulting blob.
 * Concurrent writers are not blocked. The consumer side creates the new CF and replays entries via
 * the standard put path, so import is observable as a series of atomic {@code put}s (no in-flight
 * readers will see the CF until the import call returns).
 *
 * <p><b>Tombstones</b> are intentionally not preserved across export — the consumer is creating a
 * brand-new CF, where a "tombstone" with no prior version is a no-op.
 */
@PublicEvolving
public final class ForStRsStateMigration {

    /** Filename written under the export directory; documented for callers staging blobs. */
    public static final String EXPORT_BLOB_NAME = "EXPORT.frsblob";

    private ForStRsStateMigration() {
        // utility class
    }

    /**
     * Exports every live (key, value) pair from {@code cf} to {@code exportDir/EXPORT.frsblob}. The
     * directory is created if it does not exist. Returns the absolute {@link Path} to the blob the
     * consumer side will need.
     *
     * <p>Thread-safety: safe to call from any thread; concurrent writers to {@code cf} are not
     * blocked but only writes that committed before this call took the underlying scan are
     * guaranteed to appear in the export.
     *
     * @param db open ForSt-RS database (must outlive this call)
     * @param cf source column family handle (must outlive this call)
     * @param exportDir directory to receive {@code EXPORT.frsblob}; created if missing
     * @return the absolute path to the written blob
     * @throws NullPointerException if any argument is null
     * @throws org.apache.flink.state.forstrs.FrsBackendException if the native call fails
     */
    public static Path exportColumnFamily(FrsDb db, FrsCfHandle cf, Path exportDir) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(cf, "cf");
        Objects.requireNonNull(exportDir, "exportDir");
        Path absoluteDir = exportDir.toAbsolutePath();
        // The native side mkdir-p's on its own, but creating it Java-side
        // first surfaces filesystem errors with a Java stack trace before
        // we cross the FFM boundary.
        try {
            Files.createDirectories(absoluteDir);
        } catch (java.io.IOException e) {
            throw new RuntimeException(
                    "ForStRsStateMigration: failed to create export directory " + absoluteDir, e);
        }
        db.linker().cfExport(db, cf, absoluteDir.toString());
        return absoluteDir.resolve(EXPORT_BLOB_NAME);
    }

    /**
     * Creates a new column family named {@code newCfName} in {@code db} and seeds it with every row
     * from {@code importDir/EXPORT.frsblob}. The original CF name embedded in the blob is ignored —
     * Flink callers re-namespace state when migrating between jobs.
     *
     * <p>The returned {@link FrsCfHandle} must be closed by the caller (the standard
     * try-with-resources pattern is fine). If a CF named {@code newCfName} already exists in {@code
     * db}, the call fails with {@code INVALID_ARGUMENT} and no state is imported.
     *
     * @param db destination ForSt-RS database (must outlive this call and the returned handle)
     * @param arena arena owning the returned CF handle's address slot allocation
     * @param newCfName brand-new CF name (must not already exist in {@code db})
     * @param importDir directory containing {@code EXPORT.frsblob}
     * @return a CF handle pre-populated with the imported entries
     * @throws NullPointerException if any argument is null
     * @throws org.apache.flink.state.forstrs.FrsBackendException if the blob is missing /
     *     malformed, the CF name is already in use, or the underlying replay puts fail
     */
    public static FrsCfHandle createColumnFamilyFromImport(
            FrsDb db, Arena arena, String newCfName, Path importDir) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(arena, "arena");
        Objects.requireNonNull(newCfName, "newCfName");
        Objects.requireNonNull(importDir, "importDir");
        if (newCfName.isEmpty()) {
            throw new IllegalArgumentException("newCfName must not be empty");
        }
        Path absoluteDir = importDir.toAbsolutePath();
        return db.linker().dbCreateCfFromImport(db, arena, newCfName, absoluteDir.toString());
    }

    /**
     * Drops a column family by handle (B-Prod-followup-5, spec §6g).
     *
     * <p>After this call returns, the CF is unusable from any handle held against it (subsequent
     * native operations fail with {@code INVALID_ARGUMENT}). The {@link FrsCfHandle} itself is NOT
     * closed — callers are still responsible for {@code cf.close()} to release the FFM allocation.
     *
     * <p>Combined with {@link #createColumnFamilyFromImport}, this enables same-CF-name migration:
     * drop the old CF, then re-import under the same name. Idempotent on an already-dropped CF
     * (returns silently); rejects the default CF with {@code INVALID_ARGUMENT}.
     *
     * @param db open ForSt-RS database (must outlive this call)
     * @param cf column-family handle to drop (any clone of the handle observes the drop)
     * @throws NullPointerException if any argument is null
     * @throws org.apache.flink.state.forstrs.FrsBackendException if the native call returns a
     *     non-OK status (e.g. caller attempted to drop the default CF)
     */
    public static void dropColumnFamily(FrsDb db, FrsCfHandle cf) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(cf, "cf");
        db.linker().dbDropCf(db, cf);
    }

    /**
     * Ingests pre-built SST files into the engine's L0 layer under {@code cf} (B-Prod-followup-5,
     * spec §6g).
     *
     * <p>This is the fast-path counterpart to {@link #createColumnFamilyFromImport}: instead of
     * scanning every entry in the source CF and replaying via {@code put} (O(key-count)), the
     * engine hardlinks (or copies cross-FS) the source SSTs into its own SST directory and
     * registers them at L0 via a single atomic version edit (O(file-count)). See {@code
     * crates/forst-rs-engine/src/db.rs::ingest_external_sst} for the caller contract — in
     * particular, the source SSTs MUST be readable by ForSt-RS's SST reader and the key ranges
     * SHOULD NOT overlap with non-L0 keys already present in the target.
     *
     * <p>Passing an empty or null {@code sstPaths} is a no-op.
     *
     * @param db destination ForSt-RS database (must outlive this call)
     * @param cf destination CF handle (must outlive this call)
     * @param sstPaths absolute paths to source SST files (each must exist for the duration of the
     *     call; the engine takes no ownership)
     * @throws NullPointerException if {@code db} or {@code cf} is null
     * @throws org.apache.flink.state.forstrs.FrsBackendException if hardlink+copy fails, an SST
     *     cannot be parsed, or the engine rejects the CF handle
     */
    public static void ingestExternalSst(FrsDb db, FrsCfHandle cf, java.util.List<Path> sstPaths) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(cf, "cf");
        if (sstPaths == null || sstPaths.isEmpty()) {
            return;
        }
        java.util.List<String> abs = new java.util.ArrayList<>(sstPaths.size());
        for (Path p : sstPaths) {
            Objects.requireNonNull(p, "sstPaths element");
            abs.add(p.toAbsolutePath().toString());
        }
        db.linker().dbIngestExternalSst(db, cf, abs);
    }
}

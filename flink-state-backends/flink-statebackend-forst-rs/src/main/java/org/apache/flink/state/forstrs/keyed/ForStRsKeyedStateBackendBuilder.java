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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.forstrs.ForStRsOptions;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.cf.CfRouter;
import org.apache.flink.state.forstrs.keyed.cf.PerStateCfRouter;
import org.apache.flink.state.forstrs.keyed.cf.SingleCfRouter;

import java.lang.foreign.Arena;
import java.util.Objects;

/**
 * Builder for {@link ForStRsKeyedStateBackend} resources. Currently exposes only the {@link
 * CfRouter} factory; future PRs add snapshot strategy, restore operation, etc.
 */
public final class ForStRsKeyedStateBackendBuilder<K> {

    private final ForStRsLinker linker;
    private final Arena arena;
    private final TypeSerializer<K> keySerializer;
    private final ForStRsOptions options;

    private FrsDb db;
    private FrsCfHandle defaultCf;

    public ForStRsKeyedStateBackendBuilder(
            ForStRsLinker linker,
            Arena arena,
            TypeSerializer<K> keySerializer,
            ForStRsOptions options) {
        this.linker = Objects.requireNonNull(linker);
        this.arena = Objects.requireNonNull(arena);
        this.keySerializer = Objects.requireNonNull(keySerializer);
        this.options = Objects.requireNonNull(options);
    }

    public ForStRsKeyedStateBackendBuilder<K> withDb(FrsDb db, FrsCfHandle defaultCf) {
        this.db = db;
        this.defaultCf = defaultCf;
        return this;
    }

    /**
     * Opens the engine based on {@link ForStRsOptions#storageUri()} (B-Prod-P6).
     *
     * <p>If {@code storage.uri} is set, the backend opens via {@link ForStRsLinker#dbOpenRemote} on
     * the configured OpenDAL URI with a local LRU SST cache rooted at {@link
     * ForStRsOptions#cacheDir()}. Otherwise it falls back to {@link ForStRsLinker#dbOpen} on {@code
     * localPath} (the legacy local-FS path).
     *
     * <p>The returned {@link FrsDb} is also stored on this builder so subsequent calls to {@link
     * #buildCfRouter()} pick it up. The matching default CF is opened automatically.
     */
    public ForStRsKeyedStateBackendBuilder<K> openDb(String localPath) {
        FrsDb opened;
        String uri = options.storageUri();
        if (uri != null && !uri.isEmpty()) {
            String cacheDir = options.cacheDir();
            if (cacheDir == null || cacheDir.isEmpty()) {
                throw new IllegalArgumentException(
                        "state.backend.forst-rs.storage.cache-dir must be set when storage.uri is set");
            }
            opened =
                    linker.dbOpenRemote(
                            arena,
                            uri,
                            options.opendalConfigJson(),
                            cacheDir,
                            options.cacheCapacityBytes());
        } else {
            opened =
                    linker.dbOpenWithOptions(
                            arena,
                            localPath,
                            512L * 1024
                                    * 1024, // write_buffer_size: 512MB (fits 1M+ keys in memtable)
                            3, // max_write_buffer_number
                            4, // max_background_compactions
                            2, // max_background_flushes
                            256L * 1024 * 1024, // block_cache_capacity_bytes
                            options.writeBufferManagerCapacityBytes());
        }
        // R16-M4: wrap dbDefaultCf in try/catch so a failure between dbOpen and CF
        // attachment doesn't leak the opened DB. Pre-fix, if dbDefaultCf threw the engine
        // handle from {@code linker.dbOpen} (or {@code linker.dbOpenRemote}) would be
        // dropped on the floor — the native FrsDb stays alive until process exit because
        // its close path is only driven by {@link FrsDb#close()} which never gets called.
        FrsCfHandle cf;
        try {
            cf = linker.dbDefaultCf(opened, arena);
        } catch (Throwable t) {
            try {
                opened.close();
            } catch (Throwable closeErr) {
                // Best-effort cleanup on partial-failure path. Suppress the close error
                // so the primary failure (dbDefaultCf) reaches the caller intact —
                // diagnosis of why the CF could not be attached is more useful than the
                // secondary cleanup failure.
                t.addSuppressed(closeErr);
            }
            throw t;
        }
        return withDb(opened, cf);
    }

    public CfRouter buildCfRouter() {
        if (db == null || defaultCf == null) {
            throw new IllegalStateException(
                    "withDb(db, defaultCf) or openDb(localPath) must be called first");
        }
        return switch (options.cfMode()) {
            case SINGLE -> new SingleCfRouter(defaultCf);
            case PER_STATE -> new PerStateCfRouter(linker, db, arena);
        };
    }

    /** Returns the opened FrsDb (or null if neither {@link #withDb} nor {@link #openDb} ran). */
    public FrsDb db() {
        return db;
    }

    /** Returns the default CF handle (or null if not yet opened). */
    public FrsCfHandle defaultCf() {
        return defaultCf;
    }

    public ForStRsLinker linker() {
        return linker;
    }

    public Arena arena() {
        return arena;
    }

    public TypeSerializer<K> keySerializer() {
        return keySerializer;
    }

    public ForStRsOptions options() {
        return options;
    }
}

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

package org.apache.flink.state.forstrs;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configuration options for {@link ForStRsStateBackend}. */
public final class ForStRsOptions {

    /** Optional override for the cdylib path (defaults to System.loadLibrary). */
    public static final ConfigOption<String> NATIVE_LIB_PATH =
            ConfigOptions.key("state.backend.forstrs.native-lib-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Absolute path to libforst_rs_ffi.{dylib,so,dll}. "
                                    + "If unset, java.library.path is used via System.loadLibrary.");

    /** ConfigOption for the cf.mode flag (single | per-state). */
    public static final ConfigOption<String> CF_MODE =
            ConfigOptions.key("state.backend.forst-rs.cf.mode")
                    .stringType()
                    .defaultValue("single")
                    .withDescription(
                            "Column-family routing mode for keyed states. 'single' (default): all"
                                    + " state names share the default CF (lowest engine overhead)."
                                    + " 'per-state': each state name gets its own CF (lazy"
                                    + " creation, soft limit 256 CFs).");

    // ----- B-Prod-P6: disaggregated remote storage -----

    /** OpenDAL URI for the remote storage backend (e.g. {@code s3://bucket/}). */
    public static final ConfigOption<String> STORAGE_URI =
            ConfigOptions.key("state.backend.forst-rs.storage.uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "If set, the keyed-state backend opens the engine via OpenDAL on this"
                                    + " URI (memory://, file:///abs/path, or s3://bucket/). When"
                                    + " unset, the backend falls back to a local-FS engine at the"
                                    + " usual data directory.");

    /**
     * Flat JSON object of OpenDAL backend-specific config (e.g. {@code
     * {"region":"us-east-1","endpoint":"..."}}).
     */
    public static final ConfigOption<String> OPENDAL_CONFIG =
            ConfigOptions.key("state.backend.forst-rs.storage.opendal-config")
                    .stringType()
                    .defaultValue("{}")
                    .withDescription(
                            "Flat JSON object holding OpenDAL backend-specific configuration."
                                    + " Keys must match the underlying service builder field names"
                                    + " (e.g. region, endpoint, access_key_id, secret_access_key for"
                                    + " s3). Ignored for memory:// and file:// URIs.");

    /** Local directory used for the SST LRU cache when {@link #STORAGE_URI} is set. */
    public static final ConfigOption<String> CACHE_DIR =
            ConfigOptions.key("state.backend.forst-rs.storage.cache-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Local directory used by the SST LRU cache that fronts remote storage."
                                    + " Required when storage.uri is set.");

    /** Total LRU cache budget on local disk, in MiB. */
    public static final ConfigOption<Long> CACHE_CAPACITY_MB =
            ConfigOptions.key("state.backend.forst-rs.storage.cache-capacity-mb")
                    .longType()
                    .defaultValue(1024L)
                    .withDescription(
                            "Total bytes (MiB) the SST LRU cache may occupy on local disk."
                                    + " Default 1 GiB. A value of 0 disables the cache (every read"
                                    + " hits the remote backend).");

    private CfMode cfMode = CfMode.SINGLE;
    private String storageUri;
    private String opendalConfigJson = "{}";
    private String cacheDir;
    private long cacheCapacityMb = 1024L;

    // B-Prod-P7 §6d: shared LRU block cache + cross-CF WriteBufferManager.
    // Defaults match the design doc (256 MiB cache, 512 MiB WBM); 0 means
    // "use the engine default" (same value, but skips the FFI wire-up).
    private long blockCacheCapacityBytes = 256L * 1024 * 1024;
    private long writeBufferManagerCapacityBytes = 512L * 1024 * 1024;

    public ForStRsOptions() {}

    public CfMode cfMode() {
        return cfMode;
    }

    public ForStRsOptions cfMode(CfMode m) {
        this.cfMode = m;
        return this;
    }

    /** OpenDAL URI for remote storage; {@code null} means use the local data directory. */
    public String storageUri() {
        return storageUri;
    }

    public ForStRsOptions storageUri(String uri) {
        this.storageUri = uri;
        return this;
    }

    /** Flat JSON object of OpenDAL config; never null (defaults to {@code "{}"}). */
    public String opendalConfigJson() {
        return opendalConfigJson;
    }

    public ForStRsOptions opendalConfigJson(String json) {
        this.opendalConfigJson = json == null || json.isEmpty() ? "{}" : json;
        return this;
    }

    /** Local cache directory (must be set when {@link #storageUri()} is non-null). */
    public String cacheDir() {
        return cacheDir;
    }

    public ForStRsOptions cacheDir(String dir) {
        this.cacheDir = dir;
        return this;
    }

    /** LRU cache capacity in MiB; defaults to 1 GiB. */
    public long cacheCapacityMb() {
        return cacheCapacityMb;
    }

    public ForStRsOptions cacheCapacityMb(long mb) {
        if (mb < 0) {
            throw new IllegalArgumentException("cacheCapacityMb must be >= 0, got " + mb);
        }
        // R17-M2: guard against MiB → bytes overflow. The conversion in
        // {@link #cacheCapacityBytes()} multiplies by 1024 * 1024; without this check, an
        // adversarial {@code mb == 9007199254741L} would overflow to a NEGATIVE byte count and
        // the engine would reject the cache request with an obscure error. Validate at the
        // setter so misconfiguration fails fast with a clear message.
        long maxMb = Long.MAX_VALUE / (1024L * 1024L);
        if (mb > maxMb) {
            throw new IllegalArgumentException(
                    "cacheCapacityMb="
                            + mb
                            + " exceeds the maximum representable in bytes (max="
                            + maxMb
                            + " MiB = "
                            + (maxMb * 1024L * 1024L)
                            + " bytes)");
        }
        this.cacheCapacityMb = mb;
        return this;
    }

    /**
     * Convenience: cache capacity converted to bytes for the FFI call.
     *
     * <p>R17-M2: the setter validates {@code mb <= Long.MAX_VALUE / (1024 * 1024)} so this
     * multiplication can never overflow.
     */
    public long cacheCapacityBytes() {
        return cacheCapacityMb * 1024L * 1024L;
    }

    /**
     * Returns the shared LRU block cache capacity in bytes (B-Prod-P7, spec §6d). Default: 256 MiB.
     * {@code 0} means "use the engine default".
     */
    public long blockCacheCapacityBytes() {
        return blockCacheCapacityBytes;
    }

    public ForStRsOptions blockCacheCapacityBytes(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException(
                    "blockCacheCapacityBytes must be >= 0, got " + bytes);
        }
        this.blockCacheCapacityBytes = bytes;
        return this;
    }

    /**
     * Returns the cross-CF WriteBufferManager capacity in bytes (B-Prod-P7, spec §6d). Default: 512
     * MiB. {@code 0} disables the cross-CF cap (each CF still respects its own {@code
     * write_buffer_size}).
     */
    public long writeBufferManagerCapacityBytes() {
        return writeBufferManagerCapacityBytes;
    }

    public ForStRsOptions writeBufferManagerCapacityBytes(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException(
                    "writeBufferManagerCapacityBytes must be >= 0, got " + bytes);
        }
        this.writeBufferManagerCapacityBytes = bytes;
        return this;
    }

    /** Column-family routing mode. */
    public enum CfMode {
        SINGLE("single"),
        PER_STATE("per-state");

        private final String configValue;

        CfMode(String configValue) {
            this.configValue = configValue;
        }

        public String configValue() {
            return configValue;
        }

        public static CfMode fromConfig(String value) {
            if (value == null || value.isEmpty()) {
                return SINGLE;
            }
            for (CfMode m : values()) {
                if (m.configValue.equals(value)) {
                    return m;
                }
            }
            throw new IllegalArgumentException(
                    "Unknown state.backend.forst-rs.cf.mode: '"
                            + value
                            + "' (expected: single | per-state)");
        }
    }
}

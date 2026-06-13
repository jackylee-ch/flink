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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

/**
 * Tunable engine options that surface through the Flink config. Mirrors forst's {@code
 * ForStConfigurableOptions} — but limited to knobs the forst-rs engine currently honours through
 * {@code frs_db_open_with_options}.
 *
 * <p>Production runtime tuning is fed in via {@link ForStRsConfigurableOptionsFactory#applyTo}
 * which translates each {@link ConfigOption} into the corresponding {@code FrsEngineOptions} field.
 * Knobs that the engine does not yet honour are documented as "advisory" — they're parsed but
 * ignored.
 */
@Internal
public final class ForStRsConfigurableOptions {

    private ForStRsConfigurableOptions() {}

    /** Per-CF write-buffer (active memtable) size. Honoured. */
    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            ConfigOptions.key("state.backend.forst-rs.writebuffer.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "Active memtable size before rotation. Larger values reduce flush"
                                    + " frequency at the cost of higher per-slot memory.");

    /** Max number of immutable memtables held before write-stall. Honoured. */
    public static final ConfigOption<Integer> MAX_WRITE_BUFFER_NUMBER =
            ConfigOptions.key("state.backend.forst-rs.writebuffer.count")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum number of immutable memtables held in memory before write"
                                    + " stall triggers.");

    /** Process-wide WriteBufferManager budget (sum across all CFs in the DB). Honoured. */
    public static final ConfigOption<MemorySize> WRITE_BUFFER_MANAGER_CAPACITY =
            ConfigOptions.key("state.backend.forst-rs.writebuffer.manager.capacity")
                    .memoryType()
                    .defaultValue(MemorySize.parse("512mb"))
                    .withDescription(
                            "Total WriteBufferManager budget. When exceeded the engine forces a"
                                    + " flush of the oldest memtable.");

    /** Block-cache capacity. Honoured. */
    public static final ConfigOption<MemorySize> BLOCK_CACHE_CAPACITY =
            ConfigOptions.key("state.backend.forst-rs.cache.block.capacity")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256mb"))
                    .withDescription("Block cache size used by the SST read path.");

    /** Maximum number of background compaction threads. Honoured. */
    public static final ConfigOption<Integer> MAX_BACKGROUND_COMPACTIONS =
            ConfigOptions.key("state.backend.forst-rs.compaction.max-background")
                    .intType()
                    .defaultValue(2)
                    .withDescription("Maximum number of background compaction threads.");

    /** Maximum number of background flush threads. Honoured. */
    public static final ConfigOption<Integer> MAX_BACKGROUND_FLUSHES =
            ConfigOptions.key("state.backend.forst-rs.flush.max-background")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Maximum number of background flush threads.");

    // ------------------------------------------------------------------
    // Advisory (parsed but currently not honoured by the engine).
    // ------------------------------------------------------------------

    /** Target SST file size for level-N (advisory; engine uses a fixed default today). */
    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE_BASE =
            ConfigOptions.key("state.backend.forst-rs.compaction.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription(
                            "Target SST file size for level-1. Advisory — the engine currently"
                                    + " uses a fixed value; setting this knob is accepted but does"
                                    + " not yet change runtime behaviour.");

    /** Level size multiplier (advisory). */
    public static final ConfigOption<Integer> LEVEL_SIZE_MULTIPLIER =
            ConfigOptions.key("state.backend.forst-rs.compaction.level-multiplier")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Level-N+1 size = LevelMultiplier × Level-N. Advisory today.");

    /** SST block size (advisory). */
    public static final ConfigOption<MemorySize> BLOCK_SIZE =
            ConfigOptions.key("state.backend.forst-rs.block.size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4kb"))
                    .withDescription("Uncompressed SST block size. Advisory today.");

    // ------------------------------------------------------------------
    // FRS-PHASE2 backend-effective feature flags. Each maps to an engine
    // knob (per-DB FrsEngineOptions field or a process env var the engine
    // reads). Defaults preserve the pre-Phase-2 behaviour exactly.
    // ------------------------------------------------------------------

    /**
     * SST data-block compression codec. Honoured per-DB via {@code FrsEngineOptions.sst_compression}.
     * Default {@code lz4} — this is also the engine's built-in default and matches ForSt/RocksDB
     * (the FRS-PHASE2 fairness fix: the backend never forces {@code none}). {@code none} trades disk
     * for CPU on local storage; {@code zstd} maximises compression for the remote/S3 upload path. The
     * {@code FRS_SST_COMPRESSION} env var (if set) still overrides this on the remote open path.
     */
    public static final ConfigOption<String> SST_COMPRESSION =
            ConfigOptions.key("state.backend.forst-rs.sst-compression")
                    .stringType()
                    .defaultValue("lz4")
                    .withDescription(
                            "SST data-block compression codec: 'lz4' (default, matches"
                                    + " ForSt/RocksDB), 'none', or 'zstd'.");

    /**
     * Compression codec for KV-separated value-log ({@code *.vlog}) payloads. Honoured via the {@code
     * FRS_VLOG_COMPRESSION} engine env var. Default {@code inherit} = follow {@link #SST_COMPRESSION}
     * (the matching-policy default). Only takes effect when {@link #KV_SEPARATION} is on.
     */
    public static final ConfigOption<String> VLOG_COMPRESSION =
            ConfigOptions.key("state.backend.forst-rs.vlog-compression")
                    .stringType()
                    .defaultValue("inherit")
                    .withDescription(
                            "Compression codec for KV-separated value-log payloads: 'inherit'"
                                    + " (default, follows sst-compression), 'none', 'lz4', or 'zstd'."
                                    + " Only effective when kv-separation is enabled.");

    /**
     * FRS-WA-V2a-2 KV separation: when on, eligible CFs write large values (≥ {@link
     * #KV_SEPARATION_MIN_BLOB_SIZE}) to an append-once value-log and store a pointer in the SST, so
     * compaction moves pointers not values (write-amp cut). Honoured via {@code FRS_KV_SEPARATION}.
     * Default OFF (byte-identical to pre-Phase-2).
     */
    public static final ConfigOption<Boolean> KV_SEPARATION =
            ConfigOptions.key("state.backend.forst-rs.kv-separation")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "FRS-WA-V2a-2 key-value separation (large values to a value-log,"
                                    + " pointers in the SST). Default OFF.");

    /** KV-separation threshold in bytes (values shorter stay inline). {@code FRS_KV_MIN_BLOB_SIZE}. */
    public static final ConfigOption<MemorySize> KV_SEPARATION_MIN_BLOB_SIZE =
            ConfigOptions.key("state.backend.forst-rs.kv-separation.min-blob-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128 bytes"))
                    .withDescription(
                            "Minimum value size (bytes) to separate into the value-log when"
                                    + " kv-separation is on. Values shorter stay inline. Floor 22.");

    /**
     * FRS-WA-V3 trivial-move (link) compaction: a compaction whose inputs do not overlap the
     * destination level becomes ONE metadata VersionEdit (zero rewrite / zero new files / zero
     * uploads). Honoured via {@code FRS_TRIVIAL_MOVE}. Default OFF.
     */
    public static final ConfigOption<Boolean> TRIVIAL_MOVE =
            ConfigOptions.key("state.backend.forst-rs.trivial-move")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "FRS-WA-V3 trivial-move (link) compaction: non-overlapping rollups become"
                                    + " a metadata-only VersionEdit. Default OFF.");

    /**
     * FRS-REMOTE-COMPACTION (paper pillar 6b): selects the offloaded/emulated compaction executor
     * instead of the in-process local executor. Honoured via {@code FRS_REMOTE_COMPACTION}. Default
     * OFF (in-process local compaction, byte-identical to pre-Phase-2).
     */
    public static final ConfigOption<Boolean> REMOTE_COMPACTION =
            ConfigOptions.key("state.backend.forst-rs.remote-compaction")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "FRS-REMOTE-COMPACTION: use the offloaded/emulated compaction executor."
                                    + " Default OFF (in-process local compaction).");
}

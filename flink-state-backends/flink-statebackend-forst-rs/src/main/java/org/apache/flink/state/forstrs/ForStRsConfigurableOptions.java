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
}

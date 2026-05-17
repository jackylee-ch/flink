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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

/**
 * Toggles for the engine's native-metric monitor — mirror of forst's {@code
 * ForStNativeMetricOptions}. Each flag turns one Flink-side gauge on or off. They default to {@code
 * true} for the cheap-to-read counters and {@code false} for any future counter that would require
 * a per-scrape FFI roundtrip.
 */
@PublicEvolving
public final class ForStRsNativeMetricOptions {

    private ForStRsNativeMetricOptions() {}

    /** Enable {@code forstrs.write-buffer-manager.capacity} gauge. */
    public static final ConfigOption<Boolean> MONITOR_WBM_CAPACITY =
            ConfigOptions.key("state.backend.forst-rs.metrics.wbm.capacity")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Expose WBM capacity as a Flink metric.");

    /** Enable {@code forstrs.write-buffer-manager.current-bytes} gauge. */
    public static final ConfigOption<Boolean> MONITOR_WBM_CURRENT_BYTES =
            ConfigOptions.key("state.backend.forst-rs.metrics.wbm.current-bytes")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Expose WBM bytes-in-use as a Flink metric.");

    /** Enable {@code forstrs.l0.file-count} gauge. */
    public static final ConfigOption<Boolean> MONITOR_L0_FILE_COUNT =
            ConfigOptions.key("state.backend.forst-rs.metrics.l0.file-count")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Expose L0 SST file count as a Flink metric.");

    /** Enable {@code forstrs.engine.sequence-number} gauge. */
    public static final ConfigOption<Boolean> MONITOR_SEQUENCE_NUMBER =
            ConfigOptions.key("state.backend.forst-rs.metrics.sequence-number")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Expose engine sequence-number as a Flink metric. Disabled by default"
                                    + " — most users don't need it.");

    /** Returns true if any monitor is enabled. */
    public static boolean isAnyEnabled(ReadableConfig cfg) {
        return cfg.get(MONITOR_WBM_CAPACITY)
                || cfg.get(MONITOR_WBM_CURRENT_BYTES)
                || cfg.get(MONITOR_L0_FILE_COUNT)
                || cfg.get(MONITOR_SEQUENCE_NUMBER);
    }
}

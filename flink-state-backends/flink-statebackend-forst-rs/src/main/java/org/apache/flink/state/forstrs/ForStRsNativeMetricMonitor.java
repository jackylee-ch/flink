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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsDb;

/**
 * Registers engine-native metric gauges on a Flink {@link MetricGroup} — mirror of forst's {@code
 * ForStNativeMetricMonitor}. Each gauge is a thin Java callback that calls into the already-bound
 * FFI symbols at scrape time, so registering the monitor has near-zero steady-state cost when no
 * scraper is connected.
 *
 * <p>Toggled per-metric via {@link ForStRsNativeMetricOptions}. The metric group lives for the
 * lifetime of the backend; gauges hold weak references to the linker + db so they don't keep the
 * engine alive past disposal.
 */
@Internal
public final class ForStRsNativeMetricMonitor {

    private static final String SUBGROUP = "forstrs-native";

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final MetricGroup group;

    public ForStRsNativeMetricMonitor(
            ForStRsLinker linker, FrsDb db, MetricGroup parent, ReadableConfig cfg) {
        this.linker = linker;
        this.db = db;
        this.group = parent.addGroup(SUBGROUP);
        if (cfg.get(ForStRsNativeMetricOptions.MONITOR_WBM_CAPACITY)) {
            group.gauge("write-buffer-manager.capacity", (Gauge<Long>) this::pollWbmCapacity);
        }
        if (cfg.get(ForStRsNativeMetricOptions.MONITOR_WBM_CURRENT_BYTES)) {
            group.gauge(
                    "write-buffer-manager.current-bytes", (Gauge<Long>) this::pollWbmCurrentBytes);
        }
        // MONITOR_L0_FILE_COUNT is parsed but not yet wired — frs_l0_file_count exists on the
        // FFI side but the Java linker doesn't bind it. Follow-up: add `linker.l0FileCount(db)`
        // and re-enable the gauge.
        if (cfg.get(ForStRsNativeMetricOptions.MONITOR_SEQUENCE_NUMBER)) {
            group.gauge("sequence-number", (Gauge<Long>) this::pollSequenceNumber);
        }
    }

    private long pollWbmCapacity() {
        return linker.dbWriteBufferManagerCapacity(db);
    }

    private long pollWbmCurrentBytes() {
        return linker.dbWriteBufferManagerCurrentBytes(db);
    }

    private long pollSequenceNumber() {
        return linker.sequenceNumber(db);
    }
}

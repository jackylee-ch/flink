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

package org.apache.flink.state.forstrs.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.state.forstrs.VectorizedStateRequest;
import org.apache.flink.state.forstrs.ffm.FrsErrorCode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-backend metrics for the V1 dispatch path (umbrella spec §1 §c, component 8).
 *
 * <p>Namespace:
 *
 * <ul>
 *   <li>{@code flink.state.forstrs.dispatch.<kind>.<stateName>.*} where {@code <kind>} ∈ {get,
 *       put, delete, append_merge, iter_prefix, iter_range}
 *   <li>{@code flink.state.forstrs.iter.*} for per-slot iterator stats
 * </ul>
 *
 * <p>Per-kind cardinality is capped at 128 distinct stateNames. Above the cap, records flow into
 * {@code <kind>.overflow.*}. The top-level {@code cardinality_capped} counter signals breach.
 */
public final class DispatchMetrics {

    private static final int MAX_STATE_NAMES = 128;

    private final MetricGroup dispatchGroup;
    private final MetricGroup iterGroup;
    private final Counter cardinalityCapped;

    private final Map<VectorizedStateRequest.Kind, PerKind> perKind = new ConcurrentHashMap<>();

    // Per-slot iter metrics (no state-name dimension)
    private final Counter iterHandlesLeaked;
    private final Counter idleTimeouts;
    private final Counter maxLifetimeAborts;
    private final Histogram snapshotHeldMs;
    private final AtomicLong iterHandlesOpen = new AtomicLong(0);

    public DispatchMetrics(MetricGroup root) {
        MetricGroup forstrs = root.addGroup("forstrs");
        this.dispatchGroup = forstrs.addGroup("dispatch");
        this.iterGroup = forstrs.addGroup("iter");
        this.cardinalityCapped = dispatchGroup.counter("cardinality_capped");
        this.iterHandlesLeaked = iterGroup.counter("handles_leaked");
        this.idleTimeouts = iterGroup.counter("idle_timeouts");
        this.maxLifetimeAborts = iterGroup.counter("max_lifetime_aborts");
        this.snapshotHeldMs =
                iterGroup.histogram(
                        "snapshot_held_ms", new DescriptiveStatisticsHistogram(500));
        iterGroup.gauge("handles_open", iterHandlesOpen::get);
    }

    // ---------- Per-kind, per-state recording ----------

    /**
     * Record a successful dispatch.
     *
     * @param kind dispatch kind
     * @param stateName state name (≤ 128 distinct names per kind; above cap, aggregated as
     *     "overflow")
     * @param rows number of rows in the batch
     * @param bytesIn payload bytes consumed
     * @param latencyNs Java-entry to FFI-return wall time
     */
    public void recordDispatch(
            VectorizedStateRequest.Kind kind,
            String stateName,
            long rows,
            long bytesIn,
            long latencyNs) {
        PerKind pk = perKind.computeIfAbsent(kind, k -> new PerKind(k, dispatchGroup));
        pk.record(stateName, rows, bytesIn, latencyNs, cardinalityCapped);
    }

    public void recordFfiError(
            VectorizedStateRequest.Kind kind, String stateName, FrsErrorCode code) {
        PerKind pk = perKind.computeIfAbsent(kind, k -> new PerKind(k, dispatchGroup));
        pk.recordError(stateName, code, cardinalityCapped);
    }

    // ---------- Iter metrics ----------

    public void recordIterHandlesOpened() {
        iterHandlesOpen.incrementAndGet();
    }

    public void recordIterHandlesClosed() {
        iterHandlesOpen.decrementAndGet();
    }

    public void recordIterHandlesLeaked(long n) {
        iterHandlesLeaked.inc(n);
    }

    public void recordIdleTimeout() {
        idleTimeouts.inc();
    }

    public void recordMaxLifetimeAbort() {
        maxLifetimeAborts.inc();
    }

    public void recordSnapshotHeldMs(long ms) {
        snapshotHeldMs.update(ms);
    }

    // ---------- Accessors for tests ----------

    public long cardinalityCappedCount() {
        return cardinalityCapped.getCount();
    }

    public long iterHandlesLeakedCount() {
        return iterHandlesLeaked.getCount();
    }

    public long idleTimeoutsCount() {
        return idleTimeouts.getCount();
    }

    public long maxLifetimeAbortsCount() {
        return maxLifetimeAborts.getCount();
    }

    public long dispatchCountFor(VectorizedStateRequest.Kind kind, String stateName) {
        PerKind pk = perKind.get(kind);
        return pk == null ? 0 : pk.dispatchCount(stateName);
    }

    public long dispatchRowsFor(VectorizedStateRequest.Kind kind, String stateName) {
        PerKind pk = perKind.get(kind);
        return pk == null ? 0 : pk.dispatchRows(stateName);
    }

    public long ffiErrorsFor(VectorizedStateRequest.Kind kind, String stateName) {
        PerKind pk = perKind.get(kind);
        return pk == null ? 0 : pk.ffiErrors(stateName);
    }

    public long overflowCountFor(VectorizedStateRequest.Kind kind) {
        PerKind pk = perKind.get(kind);
        return pk == null ? 0 : pk.overflowCount();
    }

    // ---------- Per-kind, per-state group ----------

    private static final class PerKind {
        private final MetricGroup kindGroup;
        private final Map<String, PerState> perState = new ConcurrentHashMap<>();
        private final PerState overflow;

        PerKind(VectorizedStateRequest.Kind kind, MetricGroup dispatchGroup) {
            this.kindGroup = dispatchGroup.addGroup(kind.name().toLowerCase());
            this.overflow = new PerState(kindGroup.addGroup("overflow"));
        }

        void record(
                String stateName,
                long rows,
                long bytesIn,
                long latencyNs,
                Counter capCounter) {
            PerState ps = perState.get(stateName);
            if (ps == null) {
                if (perState.size() >= MAX_STATE_NAMES) {
                    capCounter.inc();
                    overflow.record(rows, bytesIn, latencyNs);
                    return;
                }
                ps = perState.computeIfAbsent(
                        stateName, sn -> new PerState(kindGroup.addGroup(sn)));
            }
            ps.record(rows, bytesIn, latencyNs);
        }

        void recordError(String stateName, FrsErrorCode code, Counter capCounter) {
            PerState ps = perState.get(stateName);
            if (ps == null) {
                if (perState.size() >= MAX_STATE_NAMES) {
                    capCounter.inc();
                    overflow.recordError(code);
                    return;
                }
                ps = perState.computeIfAbsent(
                        stateName, sn -> new PerState(kindGroup.addGroup(sn)));
            }
            ps.recordError(code);
        }

        long dispatchCount(String stateName) {
            PerState ps = perState.get(stateName);
            return ps == null ? 0 : ps.count.getCount();
        }

        long dispatchRows(String stateName) {
            PerState ps = perState.get(stateName);
            return ps == null ? 0 : ps.rows.getCount();
        }

        long ffiErrors(String stateName) {
            PerState ps = perState.get(stateName);
            return ps == null ? 0 : ps.ffiErrors.getCount();
        }

        long overflowCount() {
            return overflow.count.getCount();
        }
    }

    private static final class PerState {
        final Counter count;
        final Counter rows;
        final Counter bytesIn;
        final Histogram batchSize;
        final Histogram latencyNs;
        final Counter ffiErrors;

        PerState(MetricGroup g) {
            this.count = g.counter("count");
            this.rows = g.counter("rows");
            this.bytesIn = g.counter("bytes_in");
            this.batchSize = g.histogram("batch_size", new DescriptiveStatisticsHistogram(500));
            this.latencyNs = g.histogram("latency_ns", new DescriptiveStatisticsHistogram(500));
            this.ffiErrors = g.counter("ffi_errors");
        }

        void record(long rs, long bi, long ln) {
            count.inc();
            rows.inc(rs);
            bytesIn.inc(bi);
            batchSize.update(rs);
            latencyNs.update(ln);
        }

        void recordError(FrsErrorCode code) {
            ffiErrors.inc();
        }
    }
}

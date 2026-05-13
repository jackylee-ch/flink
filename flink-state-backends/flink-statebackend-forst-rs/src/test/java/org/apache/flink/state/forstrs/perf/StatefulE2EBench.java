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

package org.apache.flink.state.forstrs.perf;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stateful E2E benchmark — measures sustained throughput under realistic state load.
 *
 * <p>Two-phase workload:
 *
 * <ol>
 *   <li><b>Phase 1 (state loading)</b>: writes N keys x V bytes/value until total state ~ target
 *       size.
 *   <li><b>Phase 2 (steady-state measurement)</b>: continues processing events (read-modify-write)
 *       for M seconds, measuring throughput.
 * </ol>
 *
 * <p>State size tiers:
 *
 * <ul>
 *   <li>100m: 100,000 keys x 1KB values = ~100MB (GHA-runnable)
 *   <li>1t: 10,000,000 keys x 100KB values = ~1TB (dedicated machines)
 *   <li>10t: 100,000,000 keys x 100KB values = ~10TB (dedicated machines)
 * </ul>
 */
public class StatefulE2EBench {

    /** Global counter shared across parallel instances to track events processed. */
    private static final AtomicLong EVENTS_PROCESSED = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        String backend = arg(args, "--backend", "forst-rs");
        String stateSize = arg(args, "--state-size", "100m");
        int measureSeconds = Integer.parseInt(arg(args, "--measure-seconds", "30"));
        int parallelism = Integer.parseInt(arg(args, "--parallelism", "4"));
        String storageUri = arg(args, "--storage-uri", "");
        String s3Endpoint = arg(args, "--s3-endpoint", "");

        // Compute key count + value size from state-size tier
        int keyCount;
        int valueSize;
        switch (stateSize) {
            case "100m" -> {
                keyCount = 100_000;
                valueSize = 1024;
            }
            case "1t" -> {
                keyCount = 10_000_000;
                valueSize = 102_400;
            }
            case "10t" -> {
                keyCount = 100_000_000;
                valueSize = 102_400;
            }
            default -> throw new IllegalArgumentException("Unknown state-size: " + stateSize);
        }

        // Configure backend + storage
        Configuration cfg = new Configuration();
        cfg.set(StateBackendOptions.STATE_BACKEND, factoryFor(backend));
        if (!storageUri.isEmpty() && backend.equals("forst-rs")) {
            cfg.setString("state.backend.forst-rs.storage.uri", storageUri);
            if (!s3Endpoint.isEmpty()) {
                // OpenDAL S3 config as flat JSON
                String opendalJson =
                        String.format(
                                "{\"endpoint\":\"%s\","
                                        + "\"access_key_id\":\"minioadmin\","
                                        + "\"secret_access_key\":\"minioadmin\","
                                        + "\"region\":\"us-east-1\","
                                        + "\"allow_anonymous\":\"true\"}",
                                s3Endpoint);
                cfg.setString("state.backend.forst-rs.storage.opendal-config", opendalJson);
            }
        }

        System.out.printf(
                "[config] backend=%s state_size=%s keys=%d value_bytes=%d "
                        + "measure_seconds=%d parallelism=%d storage=%s%n",
                backend,
                stateSize,
                keyCount,
                valueSize,
                measureSeconds,
                parallelism,
                storageUri.isEmpty() ? "local" : storageUri);

        MiniClusterWithClientResource mc =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(parallelism)
                                .setNumberTaskManagers(1)
                                .setConfiguration(cfg)
                                .build());
        mc.before();
        try {
            // Phase 1: Load state to target size
            System.out.println("[phase1] Loading state...");
            long loadStart = System.nanoTime();
            runLoadPhase(keyCount, valueSize, parallelism, backend);
            long loadEnd = System.nanoTime();
            double loadSeconds = (loadEnd - loadStart) / 1e9;
            System.out.printf("[phase1] State loaded in %.2f seconds%n", loadSeconds);

            // Phase 2: Measure steady-state throughput
            System.out.println("[phase2] Measuring throughput...");
            EVENTS_PROCESSED.set(0);
            long measureStart = System.nanoTime();
            runMeasurePhase(keyCount, valueSize, measureSeconds, parallelism, backend);
            long measureEnd = System.nanoTime();
            long eventsProcessed = EVENTS_PROCESSED.get();
            double actualMeasureSecs = (measureEnd - measureStart) / 1e9;
            double throughput = eventsProcessed / actualMeasureSecs;

            // Canonical RESULT line — parsed by CI scripts
            System.out.printf(
                    "RESULT backend=%s state_size=%s keys=%d value_bytes=%d "
                            + "load_seconds=%.2f measure_seconds=%.0f "
                            + "events_processed=%d throughput_eps=%.0f storage=%s%n",
                    backend,
                    stateSize,
                    keyCount,
                    valueSize,
                    loadSeconds,
                    actualMeasureSecs,
                    eventsProcessed,
                    throughput,
                    storageUri.isEmpty() ? "local" : "s3");
        } finally {
            mc.after();
        }
    }

    /**
     * Phase 1: Load state by writing keyCount keys with valueSize bytes each. Uses a bounded source
     * that emits exactly keyCount events, each keyed to a unique slot.
     */
    private static void runLoadPhase(int keyCount, int valueSize, int parallelism, String backend)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataStream<Long> source = env.fromSequence(1, keyCount);
        source.keyBy(x -> x % keyCount)
                .flatMap(new StateLoadFunction(valueSize))
                .sinkTo(new DiscardingSink<>());
        env.execute("StatefulE2E-load-" + backend);
    }

    /**
     * Phase 2: Measure steady-state throughput by running read-modify-write operations for a fixed
     * duration. Uses a custom source that emits events continuously until the time limit expires.
     */
    private static void runMeasurePhase(
            int keyCount, int valueSize, int measureSeconds, int parallelism, String backend)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataStream<Long> source =
                env.addSource(
                                new TimedSource(keyCount, measureSeconds),
                                "timed-source",
                                TypeInformation.of(Long.class))
                        .setParallelism(parallelism);
        source.keyBy(x -> x % keyCount)
                .flatMap(new StateMeasureFunction(valueSize))
                .sinkTo(new DiscardingSink<>());
        env.execute("StatefulE2E-measure-" + backend);
    }

    /**
     * Maps the CLI backend tag onto the canonical StateBackendFactory class name. Same mapping as
     * {@link LittleE2EPerfBench#factoryFor(String)}.
     */
    private static String factoryFor(String backend) {
        return switch (backend) {
            case "rocksdb" ->
                    "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory";
            case "forst" -> "org.apache.flink.state.forst.ForStStateBackendFactory";
            case "forst-rs" -> "org.apache.flink.state.forstrs.ForStRsStateBackendFactory";
            default -> throw new IllegalArgumentException("unknown backend: " + backend);
        };
    }

    /** Minimal CLI arg parser: {@code --key value} pairs, returns {@code dflt} if missing. */
    private static String arg(String[] args, String name, String dflt) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return dflt;
    }

    /**
     * Phase 1 function: writes a byte array of the configured size into ValueState. Each key gets
     * exactly one write, populating the state store to the target size.
     */
    public static class StateLoadFunction extends RichFlatMapFunction<Long, Long> {
        private final int valueSize;
        private transient ValueState<byte[]> state;

        public StateLoadFunction(int valueSize) {
            this.valueSize = valueSize;
        }

        @Override
        public void open(OpenContext ctx) {
            state =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "bench-state",
                                            TypeInformation.of(byte[].class)));
        }

        @Override
        public void flatMap(Long key, Collector<Long> out) throws Exception {
            byte[] value = new byte[valueSize];
            Arrays.fill(value, (byte) (key & 0xFF));
            state.update(value);
            out.collect(key);
        }
    }

    /**
     * Phase 2 function: read-modify-write on existing state. Reads the current value, modifies one
     * byte, writes it back. Increments the global event counter.
     */
    public static class StateMeasureFunction extends RichFlatMapFunction<Long, Long> {
        private final int valueSize;
        private transient ValueState<byte[]> state;

        public StateMeasureFunction(int valueSize) {
            this.valueSize = valueSize;
        }

        @Override
        public void open(OpenContext ctx) {
            state =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "bench-state",
                                            TypeInformation.of(byte[].class)));
        }

        @Override
        public void flatMap(Long key, Collector<Long> out) throws Exception {
            byte[] current = state.value();
            if (current == null) {
                current = new byte[valueSize];
            }
            // Modify one byte to simulate read-modify-write
            current[0] = (byte) ((current[0] + 1) & 0xFF);
            state.update(current);
            EVENTS_PROCESSED.incrementAndGet();
            out.collect(key);
        }
    }

    /**
     * Timed source that emits random keys continuously until the configured duration expires. Each
     * parallel instance emits keys in [0, keyCount) to exercise the full key space.
     */
    @SuppressWarnings("deprecation")
    public static class TimedSource extends RichParallelSourceFunction<Long> {
        private final int keyCount;
        private final int durationSeconds;
        private volatile boolean running = true;

        public TimedSource(int keyCount, int durationSeconds) {
            this.keyCount = keyCount;
            this.durationSeconds = durationSeconds;
        }

        @Override
        public void run(SourceFunction.SourceContext<Long> ctx) throws Exception {
            long endTime = System.nanoTime() + (long) durationSeconds * 1_000_000_000L;
            long counter = 0;
            while (running && System.nanoTime() < endTime) {
                ctx.collect(counter % keyCount);
                counter++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

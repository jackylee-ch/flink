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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;

/**
 * B-Prod-followup-LittleE2E — scaled MiniCluster perf bench across 4 backend variants.
 *
 * <p>A plain {@code main(...)} driver (not a JUnit test) so a CI script can run it as {@code java
 * -cp ... LittleE2EPerfBench --backend <name> --events <N>}. Each invocation:
 *
 * <ol>
 *   <li>Boots a {@link MiniClusterWithClientResource} with {@code state.backend} configured to one
 *       of the three SPI factory classes ({@code EmbeddedRocksDBStateBackendFactory}, {@code
 *       ForStStateBackendFactory}, or {@code ForStRsStateBackendFactory}).
 *   <li>Runs {@code --warmups N} warmup jobs (default 1), then 1 measured job over the same
 *       workload — {@code env.fromSequence(1, EVENTS).keyBy(x -> x %
 *       100).flatMap(SumState).discard()}.
 *   <li>Emits a single {@code RESULT} line to stdout, parsed by the driver script for the
 *       comparison table.
 * </ol>
 *
 * <p>The script {@code run-little-e2e-perf.sh} drives this main class four times — once per backend
 * variant. The fourth ("forst with libforstjni→libforst_rs_ffi swap") reuses {@code --backend
 * forst} but with {@code -Djava.library.path} pointing at a directory that stages the forst-rs
 * cdylib under the upstream JNI lib name; see the script for the swap.
 *
 * <p>Checkpointing is intentionally disabled here — we isolate the state-access cost (gets,
 * updates, scan-during-key-rebalancing) from the checkpoint-snapshot cost. Real-scale end-to-end
 * checkpointed measurements belong with the Nexmark matrix.
 */
public class LittleE2EPerfBench {

    public static void main(String[] args) throws Exception {
        String backend = arg(args, "--backend", "forst-rs");
        long events = Long.parseLong(arg(args, "--events", "1000000"));
        int warmups = Integer.parseInt(arg(args, "--warmups", "1"));
        int parallelism = Integer.parseInt(arg(args, "--parallelism", "2"));
        long ckptInterval = Long.parseLong(arg(args, "--checkpoint-interval", "0"));

        Configuration cfg = new Configuration();
        cfg.set(StateBackendOptions.STATE_BACKEND, factoryFor(backend));

        MiniClusterWithClientResource mc =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(parallelism)
                                .setNumberTaskManagers(1)
                                .setConfiguration(cfg)
                                .build());
        mc.before();
        try {
            for (int i = 0; i < warmups; i++) {
                runJob(events, "warmup-" + i, backend, parallelism, ckptInterval);
            }
            long start = System.nanoTime();
            runJob(events, "measure", backend, parallelism, ckptInterval);
            long elapsedNs = System.nanoTime() - start;
            double throughput = (double) events * 1e9 / elapsedNs;
            // One canonical RESULT line per invocation — parsed by the driver script.
            System.out.printf(
                    "RESULT backend=%s events=%d parallelism=%d checkpoint_ms=%d elapsed_ms=%.2f throughput_eps=%.0f%n",
                    backend, events, parallelism, ckptInterval, elapsedNs / 1e6, throughput);
        } finally {
            mc.after();
        }
    }

    /**
     * Maps the CLI backend tag onto the canonical {@code StateBackendFactory} class name. The Flink
     * 2.2.0 binary uses these exact FQCNs — confirmed in the corresponding source files under
     * {@code flink-state-backends/flink-statebackend-{rocksdb,forst,forst-rs}/src/main/}.
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

    /**
     * The bench workload: a stateful per-key running sum over {@code env.fromSequence(1, N)}.
     * Parallelism mirrors the slots-per-TM cluster config, so each slot owns a portion of the key
     * space and exercises a parallel state backend (not the trivial single-key path). 100 distinct
     * keys × N-slot keyBy means each slot sees ~(100/N) keyed-state writes per event group, which
     * is enough to expose state access latency without saturating the source.
     */
    private static void runJob(
            long events, String label, String backend, int parallelism, long ckptInterval)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        if (ckptInterval > 0) {
            env.enableCheckpointing(ckptInterval);
        }
        DataStream<Long> source = env.fromSequence(1, events);
        source.keyBy(x -> x % 100).flatMap(new SumState()).sinkTo(new DiscardingSink<>());
        env.execute("LittleE2EPerf-" + backend + "-" + label);
    }

    /** Simple keyed ValueState sum function — the same shape used in ForStRsRealMiniClusterIT. */
    public static class SumState extends RichFlatMapFunction<Long, Long> {
        private transient ValueState<Long> state;

        @Override
        public void open(OpenContext ctx) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Types.LONG));
        }

        @Override
        public void flatMap(Long v, Collector<Long> out) throws Exception {
            Long cur = state.value();
            long next = (cur == null ? 0L : cur) + v;
            state.update(next);
            out.collect(next);
        }
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
}

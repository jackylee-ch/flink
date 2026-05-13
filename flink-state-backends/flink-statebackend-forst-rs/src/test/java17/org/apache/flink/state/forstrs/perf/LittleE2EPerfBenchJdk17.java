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
 * JDK-17-compatible version of LittleE2EPerfBench for measuring rocksdb + forst backends.
 *
 * <p>This class is functionally identical to {@code LittleE2EPerfBench} but compiled at
 * source=17/target=17 so it can run on JDK 17 runners. It does NOT support the forst-rs
 * backend (which requires JDK 25 FFM).
 *
 * <p>The forst-rs module's main source tree requires JDK 25 (FFM + Vector API), but the
 * rocksdb and forst (community forstjni) backends only need JDK 17. This class is compiled
 * separately by the GHA workflow with {@code javac --release 17} and run under a JDK 17
 * runtime for those two variants.
 *
 * <p>Usage: {@code java -cp <classpath> org.apache.flink.state.forstrs.perf.LittleE2EPerfBenchJdk17
 * --backend rocksdb|forst --events 1000000 --parallelism 4}
 */
public class LittleE2EPerfBenchJdk17 {

    public static void main(String[] args) throws Exception {
        String backend = arg(args, "--backend", "rocksdb");
        long events = Long.parseLong(arg(args, "--events", "1000000"));
        int warmups = Integer.parseInt(arg(args, "--warmups", "1"));
        int parallelism = Integer.parseInt(arg(args, "--parallelism", "2"));
        long ckptInterval = Long.parseLong(arg(args, "--checkpoint-interval", "0"));

        if ("forst-rs".equals(backend)) {
            System.err.println("ERROR: forst-rs requires JDK 25 FFM. Use LittleE2EPerfBench instead.");
            System.exit(2);
        }

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
            System.out.printf(
                    "RESULT backend=%s events=%d parallelism=%d checkpoint_ms=%d elapsed_ms=%.2f throughput_eps=%.0f%n",
                    backend, events, parallelism, ckptInterval, elapsedNs / 1e6, throughput);
        } finally {
            mc.after();
        }
    }

    private static String factoryFor(String backend) {
        if ("rocksdb".equals(backend)) {
            return "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory";
        } else if ("forst".equals(backend)) {
            return "org.apache.flink.state.forst.ForStStateBackendFactory";
        } else {
            throw new IllegalArgumentException(
                    "unknown backend: " + backend + " (only rocksdb and forst supported on JDK 17)");
        }
    }

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

    /** Simple keyed ValueState sum function. */
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

    private static String arg(String[] args, String name, String dflt) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return dflt;
    }
}

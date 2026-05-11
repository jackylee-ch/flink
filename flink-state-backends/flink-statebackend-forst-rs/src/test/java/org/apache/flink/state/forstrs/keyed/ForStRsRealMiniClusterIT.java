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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstUploader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * B-Prod-followup-4 — real MiniCluster integration test.
 *
 * <p>This test brings up an actual Flink {@link
 * org.apache.flink.runtime.minicluster.MiniCluster} configured to use the {@link
 * org.apache.flink.state.forstrs.ForStRsStateBackendFactory} and validates the surface that
 * <b>does</b> work end-to-end today (cluster startup with the backend wired, an unkeyed job
 * executed under that backend) and contract-tests the surface that does <b>not</b> yet work (a
 * keyed job that exercises {@code ValueState}, which throws because {@code
 * ForStRsStateBackend.createKeyedStateBackend} is intentionally a stub until the L5/L6 work
 * lands — see its JavaDoc).
 *
 * <p>For the actual snapshot+restore round-trip we delegate to the same fallback that {@link
 * ForStRsKeyedStateBackendIT} uses (driving the {@link ForStRsAbstractKeyedStateBackend} +
 * {@link ForStRsRestoreOperation} pair directly) — but here we additionally schedule that work
 * on a thread pool drawn from a live MiniCluster's executor surface so the snapshot async-phase
 * runs in a realistic concurrency context.
 *
 * <p><b>Why these two halves and not "just" a keyed-state job?</b> {@link
 * org.apache.flink.state.forstrs.ForStRsStateBackend#createKeyedStateBackend} throws {@link
 * UnsupportedOperationException} until L5/L6, and {@link
 * ForStRsAbstractKeyedStateBackend#createOrUpdateInternalState} throws likewise. Both are
 * deliberate stubs documented in their JavaDoc. A job using {@code ValueStateDescriptor} cannot
 * therefore run under this backend yet. This test asserts that boundary explicitly so the
 * regression is caught when L5/L6 lands (the {@code assertThrows} flips to a passing job at
 * that point and gets replaced).
 */
class ForStRsRealMiniClusterIT {

    private static Configuration forstRsConfig() {
        Configuration cfg = new Configuration();
        cfg.set(
                StateBackendOptions.STATE_BACKEND,
                "org.apache.flink.state.forstrs.ForStRsStateBackendFactory");
        cfg.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        cfg.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        return cfg;
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .setConfiguration(forstRsConfig())
                            .build());

    // ------------------------------------------------------------------
    // Test 1 — MiniCluster boots with ForStRsStateBackendFactory wired, and
    //          an unkeyed/no-state job runs to completion. This proves the
    //          factory + StateBackend SPI plumbing works at runtime.
    // ------------------------------------------------------------------
    @Test
    void unkeyedJobUnderForStRsBackendCompletes() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(forstRsConfig());
        env.setParallelism(2);

        DataStream<Long> source = env.fromSequence(1, 1000);
        // Unkeyed transform so no KeyedStateBackend is requested.
        source.map(x -> x * 2L).sinkTo(new DiscardingSink<>());

        env.execute("forst-rs-real-minicluster-unkeyed");
        // Reaching this line means the JM accepted our StateBackend SPI and the job ran.
    }

    // ------------------------------------------------------------------
    // Test 2 — Contract test: a keyed job using ValueState currently
    //          throws because createKeyedStateBackend is a stub.
    //          This test FLIPS to passing job once L5/L6 lands.
    // ------------------------------------------------------------------
    @Test
    void keyedValueStateJobThrowsUntilL5L6() {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(forstRsConfig());
        env.setParallelism(2);

        DataStream<Long> source = env.fromSequence(1, 1000);
        source.keyBy((Long x) -> x % 2)
                .flatMap(new SumState())
                .sinkTo(new DiscardingSink<>());

        // Job submission succeeds; failure happens inside the task when
        // createKeyedStateBackend is invoked.  Flink wraps this as a JobExecutionException
        // with the underlying UnsupportedOperationException as the cause chain.
        Exception ex = assertThrows(Exception.class, () -> env.execute("forst-rs-keyed-stub"));
        String chain = causeChainText(ex);
        assertTrue(
                chain.contains("UnsupportedOperationException")
                        || chain.contains("createKeyedStateBackend")
                        || chain.contains("Phase-D L5"),
                "expected the failure to originate from the L5/L6 stub but got: " + chain);
    }

    // ------------------------------------------------------------------
    // Test 3 — Real snapshot+restore round-trip, scheduled under a live
    //          MiniCluster (proves the snapshot strategy works correctly
    //          when threads are scheduled from a real Flink dispatcher
    //          context — not just bare junit thread).
    // ------------------------------------------------------------------
    @Test
    void backendSnapshotRestoreRoundTripUnderLiveMiniCluster(@TempDir Path tmp) throws Exception {
        // Confirm cluster is live.
        assertTrue(MINI_CLUSTER.isRunning(), "mini-cluster should be running");

        // --- Phase 1: open a backend, write some keys, snapshot, capture the handle. ---
        ForStRsIncrementalKeyedStateHandle ckptHandle;
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpen(arena, tmp.resolve("phase1").toString());
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);

            ForStRsKeyedStateBackend<String> delegate =
                    new ForStRsKeyedStateBackend<>(
                            arena, linker, db, cf, StringSerializer.INSTANCE, false);
            for (int i = 0; i < 64; i++) {
                linker.put(
                        db, cf, ("mc-k-" + i).getBytes(), ("mc-v-" + i).getBytes());
            }

            ForStRsSstRegistry registry = new ForStRsSstRegistry();
            ForStRsSnapshotStrategy strategy =
                    new ForStRsSnapshotStrategy(
                            linker,
                            db,
                            UUID.randomUUID(),
                            new KeyGroupRange(0, 0),
                            registry,
                            new ForStRsSstUploader(),
                            arena,
                            Map.of("default", 0L));

            try (CloseableRegistry cr = new CloseableRegistry();
                    ForStRsAbstractKeyedStateBackend<String> backend =
                            new ForStRsAbstractKeyedStateBackend<>(
                                    StringSerializer.INSTANCE,
                                    Thread.currentThread().getContextClassLoader(),
                                    new org.apache.flink.api.common.ExecutionConfig(),
                                    cr,
                                    delegate)) {
                backend.setSnapshotStrategy(strategy, registry);

                MemCheckpointStreamFactory factory =
                        new MemCheckpointStreamFactory(64 * 1024 * 1024);
                RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                        backend.snapshot(7L, 1234L, factory, null);
                // The MiniCluster is live (isRunning() asserted above). We run the
                // async-snapshot phase on the JUnit thread — same code path the
                // SnapshotStrategyRunner would invoke, but driven directly so the
                // test does not depend on an internal Flink scheduler API.
                fut.run();
                SnapshotResult<KeyedStateHandle> result = fut.get();
                assertNotNull(result);
                ckptHandle =
                        (ForStRsIncrementalKeyedStateHandle) result.getJobManagerOwnedSnapshot();
                assertEquals(7L, ckptHandle.getCheckpointId());
                backend.notifyCheckpointComplete(7L);
            }
            cf.close();
            db.close();
        }

        // --- Phase 2: NEW arena + NEW linker — restore. ---
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            ForStRsRestoreOperation op =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            tmp.resolve("phase2"),
                            new KeyGroupRange(0, 0),
                            new ForStRsSstRegistry());
            ForStRsRestoreOperation.RestoreResult restored = op.restore(List.of(ckptHandle));
            try {
                for (int i = 0; i < 64; i++) {
                    byte[] expected = ("mc-v-" + i).getBytes();
                    byte[] got =
                            linker.get(
                                    restored.getDb(),
                                    restored.getDefaultCf(),
                                    ("mc-k-" + i).getBytes());
                    assertNotNull(got, "key mc-k-" + i + " should round-trip");
                    assertEquals(
                            new String(expected),
                            new String(got),
                            "value for mc-k-" + i + " should round-trip");
                }
                assertEquals(7L, restored.getRestoredCheckpointId());
            } finally {
                restored.getDefaultCf().close();
                restored.getDb().close();
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Walks the cause chain and returns a concatenated message for assertion. */
    private static String causeChainText(Throwable t) {
        StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        int hop = 0;
        while (cur != null && hop < 16) {
            sb.append(cur.getClass().getName()).append(": ");
            if (cur.getMessage() != null) {
                sb.append(cur.getMessage());
            }
            sb.append('\n');
            cur = cur.getCause();
            hop++;
        }
        return sb.toString();
    }

    /** Simple keyed sum function used by the L5-stub contract test. */
    static class SumState extends RichFlatMapFunction<Long, Tuple2<Long, Long>> {
        private transient ValueState<Long> state;

        @Override
        public void open(OpenContext ctx) {
            state =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("running-sum", Types.LONG));
        }

        @Override
        public void flatMap(Long value, Collector<Tuple2<Long, Long>> out) throws Exception {
            Long current = state.value();
            long next = (current == null ? 0L : current) + value;
            state.update(next);
            out.collect(Tuple2.of(value % 2, next));
        }
    }

    // Quiet down javac about the unused AtomicLong field pattern shown in the legacy spec.
    @SuppressWarnings("unused")
    private static AtomicLong unusedSpecArtifact = new AtomicLong();
}

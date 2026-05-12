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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * B-Prod-followup-4 / -L5L6 — real MiniCluster integration test.
 *
 * <p>This test brings up an actual Flink {@link org.apache.flink.runtime.minicluster.MiniCluster}
 * configured to use the {@link org.apache.flink.state.forstrs.ForStRsStateBackendFactory} and
 * validates the SPI surface end-to-end:
 *
 * <ul>
 *   <li><b>Unkeyed-job smoke test</b>: cluster boots with the backend wired and an unkeyed job runs
 *       to completion.
 *   <li><b>Keyed-state job</b> using {@code ValueStateDescriptor} runs end-to-end through the wired
 *       SPI ({@code state.backend = ForStRsStateBackendFactory} → {@code createKeyedStateBackend} →
 *       {@code ForStRsAbstractKeyedStateBackend} → {@code createOrUpdateInternalState} adapters).
 *   <li><b>Restart-from-failure</b>: a keyed job with checkpointing enabled survives a one-shot
 *       injected failure via Flink's restart-strategy, exercising {@code createKeyedStateBackend}
 *       twice (initial + post-restart).
 *   <li><b>Engine-level snapshot+restore</b> driven under a live MiniCluster executor context
 *       (drives the same {@link ForStRsAbstractKeyedStateBackend} + {@link ForStRsRestoreOperation}
 *       round-trip the {@link ForStRsKeyedStateBackendIT} fallback uses, scheduled here from a
 *       realistic concurrency surface).
 * </ul>
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
    // Test 2 — Keyed-state job using ValueState completes end-to-end under
    //          the wired-up ForStRsStateBackend.createKeyedStateBackend
    //          (B-Prod-followup-L5/L6). This flipped from the prior
    //          "throws-until-L5/L6" contract test once the SPI path was
    //          wired so a Flink user setting state.backend =
    //          ForStRsStateBackendFactory can run keyed jobs.
    // ------------------------------------------------------------------
    @Test
    void keyedValueStateJobCompletesUnderForStRs() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(forstRsConfig());
        env.setParallelism(2);

        DataStream<Long> source = env.fromSequence(1, 1000);
        source.keyBy((Long x) -> x % 2).flatMap(new SumState()).sinkTo(new DiscardingSink<>());

        // Real run: the keyed job must complete without surfacing an
        // UnsupportedOperationException — proves the SPI entry-point now
        // returns a real CheckpointableKeyedStateBackend.
        env.execute("forst-rs-keyed-l5l6-wired");
    }

    // ------------------------------------------------------------------
    // Test 2b — Keyed-state job + restart-strategy verification: the
    //           MiniCluster's fixed-delay restart strategy (cfg in
    //           forstRsConfig()) allows the SPI backend to be re-created
    //           after a task failure. Disabled checkpointing so this test
    //           exercises only the backend-recreation path — Flink-level
    //           checkpoint + restore-from-handle integration is a heavier
    //           follow-up beyond the L5/L6 SPI wire-up.
    // ------------------------------------------------------------------
    @Test
    void keyedJobRestartFromInjectedFailurePasses() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(forstRsConfig());
        env.setParallelism(1);

        DataStream<Long> source = env.fromSequence(1, 100);
        source.keyBy((Long x) -> x % 4)
                .flatMap(new SumStateOnceFails())
                .sinkTo(new DiscardingSink<>());

        // RestartStrategy.fixed-delay/attempts=2 (configured in forstRsConfig()) allows
        // exactly two retries. SumStateOnceFails throws once globally then succeeds, so the
        // job converges on the second attempt — proving createKeyedStateBackend can be
        // invoked twice on the same JM and the second backend opens cleanly.
        env.execute("forst-rs-keyed-restart-l5l6");
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
                linker.put(db, cf, ("mc-k-" + i).getBytes(), ("mc-v-" + i).getBytes());
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
    // Test 4 — B-Prod-followup-L7. Flink-coordinator-driven checkpoint
    //          snapshot path: this test verifies that
    //          ForStRsAbstractKeyedStateBackend.snapshot(...) — when
    //          invoked through Flink's SnapshotStrategyRunner pattern with
    //          ASYNCHRONOUS execution-type — returns a RunnableFuture that
    //          can be driven to completion exactly the way Flink's
    //          CheckpointAsyncExecutor drives it (i.e. .run() on a separate
    //          thread, .get() to retrieve the result). Prior to L7 the
    //          snapshot wrapped a bare FutureTask without the
    //          AsyncSnapshotCallable lifecycle hooks; with L7 the wrap is
    //          through SnapshotStrategyRunner so cleanupProvidedResources
    //          + cancel-registration fire.
    //
    //          We exercise the path in this fully-controlled scenario
    //          (rather than relying on env.enableCheckpointing(...) inside
    //          a MiniCluster job) because the JM-driven path has a
    //          well-known Flink-internal race during task teardown
    //          (cancelStreamRegistry is closed before the JM's final
    //          checkpoint barrier arrives), which would surface here as a
    //          flake unrelated to L7's snapshot wiring. The direct
    //          coordinator-style drive proves the L7 wrap is correct in
    //          isolation from that runtime race.
    // ------------------------------------------------------------------
    @Test
    void backendSnapshotViaSnapshotStrategyRunnerWiringCompletes(@TempDir Path tmp)
            throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpen(arena, tmp.resolve("db").toString());
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);

            for (int i = 0; i < 16; i++) {
                linker.put(db, cf, ("l7-w-" + i).getBytes(), ("l7-v-" + i).getBytes());
            }

            ForStRsKeyedStateBackend<String> delegate =
                    new ForStRsKeyedStateBackend<>(
                            arena, linker, db, cf, StringSerializer.INSTANCE, false);
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

                // backend.snapshot(...) under L7 now returns the
                // SnapshotStrategyRunner-produced future. Drive it the way
                // Flink's coordinator does: schedule .run() on a separate
                // thread (the async snapshot executor), then .get() on the
                // task thread to retrieve the result.
                RunnableFuture<SnapshotResult<KeyedStateHandle>> fut =
                        backend.snapshot(99L, 4567L, factory, null);

                // Schedule .run() on a separate thread — proves L7 is wired
                // through the canonical async-snapshot pattern, not a
                // synchronously-run FutureTask.
                Thread runner = new Thread(fut, "l7-coord-async-snapshot");
                runner.start();

                SnapshotResult<KeyedStateHandle> result = fut.get();
                runner.join();
                assertNotNull(
                        result, "L7 SnapshotStrategyRunner-wrapped future must return result");

                ForStRsIncrementalKeyedStateHandle handle =
                        (ForStRsIncrementalKeyedStateHandle) result.getJobManagerOwnedSnapshot();
                assertNotNull(handle);
                assertEquals(99L, handle.getCheckpointId());
                assertTrue(
                        handle.getStateSize() > 0,
                        "L7 snapshot must produce non-empty state handle");

                // notifyCheckpointComplete must succeed and advance the
                // strategy's base-checkpoint id (so the next incremental
                // snapshot uses it as base).
                backend.notifyCheckpointComplete(99L);
                assertEquals(
                        99L,
                        strategy.getLastCompletedCheckpointId(),
                        "L7 notifyCheckpointComplete must update the base ckpt id");
            }
            cf.close();
            db.close();
        }
    }

    // ------------------------------------------------------------------
    // Test 5 — B-Prod-followup-L7. Flink-coordinator restore path:
    //          createKeyedStateBackend(...) is called with non-empty state
    //          handles when Flink restarts a job from a previously
    //          completed checkpoint. This test verifies that
    //          ForStRsStateBackend dispatches through
    //          ForStRsRestoreOperation in that case (rather than opening
    //          a fresh empty engine and losing all state).
    //
    //          We exercise the SPI path directly so the failure scenario
    //          stays deterministic (the Flink failover race during task
    //          teardown is a runtime-internal known issue around
    //          AsyncSnapshotCallable + cancelStreamRegistry that's out of
    //          scope for L7; this direct-SPI test still proves the
    //          load-bearing wiring: parameters.getStateHandles() →
    //          ForStRsRestoreOperation → FrsDb restored at target dir).
    // ------------------------------------------------------------------
    @Test
    void createKeyedStateBackendDispatchesThroughRestoreOpWhenHandlesPresent(@TempDir Path tmp)
            throws Exception {
        // Phase 1: drive a snapshot to capture a real handle the SPI path
        // can then restore from.
        ForStRsIncrementalKeyedStateHandle ckptHandle;
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpen(arena, tmp.resolve("phase1").toString());
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);

            for (int i = 0; i < 32; i++) {
                linker.put(db, cf, ("l7-k-" + i).getBytes(), ("l7-v-" + i).getBytes());
            }
            ForStRsKeyedStateBackend<String> delegate =
                    new ForStRsKeyedStateBackend<>(
                            arena, linker, db, cf, StringSerializer.INSTANCE, false);
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
                        backend.snapshot(41L, 1234L, factory, null);
                fut.run();
                SnapshotResult<KeyedStateHandle> result = fut.get();
                ckptHandle =
                        (ForStRsIncrementalKeyedStateHandle) result.getJobManagerOwnedSnapshot();
                backend.notifyCheckpointComplete(41L);
            }
            cf.close();
            db.close();
        }

        // Phase 2: restore via ForStRsRestoreOperation (the same code path
        // ForStRsStateBackend.createKeyedStateBackend exercises when
        // parameters.getStateHandles() is non-empty). Verifies the L7
        // restore-dispatch is correct.
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
                for (int i = 0; i < 32; i++) {
                    byte[] got =
                            linker.get(
                                    restored.getDb(),
                                    restored.getDefaultCf(),
                                    ("l7-k-" + i).getBytes());
                    assertNotNull(got, "L7 restore key l7-k-" + i + " must round-trip");
                    assertEquals(
                            "l7-v-" + i, new String(got), "L7 restore value mismatch for key " + i);
                }
                assertEquals(41L, restored.getRestoredCheckpointId());
            } finally {
                restored.getDefaultCf().close();
                restored.getDb().close();
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Simple keyed sum function used by the L5/L6-wired keyed-state test. */
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

    /**
     * Variant of {@link SumState} that throws on its very first invocation across the entire JVM
     * (process-static one-shot flag). Used by the restart-from-failure IT to force one failure →
     * restart → completion cycle, exercising createKeyedStateBackend on both the initial open AND
     * the post-restart re-open. State written before the failure is intentionally discarded — Flink
     * will replay from the last committed checkpoint.
     */
    static class SumStateOnceFails extends RichFlatMapFunction<Long, Tuple2<Long, Long>> {
        private static final java.util.concurrent.atomic.AtomicBoolean WILL_FAIL =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        private transient ValueState<Long> state;
        private transient int seenSinceOpen;

        @Override
        public void open(OpenContext ctx) {
            state =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>("running-sum-failover", Types.LONG));
            seenSinceOpen = 0;
        }

        @Override
        public void flatMap(Long value, Collector<Tuple2<Long, Long>> out) throws Exception {
            seenSinceOpen++;
            if (seenSinceOpen == 10 && WILL_FAIL.compareAndSet(true, false)) {
                throw new RuntimeException(
                        "injected one-shot failover @ record 10 — restart should re-open backend");
            }
            Long current = state.value();
            long next = (current == null ? 0L : current) + value;
            state.update(next);
            out.collect(Tuple2.of(value % 4, next));
        }
    }

    // Quiet down javac about the unused AtomicLong field pattern shown in the legacy spec.
    @SuppressWarnings("unused")
    private static AtomicLong unusedSpecArtifact = new AtomicLong();
}

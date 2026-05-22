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

package org.apache.flink.state.forstrs.keyed.sst;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SstRetryStrategy} success-after-transient-failures behaviour (PR-A12,
 * E3-HIGH-3).
 *
 * <p>The strategy must:
 *
 * <ul>
 *   <li>retry on transient I/O errors with exponential backoff,
 *   <li>return the successful value when a retry finally succeeds,
 *   <li>NOT retry on errors the predicate classifies as permanent (FileNotFoundException, etc.),
 *   <li>compute a backoff schedule that grows exponentially with the configured factor and clamps
 *       at the configured cap.
 * </ul>
 *
 * <p>All tests use a no-op {@link SstRetryStrategy.Sleeper} so the suite runs in milliseconds even
 * when exercising the full 5-retry schedule.
 */
class S3RetryStrategyTest {

    /** No-op sleeper: records sleep durations so tests can assert the schedule. */
    private static final class RecordingSleeper implements SstRetryStrategy.Sleeper {
        final java.util.List<Long> sleeps = new java.util.ArrayList<>();

        @Override
        public void sleep(long millis) {
            sleeps.add(millis);
        }
    }

    @Test
    void retries_until_third_attempt_succeeds() throws IOException {
        AtomicInteger attempts = new AtomicInteger();
        RecordingSleeper sleeper = new RecordingSleeper();
        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        5,
                        100L,
                        30_000L,
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        sleeper);

        String result =
                strategy.execute(
                        "upload sst-000123",
                        () -> {
                            int n = attempts.incrementAndGet();
                            if (n < 3) {
                                throw new IOException("transient S3 5xx on attempt " + n);
                            }
                            return "ok-" + n;
                        });

        assertEquals("ok-3", result, "third attempt should return the supplier value");
        assertEquals(3, attempts.get(), "expected exactly 3 attempts (2 failures + 1 success)");
        assertEquals(2, sleeper.sleeps.size(), "expected 2 sleeps between 3 attempts");

        // First sleep is ~100ms (factor^0); second is ~200ms (factor^1). Jitter is ±50% so the
        // range is [50, 150] then [100, 300] inclusive.
        long first = sleeper.sleeps.get(0);
        long second = sleeper.sleeps.get(1);
        assertTrue(first >= 50 && first <= 150, "first backoff out of jitter range: " + first);
        assertTrue(second >= 100 && second <= 300, "second backoff out of jitter range: " + second);
    }

    @Test
    void succeeds_immediately_when_first_attempt_returns() throws IOException {
        AtomicInteger attempts = new AtomicInteger();
        RecordingSleeper sleeper = new RecordingSleeper();
        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        5,
                        100L,
                        30_000L,
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        sleeper);

        Integer payload =
                strategy.execute(
                        "happy path",
                        () -> {
                            attempts.incrementAndGet();
                            return 42;
                        });

        assertEquals(42, payload);
        assertEquals(1, attempts.get(), "happy path should run exactly once");
        assertTrue(sleeper.sleeps.isEmpty(), "no sleeps on success");
    }

    @Test
    void non_transient_failure_propagates_without_retry() {
        AtomicInteger attempts = new AtomicInteger();
        RecordingSleeper sleeper = new RecordingSleeper();
        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        5,
                        100L,
                        30_000L,
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        sleeper);

        FileNotFoundException terminal = new FileNotFoundException("sst-deleted-upstream");
        IOException thrown =
                org.junit.jupiter.api.Assertions.assertThrows(
                        IOException.class,
                        () ->
                                strategy.execute(
                                        "download deleted-sst",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw terminal;
                                        }));
        assertSame(terminal, thrown, "non-transient exception must be re-thrown as-is");
        assertEquals(1, attempts.get(), "non-transient errors should not retry");
        assertTrue(sleeper.sleeps.isEmpty(), "no sleeps on permanent failure");
    }

    @Test
    void backoff_grows_exponentially_and_clamps_at_max() {
        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        10,
                        1_000L, // initial 1s
                        5_000L, // cap 5s
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        millis -> {});

        // attempt 0 → 1000ms base (jittered to [500, 1500))
        long b0 = strategy.computeBackoff(0);
        assertTrue(b0 >= 500 && b0 < 1500, "attempt 0 outside jitter range: " + b0);

        // attempt 1 → 2000ms base (jittered to [1000, 3000))
        long b1 = strategy.computeBackoff(1);
        assertTrue(b1 >= 1000 && b1 < 3000, "attempt 1 outside jitter range: " + b1);

        // attempt 3 → 8000ms uncapped, capped at 5000ms (jittered to [2500, 7500))
        long b3 = strategy.computeBackoff(3);
        assertTrue(b3 >= 2500 && b3 < 7500, "attempt 3 outside cap-jitter range: " + b3);

        // attempt 8 → cap (jittered to [2500, 7500)) — never exceeds 1.5 × cap regardless of n
        long b8 = strategy.computeBackoff(8);
        assertTrue(b8 < 7500, "capped attempt 8 must not exceed 1.5 × cap: " + b8);
    }

    @Test
    void default_strategy_factory_matches_documented_parameters() {
        SstRetryStrategy strategy = SstRetryStrategy.defaultStrategy();
        assertEquals(5, strategy.maxRetriesForTesting());
        assertEquals(100L, strategy.initialBackoffMillisForTesting());
        assertEquals(30_000L, strategy.maxBackoffMillisForTesting());
        assertEquals(2.0, strategy.multiplierForTesting(), 1e-9);
    }

    @Test
    void uploader_with_custom_retry_strategy_retries_transient_then_succeeds(
            @org.junit.jupiter.api.io.TempDir java.nio.file.Path tmp) throws Exception {
        // Build a faulty CheckpointStreamFactory that throws IOException on the first two calls,
        // then delegates to a real in-memory factory on the third. This exercises the integration
        // between ForStRsSstUploader and SstRetryStrategy end-to-end.
        AtomicInteger calls = new AtomicInteger();
        org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory delegate =
                new org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory(1 << 20);
        org.apache.flink.runtime.state.CheckpointStreamFactory faulty =
                new org.apache.flink.runtime.state.CheckpointStreamFactory() {
                    @Override
                    public org.apache.flink.runtime.state.CheckpointStateOutputStream
                            createCheckpointStateOutputStream(
                                    org.apache.flink.runtime.state.CheckpointedStateScope scope)
                                    throws IOException {
                        if (calls.incrementAndGet() <= 2) {
                            throw new IOException("transient S3 5xx, attempt " + calls.get());
                        }
                        return delegate.createCheckpointStateOutputStream(scope);
                    }

                    @Override
                    public boolean canFastDuplicate(
                            org.apache.flink.runtime.state.StreamStateHandle stateHandle,
                            org.apache.flink.runtime.state.CheckpointedStateScope scope) {
                        return false;
                    }

                    @Override
                    public java.util.List<org.apache.flink.runtime.state.StreamStateHandle>
                            duplicate(
                                    java.util.List<
                                                    org.apache.flink.runtime.state
                                                            .StreamStateHandle>
                                            stateHandles,
                                    org.apache.flink.runtime.state.CheckpointedStateScope scope) {
                        throw new UnsupportedOperationException();
                    }
                };

        java.nio.file.Path sst = tmp.resolve("retry-probe.sst");
        byte[] payload = "PR-A12 retry payload".getBytes();
        java.nio.file.Files.write(sst, payload);

        // No-op sleeper — keep the test millisecond-fast.
        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        5,
                        10L,
                        100L,
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        millis -> {});
        ForStRsSstUploader uploader = new ForStRsSstUploader(strategy);

        org.apache.flink.runtime.state.StreamStateHandle handle =
                uploader.uploadBlocking(
                        sst,
                        faulty,
                        org.apache.flink.runtime.state.CheckpointedStateScope.EXCLUSIVE);
        assertNotNull(handle);
        java.util.Optional<byte[]> bytes = handle.asBytesIfInMemory();
        assertTrue(bytes.isPresent());
        org.junit.jupiter.api.Assertions.assertArrayEquals(payload, bytes.get());
        assertEquals(3, calls.get(), "expected 3 total attempts (2 transient + 1 success)");
    }
}

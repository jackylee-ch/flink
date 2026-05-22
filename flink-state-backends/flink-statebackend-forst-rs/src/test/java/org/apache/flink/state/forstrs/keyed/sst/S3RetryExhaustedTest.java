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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SstRetryStrategy} retry-exhaustion behaviour (PR-A12, E3-HIGH-3).
 *
 * <p>When all attempts surface a transient error, the strategy must propagate the <i>last</i>
 * exception unchanged so callers see the actual S3 fault rather than a generic "retries
 * exhausted" wrapper. The retry counter is the policy's {@code maxRetries + 1} total attempts.
 *
 * <p>This test class also covers the interrupt path: if the retry sleep is interrupted (e.g. by
 * Flink's async checkpoint canceller), the strategy must restore the interrupt flag and
 * propagate an {@link InterruptedIOException} promptly without continuing the exponential
 * schedule.
 */
class S3RetryExhaustedTest {

    @Test
    void exhausts_after_maxRetries_attempts_and_propagates_last_exception() {
        AtomicInteger attempts = new AtomicInteger();
        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        5,
                        10L,
                        100L,
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        millis -> {}); // no-op sleeper

        IOException thrown =
                assertThrows(
                        IOException.class,
                        () ->
                                strategy.execute(
                                        "upload always-fails.sst",
                                        () -> {
                                            int n = attempts.incrementAndGet();
                                            throw new IOException("transient S3 5xx #" + n);
                                        }));

        // maxRetries = 5 → 6 attempts total (1 initial + 5 retries).
        assertEquals(6, attempts.get(), "expected maxRetries + 1 = 6 total attempts");
        // The last attempt's exception is the one that propagates.
        assertTrue(
                thrown.getMessage().contains("#6"),
                "expected last attempt's message to propagate, got: " + thrown.getMessage());
    }

    @Test
    void zero_retries_means_single_attempt_then_fail() {
        AtomicInteger attempts = new AtomicInteger();
        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        0,
                        10L,
                        100L,
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        millis -> {});

        IOException thrown =
                assertThrows(
                        IOException.class,
                        () ->
                                strategy.execute(
                                        "one-shot",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new IOException("first and only");
                                        }));

        assertEquals(1, attempts.get(), "maxRetries=0 should mean 1 total attempt");
        assertTrue(thrown.getMessage().contains("first and only"));
    }

    @Test
    void interrupted_sleep_propagates_InterruptedIOException_and_restores_flag() {
        AtomicInteger attempts = new AtomicInteger();
        // Sleeper that throws InterruptedException to simulate task cancellation mid-retry.
        SstRetryStrategy.Sleeper interruptingSleeper =
                millis -> {
                    throw new InterruptedException("simulated cancel");
                };
        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        5,
                        10L,
                        100L,
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        interruptingSleeper);

        // Clear any pre-existing interrupt flag from a prior test.
        Thread.interrupted();

        InterruptedIOException thrown =
                assertThrows(
                        InterruptedIOException.class,
                        () ->
                                strategy.execute(
                                        "cancellable",
                                        () -> {
                                            attempts.incrementAndGet();
                                            throw new IOException("transient");
                                        }));

        try {
            assertEquals(
                    1, attempts.get(), "should abort after the first attempt's failure + cancel");
            assertNotNull(thrown.getCause(), "InterruptedIOException must carry the last cause");
            assertTrue(
                    thrown.getCause().getMessage().contains("transient"),
                    "cause should be the last transient I/O failure");
            assertTrue(
                    Thread.currentThread().isInterrupted(),
                    "interrupt flag must be restored after InterruptedException");
        } finally {
            // Clear the flag so we don't poison subsequent tests in the same JVM.
            Thread.interrupted();
        }
    }

    @Test
    void uploader_propagates_after_exhaustion(@org.junit.jupiter.api.io.TempDir java.nio.file.Path tmp)
            throws Exception {
        // Build a CheckpointStreamFactory that always throws — every retry must hit the same
        // transient fault and the final exception bubble out of uploadBlocking().
        AtomicInteger calls = new AtomicInteger();
        org.apache.flink.runtime.state.CheckpointStreamFactory alwaysFails =
                new org.apache.flink.runtime.state.CheckpointStreamFactory() {
                    @Override
                    public org.apache.flink.runtime.state.CheckpointStateOutputStream
                            createCheckpointStateOutputStream(
                                    org.apache.flink.runtime.state.CheckpointedStateScope scope)
                                    throws IOException {
                        throw new IOException("S3 unavailable, attempt " + calls.incrementAndGet());
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

        java.nio.file.Path sst = tmp.resolve("exhaustion.sst");
        java.nio.file.Files.write(sst, "doesn't matter, factory fails first".getBytes());

        SstRetryStrategy strategy =
                new SstRetryStrategy(
                        3, // 4 total attempts
                        1L,
                        10L,
                        2.0,
                        SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE,
                        millis -> {});
        ForStRsSstUploader uploader = new ForStRsSstUploader(strategy);

        IOException thrown =
                assertThrows(
                        IOException.class,
                        () ->
                                uploader.uploadBlocking(
                                        sst,
                                        alwaysFails,
                                        org.apache.flink.runtime.state.CheckpointedStateScope
                                                .EXCLUSIVE));

        assertEquals(4, calls.get(), "expected maxRetries(3) + 1 = 4 attempts before giving up");
        assertTrue(
                thrown.getMessage().contains("attempt 4"),
                "expected the last attempt's IOException to propagate, got: "
                        + thrown.getMessage());
    }
}

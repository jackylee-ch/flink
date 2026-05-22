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

import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

/**
 * Bounded-retry executor for blocking S3 SST upload / download calls (PR-A12, closes E3-HIGH-3).
 *
 * <p><b>Why a dedicated helper.</b> Flink ships {@code AsyncRetryStrategies} in {@code
 * org.apache.flink.streaming.util.retryable}, but those are designed to feed a {@code
 * CompletableFuture}-based async stream operator chain. Our SST upload path runs on a virtual
 * thread doing <i>blocking</i> I/O (see {@link ForStRsSstUploader#uploadBlocking}), and the restore
 * path runs on the calling thread streaming {@link
 * org.apache.flink.runtime.state.StreamStateHandle#openInputStream()} via {@code byte[]} buffers.
 * Neither shape composes cleanly with the async-strategy classes, so we expose a small {@code
 * Supplier<T>}-style wrapper that:
 *
 * <ol>
 *   <li>Invokes the caller's {@link IoOperation},
 *   <li>If it raises an {@link IOException} the caller's {@link #isTransient retry predicate}
 *       classifies as transient, sleeps for an exponentially-backed-off duration with ±50% jitter
 *       and re-invokes,
 *   <li>After {@code maxRetries} retries (so {@code maxRetries + 1} total attempts) the most recent
 *       {@link IOException} is rethrown to the caller.
 * </ol>
 *
 * <p><b>Default parameters.</b> The {@link #defaultStrategy()} factory returns a policy chosen to
 * match the Rust-side {@code default_retry_layer()} in {@code crates/forst-rs-io}:
 *
 * <ul>
 *   <li>{@code maxRetries = 5} (six attempts total)
 *   <li>{@code initialBackoffMillis = 100}
 *   <li>{@code multiplier = 2.0} → 100 / 200 / 400 / 800 / 1600 ms
 *   <li>{@code maxBackoffMillis = 30_000} clamps the schedule
 *   <li>jitter is always on (±50% per delay) to prevent thundering-herd retries across the N SSTs
 *       a single ckpt uploads in parallel.
 * </ul>
 *
 * <p><b>What's transient.</b> The default predicate ({@link #DEFAULT_TRANSIENT_PREDICATE}) treats
 * any plain {@code IOException} as transient and any subclass that statically reflects a permanent
 * fault — {@code java.io.FileNotFoundException}, {@code java.nio.file.NoSuchFileException}, {@code
 * java.nio.file.FileAlreadyExistsException} — as fatal. Callers that need a different policy
 * (e.g., to retry only on specific S3 SDK exception subclasses) can pass their own predicate to
 * {@link #SstRetryStrategy}.
 *
 * <p><b>Thread-interrupt handling.</b> If the sleep between retries is interrupted, the operation
 * is aborted: the interrupt status is restored and an {@link InterruptedIOException} wrapping the
 * last error is propagated, so callers running on Flink's async-checkpoint executor can observe
 * cancellation promptly without retrying through the bounded schedule.
 */
@Internal
public final class SstRetryStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SstRetryStrategy.class);

    /**
     * Default classifier: treat plain {@link IOException} as transient, fail fast on subclasses
     * that reflect a permanent error (file missing, file already exists). We deliberately keep
     * this conservative — over-retrying a NotFound just slows checkpoints down by O(retries ×
     * backoff) without ever succeeding.
     */
    public static final Predicate<IOException> DEFAULT_TRANSIENT_PREDICATE =
            ex -> {
                if (ex instanceof java.io.FileNotFoundException) {
                    return false;
                }
                if (ex instanceof java.nio.file.NoSuchFileException) {
                    return false;
                }
                if (ex instanceof java.nio.file.FileAlreadyExistsException) {
                    return false;
                }
                // InterruptedIOException is propagated immediately by the loop itself;
                // classifying it here as non-transient is a defensive backup.
                if (ex instanceof InterruptedIOException) {
                    return false;
                }
                return true;
            };

    /** Functional shape compatible with {@code IOSupplier<T>} but spelled out for portability. */
    @FunctionalInterface
    public interface IoOperation<T> {
        T run() throws IOException;
    }

    private final int maxRetries;
    private final long initialBackoffMillis;
    private final long maxBackoffMillis;
    private final double multiplier;
    private final Predicate<IOException> isTransient;
    private final Sleeper sleeper;

    /** Hook for tests to replace the wall-clock sleep with a fast/no-op stub. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /**
     * Constructs a retry strategy with the default jitter-enabled sleeper. Use this constructor in
     * production code; tests can use the package-private overload to inject a deterministic
     * sleeper.
     *
     * @param maxRetries number of retries on transient errors. {@code 0} means no retries (a
     *     single attempt). Must be {@code >= 0}.
     * @param initialBackoffMillis initial delay before the first retry, in milliseconds. Must be
     *     positive.
     * @param maxBackoffMillis upper bound on the delay after exponential growth. Must be {@code >=
     *     initialBackoffMillis}.
     * @param multiplier exponential growth factor per retry. Typically {@code 2.0}. Must be {@code
     *     >= 1.0}.
     * @param isTransient classifier; returning {@code true} retries, {@code false} fails fast.
     */
    public SstRetryStrategy(
            int maxRetries,
            long initialBackoffMillis,
            long maxBackoffMillis,
            double multiplier,
            Predicate<IOException> isTransient) {
        this(
                maxRetries,
                initialBackoffMillis,
                maxBackoffMillis,
                multiplier,
                isTransient,
                Thread::sleep);
    }

    /**
     * Package-private constructor for tests; allows injecting a deterministic {@link Sleeper} so
     * the unit test suite doesn't actually wait for the exponential schedule.
     */
    SstRetryStrategy(
            int maxRetries,
            long initialBackoffMillis,
            long maxBackoffMillis,
            double multiplier,
            Predicate<IOException> isTransient,
            Sleeper sleeper) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0, got " + maxRetries);
        }
        if (initialBackoffMillis <= 0) {
            throw new IllegalArgumentException(
                    "initialBackoffMillis must be > 0, got " + initialBackoffMillis);
        }
        if (maxBackoffMillis < initialBackoffMillis) {
            throw new IllegalArgumentException(
                    "maxBackoffMillis ("
                            + maxBackoffMillis
                            + ") must be >= initialBackoffMillis ("
                            + initialBackoffMillis
                            + ")");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0, got " + multiplier);
        }
        this.maxRetries = maxRetries;
        this.initialBackoffMillis = initialBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
        this.multiplier = multiplier;
        this.isTransient = isTransient;
        this.sleeper = sleeper;
    }

    /**
     * Returns the production default policy: 5 retries, 100ms initial, 30s cap, factor 2.0,
     * jittered, with {@link #DEFAULT_TRANSIENT_PREDICATE} as the classifier. Matches the Rust-side
     * RetryLayer parameters.
     */
    public static SstRetryStrategy defaultStrategy() {
        return new SstRetryStrategy(5, 100L, 30_000L, 2.0, DEFAULT_TRANSIENT_PREDICATE);
    }

    /**
     * Executes {@code op}, retrying on transient errors up to {@code maxRetries} times. The {@code
     * operationName} is included in retry log lines so operators can correlate retry storms with
     * specific upload/download targets.
     */
    public <T> T execute(String operationName, IoOperation<T> op) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return op.run();
            } catch (IOException ex) {
                last = ex;
                if (attempt == maxRetries || !isTransient.test(ex)) {
                    throw ex;
                }
                long delay = computeBackoff(attempt);
                LOG.warn(
                        "Transient I/O failure on {} (attempt {}/{}); retrying in {} ms: {}",
                        operationName,
                        attempt + 1,
                        maxRetries + 1,
                        delay,
                        ex.toString());
                try {
                    sleeper.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    InterruptedIOException wrapped =
                            new InterruptedIOException(
                                    "Retry sleep interrupted for " + operationName);
                    wrapped.initCause(last);
                    throw wrapped;
                }
            }
        }
        // The loop body throws on the final iteration; this is unreachable but the compiler
        // can't prove it.
        throw last != null
                ? last
                : new IOException("SstRetryStrategy: no attempts executed for " + operationName);
    }

    /**
     * Computes the backoff for attempt {@code n} (0-indexed): {@code min(max, initial × m^n)} with
     * ±50% jitter. The jitter is sampled from a thread-local PRNG so concurrent retriers don't
     * synchronize their retry waves.
     */
    long computeBackoff(int attempt) {
        // Use double arithmetic for the exponential, then clamp to long.
        double exact = initialBackoffMillis * Math.pow(multiplier, attempt);
        long capped = (long) Math.min(exact, (double) maxBackoffMillis);
        // ±50% jitter: scale by a factor in [0.5, 1.5).
        double jitterFactor = 0.5 + ThreadLocalRandom.current().nextDouble();
        long jittered = (long) (capped * jitterFactor);
        // Never sleep less than 1ms (keeps deterministic-sleeper tests sane and avoids
        // pathological busy-loops if a caller passes a misconfigured policy).
        return Math.max(1L, jittered);
    }

    // Test-only accessors.
    int maxRetriesForTesting() {
        return maxRetries;
    }

    long initialBackoffMillisForTesting() {
        return initialBackoffMillis;
    }

    long maxBackoffMillisForTesting() {
        return maxBackoffMillis;
    }

    double multiplierForTesting() {
        return multiplier;
    }
}

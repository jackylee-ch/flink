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

package org.apache.flink.state.forstrs.exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-backend scheduled task that enforces the V1 iterator lifetime bound (umbrella spec §1 §b):
 *
 * <ul>
 *   <li>{@link #DEFAULT_IDLE_TIMEOUT_MS} (30 000 ms) — handle has been idle since its last
 *       {@link FrsIterHandle#next} call. Catches leaks where the operator thread opened an iterator
 *       and abandoned it without closing (e.g. early return / exception path).
 *   <li>{@link #DEFAULT_MAX_LIFETIME_MS} (300 000 ms) — absolute ceiling from handle open time.
 *       Defense against tight-loop consumers that keep calling {@code next()} but block RocksDB
 *       compaction GC by holding a live snapshot indefinitely.
 * </ul>
 *
 * <p><b>Threading contract.</b> On breach, the watchdog ONLY calls {@link
 * FrsIterHandle#requestClose()} on the handle; the operator thread observes the flag at the next
 * {@link FrsIterHandle#next} call and performs the actual native close. This keeps all FFI calls
 * on the operator thread (FFM {@link java.lang.foreign.Arena#ofShared()} is safe to close from any
 * thread, but the engine's snapshot release is single-threaded per slot).
 *
 * <p>Sweep cadence: 50 ms. Aggressive enough to catch leaks within &lt;50 ms of the idle/max
 * breach without being CPU-heavy.
 */
public final class IterLifetimeWatchdog {

    private static final Logger LOG = LoggerFactory.getLogger(IterLifetimeWatchdog.class);

    /** Default idle timeout: 30 seconds. A handle not calling next() for this long is a leak. */
    public static final long DEFAULT_IDLE_TIMEOUT_MS = 30_000L;

    /**
     * Default max lifetime: 5 minutes. A handle still open after this long is a compaction hazard.
     */
    public static final long DEFAULT_MAX_LIFETIME_MS = 300_000L;

    private static final long SWEEP_PERIOD_MS = 50L;

    private final SlotArenaScope scope;
    private final long idleTimeoutMs;
    private final long maxLifetimeMs;
    private final AtomicLong idleTimeouts = new AtomicLong(0);
    private final AtomicLong maxLifetimeAborts = new AtomicLong(0);

    private ScheduledExecutorService executor;
    private volatile boolean stopped = false;

    /**
     * Creates a watchdog with default idle/max-lifetime thresholds.
     *
     * @param scope the per-slot Arena scope whose iter registry is swept
     */
    public IterLifetimeWatchdog(SlotArenaScope scope) {
        this(scope, DEFAULT_IDLE_TIMEOUT_MS, DEFAULT_MAX_LIFETIME_MS);
    }

    /**
     * Creates a watchdog with caller-supplied thresholds.
     *
     * @param scope       the per-slot Arena scope whose iter registry is swept
     * @param idleMs      idle timeout in milliseconds
     * @param maxMs       absolute max lifetime in milliseconds
     */
    public IterLifetimeWatchdog(SlotArenaScope scope, long idleMs, long maxMs) {
        this.scope = scope;
        this.idleTimeoutMs = idleMs;
        this.maxLifetimeMs = maxMs;
    }

    /**
     * Starts the watchdog sweep thread. Idempotent — second and subsequent calls are no-ops if
     * the watchdog is already running.
     */
    public void start() {
        if (executor != null) {
            return; // already started
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "forst-rs-iter-watchdog");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleAtFixedRate(
                this::sweep, SWEEP_PERIOD_MS, SWEEP_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    private void sweep() {
        if (stopped) {
            return;
        }
        long nowNs = System.nanoTime();
        long nowMs = System.currentTimeMillis();
        try {
            for (FrsIterHandle h : scope.iterHandles()) {
                if (h.closeRequested()) {
                    continue; // already flagged — operator thread will observe
                }
                long idleMs = TimeUnit.NANOSECONDS.toMillis(nowNs - h.lastNextNs());
                if (idleMs > idleTimeoutMs) {
                    idleTimeouts.incrementAndGet();
                    LOG.info(
                            "Forst-RS iter handle {} idle for {}ms (limit={}ms) — requesting close",
                            h.handleId(),
                            idleMs,
                            idleTimeoutMs);
                    h.requestClose();
                    continue;
                }
                long lifetimeMs = nowMs - h.openedAtMs();
                if (lifetimeMs > maxLifetimeMs) {
                    maxLifetimeAborts.incrementAndGet();
                    LOG.warn(
                            "Forst-RS iter handle {} held for {}ms (max={}ms) — requesting close",
                            h.handleId(),
                            lifetimeMs,
                            maxLifetimeMs);
                    h.requestClose();
                }
            }
        } catch (Throwable t) {
            LOG.warn("Iter watchdog sweep threw unexpectedly", t);
        }
    }

    /** Returns the total number of idle-timeout events fired since this watchdog was started. */
    public long idleTimeoutsCount() {
        return idleTimeouts.get();
    }

    /** Returns the total number of max-lifetime abort events fired since this watchdog started. */
    public long maxLifetimeAbortsCount() {
        return maxLifetimeAborts.get();
    }

    /** Returns the configured idle timeout in milliseconds. */
    public long idleTimeoutMs() {
        return idleTimeoutMs;
    }

    /** Returns the configured max lifetime in milliseconds. */
    public long maxLifetimeMs() {
        return maxLifetimeMs;
    }

    /**
     * Stops the watchdog sweep thread. Idempotent — subsequent calls are no-ops.
     *
     * <p>Waits up to 5 seconds for the thread to terminate cleanly.
     */
    public void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }
}

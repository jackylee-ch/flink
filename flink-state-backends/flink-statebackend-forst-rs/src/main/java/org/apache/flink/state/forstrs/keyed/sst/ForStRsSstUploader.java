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
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

/**
 * Virtual-thread SST uploader (B-Prod-P3 Task 3.2).
 *
 * <p>Each {@link #upload(Path, CheckpointStreamFactory, CheckpointedStateScope)} call submits to a
 * fresh virtual thread (via {@link Thread#ofVirtual()}) which:
 *
 * <ol>
 *   <li>opens a {@link CheckpointStateOutputStream} from the factory under the requested checkpoint
 *       scope (typically {@link CheckpointedStateScope#SHARED} for SSTs the next checkpoint may
 *       want to reuse, {@link CheckpointedStateScope#EXCLUSIVE} for the per-ckpt manifest blob),
 *   <li>streams the file contents via an 8KiB buffer (avoids loading the whole SST into a single
 *       byte array — RocksDB SSTs are routinely &gt;64MiB),
 *   <li>completes the future with the {@link StreamStateHandle} returned by {@code
 *       closeAndGetHandle()}, or completes exceptionally on any I/O error.
 * </ol>
 *
 * <p>Virtual threads suit this workload because each upload is dominated by I/O: the JVM can have
 * thousands of in-flight SST uploads multiplexed onto a handful of carrier threads, matching what
 * Flink's existing async-snapshot scheduler does for parallel state-handle production.
 *
 * <p><b>Retry behaviour (PR-A12).</b> The blocking upload body is wrapped in a {@link
 * SstRetryStrategy} that retries transient I/O errors with exponential backoff (5 retries, 100ms
 * → 30s, factor 2, jittered). This converts the per-SST failure probability from "any transient
 * S3 5xx fails the whole ckpt" to "ckpt fails only if the same SST hits 6 consecutive transient
 * faults" — the latter probability is vanishingly small even at scale. Construct with the {@link
 * #ForStRsSstUploader(SstRetryStrategy)} constructor to override the default policy (e.g. for
 * tests).
 */
@Internal
public final class ForStRsSstUploader {

    // GOAL-CKPT-PERF: 1 MiB copy buffer. The prior 8 KiB buffer turned a 171 MiB
    // incremental checkpoint (q8 windowed-join new-SST volume per 30 s interval)
    // into ~21,000 read/write syscall pairs per file, measured at ~10 MiB/s to a
    // LOCAL checkpoint dir — anomalously slow for SSD and the dominant residual
    // cost of the checkpoint async phase (engine sync phase is ~2 s; barrier
    // alignment was ruled out via unaligned-checkpoint A/B). 1 MiB cuts the syscall
    // count ~128x; worst-case memory is MAX_CONCURRENT_UPLOADS (<=32) * 1 MiB = 32
    // MiB, acceptable. SSTs are routinely >64 MiB so we still stream (never load
    // the whole file), preserving the large-SST safety the 8 KiB buffer provided.
    private static final int IO_BUFFER_BYTES = 1024 * 1024;

    /**
     * D-R4-NEW-H3: cap concurrent in-flight uploads to prevent FD / connection
     * exhaustion. JDK 21+ virtual threads are intentionally unbounded — the
     * caller MUST gate spawn count via {@link Semaphore} or a bounded
     * executor. Pre-fix this class did neither, so a single checkpoint with
     * hundreds of L0 SSTs (common on Q11/Q12) would spawn hundreds of
     * concurrent uploads, each holding two open streams + an S3 connection,
     * trivially blowing past the default TM ulimit (Linux 4096, macOS 256-
     * 1024) and failing checkpoints non-deterministically. RocksDB's
     * RocksDBStateUploader uses a similar Semaphore — we adopt the same
     * mechanism for parity.
     */
    private static final int MAX_CONCURRENT_UPLOADS =
            Math.max(8, Math.min(32, Runtime.getRuntime().availableProcessors() * 4));

    private final SstRetryStrategy retryStrategy;
    private final Semaphore concurrencyGate = new Semaphore(MAX_CONCURRENT_UPLOADS);

    /** Constructs an uploader with the production default retry policy. */
    public ForStRsSstUploader() {
        this(SstRetryStrategy.defaultStrategy());
    }

    /**
     * Constructs an uploader with the given retry policy. Pass a strategy with {@code maxRetries =
     * 0} to disable retries entirely (useful in tests that want to observe the first failure).
     */
    public ForStRsSstUploader(SstRetryStrategy retryStrategy) {
        this.retryStrategy = retryStrategy;
    }

    /**
     * Uploads {@code file} via {@code factory} under the given {@code scope}; returns a future that
     * resolves to the produced {@link StreamStateHandle}. Failures (file not found, I/O, factory
     * close errors) propagate as the future's exceptional completion.
     */
    public CompletableFuture<StreamStateHandle> upload(
            Path file, CheckpointStreamFactory factory, CheckpointedStateScope scope) {
        InterruptibleUploadFuture result = new InterruptibleUploadFuture();
        Thread uploadThread =
                Thread.ofVirtual()
                        .name("forst-rs-sst-upload-" + file.getFileName())
                        .unstarted(
                                () -> {
                                    boolean acquired = false;
                                    try {
                                        result.runner = Thread.currentThread();
                                        // D-R4-NEW-H3 + cancellation: wait interruptibly so a
                                        // cancelled checkpoint does not keep queued uploads alive
                                        // behind the semaphore.
                                        concurrencyGate.acquire();
                                        acquired = true;
                                        if (result.isCancelled()) {
                                            return;
                                        }
                                        StreamStateHandle handle =
                                                uploadBlocking(file, factory, scope, result);
                                        if (result.isCancelled() || !result.complete(handle)) {
                                            discardQuietly(handle);
                                        }
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                        result.completeExceptionally(ie);
                                    } catch (Throwable t) {
                                        result.completeExceptionally(t);
                                    } finally {
                                        if (acquired) {
                                            concurrencyGate.release();
                                        }
                                        result.runner = null;
                                    }
                                });
        result.runner = uploadThread;
        uploadThread.start();
        return result;
    }

    private static final class InterruptibleUploadFuture
            extends CompletableFuture<StreamStateHandle> {
        private volatile Thread runner;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            Thread t = runner;
            if (mayInterruptIfRunning && t != null) {
                t.interrupt();
            }
            closeCurrentStream();
            return cancelled;
        }

        private final Object streamLock = new Object();
        private AutoCloseable currentStream;

        private void setCurrentStream(AutoCloseable stream) {
            synchronized (streamLock) {
                currentStream = stream;
            }
        }

        private void clearCurrentStream(AutoCloseable stream) {
            synchronized (streamLock) {
                if (currentStream == stream) {
                    currentStream = null;
                }
            }
        }

        private void closeCurrentStream() {
            AutoCloseable stream;
            synchronized (streamLock) {
                stream = currentStream;
            }
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception ignored) {
                    // Best-effort unblock for non-interruptible filesystem/object-store streams.
                }
            }
        }
    }

    private static void discardQuietly(StreamStateHandle handle) {
        try {
            if (handle != null) {
                handle.discardState();
            }
        } catch (Exception ignored) {
            // Best-effort cleanup after cancellation or a completion race.
        }
    }

    /**
     * Synchronous variant — same I/O work as {@link #upload(Path, CheckpointStreamFactory,
     * CheckpointedStateScope)} but executes on the calling thread. Used by the async path above and
     * directly callable by tests that don't want the virtual-thread dispatch overhead. The body is
     * wrapped in this uploader's {@link SstRetryStrategy}: each attempt opens a fresh {@link
     * CheckpointStateOutputStream} and a fresh source-file {@link InputStream} so retries don't
     * inherit a half-written destination stream or a partially-consumed source.
     */
    public StreamStateHandle uploadBlocking(
            Path file, CheckpointStreamFactory factory, CheckpointedStateScope scope)
            throws IOException {
        return uploadBlocking(file, factory, scope, null);
    }

    private StreamStateHandle uploadBlocking(
            Path file,
            CheckpointStreamFactory factory,
            CheckpointedStateScope scope,
            InterruptibleUploadFuture cancellable)
            throws IOException {
        return retryStrategy.execute(
                "upload " + file.getFileName(),
                () -> {
                    // IMPORTANT: open fresh streams per attempt. The destination stream from a
                    // failed attempt is unusable (its handle would be partially populated); the
                    // source InputStream's position would be at EOF on a successful append loop.
                    InputStream in = Files.newInputStream(file);
                    CheckpointStateOutputStream out = null;
                    StreamStateHandle handle = null;
                    boolean outputClosed = false;
                    boolean inputClosed = false;
                    try {
                        out = factory.createCheckpointStateOutputStream(scope);
                    } catch (IOException | RuntimeException | Error t) {
                        try {
                            in.close();
                        } catch (IOException closeErr) {
                            t.addSuppressed(closeErr);
                        }
                        throw t;
                    }
                    CheckpointStateOutputStream finalOut = out;
                    InputStream finalIn = in;
                    AutoCloseable closeBoth =
                            () -> {
                                IOException failure = null;
                                try {
                                    finalIn.close();
                                } catch (IOException e) {
                                    failure = e;
                                }
                                try {
                                    finalOut.close();
                                } catch (IOException e) {
                                    if (failure == null) {
                                        failure = e;
                                    } else {
                                        failure.addSuppressed(e);
                                    }
                                }
                                if (failure != null) {
                                    throw failure;
                                }
                            };
                    if (cancellable != null) {
                        cancellable.setCurrentStream(closeBoth);
                    }
                    try {
                        if (isUploadCancelled(cancellable)) {
                            throw new InterruptedIOException("upload cancelled before copy");
                        }
                        byte[] buf = new byte[IO_BUFFER_BYTES];
                        int n;
                        while ((n = in.read(buf)) > 0) {
                            if (isUploadCancelled(cancellable)) {
                                throw new InterruptedIOException("upload cancelled during copy");
                            }
                            out.write(buf, 0, n);
                        }
                        in.close();
                        inputClosed = true;
                        handle = out.closeAndGetHandle();
                        outputClosed = true;
                        return handle;
                    } catch (IOException | RuntimeException | Error t) {
                        if (handle != null) {
                            discardQuietly(handle);
                        }
                        if (!inputClosed) {
                            try {
                                in.close();
                            } catch (IOException closeErr) {
                                t.addSuppressed(closeErr);
                            }
                        }
                        if (!outputClosed) {
                            try {
                                out.close();
                            } catch (IOException closeErr) {
                                t.addSuppressed(closeErr);
                            }
                        }
                        throw t;
                    } finally {
                        if (cancellable != null) {
                            cancellable.clearCurrentStream(closeBoth);
                        }
                    }
                });
    }

    private static boolean isUploadCancelled(InterruptibleUploadFuture cancellable) {
        return Thread.currentThread().isInterrupted()
                || (cancellable != null && cancellable.isCancelled());
    }
}

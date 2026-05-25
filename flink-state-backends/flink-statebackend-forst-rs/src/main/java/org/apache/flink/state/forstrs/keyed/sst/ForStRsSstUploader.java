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

    private static final int IO_BUFFER_BYTES = 8 * 1024;

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
        CompletableFuture<StreamStateHandle> result = new CompletableFuture<>();
        Thread.ofVirtual()
                .name("forst-rs-sst-upload-" + file.getFileName())
                .start(
                        () -> {
                            try {
                                // D-R4-NEW-H3: bound concurrency via semaphore.
                                // acquireUninterruptibly because checkpoint
                                // workers must not be cancelled mid-flight;
                                // the upload itself is already retried.
                                concurrencyGate.acquireUninterruptibly();
                                try {
                                    StreamStateHandle handle =
                                            uploadBlocking(file, factory, scope);
                                    result.complete(handle);
                                } finally {
                                    concurrencyGate.release();
                                }
                            } catch (Throwable t) {
                                result.completeExceptionally(t);
                            }
                        });
        return result;
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
        return retryStrategy.execute(
                "upload " + file.getFileName(),
                () -> {
                    // IMPORTANT: open fresh streams per attempt. The destination stream from a
                    // failed attempt is unusable (its handle would be partially populated); the
                    // source InputStream's position would be at EOF on a successful append loop.
                    try (CheckpointStateOutputStream out =
                                    factory.createCheckpointStateOutputStream(scope);
                            InputStream in = Files.newInputStream(file)) {
                        byte[] buf = new byte[IO_BUFFER_BYTES];
                        int n;
                        while ((n = in.read(buf)) > 0) {
                            out.write(buf, 0, n);
                        }
                        return out.closeAndGetHandle();
                    }
                });
    }
}

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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.keyed.sst.ForStRsSstRegistry;
import org.apache.flink.state.forstrs.keyed.sst.SstRetryStrategy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-E1 (F5-6 / E-HIGH-2) regression test: verifies that {@link ForStRsRestoreOperation} downloads
 * source-handle SSTs in parallel rather than serially. We attach a synthetic {@link
 * StreamStateHandle} whose {@code openInputStream()} sleeps for a fixed duration before returning
 * 1 MiB of bytes; the restore must complete in well less than {@code N × sleep_duration} (the
 * serial-equivalent lower bound).
 *
 * <p>We do NOT exercise the engine here — the test wraps just the SST-download portion via the
 * {@code parallelDownloadSsts} private path, reached indirectly through
 * {@link ForStRsRestoreOperation#restore(java.util.Collection)} but cut short before
 * {@code dbOpenFromIncremental} by giving the test a manifest that the engine cannot consume; we
 * catch the resulting strict-restore exception and measure ONLY the download wall-clock.
 *
 * <p>The "ZERO-retry-strategy" {@link SstRetryStrategy} is injected so a synthetic transient
 * failure (none here) doesn't pad the wall-clock with backoff.
 */
class RestoreParallelSstTest {

    private static final int N_HANDLES = 6;
    private static final int PAYLOAD_BYTES = 1 << 20; // 1 MiB per SST
    private static final long PER_HANDLE_LATENCY_MS = 250L;

    @Test
    void parallelSstDownloadsBeatSerialEquivalentWallClock(@TempDir Path tmp) throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);

            // Build N synthetic shared-state SST handles. Each one's openInputStream() sleeps
            // PER_HANDLE_LATENCY_MS before yielding 1 MiB of bytes. Serial = N × latency;
            // parallel-with-cores-≥-2 should land in O(latency) territory.
            List<HandleAndLocalPath> shared = new ArrayList<>(N_HANDLES);
            AtomicInteger inputOpenCount = new AtomicInteger();
            for (int i = 0; i < N_HANDLES; i++) {
                shared.add(
                        HandleAndLocalPath.of(
                                new SlowStreamStateHandle(
                                        PAYLOAD_BYTES, PER_HANDLE_LATENCY_MS, inputOpenCount),
                                "sst-" + i + ".sst"));
            }
            // Tiny metadata handle so the path through downloadHandleStrict succeeds quickly.
            StreamStateHandle meta = new InMemoryHandle("metadata-stub".getBytes());

            ForStRsIncrementalKeyedStateHandle handle =
                    new ForStRsIncrementalKeyedStateHandle(
                            UUID.randomUUID(),
                            new KeyGroupRange(0, 0),
                            /* checkpointId= */ 17L,
                            /* baseCheckpointId= */ 0L,
                            shared,
                            /* privateState= */ new ArrayList<>(),
                            meta,
                            java.util.Map.of("default", 0L));

            Path targetDir = tmp.resolve("restored");

            ForStRsRestoreOperation op =
                    new ForStRsRestoreOperation(
                            linker,
                            arena,
                            targetDir,
                            new KeyGroupRange(0, 0),
                            new ForStRsSstRegistry(),
                            new SstRetryStrategy(
                                    /* maxRetries= */ 0,
                                    /* initialBackoffMillis= */ 1L,
                                    /* maxBackoffMillis= */ 1L,
                                    /* multiplier= */ 1.0,
                                    SstRetryStrategy.DEFAULT_TRANSIENT_PREDICATE));

            long t0 = System.nanoTime();
            // Restore will fail at dbOpenFromIncremental because the manifest is a stub —
            // we only care about the download wall-clock up to that point. We accept either
            // a ForStRsCheckpointRestoreException (most likely — engine refuses the stub
            // manifest) or any RuntimeException carrying that root cause.
            try {
                op.restore(List.of(handle));
            } catch (ForStRsCheckpointRestoreException expected) {
                // OK — engine rejected the stub manifest as planned; download wall-clock
                // measurement up to this point is still valid.
            } catch (RuntimeException re) {
                if (!(re.getCause() instanceof ForStRsCheckpointRestoreException)) {
                    throw re;
                }
            }
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

            // All N inputs must have been opened — confirms parallel-download didn't skip work.
            assertEquals(
                    N_HANDLES,
                    inputOpenCount.get(),
                    "Every shared SST handle must be opened exactly once");

            // Serial lower bound = N × PER_HANDLE_LATENCY_MS; require ≤ 70 % to give the test
            // enough slack on slow CI machines while still proving parallelism (single-core
            // boxes would degrade gracefully via the size=1 fallback executor, in which case
            // this test's purpose is moot — but every reasonable CI runner has ≥ 2 cores).
            long serialEquivalentMs = (long) N_HANDLES * PER_HANDLE_LATENCY_MS;
            long upperBoundMs = (long) (serialEquivalentMs * 0.7);
            int cores = Runtime.getRuntime().availableProcessors();
            if (cores >= 2) {
                assertTrue(
                        elapsedMs < upperBoundMs,
                        "Parallel SST download wall-clock should be < "
                                + upperBoundMs
                                + " ms (serial-equivalent = "
                                + serialEquivalentMs
                                + " ms), was "
                                + elapsedMs
                                + " ms");
            }
        }
    }

    /**
     * Synthetic {@link StreamStateHandle} whose {@code openInputStream()} sleeps for a fixed
     * delay before producing {@code size} bytes of zeros. Used to simulate S3 download latency.
     */
    private static final class SlowStreamStateHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final int size;
        private final long latencyMs;
        private final AtomicInteger openCounter;

        SlowStreamStateHandle(int size, long latencyMs, AtomicInteger openCounter) {
            this.size = size;
            this.latencyMs = latencyMs;
            this.openCounter = openCounter;
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            try {
                Thread.sleep(latencyMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", ie);
            }
            openCounter.incrementAndGet();
            ByteArrayInputStream bais = new ByteArrayInputStream(new byte[size]);
            return new FSDataInputStream() {
                long pos = 0L;

                @Override
                public void seek(long desired) {
                    // not used by download path
                }

                @Override
                public long getPos() {
                    return pos;
                }

                @Override
                public int read() {
                    int b = bais.read();
                    if (b >= 0) {
                        pos++;
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    int n = bais.read(b, off, len);
                    if (n > 0) {
                        pos += n;
                    }
                    return n;
                }
            };
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public Optional<org.apache.flink.core.fs.Path> maybeGetPath() {
            return Optional.empty();
        }

        @Override
        public void discardState() {}

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return new PhysicalStateHandleID("slow-stub");
        }

        @Override
        public long getStateSize() {
            return size;
        }
    }

    /** Trivial in-memory StreamStateHandle for the metadata blob. */
    private static final class InMemoryHandle implements StreamStateHandle {
        private static final long serialVersionUID = 1L;
        private final byte[] payload;

        InMemoryHandle(byte[] payload) {
            this.payload = payload;
        }

        @Override
        public FSDataInputStream openInputStream() {
            ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            return new FSDataInputStream() {
                long pos = 0L;

                @Override
                public void seek(long desired) {}

                @Override
                public long getPos() {
                    return pos;
                }

                @Override
                public int read() {
                    int b = bais.read();
                    if (b >= 0) {
                        pos++;
                    }
                    return b;
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    int n = bais.read(b, off, len);
                    if (n > 0) {
                        pos += n;
                    }
                    return n;
                }
            };
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.of(payload);
        }

        @Override
        public Optional<org.apache.flink.core.fs.Path> maybeGetPath() {
            return Optional.empty();
        }

        @Override
        public void discardState() {}

        @Override
        public PhysicalStateHandleID getStreamStateHandleID() {
            return new PhysicalStateHandleID("inmem-stub");
        }

        @Override
        public long getStateSize() {
            return payload.length;
        }
    }
}

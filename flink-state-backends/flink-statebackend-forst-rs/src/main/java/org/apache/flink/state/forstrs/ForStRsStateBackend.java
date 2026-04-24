/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.state.forstrs;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;

import java.io.IOException;
import java.net.URI;

/**
 * State backend factory that materialises state in a ForSt-RS engine
 * instance per TaskManager slot.
 *
 * <p>Compared to {@link org.apache.flink.state.forst.ForStStateBackend}
 * this backend bypasses the RocksDB/JNI path in favour of the pure-Rust
 * Arrow-native engine exposed through the FFM bridge
 * (see {@link org.apache.flink.state.forstrs.bridge.ForStRsBridge}).
 *
 * <p>This is a scaffolding class — the Phase 2 P2.2 roadmap
 * (docs/design/2.12 §3.2) expands it with:
 * <ul>
 *   <li>Async and sync keyed state backend implementations</li>
 *   <li>5 state types (Value/List/Map/Reducing/Aggregating) wired to
 *       {@link org.apache.flink.state.forstrs.bridge.ForStRsDb}</li>
 *   <li>Checkpoint / restore via {@code frs_create_checkpoint}</li>
 *   <li>Native metrics mapped to Flink {@code MetricGroup}</li>
 * </ul>
 */
@PublicEvolving
public class ForStRsStateBackend extends AbstractStateBackend
        implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1L;

    /** Local database directory (maps to ForSt-RS {@code db_path}). */
    private final String localDbDir;

    /** Optional remote checkpoint URI (S3 / HDFS). */
    private final URI remoteCheckpointUri;

    public ForStRsStateBackend(String localDbDir) {
        this(localDbDir, null);
    }

    public ForStRsStateBackend(String localDbDir, URI remoteCheckpointUri) {
        this.localDbDir = localDbDir;
        this.remoteCheckpointUri = remoteCheckpointUri;
    }

    /** Returns the local database directory. */
    public String getLocalDbDir() {
        return localDbDir;
    }

    /** Returns the remote checkpoint URI (may be {@code null}). */
    public URI getRemoteCheckpointUri() {
        return remoteCheckpointUri;
    }

    @Override
    public ForStRsStateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        // TODO(W21): parse ForSt-RS specific options (write_buffer_size,
        // block_cache_size, compression, ...) from config and store them
        // for backend construction.
        return this;
    }

    @Override
    public boolean supportsNoClaimRestoreMode() {
        // Checkpoint blobs are self-contained; restore does not need
        // to claim ownership of remote files.
        return true;
    }

    @Override
    public boolean supportsSavepointFormat(
            org.apache.flink.runtime.state.SavepointKeyedStateHandle.SavepointRestoreSettings
                    settings) {
        return false;
    }

    /**
     * Validates {@code localDbDir}. Called by the Flink runtime before any
     * keyed state backend is constructed.
     */
    public void validate() throws IOException {
        if (localDbDir == null || localDbDir.isEmpty()) {
            throw new IOException("ForStRsStateBackend: localDbDir must be set");
        }
        Path p = new Path(localDbDir);
        // Existence is not required — the directory is created on open.
        if (p.getPath().isBlank()) {
            throw new IOException("ForStRsStateBackend: localDbDir path is blank");
        }
    }

    @Override
    public String toString() {
        return "ForStRsStateBackend{"
                + "localDbDir='"
                + localDbDir
                + "', remoteCheckpointUri="
                + remoteCheckpointUri
                + '}';
    }
}

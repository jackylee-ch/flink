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

package org.apache.flink.state.forstrs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForStRsStateBackendFactoryTest {

    @Test
    void createFromConfigCarriesRemoteStorageOptionsIntoBackend() {
        Configuration config = new Configuration();
        config.set(ForStRsOptions.CF_MODE, "per-state");
        config.set(ForStRsOptions.STORAGE_URI, "s3://forst-rs/checkpoints");
        config.set(ForStRsOptions.OPENDAL_CONFIG, "{\"region\":\"us-east-1\"}");
        config.set(ForStRsOptions.CACHE_DIR, "/tmp/forst-rs-cache");
        config.set(ForStRsOptions.CACHE_CAPACITY_MB, 2048L);
        config.set(
                ForStRsConfigurableOptions.WRITE_BUFFER_SIZE, MemorySize.parse("128mb"));
        config.set(ForStRsConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, 6);
        config.set(
                ForStRsConfigurableOptions.WRITE_BUFFER_MANAGER_CAPACITY,
                MemorySize.parse("768mb"));
        config.set(
                ForStRsConfigurableOptions.BLOCK_CACHE_CAPACITY, MemorySize.parse("384mb"));
        config.set(ForStRsConfigurableOptions.MAX_BACKGROUND_COMPACTIONS, 5);
        config.set(ForStRsConfigurableOptions.MAX_BACKGROUND_FLUSHES, 3);

        ForStRsStateBackend backend =
                new ForStRsStateBackendFactory()
                        .createFromConfig(
                                config, Thread.currentThread().getContextClassLoader());

        ForStRsOptions options = backend.optionsForTesting();
        assertEquals(ForStRsOptions.CfMode.PER_STATE, options.cfMode());
        assertEquals("s3://forst-rs/checkpoints", options.storageUri());
        assertEquals("{\"region\":\"us-east-1\"}", options.opendalConfigJson());
        assertEquals("/tmp/forst-rs-cache", options.cacheDir());
        assertEquals(2048L, options.cacheCapacityMb());
        assertEquals(128L * 1024 * 1024, options.writeBufferSizeBytes());
        assertEquals(6, options.maxWriteBufferNumber());
        assertEquals(768L * 1024 * 1024, options.writeBufferManagerCapacityBytes());
        assertEquals(384L * 1024 * 1024, options.blockCacheCapacityBytes());
        assertEquals(5, options.maxBackgroundCompactions());
        assertEquals(3, options.maxBackgroundFlushes());
    }

    /**
     * FRS-PHASE2 backend-effective feature flags: defaults preserve pre-Phase-2 behaviour — SST
     * compression is lz4 (engine default, discriminant 2), everything else OFF.
     */
    @Test
    void featureFlagDefaultsPreservePrePhase2Behaviour() {
        Configuration config = new Configuration();
        ForStRsStateBackend backend =
                new ForStRsStateBackendFactory()
                        .createFromConfig(
                                config, Thread.currentThread().getContextClassLoader());
        ForStRsOptions options = backend.optionsForTesting();
        assertEquals("lz4", options.sstCompression());
        assertEquals(2, options.sstCompressionDiscriminant());
        assertEquals("inherit", options.vlogCompression());
        assertFalse(options.kvSeparation());
        assertEquals(128L, options.kvSeparationMinBlobSizeBytes());
        assertFalse(options.trivialMove());
        assertFalse(options.remoteCompaction());
    }

    /** FRS-PHASE2: every feature-flag config key plumbs through to {@link ForStRsOptions}. */
    @Test
    void featureFlagConfigPlumbsIntoOptions() {
        Configuration config = new Configuration();
        config.set(ForStRsConfigurableOptions.SST_COMPRESSION, "zstd");
        config.set(ForStRsConfigurableOptions.VLOG_COMPRESSION, "none");
        config.set(ForStRsConfigurableOptions.KV_SEPARATION, true);
        config.set(
                ForStRsConfigurableOptions.KV_SEPARATION_MIN_BLOB_SIZE, MemorySize.parse("256 bytes"));
        config.set(ForStRsConfigurableOptions.TRIVIAL_MOVE, true);
        config.set(ForStRsConfigurableOptions.REMOTE_COMPACTION, true);

        ForStRsStateBackend backend =
                new ForStRsStateBackendFactory()
                        .createFromConfig(
                                config, Thread.currentThread().getContextClassLoader());
        ForStRsOptions options = backend.optionsForTesting();
        assertEquals("zstd", options.sstCompression());
        assertEquals(3, options.sstCompressionDiscriminant());
        assertEquals("none", options.vlogCompression());
        assertTrue(options.kvSeparation());
        assertEquals(256L, options.kvSeparationMinBlobSizeBytes());
        assertTrue(options.trivialMove());
        assertTrue(options.remoteCompaction());
    }

    /** FRS-PHASE2: {@code none} SST compression maps to discriminant 1. */
    @Test
    void sstCompressionNoneMapsToDiscriminantOne() {
        Configuration config = new Configuration();
        config.set(ForStRsConfigurableOptions.SST_COMPRESSION, "none");
        ForStRsStateBackend backend =
                new ForStRsStateBackendFactory()
                        .createFromConfig(
                                config, Thread.currentThread().getContextClassLoader());
        assertEquals(1, backend.optionsForTesting().sstCompressionDiscriminant());
    }
}

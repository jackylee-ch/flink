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
}

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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;

/**
 * Resolved memory configuration for a forst-rs backend instance — mirrors forst's
 * {@code ForStMemoryConfiguration}. Captures the WriteBufferManager budget and the block-cache
 * capacity as they will be passed to the engine's {@code frs_db_open_with_options}.
 *
 * <p>Constructed by {@link #fromConfig(ReadableConfig)} via {@link ForStRsConfigurableOptions}.
 */
@PublicEvolving
public final class ForStRsMemoryConfiguration {

    private final long writeBufferManagerCapacityBytes;
    private final long blockCacheCapacityBytes;

    public ForStRsMemoryConfiguration(
            long writeBufferManagerCapacityBytes, long blockCacheCapacityBytes) {
        this.writeBufferManagerCapacityBytes = writeBufferManagerCapacityBytes;
        this.blockCacheCapacityBytes = blockCacheCapacityBytes;
    }

    public long getWriteBufferManagerCapacityBytes() {
        return writeBufferManagerCapacityBytes;
    }

    public long getBlockCacheCapacityBytes() {
        return blockCacheCapacityBytes;
    }

    /** Builds the configuration from a Flink {@link ReadableConfig}. */
    public static ForStRsMemoryConfiguration fromConfig(ReadableConfig cfg) {
        MemorySize wbm = cfg.get(ForStRsConfigurableOptions.WRITE_BUFFER_MANAGER_CAPACITY);
        MemorySize bc = cfg.get(ForStRsConfigurableOptions.BLOCK_CACHE_CAPACITY);
        return new ForStRsMemoryConfiguration(
                wbm == null ? 0L : wbm.getBytes(), bc == null ? 0L : bc.getBytes());
    }

    @Override
    public String toString() {
        return "ForStRsMemoryConfiguration{wbm="
                + writeBufferManagerCapacityBytes
                + " bytes, blockCache="
                + blockCacheCapacityBytes
                + " bytes}";
    }
}

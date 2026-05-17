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
 * Default {@link ForStRsOptionsFactory} that reads each engine knob from the supplied Flink {@link
 * ReadableConfig} using the {@link ForStRsConfigurableOptions} keys. Mirrors forst's {@code
 * DefaultConfigurableOptionsFactory}.
 *
 * <p>Honoured today: {@code writeBufferSize}, {@code maxWriteBufferNumber}, {@code
 * writeBufferManagerCapacity}, {@code blockCacheCapacity}, {@code maxBackgroundCompactions}, {@code
 * maxBackgroundFlushes}. Advisory: target-file-size, level-size-multiplier, block-size.
 */
@PublicEvolving
public final class ForStRsConfigurableOptionsFactory implements ForStRsOptionsFactory {

    private static final long serialVersionUID = 1L;

    private final ReadableConfig config;

    public ForStRsConfigurableOptionsFactory(ReadableConfig config) {
        this.config = config;
    }

    @Override
    public ForStRsEngineOptionsBuilder createForStRsOptions(ForStRsEngineOptionsBuilder defaults) {
        defaults.writeBufferSize =
                config.get(ForStRsConfigurableOptions.WRITE_BUFFER_SIZE).getBytes();
        defaults.maxWriteBufferNumber =
                config.get(ForStRsConfigurableOptions.MAX_WRITE_BUFFER_NUMBER);
        defaults.writeBufferManagerCapacity =
                config.get(ForStRsConfigurableOptions.WRITE_BUFFER_MANAGER_CAPACITY).getBytes();
        defaults.blockCacheCapacity =
                config.get(ForStRsConfigurableOptions.BLOCK_CACHE_CAPACITY).getBytes();
        defaults.maxBackgroundCompactions =
                config.get(ForStRsConfigurableOptions.MAX_BACKGROUND_COMPACTIONS);
        defaults.maxBackgroundFlushes =
                config.get(ForStRsConfigurableOptions.MAX_BACKGROUND_FLUSHES);
        MemorySize tfs = config.get(ForStRsConfigurableOptions.TARGET_FILE_SIZE_BASE);
        defaults.targetFileSizeBase = tfs == null ? 0L : tfs.getBytes();
        Integer lm = config.get(ForStRsConfigurableOptions.LEVEL_SIZE_MULTIPLIER);
        defaults.levelSizeMultiplier = lm == null ? 0 : lm;
        MemorySize bs = config.get(ForStRsConfigurableOptions.BLOCK_SIZE);
        defaults.blockSize = bs == null ? 0L : bs.getBytes();
        return defaults;
    }
}

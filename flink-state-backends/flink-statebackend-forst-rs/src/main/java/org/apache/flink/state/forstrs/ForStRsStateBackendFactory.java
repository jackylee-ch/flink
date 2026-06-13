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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackendFactory;

/** SPI factory for {@link ForStRsStateBackend}. */
public class ForStRsStateBackendFactory implements StateBackendFactory<ForStRsStateBackend> {

    @Override
    public ForStRsStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader) {
        ForStRsOptions options = new ForStRsOptions();
        options.cfMode(ForStRsOptions.CfMode.fromConfig(config.get(ForStRsOptions.CF_MODE)));
        options.storageUri(config.get(ForStRsOptions.STORAGE_URI));
        options.opendalConfigJson(config.get(ForStRsOptions.OPENDAL_CONFIG));
        options.cacheDir(config.get(ForStRsOptions.CACHE_DIR));
        options.cacheCapacityMb(config.get(ForStRsOptions.CACHE_CAPACITY_MB));
        options.walDir(config.get(ForStRsOptions.WAL_DIR));
        ForStRsEngineOptionsBuilder engineOptions =
                new ForStRsConfigurableOptionsFactory(config)
                        .createForStRsOptions(new ForStRsEngineOptionsBuilder());
        options.writeBufferSizeBytes(engineOptions.writeBufferSize);
        options.maxWriteBufferNumber(engineOptions.maxWriteBufferNumber);
        options.maxBackgroundCompactions(engineOptions.maxBackgroundCompactions);
        options.maxBackgroundFlushes(engineOptions.maxBackgroundFlushes);
        options.blockCacheCapacityBytes(engineOptions.blockCacheCapacity);
        options.writeBufferManagerCapacityBytes(engineOptions.writeBufferManagerCapacity);
        // FRS-PHASE2 backend-effective feature flags (defaults preserve
        // pre-Phase-2 behaviour exactly: lz4 SST = engine default, rest OFF).
        options.sstCompression(config.get(ForStRsConfigurableOptions.SST_COMPRESSION));
        options.vlogCompression(config.get(ForStRsConfigurableOptions.VLOG_COMPRESSION));
        options.kvSeparation(config.get(ForStRsConfigurableOptions.KV_SEPARATION));
        options.kvSeparationMinBlobSizeBytes(
                config.get(ForStRsConfigurableOptions.KV_SEPARATION_MIN_BLOB_SIZE).getBytes());
        options.trivialMove(config.get(ForStRsConfigurableOptions.TRIVIAL_MOVE));
        options.remoteCompaction(config.get(ForStRsConfigurableOptions.REMOTE_COMPACTION));
        return new ForStRsStateBackend(options);
    }
}

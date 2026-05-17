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

import org.apache.flink.annotation.Internal;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsDb;

/**
 * Runtime view over the engine's shared memory resources — mirror of forst's {@code
 * ForStSharedResources}. Exposes the WriteBufferManager capacity and current usage so the
 * native-metric monitor (and any operator-level back-pressure logic) can poll without a fresh FFI
 * binding at every callsite.
 *
 * <p>Block-cache stats are not exposed yet because the engine doesn't currently surface them as a
 * single number (it has multi-tier caching with per-tier counters; a follow-up FFI {@code
 * frs_db_block_cache_stats_arrow} should land in the SP4 native-metrics work).
 */
@Internal
public final class ForStRsSharedResources {

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final ForStRsMemoryConfiguration config;

    public ForStRsSharedResources(
            ForStRsLinker linker, FrsDb db, ForStRsMemoryConfiguration config) {
        this.linker = linker;
        this.db = db;
        this.config = config;
    }

    /** Returns the configured WBM budget (from {@link ForStRsMemoryConfiguration}). */
    public long getConfiguredWbmCapacity() {
        return config.getWriteBufferManagerCapacityBytes();
    }

    /** Returns the WBM capacity actually in use by the engine (read live from native). */
    public long getEngineWbmCapacity() {
        return linker.dbWriteBufferManagerCapacity(db);
    }

    /** Returns the WBM bytes currently reserved by live memtables (read live from native). */
    public long getEngineWbmCurrentBytes() {
        return linker.dbWriteBufferManagerCurrentBytes(db);
    }

    /** Returns the configured block-cache capacity (from {@link ForStRsMemoryConfiguration}). */
    public long getConfiguredBlockCacheCapacity() {
        return config.getBlockCacheCapacityBytes();
    }

    public ForStRsMemoryConfiguration getMemoryConfiguration() {
        return config;
    }
}

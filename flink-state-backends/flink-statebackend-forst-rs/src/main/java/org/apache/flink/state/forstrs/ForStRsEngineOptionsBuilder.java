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

/**
 * Mutable builder for the engine options passed to {@code frs_db_open_with_options}. Plain POJO
 * — pluggable {@link ForStRsOptionsFactory} implementations read defaults and overwrite the
 * fields they want to change.
 *
 * <p>All sizes are bytes; zero means "engine default" (the Rust side picks). The advisory knobs
 * are accepted but currently ignored by the engine — see {@link ForStRsConfigurableOptions} for
 * which ones are live.
 */
@PublicEvolving
public final class ForStRsEngineOptionsBuilder {

    /** Bytes. 0 = engine default. */
    public long writeBufferSize;

    /** Count. 0 = engine default. */
    public int maxWriteBufferNumber;

    /** Bytes. 0 = engine default. */
    public long writeBufferManagerCapacity;

    /** Bytes. 0 = engine default. */
    public long blockCacheCapacity;

    /** Count. 0 = engine default. */
    public int maxBackgroundCompactions;

    /** Count. 0 = engine default. */
    public int maxBackgroundFlushes;

    /** Advisory only. */
    public long targetFileSizeBase;

    /** Advisory only. */
    public int levelSizeMultiplier;

    /** Advisory only. */
    public long blockSize;

    public ForStRsEngineOptionsBuilder() {}

    /** Convenience: fluent setter. */
    public ForStRsEngineOptionsBuilder withWriteBufferSize(long bytes) {
        this.writeBufferSize = bytes;
        return this;
    }

    public ForStRsEngineOptionsBuilder withMaxWriteBufferNumber(int n) {
        this.maxWriteBufferNumber = n;
        return this;
    }

    public ForStRsEngineOptionsBuilder withWriteBufferManagerCapacity(long bytes) {
        this.writeBufferManagerCapacity = bytes;
        return this;
    }

    public ForStRsEngineOptionsBuilder withBlockCacheCapacity(long bytes) {
        this.blockCacheCapacity = bytes;
        return this;
    }

    public ForStRsEngineOptionsBuilder withMaxBackgroundCompactions(int n) {
        this.maxBackgroundCompactions = n;
        return this;
    }

    public ForStRsEngineOptionsBuilder withMaxBackgroundFlushes(int n) {
        this.maxBackgroundFlushes = n;
        return this;
    }
}

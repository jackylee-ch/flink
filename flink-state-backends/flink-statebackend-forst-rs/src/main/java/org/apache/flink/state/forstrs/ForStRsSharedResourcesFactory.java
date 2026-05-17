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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsDb;

/**
 * Factory for {@link ForStRsSharedResources} — mirrors forst's {@code ForStSharedResourcesFactory}.
 * Resolves the memory configuration from a Flink config and binds it to a freshly-opened engine
 * handle.
 *
 * <p>In a future cycle this factory will become the place where per-TaskSlot sharing is implemented
 * (one shared block-cache + WBM across multiple keyed backends in the same slot). Today each
 * backend gets its own engine + its own shared-resources view.
 */
@Internal
public final class ForStRsSharedResourcesFactory {

    private ForStRsSharedResourcesFactory() {}

    /** Builds the shared-resources view for a freshly-opened {@link FrsDb}. */
    public static ForStRsSharedResources create(
            ForStRsLinker linker, FrsDb db, ReadableConfig cfg) {
        ForStRsMemoryConfiguration mem = ForStRsMemoryConfiguration.fromConfig(cfg);
        return new ForStRsSharedResources(linker, db, mem);
    }
}

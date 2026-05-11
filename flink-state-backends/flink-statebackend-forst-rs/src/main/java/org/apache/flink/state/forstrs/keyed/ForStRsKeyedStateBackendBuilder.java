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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.forstrs.ForStRsOptions;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.cf.CfRouter;
import org.apache.flink.state.forstrs.keyed.cf.PerStateCfRouter;
import org.apache.flink.state.forstrs.keyed.cf.SingleCfRouter;

import java.lang.foreign.Arena;
import java.util.Objects;

/**
 * Builder for {@link ForStRsKeyedStateBackend} resources. Currently exposes only the {@link
 * CfRouter} factory; future PRs add snapshot strategy, restore operation, etc.
 */
public final class ForStRsKeyedStateBackendBuilder<K> {

    private final ForStRsLinker linker;
    private final Arena arena;
    private final TypeSerializer<K> keySerializer;
    private final ForStRsOptions options;

    private FrsDb db;
    private FrsCfHandle defaultCf;

    public ForStRsKeyedStateBackendBuilder(
            ForStRsLinker linker,
            Arena arena,
            TypeSerializer<K> keySerializer,
            ForStRsOptions options) {
        this.linker = Objects.requireNonNull(linker);
        this.arena = Objects.requireNonNull(arena);
        this.keySerializer = Objects.requireNonNull(keySerializer);
        this.options = Objects.requireNonNull(options);
    }

    public ForStRsKeyedStateBackendBuilder<K> withDb(FrsDb db, FrsCfHandle defaultCf) {
        this.db = db;
        this.defaultCf = defaultCf;
        return this;
    }

    public CfRouter buildCfRouter() {
        if (db == null || defaultCf == null) {
            throw new IllegalStateException("withDb(db, defaultCf) must be called first");
        }
        return switch (options.cfMode()) {
            case SINGLE -> new SingleCfRouter(defaultCf);
            case PER_STATE -> new PerStateCfRouter(linker, db, arena);
        };
    }

    public ForStRsLinker linker() {
        return linker;
    }

    public Arena arena() {
        return arena;
    }

    public TypeSerializer<K> keySerializer() {
        return keySerializer;
    }

    public ForStRsOptions options() {
        return options;
    }
}

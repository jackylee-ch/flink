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

package org.apache.flink.state.forstrs.keyed.cf;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link CfRouter} that lazily creates one CF per state name on first {@link
 * #getCfForState(String)} call. Enforces a soft limit of 256 CFs to bound engine resource use; once
 * exceeded throws {@link IllegalStateException} with explicit guidance to switch to {@code
 * cf.mode=single}.
 */
public final class PerStateCfRouter implements CfRouter {

    private static final int SOFT_LIMIT_CFS = 256;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final Arena arena;
    private final Map<String, FrsCfHandle> stateToCf = new LinkedHashMap<>();
    private final Map<FrsCfHandle, String> cfToState = new LinkedHashMap<>();

    public PerStateCfRouter(ForStRsLinker linker, FrsDb db, Arena arena) {
        this.linker = linker;
        this.db = db;
        this.arena = arena;
    }

    @Override
    public synchronized FrsCfHandle getCfForState(String stateName) {
        FrsCfHandle existing = stateToCf.get(stateName);
        if (existing != null) {
            return existing;
        }
        if (stateToCf.size() >= SOFT_LIMIT_CFS) {
            throw new IllegalStateException(
                    "PerStateCfRouter exceeded soft limit of "
                            + SOFT_LIMIT_CFS
                            + " CFs (state="
                            + stateName
                            + "). Switch to cf.mode=single or reduce state count.");
        }
        FrsCfHandle cf = linker.dbCreateCf(db, arena, stateName);
        stateToCf.put(stateName, cf);
        cfToState.put(cf, stateName);
        return cf;
    }

    @Override
    public synchronized Collection<FrsCfHandle> allCfs() {
        return new ArrayList<>(stateToCf.values());
    }

    @Override
    public synchronized String stateNameForCf(FrsCfHandle cf) {
        String name = cfToState.get(cf);
        if (name == null) {
            throw new IllegalArgumentException("Unknown CF passed to stateNameForCf");
        }
        return name;
    }

    @Override
    public boolean isSingleCf() {
        return false;
    }

    @Override
    public synchronized void close() {
        for (FrsCfHandle cf : stateToCf.values()) {
            cf.close();
        }
        stateToCf.clear();
        cfToState.clear();
    }
}

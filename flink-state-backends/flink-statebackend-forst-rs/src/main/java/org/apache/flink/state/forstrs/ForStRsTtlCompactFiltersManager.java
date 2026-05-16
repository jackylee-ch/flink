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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.util.HashMap;
import java.util.Map;

/**
 * Engine-side TTL compaction-filter manager — mirror of forst's {@code
 * ForStDBTtlCompactFiltersManager}. Tracks per-state TTL configuration and installs the engine TTL
 * filter via {@link ForStRsLinker#setCompactionFilterTtl}.
 *
 * <p>The actual TTL enforcement runs on the Rust side in {@code
 * crates/forst-rs-engine/src/compaction_filter.rs}: at flush + L0→L1 compaction the filter drops
 * entries whose embedded timestamp is older than {@code ttlMs}. Entries are NOT filtered at read
 * time — read filtering is the Flink-runtime layer's responsibility (matches forst's contract).
 */
@Internal
public final class ForStRsTtlCompactFiltersManager {

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle defaultCf;
    private final Map<String, TtlRegistration> registrations = new HashMap<>();

    public ForStRsTtlCompactFiltersManager(ForStRsLinker linker, FrsDb db, FrsCfHandle defaultCf) {
        this.linker = linker;
        this.db = db;
        this.defaultCf = defaultCf;
    }

    /**
     * Registers TTL for a state. {@code stateType} is one of {@link ForStRsLinker#STATE_TYPE_VALUE}
     * or {@link ForStRsLinker#STATE_TYPE_LIST}; pass {@link ForStRsLinker#STATE_TYPE_DISABLED} to
     * tear down a prior registration. {@code timestampOffset} is the byte offset of the embedded
     * timestamp in the serialized value (0 for value-state, list-element offset for list-state).
     */
    public void setTtlForState(
            String stateName,
            long ttlMs,
            int stateType,
            long timestampOffset) {
        setTtlForState(stateName, defaultCf, ttlMs, stateType, timestampOffset);
    }

    public void setTtlForState(
            String stateName,
            FrsCfHandle cf,
            long ttlMs,
            int stateType,
            long timestampOffset) {
        linker.setCompactionFilterTtl(db, cf, ttlMs, stateType, timestampOffset);
        registrations.put(stateName, new TtlRegistration(cf, ttlMs, stateType, timestampOffset));
    }

    /** Returns {@code true} if the named state has an active TTL registration. */
    public boolean hasTtl(String stateName) {
        TtlRegistration r = registrations.get(stateName);
        return r != null && r.stateType != ForStRsLinker.STATE_TYPE_DISABLED;
    }

    /**
     * Maps a Flink {@link StateTtlConfig} onto the engine TTL surface. Update / visibility
     * semantics are enforced at the Flink runtime layer; this manager only installs the
     * background compaction filter that physically removes expired entries.
     */
    public void register(
            String stateName,
            StateTtlConfig ttlConfig,
            int stateType,
            long timestampOffset) {
        if (ttlConfig == null || !ttlConfig.isEnabled()) {
            return;
        }
        // OnCreateAndWrite + NeverReturnExpired is the only fully-supported combo for
        // background-cleanup-only enforcement; other combos still install the filter but
        // require additional runtime-side filtering (matches forst).
        UpdateType update = ttlConfig.getUpdateType();
        StateVisibility vis = ttlConfig.getStateVisibility();
        if (update == UpdateType.Disabled) {
            return;
        }
        long ttlMs = ttlConfig.getTimeToLive().toMillis();
        setTtlForState(stateName, ttlMs, stateType, timestampOffset);
    }

    private static final class TtlRegistration {
        final FrsCfHandle cf;
        final long ttlMs;
        final int stateType;
        final long timestampOffset;

        TtlRegistration(FrsCfHandle cf, long ttlMs, int stateType, long timestampOffset) {
            this.cf = cf;
            this.ttlMs = ttlMs;
            this.stateType = stateType;
            this.timestampOffset = timestampOffset;
        }
    }
}

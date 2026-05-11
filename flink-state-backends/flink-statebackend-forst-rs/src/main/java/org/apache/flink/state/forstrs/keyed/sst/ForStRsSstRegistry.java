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

package org.apache.flink.state.forstrs.keyed.sst;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Per-backend ref-counted SST registry (B-Prod-P3 Task 3.1).
 *
 * <p>Tracks each uploaded SST file by its {@link StateHandleID} along with the {@link
 * StreamStateHandle} returned from the checkpoint stream factory. The entry is reference-counted: a
 * subsequent {@link #register(StateHandleID, StreamStateHandle)} on an existing id bumps the
 * ref-count without replacing the handle, while {@link #unregister(StateHandleID)} decrements the
 * count and evicts the entry once it reaches zero (so the underlying {@link StreamStateHandle} is
 * eligible for discard once the last referencing checkpoint is subsumed).
 *
 * <p>Thread safety is provided by an intrinsic monitor on the registry instance, sufficient for the
 * checkpoint-coordination paths that drive it (one thread captures a snapshot + registers, one
 * thread notifies completion / abort + unregisters). Concurrent stress is exercised by {@link
 * ForStRsSstRegistryTest#concurrentRegisterUnregister()}.
 *
 * <p>This is the Flink-side counterpart to the engine's pinned-SST tracking; the engine ensures
 * each `live` SST stays on disk until its retaining snapshots release, the Java registry ensures
 * each uploaded {@link StreamStateHandle} stays referenceable on remote storage until its retaining
 * checkpoints subsume.
 */
@Internal
public final class ForStRsSstRegistry {

    /** A registered entry: the uploaded handle plus a ref-count. */
    private static final class RegistryEntry {
        final StreamStateHandle handle;
        int refCount;

        RegistryEntry(StreamStateHandle handle) {
            this.handle = handle;
            this.refCount = 1;
        }
    }

    private final Map<StateHandleID, RegistryEntry> entries = new HashMap<>();

    /**
     * Registers an SST with its uploaded stream handle. If {@code id} is already registered the
     * ref-count is incremented and the supplied {@code handle} is ignored (the original entry is
     * authoritative — the SST bytes are immutable so the duplicate handle would point at the same
     * remote bytes anyway).
     *
     * <p>Returns {@code true} if this call created a new entry, {@code false} if it bumped an
     * existing one.
     */
    public synchronized boolean register(StateHandleID id, StreamStateHandle handle) {
        RegistryEntry existing = entries.get(id);
        if (existing != null) {
            existing.refCount++;
            return false;
        }
        entries.put(id, new RegistryEntry(handle));
        return true;
    }

    /**
     * Decrements the ref-count for {@code id}; evicts the entry once the count reaches zero.
     *
     * <p>Returns {@code true} if the entry was evicted (final ref dropped), {@code false} if the
     * entry remains because other checkpoints still reference it, and also {@code false} if the id
     * was not present.
     */
    public synchronized boolean unregister(StateHandleID id) {
        RegistryEntry entry = entries.get(id);
        if (entry == null) {
            return false;
        }
        entry.refCount--;
        if (entry.refCount <= 0) {
            entries.remove(id);
            return true;
        }
        return false;
    }

    /** Returns the stream handle for {@code id} if registered, empty otherwise. */
    public synchronized Optional<StreamStateHandle> get(StateHandleID id) {
        RegistryEntry entry = entries.get(id);
        return entry == null ? Optional.empty() : Optional.of(entry.handle);
    }

    /** Returns the current ref-count for {@code id}; 0 if not registered. */
    public synchronized int refCount(StateHandleID id) {
        RegistryEntry entry = entries.get(id);
        return entry == null ? 0 : entry.refCount;
    }

    /** Returns the number of distinct registered ids (entries with ref-count &gt; 0). */
    public synchronized int size() {
        return entries.size();
    }

    /** Returns {@code true} iff {@code id} has at least one outstanding reference. */
    public synchronized boolean contains(StateHandleID id) {
        return entries.containsKey(id);
    }
}

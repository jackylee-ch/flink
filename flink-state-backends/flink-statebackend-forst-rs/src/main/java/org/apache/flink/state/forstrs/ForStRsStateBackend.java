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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;

import java.lang.foreign.Arena;

/**
 * {@link StateBackend} backed by ForSt-RS via JDK 25 FFM.
 *
 * <p><b>v3.2 Phase-D L5 status.</b> This backend exposes:
 *
 * <ul>
 *   <li>{@link #createKeyedStateBackend(KeyedStateBackendParameters)} — currently throws because
 *       the simpler {@link ForStRsKeyedStateBackend} stepping-stone does <i>not</i> implement
 *       {@link CheckpointableKeyedStateBackend} (no snapshot / key-group / savepoint plumbing yet).
 *       Use {@link #createBasicKeyedBackend(TypeSerializer)} for proof-of-concept and unit tests
 *       until the L5 sync-v1 / L6 rescaling work lands.
 *   <li>{@link #createOperatorStateBackend(OperatorStateBackendParameters)} — delegates to Flink's
 *       {@link DefaultOperatorStateBackendBuilder}, which is the standard pattern for backends
 *       whose operator state is just a serialized bytestream rather than a KV store.
 * </ul>
 *
 * @see ForStRsOptions
 * @see ForStRsLinker
 * @see ForStRsKeyedStateBackend
 */
public class ForStRsStateBackend implements StateBackend {

    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "forst-rs";
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            StateBackend.KeyedStateBackendParameters<K> parameters) throws Exception {
        // ForStRsKeyedStateBackend (Phase-D L5 stepping stone) does not yet implement the full
        // CheckpointableKeyedStateBackend surface (snapshot, key-groups, savepoint, applyToAllKeys
        // — ~25 methods). Tests should call createBasicKeyedBackend directly until L5/L6 lands.
        throw new UnsupportedOperationException(
                "ForStRsStateBackend.createKeyedStateBackend returning a "
                        + "CheckpointableKeyedStateBackend is a Phase-D L5/L6 follow-up. "
                        + "Use ForStRsStateBackend.createBasicKeyedBackend(keySerializer) for the "
                        + "stepping-stone proof-of-concept that wires the 5 state types end-to-end.");
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            StateBackend.OperatorStateBackendParameters parameters) throws Exception {
        // ForSt-RS does not store operator state itself — it is a key-value store. Operator state
        // is a serialized bytestream that Flink's default builder handles directly.
        return new DefaultOperatorStateBackendBuilder(
                        Thread.currentThread().getContextClassLoader(),
                        parameters.getEnv().getExecutionConfig(),
                        /* asynchronousSnapshots= */ true,
                        parameters.getStateHandles(),
                        parameters.getCancelStreamRegistry())
                .build();
    }

    /**
     * Phase-D L5 stepping-stone factory: opens an in-memory ForSt-RS engine and returns a {@link
     * ForStRsKeyedStateBackend} bound to it. The returned backend owns the underlying {@link
     * Arena}, {@link ForStRsLinker}, {@link FrsDb} and default {@link FrsCfHandle}; closing it
     * releases all of them.
     *
     * <p>This entry-point is provided because {@link
     * #createKeyedStateBackend(KeyedStateBackendParameters)} cannot yet return a {@link
     * ForStRsKeyedStateBackend} — that class does not implement the {@link
     * CheckpointableKeyedStateBackend} contract. Once snapshot / key-group / savepoint plumbing is
     * wired in Phase-D L5/L6 the two factory methods will collapse into one.
     */
    public <K> ForStRsKeyedStateBackend<K> createBasicKeyedBackend(
            TypeSerializer<K> keySerializer) {
        Arena arena = Arena.ofShared();
        try {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpenMemory(arena);
            FrsCfHandle cf;
            try {
                cf = linker.dbDefaultCf(db, arena);
            } catch (RuntimeException e) {
                db.close();
                throw e;
            }
            return new ForStRsKeyedStateBackend<>(arena, linker, db, cf, keySerializer);
        } catch (RuntimeException e) {
            arena.close();
            throw e;
        }
    }
}

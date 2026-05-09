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

import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;

/**
 * SKELETON {@link StateBackend} backed by ForSt-RS via JDK 25 FFM.
 *
 * <p>v3.2 Phase-A MVP scope: this class exists and is SPI-discoverable; it does NOT yet implement
 * {@code createKeyedStateBackend} or {@code createOperatorStateBackend}. Those land in subsequent
 * Phase-D L4 (Async v2) and L5 (Sync v1) units per {@code
 * docs/superpowers/planning/v3.2/reports/B1_pr_split_plan.md}.
 *
 * @see ForStRsOptions
 * @see org.apache.flink.state.forstrs.ffm.ForStRsLinker
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
        throw new UnsupportedOperationException(
                "ForStRsStateBackend.createKeyedStateBackend is not yet implemented "
                        + "(v3.2 Phase-D L4/L5 work; current state is Phase-A MVP skeleton).");
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            StateBackend.OperatorStateBackendParameters parameters) throws Exception {
        throw new UnsupportedOperationException(
                "ForStRsStateBackend.createOperatorStateBackend is not yet implemented "
                        + "(v3.2 Phase-D L4/L5 work; current state is Phase-A MVP skeleton).");
    }
}

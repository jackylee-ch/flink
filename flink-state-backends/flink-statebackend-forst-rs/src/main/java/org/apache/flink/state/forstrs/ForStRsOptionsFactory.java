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
 * User-pluggable factory for customising the engine-options struct that the {@link
 * ForStRsStateBackend} passes to {@code frs_db_open_with_options}. Mirrors forst's {@code
 * ForStOptionsFactory}.
 *
 * <p>Implementations can override individual fields on the supplied {@link
 * ForStRsEngineOptionsBuilder}; the framework supplies defaults derived from the Flink {@code
 * Configuration} and from {@link ForStRsConfigurableOptions}, so a factory typically only touches
 * the knobs it wants to override.
 *
 * <p>For most users {@link ForStRsConfigurableOptionsFactory} (auto-wired by the backend from the
 * Flink {@code Configuration}) is sufficient and no custom factory is needed.
 */
@PublicEvolving
@FunctionalInterface
public interface ForStRsOptionsFactory extends java.io.Serializable {

    /**
     * Applies user customizations to the engine options. Called once per backend-init, after the
     * framework has applied defaults from the Flink {@code Configuration}.
     */
    ForStRsEngineOptionsBuilder createForStRsOptions(ForStRsEngineOptionsBuilder defaults);
}

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

import org.apache.flink.state.forstrs.ffm.FrsCfHandle;

import java.io.Closeable;
import java.util.Collection;

/**
 * Routes a Flink keyed-state name to a {@link FrsCfHandle}. Implementations decide whether all
 * states share one CF ({@link SingleCfRouter}) or each state gets its own ({@link
 * PerStateCfRouter}). Per spec §7.
 */
public interface CfRouter extends Closeable {

    FrsCfHandle getCfForState(String stateName);

    Collection<FrsCfHandle> allCfs();

    String stateNameForCf(FrsCfHandle cf);

    boolean isSingleCf();

    @Override
    void close();
}

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

import java.util.Collection;
import java.util.Collections;

/**
 * {@link CfRouter} that returns the same default {@link FrsCfHandle} for every state name. This is
 * the lowest-overhead config and the project default.
 */
public final class SingleCfRouter implements CfRouter {

    public static final String SHARED_CF_NAME = "default";

    private final FrsCfHandle cf;

    public SingleCfRouter(FrsCfHandle cf) {
        this.cf = cf;
    }

    @Override
    public FrsCfHandle getCfForState(String stateName) {
        return cf;
    }

    @Override
    public Collection<FrsCfHandle> allCfs() {
        return Collections.singletonList(cf);
    }

    @Override
    public String stateNameForCf(FrsCfHandle cf) {
        return SHARED_CF_NAME;
    }

    @Override
    public boolean isSingleCf() {
        return true;
    }

    @Override
    public void close() {
        // The default CF is owned by the backend, not by us.
    }
}

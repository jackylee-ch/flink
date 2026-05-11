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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ForStRsOptionsTest {

    @Test
    void cfModeDefaultsSingle() {
        assertEquals(ForStRsOptions.CfMode.SINGLE, ForStRsOptions.CfMode.fromConfig(null));
        assertEquals(ForStRsOptions.CfMode.SINGLE, ForStRsOptions.CfMode.fromConfig(""));
    }

    @Test
    void cfModeParsesPerState() {
        assertEquals(
                ForStRsOptions.CfMode.PER_STATE, ForStRsOptions.CfMode.fromConfig("per-state"));
    }

    @Test
    void cfModeRejectsUnknown() {
        assertThrows(
                IllegalArgumentException.class, () -> ForStRsOptions.CfMode.fromConfig("multi"));
    }

    @Test
    void buildsCorrectRouterFromCfMode() {
        try (java.lang.foreign.Arena arena = java.lang.foreign.Arena.ofShared()) {
            org.apache.flink.state.forstrs.ffm.ForStRsLinker linker =
                    new org.apache.flink.state.forstrs.ffm.ForStRsLinker(arena);
            try (org.apache.flink.state.forstrs.ffm.FrsDb db = linker.dbOpenMemory(arena);
                    org.apache.flink.state.forstrs.ffm.FrsCfHandle cf =
                            linker.dbDefaultCf(db, arena)) {

                ForStRsOptions optsSingle =
                        new ForStRsOptions().cfMode(ForStRsOptions.CfMode.SINGLE);
                org.apache.flink.state.forstrs.keyed.cf.CfRouter rs =
                        new org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackendBuilder<
                                        String>(
                                        linker,
                                        arena,
                                        org.apache.flink.api.common.typeutils.base.StringSerializer
                                                .INSTANCE,
                                        optsSingle)
                                .withDb(db, cf)
                                .buildCfRouter();
                org.junit.jupiter.api.Assertions.assertTrue(rs.isSingleCf());

                ForStRsOptions optsPer =
                        new ForStRsOptions().cfMode(ForStRsOptions.CfMode.PER_STATE);
                org.apache.flink.state.forstrs.keyed.cf.CfRouter rp =
                        new org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackendBuilder<
                                        String>(
                                        linker,
                                        arena,
                                        org.apache.flink.api.common.typeutils.base.StringSerializer
                                                .INSTANCE,
                                        optsPer)
                                .withDb(db, cf)
                                .buildCfRouter();
                org.junit.jupiter.api.Assertions.assertFalse(rp.isSingleCf());
                rp.close();
            }
        }
    }
}

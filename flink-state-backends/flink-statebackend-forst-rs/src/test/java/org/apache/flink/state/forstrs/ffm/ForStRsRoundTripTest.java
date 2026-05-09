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

package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * End-to-end FFM round-trip: open in-memory ForSt-RS engine via JDK 25 FFM, put and get a value,
 * close. Proves Phase-A Flink-side bootstrap is functional.
 *
 * <p>Requires the system property {@code forstrs.native.libpath} pointing to {@code
 * libforst_rs_ffi.{dylib,so,dll}}. Surefire is configured to pass this at module level — see module
 * pom.xml.
 */
class ForStRsRoundTripTest {

    @Test
    void putGetRoundTrip() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                byte[] key = "hello".getBytes(StandardCharsets.UTF_8);
                byte[] expectedValue = "world".getBytes(StandardCharsets.UTF_8);

                // Initially absent
                assertNull(linker.get(db, cf, key));

                // Put + get round-trip
                linker.put(db, cf, key, expectedValue);
                byte[] actualValue = linker.get(db, cf, key);

                assertArrayEquals(expectedValue, actualValue);
            }
        }
    }
}

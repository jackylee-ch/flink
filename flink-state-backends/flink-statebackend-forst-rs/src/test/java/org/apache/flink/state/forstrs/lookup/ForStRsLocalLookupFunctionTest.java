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

package org.apache.flink.state.forstrs.lookup;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ForStRsLocalLookupFunction}: exact-match lookup via {@code lookupKv} and
 * multi-row prefix scan via {@code evalPrefix}.
 */
class ForStRsLocalLookupFunctionTest {

    /** Encode the first column of {@link RowData} as UTF-8 bytes. */
    private static final Function<RowData, byte[]> KEY_ENCODER =
            row -> row.getString(0).toString().getBytes(StandardCharsets.UTF_8);

    /** Decode a UTF-8 byte payload into a single-column row of strings. */
    private static final Function<byte[], RowData> VALUE_DECODER =
            bytes ->
                    GenericRowData.of(
                            StringData.fromString(new String(bytes, StandardCharsets.UTF_8)));

    private static GenericRowData rowOf(String s) {
        return GenericRowData.of(StringData.fromString(s));
    }

    @Test
    void exactMatchEvalReturnsDecodedValue() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                linker.put(
                        db,
                        cf,
                        "k1".getBytes(StandardCharsets.UTF_8),
                        "v1".getBytes(StandardCharsets.UTF_8));

                ForStRsLocalLookupFunction fn =
                        new ForStRsLocalLookupFunction(linker, db, cf, KEY_ENCODER, VALUE_DECODER);

                CompletableFuture<RowData> hit = new CompletableFuture<>();
                fn.eval(hit, rowOf("k1"));
                RowData hitResult = hit.get();
                assertNotNull(hitResult);
                assertEquals("v1", hitResult.getString(0).toString());

                CompletableFuture<RowData> miss = new CompletableFuture<>();
                fn.eval(miss, rowOf("nope"));
                assertNull(miss.get());
            }
        }
    }

    @Test
    void evalPrefixReturnsAllMatchingRows() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                // Three rows sharing prefix "p:" plus one outside the prefix.
                linker.put(
                        db,
                        cf,
                        "p:1".getBytes(StandardCharsets.UTF_8),
                        "alpha".getBytes(StandardCharsets.UTF_8));
                linker.put(
                        db,
                        cf,
                        "p:2".getBytes(StandardCharsets.UTF_8),
                        "beta".getBytes(StandardCharsets.UTF_8));
                linker.put(
                        db,
                        cf,
                        "p:3".getBytes(StandardCharsets.UTF_8),
                        "gamma".getBytes(StandardCharsets.UTF_8));
                linker.put(
                        db,
                        cf,
                        "x:1".getBytes(StandardCharsets.UTF_8),
                        "outside".getBytes(StandardCharsets.UTF_8));

                ForStRsLocalLookupFunction fn =
                        new ForStRsLocalLookupFunction(linker, db, cf, KEY_ENCODER, VALUE_DECODER);

                CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
                fn.evalPrefix(future, "p:".getBytes(StandardCharsets.UTF_8));

                Collection<RowData> rows = future.get();
                assertEquals(3, rows.size());
                List<String> values =
                        rows.stream()
                                .map(r -> r.getString(0).toString())
                                .collect(Collectors.toList());
                assertEquals(List.of("alpha", "beta", "gamma"), values);
            }
        }
    }

    @Test
    void evalPrefixEmptyPrefixReturnsAllRows() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {
                linker.put(
                        db,
                        cf,
                        "a".getBytes(StandardCharsets.UTF_8),
                        "1".getBytes(StandardCharsets.UTF_8));
                linker.put(
                        db,
                        cf,
                        "b".getBytes(StandardCharsets.UTF_8),
                        "2".getBytes(StandardCharsets.UTF_8));

                ForStRsLocalLookupFunction fn =
                        new ForStRsLocalLookupFunction(linker, db, cf, KEY_ENCODER, VALUE_DECODER);

                CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
                fn.evalPrefix(future, new byte[0]);
                Collection<RowData> rows = future.get();
                assertTrue(rows.size() >= 2, "expected at least 2 rows, got " + rows.size());
            }
        }
    }
}

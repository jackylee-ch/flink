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

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker.IteratorEntry;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A12-M2 — backend-level regression for the D10-M3 / A11-H1 fix.
 *
 * <p>{@code ForStRsLinkerExtendedTest.emptyValueRoundTripsAsPutNotDelete} exercises the raw
 * {@code linker.batchPut} surface. That test does not, however, run through
 * {@link ForStRsKeyedStateBackend#flushWriteBuffer()}, which is the actual D10-M3 regression site:
 * the chunk loop there allocates per-entry payload segments and must substitute a 1-byte non-NULL
 * sentinel for empty values so the Rust FFI does not interpret a NULL pointer as DELETE.
 *
 * <p>This test:
 *
 * <ol>
 *   <li>constructs a real {@link ForStRsKeyedStateBackend} backed by an in-memory FrsDb;
 *   <li>buffers a write with a zero-length value (via the same {@code putToWriteBuffer} entry that
 *       the V1-sync ValueState {@code update()} path uses);
 *   <li>buffers a companion non-empty write so we exercise the chunked loop with mixed payload
 *       sizes;
 *   <li>calls {@code flushWriteBuffer()} explicitly to invoke the actual D10-M3 site;
 *   <li>opens a full-range iterator on the underlying db and asserts BOTH keys are present, with
 *       the empty-value key reporting a zero-length payload (NOT absent / tombstoned).
 * </ol>
 *
 * <p>The iterator witness is necessary because the Rust FFI's {@code FrsBytes::from_vec} of an
 * empty Vec returns {@code data=NULL}, which {@code linker.get} cannot distinguish from a true
 * absence. The iterator iterates the engine's actual visible key set and is unambiguous.
 */
class FlushWriteBufferEmptyValueTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void emptyValuePassesThroughFlushWriteBufferAsPutNotTombstone() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpenMemory(arena);
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);
            // The backend owns the db/cf/linker; close() releases everything when ownsResources=true.
            // We pass ownsResources=false here because we want to control teardown order ourselves
            // (the arena try-with-resources owns the underlying handles).
            ForStRsKeyedStateBackend<Integer> backend =
                    new ForStRsKeyedStateBackend<>(
                            arena, linker, db, cf, IntSerializer.INSTANCE,
                            /* ownsResources= */ false);
            try {
                byte[] emptyKey = utf8("empty-val-key");
                byte[] nonEmptyKey = utf8("non-empty-val-key");
                byte[] emptyValue = new byte[0];
                byte[] nonEmptyValue = utf8("payload");

                // Route through the same entry the V1-sync ValueState.update() uses. The convenience
                // overload `putToWriteBuffer(byte[], byte[])` delegates to the slice form with
                // (valBuf, 0, value.length). For the empty case the resulting slice has valLen == 0,
                // which is precisely the case the D10-M3 chunk loop must NOT translate to NULL.
                backend.putToWriteBuffer(emptyKey, emptyValue);
                backend.putToWriteBuffer(nonEmptyKey, nonEmptyValue);

                // Invoke the actual D10-M3 site: the chunked loop in flushWriteBuffer must emit a
                // non-NULL pointer for the zero-length entry (1-byte sentinel allocation guards
                // against the FFI NULL-as-DELETE collision documented in `flushWriteBuffer`'s
                // method-level comment).
                backend.flushWriteBuffer();

                // Witness via full-range iterator on the underlying engine. If D10-M3 had been
                // reverted (or never applied), the empty-value key would be tombstoned and only one
                // key would be observed.
                List<String> observedKeys = new ArrayList<>();
                List<Integer> observedValueLens = new ArrayList<>();
                try (FrsIterator iter = linker.iteratorOpen(db, cf, arena)) {
                    IteratorEntry entry;
                    while ((entry = linker.iteratorNext(iter)) != null) {
                        observedKeys.add(new String(entry.key(), StandardCharsets.UTF_8));
                        observedValueLens.add(entry.value().length);
                    }
                }

                assertTrue(
                        observedKeys.contains("empty-val-key"),
                        "empty-value key must be PRESENT after flushWriteBuffer (NULL-as-DELETE"
                                + " collision would tombstone it)");
                assertTrue(
                        observedKeys.contains("non-empty-val-key"),
                        "non-empty companion must also be present after flushWriteBuffer");

                int idxEmpty = observedKeys.indexOf("empty-val-key");
                assertEquals(
                        0,
                        observedValueLens.get(idxEmpty).intValue(),
                        "empty-value key must retrieve a zero-length payload via flushWriteBuffer");

                int idxNonEmpty = observedKeys.indexOf("non-empty-val-key");
                assertEquals(
                        nonEmptyValue.length,
                        observedValueLens.get(idxNonEmpty).intValue(),
                        "non-empty companion payload length must be preserved through flushWriteBuffer");
            } finally {
                backend.close();
                cf.close();
                db.close();
            }
        }
    }
}

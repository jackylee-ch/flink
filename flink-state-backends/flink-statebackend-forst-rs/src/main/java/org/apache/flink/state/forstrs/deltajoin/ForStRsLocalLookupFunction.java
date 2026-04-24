/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.state.forstrs.deltajoin;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.state.forstrs.bridge.ForStRsBridge;
import org.apache.flink.state.forstrs.bridge.ForStRsColumnFamily;
import org.apache.flink.state.forstrs.bridge.ForStRsDb;
import org.apache.flink.state.forstrs.bridge.ForStRsException;
import org.apache.flink.state.forstrs.bridge.ForStRsStatus;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

/**
 * DeltaJoin local Lookup function.
 *
 * <p>Invokes the Rust engine's {@code frs_prefix_scan_arrow} to return the
 * full set of dimension-table rows matching a composite join key prefix,
 * entirely in-process (zero RPC, zero JNI copy). See roadmap §3.3
 * (P2.3 DeltaJoin Localization).
 *
 * <p>The returned Arrow RecordBatch is consumed by the caller without
 * intermediate serialization. Release is managed by Arrow's C Data
 * Interface release callbacks — the caller simply closes the returned
 * {@link VectorSchemaRoot}.
 */
public final class ForStRsLocalLookupFunction implements AutoCloseable {

    private final ForStRsDb db;
    private final ForStRsColumnFamily cf;
    private final BufferAllocator allocator;

    public ForStRsLocalLookupFunction(
            ForStRsDb db, ForStRsColumnFamily cf, BufferAllocator allocator) {
        this.db = db;
        this.cf = cf;
        this.allocator = allocator;
    }

    /**
     * Returns every (key, value) pair whose key starts with {@code prefix}.
     */
    public List<Row> lookup(byte[] prefix) {
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outSchema = ArrowSchema.allocateNew(allocator);
                Arena call = Arena.ofConfined()) {
            MemorySegment cPrefix = call.allocate(prefix.length);
            MemorySegment.copy(prefix, 0, cPrefix, ValueLayout.JAVA_BYTE, 0, prefix.length);
            MemorySegment arrSeg =
                    MemorySegment.ofAddress(outArray.memoryAddress()).reinterpret(Long.MAX_VALUE);
            MemorySegment schSeg =
                    MemorySegment.ofAddress(outSchema.memoryAddress())
                            .reinterpret(Long.MAX_VALUE);
            int status =
                    (int)
                            ForStRsBridge.FRS_PREFIX_SCAN_ARROW.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    cPrefix,
                                    (long) prefix.length,
                                    arrSeg,
                                    schSeg);
            ForStRsBridge.check(status, "frs_prefix_scan_arrow");

            try (VectorSchemaRoot root =
                    Data.importVectorSchemaRoot(allocator, outArray, outSchema, null)) {
                VarBinaryVector keys = (VarBinaryVector) root.getVector("key");
                VarBinaryVector values = (VarBinaryVector) root.getVector("value");
                int rows = root.getRowCount();
                List<Row> out = new ArrayList<>(rows);
                for (int i = 0; i < rows; i++) {
                    out.add(new Row(keys.get(i), values.get(i)));
                }
                return out;
            }
        } catch (Throwable t) {
            if (t instanceof ForStRsException e) {
                throw e;
            }
            throw new ForStRsException(
                    ForStRsStatus.ERROR, "ForStRsLocalLookupFunction.lookup", t);
        }
    }

    /** A single (key, value) row returned by the lookup. */
    public record Row(byte[] key, byte[] value) {}

    @Override
    public void close() {
        // Allocator is owned by the caller.
    }
}

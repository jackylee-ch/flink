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
package org.apache.flink.state.forstrs.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.state.forstrs.bridge.ForStRsBridge;
import org.apache.flink.state.forstrs.bridge.ForStRsColumnFamily;
import org.apache.flink.state.forstrs.bridge.ForStRsDb;
import org.apache.flink.state.forstrs.bridge.ForStRsException;
import org.apache.flink.state.forstrs.bridge.ForStRsStatus;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Arrow-native batch-put adapter.
 *
 * <p>Builds a RecordBatch with the schema expected by {@code
 * frs_batch_put_arrow} (key: Binary, value: Binary nullable, op_type:
 * UInt8) and ships it across the FFI boundary via the Arrow C Data
 * Interface — zero extra copies between Java heap and the Rust engine.
 *
 * <p>Consumers should reuse a single adapter per operator task so the
 * backing Arrow allocator can pool buffers across batches.
 */
public final class ArrowBatchPut implements AutoCloseable {

    /** OpType encoding matching the Rust side {@code common::types::OpType}. */
    public static final byte OP_PUT = 0;
    public static final byte OP_DELETE = 1;
    public static final byte OP_MERGE = 3;

    private static final Schema BATCH_SCHEMA =
            new Schema(
                    List.of(
                            new Field(
                                    "key",
                                    new FieldType(false, new ArrowType.Binary(), null),
                                    Collections.emptyList()),
                            new Field(
                                    "value",
                                    new FieldType(true, new ArrowType.Binary(), null),
                                    Collections.emptyList()),
                            new Field(
                                    "op_type",
                                    new FieldType(
                                            false, new ArrowType.Int(8, false), null),
                                    Collections.emptyList())));

    private final BufferAllocator allocator;

    public ArrowBatchPut(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /** Writes a batch via the Arrow zero-copy path. */
    public void flush(ForStRsDb db, ForStRsColumnFamily cf, List<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(BATCH_SCHEMA, allocator)) {
            root.allocateNew();
            VarBinaryVector keys = (VarBinaryVector) root.getVector("key");
            VarBinaryVector values = (VarBinaryVector) root.getVector("value");
            UInt1Vector ops = (UInt1Vector) root.getVector("op_type");
            for (int i = 0; i < entries.size(); i++) {
                Entry e = entries.get(i);
                keys.setSafe(i, e.key);
                if (e.value == null) {
                    values.setNull(i);
                } else {
                    values.setSafe(i, e.value);
                }
                ops.setSafe(i, e.opType & 0xFF);
            }
            root.setRowCount(entries.size());

            try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
                    ArrowSchema outSchema = ArrowSchema.allocateNew(allocator);
                    Arena callArena = Arena.ofConfined()) {
                Data.exportVectorSchemaRoot(allocator, root, null, outArray, outSchema);
                // ArrowArray.memoryAddress() / ArrowSchema.memoryAddress()
                // return the raw C struct pointers; wrap them as
                // MemorySegments sized to the layout so the downcall can
                // pass them unchanged to Rust.
                MemorySegment arrSeg =
                        MemorySegment.ofAddress(outArray.memoryAddress()).reinterpret(Long.MAX_VALUE);
                MemorySegment schSeg =
                        MemorySegment.ofAddress(outSchema.memoryAddress())
                                .reinterpret(Long.MAX_VALUE);
                int status =
                        (int)
                                ForStRsBridge.FRS_BATCH_PUT_ARROW.invokeExact(
                                        db.handle(), cf.handle(), arrSeg, schSeg);
                ForStRsBridge.check(status, "frs_batch_put_arrow");
                // Rust side zeroes the structs on success so ArrowArray /
                // ArrowSchema's own release callbacks become no-ops here.
            }
        } catch (Throwable t) {
            if (t instanceof ForStRsException e) {
                throw e;
            }
            throw new ForStRsException(ForStRsStatus.ERROR, "ArrowBatchPut.flush", t);
        }
    }

    /** A single entry to batch. */
    public record Entry(byte[] key, byte[] value, byte opType) {}

    /** Convenience builder. */
    public static List<Entry> entries() {
        return new ArrayList<>();
    }

    @Override
    public void close() {
        // allocator is owned by the caller
    }

    /** Exposes the schema for consumers that want to validate inputs. */
    public static Schema schema() {
        return BATCH_SCHEMA;
    }

    @SuppressWarnings("unused")
    private static UInt1Vector asUInt1(BitVector v) {
        // Placeholder — kept for future use when batch contains boolean columns.
        return null;
    }
}

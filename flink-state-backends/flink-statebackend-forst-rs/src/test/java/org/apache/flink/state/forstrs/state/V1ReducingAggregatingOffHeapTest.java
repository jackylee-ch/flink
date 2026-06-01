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

/*
 * FRS-V1-VEC (2026-06-01): correctness test for the V1 Reducing/Aggregating off-heap
 * batch-execution rewrite. Verifies: (a) reduce/aggregate is applied correctly across
 * adds via the off-heap statebuf, (b) get() reads the buffered accumulator, (c) the
 * batched flush (flushAllOffHeapValueStateBuffers) drains to the engine AND a later add
 * reads it back via getPinnedSegment and continues reducing (the critical coherence path),
 * (d) clear() invalidates both buffer + engine.
 */

package org.apache.flink.state.forstrs.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class V1ReducingAggregatingOffHeapTest {

    private static final ReduceFunction<Long> SUM = (a, b) -> a + b;

    private static final AggregateFunction<Long, Long, Long> AGG_SUM =
            new AggregateFunction<>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Long value, Long acc) {
                    return acc + value;
                }

                @Override
                public Long getResult(Long acc) {
                    return acc;
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            };

    @Test
    void reducingState_offheap_batchedReduceAndFlushReadback() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpenMemory(arena);
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);
            try {
                ForStRsKeyedStateBackend<String> backend =
                        new ForStRsKeyedStateBackend<>(
                                arena, linker, db, cf, StringSerializer.INSTANCE);
                backend.setCurrentKey("k1");
                ReducingState<Long> rs = backend.getReducingState("sum", LongSerializer.INSTANCE, SUM);

                assertNull(rs.get(), "empty initially");
                rs.add(1L);
                rs.add(2L);
                rs.add(3L);
                assertEquals(6L, rs.get(), "reduce across buffered adds");

                // Drain the off-heap buffer to the engine (the checkpoint batch flush).
                backend.flushAllOffHeapValueStateBuffers();
                assertEquals(6L, rs.get(), "value survives batch flush (engine read-back)");

                // A later add must read the flushed accumulator back (getPinnedSegment) + reduce.
                rs.add(4L);
                assertEquals(10L, rs.get(), "reduce continues after flush+evict");

                rs.clear();
                assertNull(rs.get(), "cleared");
                rs.add(7L);
                assertEquals(7L, rs.get(), "fresh after clear");
            } finally {
                cf.close();
                db.close();
            }
        }
    }

    @Test
    void aggregatingState_offheap_batchedAddAndFlushReadback() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpenMemory(arena);
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);
            try {
                ForStRsKeyedStateBackend<String> backend =
                        new ForStRsKeyedStateBackend<>(
                                arena, linker, db, cf, StringSerializer.INSTANCE);
                backend.setCurrentKey("k1");
                AggregatingState<Long, Long> as =
                        backend.getAggregatingState("agg", LongSerializer.INSTANCE, AGG_SUM);

                assertNull(as.get(), "empty initially");
                as.add(10L);
                as.add(20L);
                assertEquals(30L, as.get(), "aggregate across buffered adds");

                backend.flushAllOffHeapValueStateBuffers();
                assertEquals(30L, as.get(), "value survives batch flush (engine read-back)");

                as.add(5L);
                assertEquals(35L, as.get(), "aggregate continues after flush+evict");

                as.clear();
                assertNull(as.get(), "cleared");
            } finally {
                cf.close();
                db.close();
            }
        }
    }
}

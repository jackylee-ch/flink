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

package org.apache.flink.state.forstrs.cache;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PendingMissTableTest {

    @Test
    void firstMissCreatesEntry() {
        PendingMissTable<Integer, Integer> table = new PendingMissTable<>();
        AtomicInteger getsIssued = new AtomicInteger();
        Object key = new byte[] {1, 2, 3};
        table.beginOrJoin(
                "rstate",
                key,
                10,
                () -> {
                    getsIssued.incrementAndGet();
                    return null;
                });
        assertEquals(1, getsIssued.get(), "first miss must issue one GET");
        assertEquals(1, table.activeMissCount());
    }

    @Test
    void subsequentMissesOnSameKeyJoinConvoy() {
        PendingMissTable<Integer, Integer> table = new PendingMissTable<>();
        AtomicInteger getsIssued = new AtomicInteger();
        Object key = new byte[] {1, 2, 3};
        table.beginOrJoin(
                "rstate",
                key,
                10,
                () -> {
                    getsIssued.incrementAndGet();
                    return null;
                });
        table.beginOrJoin(
                "rstate",
                key,
                20,
                () -> {
                    getsIssued.incrementAndGet();
                    return null;
                });
        table.beginOrJoin(
                "rstate",
                key,
                30,
                () -> {
                    getsIssued.incrementAndGet();
                    return null;
                });
        assertEquals(1, getsIssued.get(), "ALL three calls must coalesce into one GET");
        assertEquals(1, table.activeMissCount());
        assertEquals(3, table.pendingInputsCount("rstate", key));
    }

    @Test
    void resolveFoldsInOrder() {
        PendingMissTable<Integer, Integer> table = new PendingMissTable<>();
        Object key = new byte[] {1, 2, 3};
        CopyOnWriteArrayList<Integer> acks = new CopyOnWriteArrayList<>();
        table.beginOrJoin("rstate", key, 10, () -> null);
        table.beginOrJoin("rstate", key, 20, () -> null);
        table.beginOrJoin("rstate", key, 30, () -> null);
        // Simulate engine returning null (key didn't exist before this convoy)
        Integer finalAcc =
                table.resolve(
                        "rstate", key, null, (acc, in) -> acc == null ? in : acc + in, acks::add);
        assertEquals(Integer.valueOf(60), finalAcc);
        assertEquals(3, acks.size());
        // All acks should carry the final accumulator value (60)
        for (Integer ack : acks) {
            assertEquals(Integer.valueOf(60), ack, "each ack must carry the final accumulator");
        }
    }

    @Test
    void combinerThrowFailsConvoy() {
        PendingMissTable<Integer, Integer> table = new PendingMissTable<>();
        Object key = new byte[] {9, 9};
        CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
        table.beginOrJoin("rstate", key, 1, () -> null);
        table.beginOrJoin("rstate", key, 2, () -> null);
        table.beginOrJoin("rstate", key, 3, () -> null);
        table.resolveWithFailureHandler(
                "rstate",
                key,
                null,
                (acc, in) -> {
                    throw new RuntimeException("bad combiner");
                },
                ack -> {},
                failures::add);
        // All three callers must be notified of the failure
        assertEquals(3, failures.size());
        for (Throwable t : failures) {
            assertTrue(t.getMessage().contains("bad combiner"));
        }
    }
}

/*
 * FRS-V1-VEC (2026-06-01): correctness test for the V1 ListState append-merge rewrite
 * (add/addAll now append a RawConcat merge operand instead of read-modify-write; get
 * decodes the concatenated [count][elems] chunks).
 */
package org.apache.flink.state.forstrs.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackend;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class V1ListStateMergeAppendTest {

    private static List<Long> drain(Iterable<Long> it) {
        List<Long> out = new ArrayList<>();
        if (it != null) {
            for (Long v : it) {
                out.add(v);
            }
        }
        return out;
    }

    @Test
    void mergeAppendRoundTrip() throws Exception {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            FrsDb db = linker.dbOpenMemory(arena);
            FrsCfHandle cf = linker.dbDefaultCf(db, arena);
            try {
                ForStRsKeyedStateBackend<String> backend =
                        new ForStRsKeyedStateBackend<>(
                                arena, linker, db, cf, StringSerializer.INSTANCE);
                backend.setCurrentKey("k1");
                ListState<Long> ls =
                        backend.getListState("mylist", LongSerializer.INSTANCE);

                assertTrue(drain(ls.get()).isEmpty(), "empty initially");

                // add() -> two separate merge operands; get() concatenates.
                ls.add(10L);
                ls.add(20L);
                assertEquals(Arrays.asList(10L, 20L), drain(ls.get()), "two single adds");

                // addAll() -> one merge operand [count=2].
                ls.addAll(Arrays.asList(30L, 40L));
                assertEquals(
                        Arrays.asList(10L, 20L, 30L, 40L), drain(ls.get()), "addAll appends");

                // update() -> Put base (replaces); subsequent add() merges on top.
                ls.update(Arrays.asList(99L));
                assertEquals(Arrays.asList(99L), drain(ls.get()), "update replaces");
                ls.add(100L);
                assertEquals(
                        Arrays.asList(99L, 100L),
                        drain(ls.get()),
                        "merge operand resolves on top of a Put base");

                // clear() -> delete; get() empty.
                ls.clear();
                assertTrue(drain(ls.get()).isEmpty(), "cleared");

                // After clear, adds start a fresh merge chain.
                ls.add(7L);
                assertEquals(Arrays.asList(7L), drain(ls.get()), "fresh chain after clear");
            } finally {
                cf.close();
                db.close();
            }
        }
    }
}

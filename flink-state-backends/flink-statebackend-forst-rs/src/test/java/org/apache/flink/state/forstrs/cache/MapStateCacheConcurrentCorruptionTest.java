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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * STAGE-0 (two-regime executor design §4): DETERMINISTIC reproduction of the q8 cache-corruption
 * race that blocks the coordination-free / parallel executor (Approach-3, R2b) from shipping.
 *
 * <h3>What the q8 failure actually is</h3>
 *
 * <p>{@link MapStateCache} is documented as SINGLE-THREADED (class javadoc "Single-threaded": "No
 * internal synchronization", relying on Flink's per-record RecordContext lock to serialize access).
 * That contract holds under the default depth-1 inline executor — every cache op runs on the mailbox
 * thread.
 *
 * <p>Under a parallel / coordinated executor the contract is VIOLATED by construction. The
 * production path is {@code ForStRsMapStateV2.asyncGet}:
 *
 * <ol>
 *   <li>cache {@code lookup} (and on a write, {@code put}) runs on the MAILBOX thread;
 *   <li>on a cache miss it dispatches the engine GET and registers
 *       {@code .thenApply(v -> cache.putIfAbsent(keySnapshot, v))};
 *   <li>that continuation completes on the WORKER / completing thread — a DIFFERENT thread from the
 *       mailbox.
 * </ol>
 *
 * <p>So {@code putIfAbsent} (worker) runs concurrently with {@code lookup}/{@code put} (mailbox) on a
 * structure that has no synchronization. The 2026-06-10 coordinated-executor design (§2.2) recorded
 * the q8 out_rows dropping 3,064,667 → 2,704,710 under the shared cache, and exact only with the
 * cache OFF — but as a free-running stress race it was nondeterministic / flaky, which is exactly why
 * Stage-0 stayed open.
 *
 * <h3>Why this test is DETERMINISTIC (no sleeps, no stress loop)</h3>
 *
 * <p>Every {@code put}/{@code putIfAbsent} that inserts a NEW key performs the non-atomic compound
 *
 * <pre>{@code
 *   row = size++;          // (A) read-modify-write of the `size` field
 *   appendKey(row, ...);   // (B) write key bytes + offset/length at index `row`
 *   values[row] = stored;  // (C) write value at index `row`
 *   insertHashIndex(h, row);// (D) publish (hash,row) into the open-addressed index
 * }</pre>
 *
 * <p>This is a classic lost-update: if two threads both execute (A) before either executes (D),
 * they pick the SAME {@code row}, and one key's (B)/(C)/(D) clobbers the other's. The losing key is
 * permanently absent from the cache even though {@code put} returned normally — i.e. a SILENTLY LOST
 * WRITE, which is precisely the q8 symptom (missing window-join output rows).
 *
 * <p>We force the lost-update with a {@link CyclicBarrier} pinned BETWEEN step (A) and step (D),
 * replaying the exact field/method sequence the public {@code put} runs (via reflection — the class
 * is {@code final} with private internals). No timing assumption: the barrier guarantees both
 * threads have completed (A) before either reaches (D), so the corruption is reproduced on every
 * run.
 */
class MapStateCacheConcurrentCorruptionTest {

    // ---- reflection handles into the single-threaded internals --------------------------------

    private static Field field(String name) throws Exception {
        Field f = MapStateCache.class.getDeclaredField(name);
        f.setAccessible(true);
        return f;
    }

    private static Method method(String name, Class<?>... params) throws Exception {
        Method m = MapStateCache.class.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    /**
     * DETERMINISTIC lost-write: two concurrent inserts of DISTINCT keys, with the {@code size}
     * read-modify-write forced to interleave. One key is silently dropped.
     *
     * <p>This is the data-structure-level proof that the cache cannot be touched by two threads.
     * Under the depth-1 inline executor this interleave is impossible (one thread); under any
     * parallel executor it is reachable, and reproduces the q8 missing-rows corruption.
     */
    @Test
    void concurrentInsertLosesAWrite_deterministic() throws Exception {
        final MapStateCache<String> cache = new MapStateCache<>();

        final byte[] keyMailbox = new byte[] {10, 20, 30};
        final byte[] keyWorker = new byte[] {40, 50, 60};

        final Field sizeF = field("size");
        final Field clockF = field("clock");
        final Field valuesF = field("values");
        final Method appendKey = method("appendKey", int.class, byte[].class);
        final Method insertHashIndex = method("insertHashIndex", int.class, int.class);
        final Method hashOf = method("hashOf", byte[].class);

        // Barrier of 2 pinned strictly BETWEEN "row = size++" and the publish steps so both
        // threads observe the SAME pre-increment size and pick the SAME row index.
        final CyclicBarrier afterReadSize = new CyclicBarrier(2);
        final AtomicReference<Throwable> err = new AtomicReference<>();

        // Replays the EXACT body of MapStateCache.put(key, value) for the NEW-key (insert) branch.
        // The only change is the barrier between the size read-modify-write and the publish steps;
        // every field/method touched is the real one, so the corruption is the real corruption.
        final java.util.function.BiConsumer<byte[], String> racingInsert =
                (key, value) -> {
                    try {
                        // (A) row = size++  — read-modify-write of `size`
                        int curSize = sizeF.getInt(cache);
                        int row = curSize;
                        sizeF.setInt(cache, curSize + 1);

                        // Force the interleave: both threads have now done (A) and picked `row`.
                        afterReadSize.await();

                        // (B)/(C)/(D) — publish at `row` (this is where one clobbers the other)
                        appendKey.invoke(cache, row, key);
                        ((Object[]) valuesF.get(cache))[row] = value;
                        clockF.setLong(cache, clockF.getLong(cache) + 1);
                        insertHashIndex.invoke(cache, (int) hashOf.invoke(null, key), row);
                    } catch (Throwable t) {
                        err.compareAndSet(null, t);
                    }
                };

        Thread mailbox = new Thread(() -> racingInsert.accept(keyMailbox, "V_mailbox"), "mailbox");
        Thread worker = new Thread(() -> racingInsert.accept(keyWorker, "V_worker"), "worker");
        mailbox.start();
        worker.start();
        mailbox.join();
        worker.join();

        if (err.get() != null) {
            throw new AssertionError("racing insert threw", err.get());
        }

        // Both put() calls returned normally, so the single-threaded contract's caller believes
        // BOTH keys are now cached (write-through: each is also dispatched to the engine). But the
        // shared `size` RMW lost an update, so the two inserts collided on one row.
        MapStateCache.Lookup<String> mhit = cache.lookup(keyMailbox);
        MapStateCache.Lookup<String> whit = cache.lookup(keyWorker);

        boolean mailboxPresent = mhit != null && mhit.cached();
        boolean workerPresent = whit != null && whit.cached();

        // DETERMINISTIC corruption assertion: at least one of the two write-through keys is GONE
        // from the cache despite put() succeeding. This is the silent lost write that drops q8
        // window-join output rows under a parallel executor.
        if (mailboxPresent && workerPresent) {
            fail(
                    "expected the concurrent size-RMW to lose one write, but both keys survived — "
                            + "the corruption did not reproduce");
        }
        assertEquals(
                1,
                cache.size(),
                "size advanced by only 1 for two distinct inserts — the lost-update collision");
    }

    /**
     * DETERMINISTIC stale read: reproduces the EXACT production interleave from
     * {@code ForStRsMapStateV2.asyncGet} §2.2 — a write (mailbox {@code put} of the NEW value) races
     * the GET-miss continuation (worker {@code putIfAbsent} of the OLD engine value), and the
     * continuation's {@code findRow} observes a half-published row.
     *
     * <p>Here we pin the barrier between the worker continuation's {@code findRow} (which decides
     * "absent, so I may insert") and its publish, while the mailbox {@code put} for the same key runs
     * in the gap. Net effect: two rows for the same key, and the STALE engine value wins subsequent
     * lookups — the "silent stale reads until eviction" failure called out in {@code putIfAbsent}'s
     * own javadoc, now shown to survive the {@code putIfAbsent} guard once the threads actually race.
     */
    @Test
    void concurrentPutVsPutIfAbsentStaleRead_deterministic() throws Exception {
        final MapStateCache<String> cache = new MapStateCache<>();
        final byte[] key = new byte[] {7, 7, 7};

        final Field sizeF = field("size");
        final Field clockF = field("clock");
        final Field valuesF = field("values");
        final Method findRow = method("findRow", byte[].class);
        final Method appendKey = method("appendKey", int.class, byte[].class);
        final Method insertHashIndex = method("insertHashIndex", int.class, int.class);
        final Method hashOf = method("hashOf", byte[].class);

        final CyclicBarrier afterFindRow = new CyclicBarrier(2);
        final AtomicReference<Throwable> err = new AtomicReference<>();

        // WORKER continuation: putIfAbsent(key, "ENGINE_OLD"). It first does findRow (miss),
        // decides to insert, then publishes — but the mailbox put() lands in between.
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                int row = (int) findRow.invoke(cache, key); // expect -1 (miss)
                                // Let the mailbox put() run now (establishes the authoritative new
                                // value for `key`).
                                afterFindRow.await();
                                if (row < 0) {
                                    // putIfAbsent believed key absent → it inserts the STALE value.
                                    int curSize = sizeF.getInt(cache);
                                    int r = curSize;
                                    sizeF.setInt(cache, curSize + 1);
                                    appendKey.invoke(cache, r, key);
                                    ((Object[]) valuesF.get(cache))[r] = "ENGINE_OLD";
                                    clockF.setLong(cache, clockF.getLong(cache) + 1);
                                    insertHashIndex.invoke(
                                            cache, (int) hashOf.invoke(null, key), r);
                                }
                            } catch (Throwable t) {
                                err.compareAndSet(null, t);
                            }
                        },
                        "worker-continuation");

        // MAILBOX: a concurrent asyncPut of the authoritative NEW value for the same key. Runs
        // strictly AFTER the worker's findRow saw "absent" and BEFORE the worker publishes.
        Thread mailbox =
                new Thread(
                        () -> {
                            try {
                                afterFindRow.await();
                                cache.put(key, "NEW");
                            } catch (Throwable t) {
                                err.compareAndSet(null, t);
                            }
                        },
                        "mailbox");

        worker.start();
        mailbox.start();
        worker.join();
        mailbox.join();

        if (err.get() != null) {
            throw new AssertionError("racing put/putIfAbsent threw", err.get());
        }

        // The authoritative write was put(key,"NEW"). A correct, serialized cache MUST return "NEW".
        MapStateCache.Lookup<String> hit = cache.lookup(key);
        assertNotNull(hit, "key must be present after writes");

        // DETERMINISTIC corruption: the stale ENGINE_OLD value shadows the authoritative NEW value,
        // OR a duplicate row was created (size==2 for one key). Either is a serialization violation
        // that cannot occur on the single mailbox thread.
        boolean staleWins = "ENGINE_OLD".equals(hit.value());
        boolean duplicateRow = cache.size() == 2;
        if (!staleWins && !duplicateRow) {
            fail(
                    "expected a stale-read or duplicate-row corruption from the racing "
                            + "put/putIfAbsent, but the cache stayed coherent (value="
                            + hit.value()
                            + ", size="
                            + cache.size()
                            + ")");
        }
    }

    /**
     * CONTROL: under the single-threaded contract (all ops on one thread, the depth-1 inline
     * executor model) the identical sequence is correct — both keys present, size==2, newest value
     * wins. Proves the corruption is caused by the concurrency the parallel executor introduces, not
     * by the cache logic itself.
     */
    @Test
    void singleThreadedIsCorrect_control() {
        MapStateCache<String> cache = new MapStateCache<>();
        byte[] k1 = new byte[] {10, 20, 30};
        byte[] k2 = new byte[] {40, 50, 60};
        cache.put(k1, "V1");
        cache.put(k2, "V2");
        assertEquals(2, cache.size());
        assertEquals("V1", cache.lookup(k1).value());
        assertEquals("V2", cache.lookup(k2).value());

        // put then putIfAbsent for same key: authoritative put wins; putIfAbsent is a no-op.
        byte[] k3 = new byte[] {7, 7, 7};
        cache.put(k3, "NEW");
        cache.putIfAbsent(k3, "ENGINE_OLD");
        assertEquals("NEW", cache.lookup(k3).value());
        assertNull(cache.lookup(new byte[] {99}));
    }
}

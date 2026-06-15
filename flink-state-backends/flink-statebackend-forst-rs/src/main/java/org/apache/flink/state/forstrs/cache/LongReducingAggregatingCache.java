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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongBinaryOperator;

/**
 * B10-H2: primitive-{@code long} specialization of {@link ReducingAggregatingCache}.
 *
 * <p>The general-purpose {@link ReducingAggregatingCache}{@code <Long, Long>} stores its
 * accumulator as a boxed {@code Long} reference inside each {@code Entry}: every
 * {@code combiner.apply} call returns a freshly-allocated {@code Long} for any value outside
 * {@link Long#valueOf}'s cached {@code [-128, 127]} range, and the prior box becomes garbage. On
 * Q12 / sum-aggregator workloads where the cache fires millions of times per second this dominates
 * the operator-thread allocation profile (visible in async-profiler as a top-3 {@code Long.valueOf}
 * frame).
 *
 * <p>This specialization keeps the LRU shape identical (insertion-bounded {@link LinkedHashMap}
 * with access-order + {@code removeEldestEntry} + deferred eviction flush + {@code BytesKey}
 * dual-mode probe scratch) but stores the accumulator as a {@code long} field on each entry. On
 * cache hit the combiner runs as a {@link LongBinaryOperator} ({@code (long, long) -> long}); the
 * result is written back into the entry's {@code long} field without ever materializing a {@code
 * Long}. The {@code Long.valueOf(...)} box only happens at the flush boundary — once per dirty
 * entry per checkpoint barrier (cold path) — when we hand the value to the flush callback.
 *
 * <p>Detection / routing is the caller's responsibility (typically: check
 * {@code valueSerializer instanceof LongSerializer} at state-class construction and choose
 * between this and the general cache). If the user's {@code ReduceFunction<Long>} cannot be
 * unwrapped into a {@link LongBinaryOperator}, callers can supply a thin shim
 * {@code (a, b) -> reduce.reduce(a, b)} — that shim still allocates one box per call on the input
 * side, but the WRITE-BACK box is eliminated, halving the per-hit allocation cost. For full
 * zero-alloc the caller's {@code ReduceFunction} must itself accept primitive longs (rare in
 * practice — Flink's stock APIs are object-typed).
 *
 * <p>Concurrency / generation semantics (A6-H1 / A7-M1 / E8-H1 / E9-H2) are identical to the
 * general cache and live in the same {@code generationsLock} contract. The only signature
 * differences are:
 *
 * <ul>
 *   <li>{@code combiner} type — {@link LongBinaryOperator} instead of {@code BiFunction}
 *   <li>{@code flushCallback} type — {@link LongFlushCallback} ({@code (byte[], long) -> void})
 *       instead of {@code BiConsumer<byte[], ACC>}
 *   <li>{@code peek(...)} returns a {@code long} (no {@code Long} box on read) and supplies a
 *       sentinel via {@link #peekOr} for the miss case
 * </ul>
 */
public final class LongReducingAggregatingCache {

    /** Default maximum number of entries before LRU eviction kicks in. */
    private static final int DEFAULT_MAX_ENTRIES = 64 * 1024;

    /**
     * Primitive-long flush callback. Equivalent to a {@code BiConsumer<byte[], Long>} but
     * accepts a primitive {@code long} so no {@code Long.valueOf(...)} box is needed at the
     * call site (the box, if any, happens inside the callback's own serializer).
     */
    @FunctionalInterface
    public interface LongFlushCallback {
        void accept(byte[] compositeKey, long acc);
    }

    private final LongBinaryOperator combiner;
    private final LongFlushCallback flushCallback;
    private final LinkedHashMap<BytesKey, LongEntry> entries;
    private final int maxEntries;

    /**
     * B11-H1: per-key invalidation-generation counter, primitive-{@code long} valued.
     *
     * <p>Was {@code LinkedHashMap<BytesKey, Long>}: every {@code generations.merge(owned, 1L,
     * Long::sum)} on {@link #invalidate} allocated a fresh {@link Long} box on the {@code onClear}
     * hot path because the counter quickly exceeds {@link Long}'s cached {@code [-128, 127]}
     * range. Now stores a {@link MutableLong} per slot — one allocation per first-time-insert,
     * reused on every subsequent bump via {@code .value++}. Zero {@code Long.valueOf} on steady
     * state.
     *
     * <p>{@link MutableLong} is a tiny final class with a single primitive field; the read APIs
     * ({@link #currentGen} / {@link #putIfGen}) unbox to a primitive {@code long} directly from
     * the cell, so no {@link Long} box ever materializes.
     */
    private final LinkedHashMap<BytesKey, MutableLong> generations = new LinkedHashMap<>();

    private final Object generationsLock = new Object();

    /**
     * B11-H1: mutable primitive-{@code long} cell. Used as the value type for {@link #generations}
     * so the invalidate-bump path can increment in place ({@code cell.value++}) without ever
     * allocating a fresh {@link Long} box. One allocation per first-time-insert; reused forever
     * after.
     */
    private static final class MutableLong {
        long value;

        MutableLong(long value) {
            this.value = value;
        }
    }

    /** Deferred-flush slot — same E8-H1 / E9-H2 contract as {@link ReducingAggregatingCache}. */
    private BytesKey pendingFlushKey;

    private long pendingFlushValue;
    private boolean pendingFlushPresent;

    private final BytesKey scratch = new BytesKey();

    private static final class LongEntry {
        long acc;
        boolean dirty;

        LongEntry(long acc, boolean dirty) {
            this.acc = acc;
            this.dirty = dirty;
        }
    }

    /** Sentinel returned by {@link #peekOr(byte[], int, int, long)} when the slot is absent. */
    public static final long ABSENT_SENTINEL = Long.MIN_VALUE;

    public LongReducingAggregatingCache(LongBinaryOperator combiner, LongFlushCallback flushCallback) {
        this(combiner, flushCallback, DEFAULT_MAX_ENTRIES);
    }

    public LongReducingAggregatingCache(
            LongBinaryOperator combiner, LongFlushCallback flushCallback, int maxEntries) {
        this.combiner = combiner;
        this.flushCallback = flushCallback;
        this.maxEntries = maxEntries;
        this.entries =
                new LinkedHashMap<>(Math.min(maxEntries, 1024), 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<BytesKey, LongEntry> eldest) {
                        if (size() > maxEntries) {
                            LongEntry e = eldest.getValue();
                            if (e.dirty) {
                                pendingFlushKey = eldest.getKey();
                                pendingFlushValue = e.acc;
                                pendingFlushPresent = true;
                                e.dirty = false;
                            }
                            return true;
                        }
                        return false;
                    }
                };
    }

    public boolean contains(byte[] compositeKey) {
        return contains(compositeKey, 0, compositeKey.length);
    }

    public boolean contains(byte[] buf, int off, int len) {
        return entries.containsKey(scratch.view(buf, off, len));
    }

    /**
     * Returns the current accumulator for {@code compositeKey} as a primitive {@code long}, or
     * {@code defaultIfAbsent} on miss. Use this in preference to {@link #peek(byte[])} for
     * zero-alloc reads when {@code defaultIfAbsent} can encode "absent" without a box.
     */
    public long peekOr(byte[] compositeKey, long defaultIfAbsent) {
        return peekOr(compositeKey, 0, compositeKey.length, defaultIfAbsent);
    }

    public long peekOr(byte[] buf, int off, int len, long defaultIfAbsent) {
        LongEntry e = entries.get(scratch.view(buf, off, len));
        return e == null ? defaultIfAbsent : e.acc;
    }

    /**
     * Boxed peek — kept for API symmetry with the general cache. Returns {@code null} on miss.
     * Allocates a {@link Long} on hit; prefer {@link #peekOr} on the hot path.
     */
    public Long peek(byte[] compositeKey) {
        LongEntry e = entries.get(scratch.view(compositeKey, 0, compositeKey.length));
        return e == null ? null : e.acc;
    }

    public boolean isDirty(byte[] compositeKey) {
        LongEntry e = entries.get(scratch.view(compositeKey, 0, compositeKey.length));
        return e != null && e.dirty;
    }

    /**
     * Cache-hit fold (primitive {@code long}). Combines {@code input} in-place via
     * {@link LongBinaryOperator#applyAsLong}; the result is written back to the entry's primitive
     * field — no {@link Long#valueOf} box on the write path. Marks dirty, updates LRU order.
     *
     * @return {@code true} on hit (folded in place), {@code false} on miss
     */
    public boolean tryFold(byte[] compositeKey, long input) {
        return tryFold(compositeKey, 0, compositeKey.length, input);
    }

    public boolean tryFold(byte[] buf, int off, int len, long input) {
        LongEntry e = entries.get(scratch.view(buf, off, len));
        if (e == null) {
            return false;
        }
        e.acc = combiner.applyAsLong(e.acc, input);
        e.dirty = true;
        return true;
    }

    /** Inserts or replaces the accumulator. Same semantics as the general cache's {@code put}. */
    public void put(byte[] compositeKey, long acc) {
        entries.put(new BytesKey(compositeKey), new LongEntry(acc, true));
        drainPendingFlush();
    }

    public void put(byte[] buf, int off, int len, long acc) {
        byte[] owned = new byte[len];
        System.arraycopy(buf, off, owned, 0, len);
        entries.put(new BytesKey(owned), new LongEntry(acc, true));
        drainPendingFlush();
    }

    public long currentGen(byte[] compositeKey) {
        return currentGen(compositeKey, 0, compositeKey.length);
    }

    public long currentGen(byte[] buf, int off, int len) {
        synchronized (generationsLock) {
            BytesKey probe = new BytesKey().view(buf, off, len);
            // B11-H1: primitive-long cell — no Long.valueOf box on read.
            MutableLong g = generations.get(probe);
            return g == null ? 0L : g.value;
        }
    }

    public boolean putIfGen(byte[] compositeKey, long acc, long expectedGen) {
        boolean inserted;
        synchronized (generationsLock) {
            BytesKey probe = new BytesKey().view(compositeKey, 0, compositeKey.length);
            // B11-H1: primitive-long cell — no Long.valueOf box on read.
            MutableLong g = generations.get(probe);
            long current = g == null ? 0L : g.value;
            if (current != expectedGen) {
                return false;
            }
            entries.put(new BytesKey(compositeKey), new LongEntry(acc, true));
            if (expectedGen == 0L) {
                generations.remove(probe);
            }
            inserted = true;
        }
        drainPendingFlush();
        return inserted;
    }

    /**
     * Flushes all dirty entries via the flush callback. Called on checkpoint barrier (§3 Trace E).
     * Marks entries clean after flushing.
     *
     * <p>A10-H1 / A11-H2: drain any in-flight {@link #pendingFlushKey}/{@link #pendingFlushValue}
     * slot FIRST. The slot is populated by {@code removeEldestEntry} when an LRU eviction was
     * deferred (engine-PUT FFI must not run under {@code generationsLock}). If a prior
     * {@code drainPendingFlush} throw re-stashed an entry, {@code drainPendingFlush} placed it
     * back into {@code entries} marked dirty and the {@code entries} iteration would have picked
     * it up — so the re-stash path is covered without this drain. BUT the SUCCESS-but-not-yet-
     * drained path (a deferred eviction captured into the slot that has not been drained by a
     * subsequent {@code put} or {@code putIfGen}) needs us to materialize the slot before
     * iterating. Failing to drain here would leave the captured (key, value) unflushed forever —
     * {@code removeEldestEntry} already removed it from {@code entries}, marked the original
     * Entry clean, and stashed it into the slot. The {@code entries} iteration alone cannot
     * see it.
     *
     * <p>This mirrors the A10-H1 fix in {@link ReducingAggregatingCache#flushAllDirty()}. The
     * peer class was introduced in B10-H2 without this drain; it was the silent loss of cascaded
     * eviction's dirty accumulator on Q12 SumAgg/CountAgg checkpoints (A11-H2 / D11-H1 / E11-H1).
     */
    public void flushAllDirty() {
        drainPendingFlush();
        for (Map.Entry<BytesKey, LongEntry> me : entries.entrySet()) {
            LongEntry e = me.getValue();
            if (e.dirty) {
                flushCallback.accept(me.getKey().bytes, e.acc);
                e.dirty = false;
            }
        }
    }

    /**
     * OPT-N04 (A2 / J3): delta-mode flush. Emits each dirty entry's accumulated value via the flush
     * callback, then RESETS the value to {@code 0} (not merely clearing the dirty flag). This is the
     * required semantic when the entry holds a PENDING DELTA rather than an absolute value: once the
     * delta has been handed to the engine as a Merge operand, the in-cache delta must be zeroed so a
     * subsequent fold starts a fresh delta — otherwise the already-flushed delta would be
     * re-folded and double-counted on the next barrier. Entries stay resident (delta 0) so the
     * alloc-free {@link #tryFold} hot path keeps hitting.
     */
    public void flushDeltasAndReset() {
        drainPendingFlush();
        for (Map.Entry<BytesKey, LongEntry> me : entries.entrySet()) {
            LongEntry e = me.getValue();
            if (e.dirty) {
                flushCallback.accept(me.getKey().bytes, e.acc);
                e.acc = 0L;
                e.dirty = false;
            }
        }
    }

    public int size() {
        return entries.size();
    }

    public void clear() {
        entries.clear();
    }

    public boolean invalidate(byte[] compositeKey) {
        return invalidate(compositeKey, 0, compositeKey.length);
    }

    public boolean invalidate(byte[] buf, int off, int len) {
        synchronized (generationsLock) {
            // B11-H1: in-place primitive-long bump. We do TWO probes here: one with the scratch
            // BytesKey (zero-alloc), and on miss, one allocating snapshot for first-time-insert.
            // The steady-state hot path (key already present) takes ONLY the scratch probe — no
            // BytesKey snapshot, no Long box, no map-restructure.
            BytesKey probe = scratch.view(buf, off, len);
            MutableLong cell = generations.get(probe);
            if (cell != null) {
                cell.value++;
            } else {
                // First-time bump: must own the key bytes (scratch is a view into the caller's
                // buffer). One allocation per never-before-invalidated key — same as the legacy
                // implementation's snapshot() call; no extra cost.
                BytesKey owned = probe.snapshot();
                generations.put(owned, new MutableLong(1L));
                if (generations.size() > maxEntries) {
                    java.util.Iterator<Map.Entry<BytesKey, MutableLong>> it =
                            generations.entrySet().iterator();
                    int overflow = generations.size() - maxEntries;
                    while (it.hasNext() && overflow > 0) {
                        Map.Entry<BytesKey, MutableLong> me = it.next();
                        if (!entries.containsKey(me.getKey())) {
                            it.remove();
                            overflow--;
                        }
                    }
                }
            }
        }
        return entries.remove(scratch.view(buf, off, len)) != null;
    }

    private void drainPendingFlush() {
        BytesKey k;
        long v;
        synchronized (generationsLock) {
            if (!pendingFlushPresent) {
                return;
            }
            k = pendingFlushKey;
            v = pendingFlushValue;
        }
        boolean delivered = false;
        try {
            flushCallback.accept(k.bytes, v);
            delivered = true;
        } finally {
            synchronized (generationsLock) {
                if (!delivered) {
                    if (!entries.containsKey(k)) {
                        entries.put(new BytesKey(k.bytes), new LongEntry(v, true));
                    }
                }
                if (pendingFlushKey == k) {
                    pendingFlushKey = null;
                    pendingFlushValue = 0L;
                    pendingFlushPresent = false;
                }
            }
        }
    }

    /**
     * Dual-mode byte-slice key — identical layout to {@link ReducingAggregatingCache.BytesKey}.
     * Duplicated here (rather than promoted to package-shared) to keep this class free of
     * cross-cache visibility risks during future refactors; the two implementations evolve
     * independently and the {@code BytesKey} contract (hash/equals over slice) is small enough
     * to copy.
     */
    static final class BytesKey {
        byte[] bytes;
        int off;
        int len;

        BytesKey() {}

        BytesKey(byte[] bytes) {
            this.bytes = bytes;
            this.off = 0;
            this.len = bytes.length;
        }

        BytesKey view(byte[] buf, int off, int len) {
            this.bytes = buf;
            this.off = off;
            this.len = len;
            return this;
        }

        BytesKey snapshot() {
            byte[] copy = new byte[len];
            System.arraycopy(bytes, off, copy, 0, len);
            return new BytesKey(copy);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof BytesKey)) {
                return false;
            }
            BytesKey bk = (BytesKey) o;
            if (len != bk.len) {
                return false;
            }
            return java.util.Arrays.equals(bytes, off, off + len, bk.bytes, bk.off, bk.off + bk.len);
        }

        @Override
        public int hashCode() {
            int h = 1;
            int end = off + len;
            for (int i = off; i < end; i++) {
                h = 31 * h + bytes[i];
            }
            return h;
        }
    }
}

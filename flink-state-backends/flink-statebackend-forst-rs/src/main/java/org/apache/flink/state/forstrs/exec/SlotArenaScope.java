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

package org.apache.flink.state.forstrs.exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-TaskManager-slot Arena management for the V1 vectorized dispatch path. Umbrella spec
 * component 7. Layout:
 *
 * <pre>
 *   slotArena = Arena.ofShared()                     // one-time at slot init
 *   ├─ turnRegion  (SLOT_TURN_BYTES bump-allocated)  // resets each async-v2 turn
 *   └─ cacheRegion (SLOT_CACHE_BYTES LRU-managed)    // slot lifetime
 *
 *   Independent on-demand Arenas:
 *   ├─ overflow Arenas per turn (closed at exit)
 *   └─ iterator Arenas per FrsIterHandle (closed on handle close)
 * </pre>
 *
 * <h3>Lifecycle invariants</h3>
 *
 * <ul>
 *   <li>{@code enterTurn()} asserts {@code iterRegistry.isEmpty()} — defense-in-depth, force-closes
 *       any leaked handles and logs.
 *   <li>{@code exitTurn()} closes outstanding iters, restores turnBumpOffset to the mark, closes
 *       overflow arenas.
 *   <li>{@code closeSlot()} drops the slot arena and everything in it.
 * </ul>
 *
 * <p>This class is single-threaded per slot (async-v2 contract). The {@code iterRegistry} uses
 * ConcurrentHashMap because the watchdog (P3 component 6) reads it from a scheduled-executor
 * thread.
 */
public final class SlotArenaScope {

    private static final Logger LOG = LoggerFactory.getLogger(SlotArenaScope.class);

    private final Arena slotArena;
    private final MemorySegment turnRegion;
    private final MemorySegment cacheRegion;
    private final long turnRegionBytes;
    private final long cacheRegionBytes;

    private long turnBumpOffset = 0L;
    private long turnBumpMark = 0L;
    private final AtomicLong cacheBumpOffset = new AtomicLong(0L);

    private final List<Arena> overflowArenasThisTurn = new ArrayList<>();
    private final ConcurrentHashMap<Long, FrsIterHandle> iterRegistry = new ConcurrentHashMap<>();

    private volatile boolean closed = false;

    private SlotArenaScope(long turnBytes, long cacheBytes) {
        this.turnRegionBytes = turnBytes;
        this.cacheRegionBytes = cacheBytes;
        this.slotArena = Arena.ofShared();
        this.turnRegion = slotArena.allocate(turnBytes, 64);
        this.cacheRegion = slotArena.allocate(cacheBytes, 64);
    }

    /**
     * Opens a new {@link SlotArenaScope} with the given turn-region and cache-region sizes.
     *
     * @param turnBytes size of the bump-allocated turn region in bytes (reset each turn)
     * @param cacheBytes size of the monotonically-allocated cache region in bytes
     * @return a freshly initialized scope ready for use
     */
    public static SlotArenaScope openForSlot(long turnBytes, long cacheBytes) {
        return new SlotArenaScope(turnBytes, cacheBytes);
    }

    /**
     * Enters a new async-v2 turn. Asserts that the iter registry is empty (defense-in-depth);
     * force-closes any leaked handles and logs them. Records the current bump mark for {@link
     * #exitTurn()}.
     *
     * @throws IllegalStateException if the scope has been closed via {@link #closeSlot()}
     */
    public void enterTurn() {
        if (closed) {
            throw new IllegalStateException("SlotArenaScope is closed");
        }
        if (!iterRegistry.isEmpty()) {
            int leakedCount = iterRegistry.size();
            LOG.error(
                    "Forst-RS: {} iter handle(s) leaked across turn boundary — force-closing",
                    leakedCount);
            for (FrsIterHandle h : iterRegistry.values()) {
                try {
                    h.forceClose();
                } catch (Throwable t) {
                    LOG.warn("forceClose threw", t);
                }
            }
            iterRegistry.clear();
        }
        turnBumpMark = turnBumpOffset;
        overflowArenasThisTurn.clear();
    }

    /**
     * Exits the current async-v2 turn. Closes any outstanding iter handles, restores the turn bump
     * offset to the mark set at {@link #enterTurn()}, and closes all per-turn overflow Arenas.
     *
     * <p>Graceful no-op if the scope has already been closed.
     */
    public void exitTurn() {
        if (closed) {
            return;
        }
        // Close outstanding iter handles (normal close, not forceClose)
        for (FrsIterHandle h : iterRegistry.values()) {
            try {
                h.close();
            } catch (Throwable t) {
                LOG.warn("iter close threw on exitTurn", t);
            }
        }
        iterRegistry.clear();
        // Restore turnRegion bump pointer
        turnBumpOffset = turnBumpMark;
        // Close any per-turn overflow arenas
        for (Arena a : overflowArenasThisTurn) {
            try {
                a.close();
            } catch (Throwable t) {
                LOG.warn("overflow Arena close threw", t);
            }
        }
        overflowArenasThisTurn.clear();
    }

    /**
     * Allocates {@code nbytes} aligned to {@code align} from the turn region. On overflow (request
     * does not fit in the remaining turn region), falls back to a freshly created per-turn {@code
     * Arena.ofShared()} that is closed at {@link #exitTurn()}.
     *
     * @param nbytes number of bytes to allocate
     * @param align required alignment (must be a power of two)
     * @return a {@link MemorySegment} covering exactly {@code nbytes} bytes
     * @throws IllegalStateException if the scope has been closed
     */
    public MemorySegment allocateTurn(long nbytes, long align) {
        // D-R14-H1: serialize allocator vs closeSlot through the same
        // iterRegistry monitor so the slotArena.close() global handshake
        // cannot land mid-allocation, invalidating the segment we're
        // about to return. Pre-fix only the closed flag was checked, but
        // a concurrent closeSlot could flip closed=true + close arena
        // BETWEEN our read of `closed` and the asSlice/Arena.allocate
        // calls below — JDK 25 FFM rejects the access with
        // IllegalStateException or, worse, races the handshake checkpoint
        // and SIGSEGVs. closeSlot now also holds this monitor across the
        // arena close.
        synchronized (iterRegistry) {
            if (closed) {
                throw new IllegalStateException("SlotArenaScope is closed");
            }
            long aligned = (turnBumpOffset + align - 1) & ~(align - 1);
            if (aligned + nbytes <= turnRegionBytes) {
                MemorySegment seg = turnRegion.asSlice(aligned, nbytes);
                turnBumpOffset = aligned + nbytes;
                return seg;
            }
            // Overflow path — per-turn Arena
            Arena overflow = Arena.ofShared();
            overflowArenasThisTurn.add(overflow);
            return overflow.allocate(nbytes, align);
        }
    }

    /**
     * Allocates {@code nbytes} aligned to {@code align} from the cache region. Cache region
     * survives turn boundaries; allocation is monotonic in V1 (LRU eviction comes in P7).
     *
     * @param nbytes number of bytes to allocate
     * @param align required alignment (must be a power of two)
     * @return a {@link MemorySegment} covering exactly {@code nbytes} bytes
     * @throws IllegalStateException if the scope has been closed
     * @throws OutOfMemoryError if the cache region is exhausted
     */
    public MemorySegment allocateCache(long nbytes, long align) {
        // D-R14-H1: serialize allocator vs closeSlot (see allocateTurn).
        // The CAS loop on cacheBumpOffset is preserved (other readers of
        // the cache region from the same slot thread may interleave) but
        // the asSlice call must observe a non-closed slotArena, so we
        // serialise the asSlice itself under the close monitor.
        synchronized (iterRegistry) {
            if (closed) {
                throw new IllegalStateException("SlotArenaScope is closed");
            }
            long aligned;
            long after;
            long current;
            do {
                current = cacheBumpOffset.get();
                aligned = (current + align - 1) & ~(align - 1);
                after = aligned + nbytes;
                if (after > cacheRegionBytes) {
                    throw new OutOfMemoryError(
                            "SlotArenaScope cacheRegion exhausted: requested="
                                    + nbytes
                                    + " offset="
                                    + aligned
                                    + " capacity="
                                    + cacheRegionBytes);
                }
            } while (!cacheBumpOffset.compareAndSet(current, after));
            return cacheRegion.asSlice(aligned, nbytes);
        }
    }

    /**
     * Registers an iter handle so the scope can force-close leaked handles at turn boundaries.
     *
     * <p>R24-M2: the {@code closed} check and {@code put} were not atomic. Race window: the
     * watchdog thread (P3 component 6) calls {@link #closeSlot()} concurrently with the mailbox
     * thread calling {@code registerIter}. Pre-fix, the mailbox could read {@code closed=false},
     * pause; {@code closeSlot()} could flip {@code closed=true} and iterate an empty registry;
     * the mailbox could then complete the {@code put}, leaking the handle past slot teardown
     * (its arena is closed, but the handle survives in a registry that will never be drained).
     *
     * <p>Fix: synchronize check+put against {@link #iterRegistry} as the monitor. {@link
     * #closeSlot()} takes the same monitor around its drain-and-clear loop so the two
     * operations are mutually exclusive. ConcurrentHashMap itself stays — we still want
     * lock-free reads from the watchdog (which observes via {@link #iterHandles()} /
     * {@link #iterRegistrySize()}) — and the synchronized block only serializes the
     * close/register edges.
     *
     * @throws IllegalStateException if the scope has been closed
     */
    public void registerIter(FrsIterHandle h) {
        synchronized (iterRegistry) {
            if (closed) {
                throw new IllegalStateException("SlotArenaScope is closed");
            }
            iterRegistry.put(h.handleId(), h);
        }
    }

    /** Unregisters an iter handle by its ID (called from {@link FrsIterHandle#close()}). */
    public void unregisterIter(long handleId) {
        iterRegistry.remove(handleId);
    }

    /** Returns the number of currently registered iter handles. */
    public int iterRegistrySize() {
        return iterRegistry.size();
    }

    /** Returns a live view of all currently registered iter handles. */
    public Collection<FrsIterHandle> iterHandles() {
        return iterRegistry.values();
    }

    /** Returns the number of overflow Arenas created in the current turn. */
    public int overflowArenaCountForCurrentTurn() {
        return overflowArenasThisTurn.size();
    }

    /** Returns the current turn-region bump offset (for diagnostics/testing). */
    public long turnBumpOffset() {
        return turnBumpOffset;
    }

    /**
     * Closes this scope and releases all resources. Force-closes any registered iter handles,
     * closes overflow Arenas, and closes the slot Arena (dropping turnRegion + cacheRegion).
     * Idempotent — subsequent calls are no-ops.
     */
    public void closeSlot() {
        // R24-M2: serialize the close-flip + drain against {@link #registerIter} so a
        // concurrent watchdog-vs-mailbox race cannot deposit a handle after the registry has
        // been cleared. The synchronized block straddles BOTH the {@code closed=true} write
        // and the iteration so a registerIter blocked on the same monitor sees closed=true
        // when it resumes and throws (its handle is never inserted), while any handle that
        // raced ahead is observed by this drain loop.
        // D-R14-H1: extend the iterRegistry-monitor envelope to include
        // the slotArena.close() and overflowArenas drain. Pre-fix the
        // close ran OUTSIDE the synchronized block, so allocateCache /
        // allocateTurn racing the close could read closed=false, enter
        // the slow path, then see the arena invalidated mid-allocation.
        // The single monitor now serializes (close edges) ↔
        // (allocator edges); the operator thread inside an allocator
        // call holds the monitor and blocks the watchdog-driven close
        // until the slice has been returned.
        synchronized (iterRegistry) {
            if (closed) {
                return;
            }
            closed = true;
            for (FrsIterHandle h : iterRegistry.values()) {
                try {
                    h.forceClose();
                } catch (Throwable t) {
                    LOG.warn("forceClose on closeSlot", t);
                }
            }
            iterRegistry.clear();
            for (Arena a : overflowArenasThisTurn) {
                try {
                    a.close();
                } catch (Throwable t) {
                    LOG.warn("overflow close on closeSlot", t);
                }
            }
            overflowArenasThisTurn.clear();
            slotArena.close();
        }
    }
}

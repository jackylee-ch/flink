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

import org.apache.flink.annotation.Internal;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker.IteratorEntry;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Embedded local-lookup function for Flink Delta Join (G-C).
 *
 * <p>Replaces the Fluss-backed lookup path: instead of an RPC to a remote Fluss tablet server, this
 * function performs a synchronous in-process lookup against an embedded ForSt-RS instance reached
 * through JDK 25 FFM. The returned {@link CompletableFuture} is completed inline (no thread pool)
 * because a local KV lookup is sub-microsecond and async scheduling overhead would dominate.
 *
 * <p>Reference: {@code docs/design/2.13_deltajoin_localization.md} (architectural invariant 5:
 * "DeltaJoin = embedded library, not a service").
 *
 * <h2>Lifecycle</h2>
 *
 * <p>The function expects an externally-managed {@link FrsDb} + {@link FrsCfHandle} pair (typically
 * owned by the {@link org.apache.flink.state.forstrs.ForStRsStateBackend} for the operator's
 * keyed-state CF, or by a dedicated lookup-CF). The function does NOT close them on {@link
 * #close()} — the owning state backend does.
 *
 * <h2>Key encoding</h2>
 *
 * <p>The {@code keyEncoder} converts a probing {@link RowData} into the bytes used as the ForSt-RS
 * key. The encoding must match what the upstream operator wrote via the same state backend;
 * otherwise lookups will miss.
 *
 * <h2>Status</h2>
 *
 * <p>Phase-A skeleton: open / closeable, single-key {@link #eval(CompletableFuture, RowData)}
 * dispatches through {@link ForStRsLinker#lookupKv} (the dedicated Delta-Join exact-match path).
 * Multi-row prefix scans are exposed via {@link #evalPrefix(CompletableFuture, byte[])}.
 */
@Internal
public class ForStRsLocalLookupFunction extends AsyncTableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private final transient ForStRsLinker linker;
    private final transient FrsDb db;
    private final transient FrsCfHandle cf;
    private final Function<RowData, byte[]> keyEncoder;
    private final Function<byte[], RowData> valueDecoder;

    public ForStRsLocalLookupFunction(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Function<RowData, byte[]> keyEncoder,
            Function<byte[], RowData> valueDecoder) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.keyEncoder = keyEncoder;
        this.valueDecoder = valueDecoder;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    /**
     * Async lookup invoked by Flink Delta Join. Completes the future inline because the lookup is
     * local and sub-microsecond.
     */
    public void eval(CompletableFuture<RowData> resultFuture, RowData probe) {
        try {
            byte[] key = keyEncoder.apply(probe);
            byte[] value = linker.lookupKv(db, cf, key);
            if (value == null) {
                resultFuture.complete(null);
            } else {
                resultFuture.complete(valueDecoder.apply(value));
            }
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
    }

    /**
     * Prefix-scan variant for multi-row lookups (e.g. Delta-Join's "all rows with build-side join
     * key K"). Materializes every row whose key has {@code prefix} as a byte prefix, decodes via
     * the configured {@code valueDecoder}, and completes {@code resultFuture} inline with the
     * collection. Sub-microsecond locally; no thread-pool hop.
     */
    public void evalPrefix(CompletableFuture<Collection<RowData>> resultFuture, byte[] prefix) {
        try (Arena arena = Arena.ofConfined()) {
            FrsIterator iter = linker.prefixLookupOpen(db, cf, prefix, arena);
            try (iter) {
                List<RowData> out = new ArrayList<>();
                IteratorEntry entry;
                while ((entry = linker.iteratorNext(iter)) != null) {
                    out.add(valueDecoder.apply(entry.value()));
                }
                resultFuture.complete(out);
            }
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
    }

    @Override
    public void close() throws Exception {
        // Intentionally does NOT close db / cf — those are owned by the state backend.
        super.close();
    }
}

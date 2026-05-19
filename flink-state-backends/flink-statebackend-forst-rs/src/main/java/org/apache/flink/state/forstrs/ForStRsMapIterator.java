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

package org.apache.flink.state.forstrs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.AbstractStateIterator;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.ffm.FrsIterator;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * ForSt-RS implementation of {@link AbstractStateIterator} for map state iteration. Carries the
 * native {@link FrsIterator} handle for continuation loading.
 */
@Internal
public class ForStRsMapIterator<T> extends AbstractStateIterator<T> {

    private final boolean encounterEnd;
    private final StateRequestType originalRequestType;
    @Nullable private final FrsIterator frsIterator;

    /** Non-zero if the continuation uses the vectorized iter path (frs_vec_iter_prefix_*). */
    private final long continuationVecHandle;

    /** Legacy ctor — retains {@link FrsIterator} continuation handle. */
    public ForStRsMapIterator(
            State originalState,
            StateRequestType originalRequestType,
            StateRequestHandler stateHandler,
            Collection<T> partialResult,
            boolean encounterEnd,
            @Nullable FrsIterator frsIterator) {
        this(
                originalState,
                originalRequestType,
                stateHandler,
                partialResult,
                encounterEnd,
                frsIterator,
                0L);
    }

    /**
     * Full ctor — carries either a {@link FrsIterator} legacy handle or a vectorized native handle
     * for continuation. Exactly one is expected to be set; both null/zero means the iterator has
     * already drained.
     */
    public ForStRsMapIterator(
            State originalState,
            StateRequestType originalRequestType,
            StateRequestHandler stateHandler,
            Collection<T> partialResult,
            boolean encounterEnd,
            @Nullable FrsIterator frsIterator,
            long continuationVecHandle) {
        super(originalState, StateRequestType.ITERATOR_LOADING, stateHandler, partialResult);
        this.originalRequestType = originalRequestType;
        this.encounterEnd = encounterEnd;
        this.frsIterator = frsIterator;
        this.continuationVecHandle = continuationVecHandle;
    }

    @Override
    public boolean hasNextLoading() {
        return !encounterEnd;
    }

    @Override
    protected Object nextPayloadForContinuousLoading() {
        // Payload carries: (original type, legacy FrsIterator, vec handle).
        // The classifier picks whichever continuation form is set.
        return Tuple2.of(
                originalRequestType, new IterContinuation(frsIterator, continuationVecHandle));
    }

    /** Payload carrier for continuation-loading: either {@link #iter} or {@link #vecHandle}. */
    public static final class IterContinuation {
        @Nullable public final FrsIterator iter;
        public final long vecHandle;

        public IterContinuation(@Nullable FrsIterator iter, long vecHandle) {
            this.iter = iter;
            this.vecHandle = vecHandle;
        }
    }
}

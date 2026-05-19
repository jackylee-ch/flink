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

import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link ForStRsDBIterRequest#process} uses the chunked vectorized iterator API
 * ({@code frs_vec_iter_prefix_next}) instead of the per-entry {@code iteratorNext} loop. Commit A
 * of the vectorization-violation #1 fix — wire-up only, byte[] copy semantics preserved.
 */
class ForStRsDBIterRequestTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void process_uses_one_chunked_call_not_per_entry_loop() {
        ForStRsLinker linker = mock(ForStRsLinker.class);
        FrsDb db = mock(FrsDb.class);
        FrsCfHandle cf = mock(FrsCfHandle.class);

        // frs_vec_iter_prefix_open: write 0 rows / 0 bytes into the out-params and return OK.
        when(linker.frsVecIterPrefixOpen(
                        any(), any(), any(), anyInt(), any(), anyInt(), any(), any(), any()))
                .thenReturn(0); // FrsErrorCode.OK
        when(linker.frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any())).thenReturn(0);
        when(linker.frsVecIterPrefixClose(anyLong())).thenReturn(0);

        byte[] prefix = "k/test/".getBytes();
        ForStRsIterableState mockState = mock(ForStRsIterableState.class);
        StateRequest sr = mock(StateRequest.class);
        InternalAsyncFuture future = mock(InternalAsyncFuture.class);
        when(sr.getFuture()).thenReturn(future);

        ForStRsDBIterRequest<?, ?, ?, ?> req =
                new ForStRsDBIterRequest<>(
                        prefix, sr, StateRequestType.MAP_ITER, mockState, null);

        try (Arena arena = Arena.ofConfined()) {
            req.process(linker, db, cf, arena);
        }

        verify(linker, times(1)).frsVecIterPrefixNext(anyLong(), any(), anyInt(), any(), any());
        verify(linker, never()).iteratorNext(any());
        verify(linker, never()).prefixLookupOpen(any(), any(), any(), any());
    }
}

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
package org.apache.flink.state.forstrs.bridge;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/** Wrapper around a CF handle returned by the Rust engine. */
public final class ForStRsColumnFamily implements AutoCloseable {
    private final MemorySegment handle;
    private final Arena arena;

    ForStRsColumnFamily(MemorySegment handle, Arena arena) {
        this.handle = handle;
        this.arena = arena;
    }

    public MemorySegment handle() {
        return handle;
    }

    @Override
    public void close() {
        try {
            int status = (int) ForStRsBridge.FRS_CF_CLOSE.invokeExact(handle);
            ForStRsBridge.check(status, "frs_cf_close");
        } catch (Throwable t) {
            // swallow
        } finally {
            arena.close();
        }
    }
}

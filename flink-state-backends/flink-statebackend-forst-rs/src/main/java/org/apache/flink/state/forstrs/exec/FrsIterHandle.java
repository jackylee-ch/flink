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

/**
 * Stub for the per-iterator handle. Real implementation lands in P3 (umbrella spec §2 component 5).
 * For now it just exposes the surface {@link SlotArenaScope} needs to register/unregister/close
 * handles.
 */
public abstract class FrsIterHandle implements AutoCloseable {

    /** Returns the unique handle ID used as the key in the iter registry. */
    public abstract long handleId();

    @Override
    public abstract void close();

    /**
     * Force-close on watchdog timeout or scope teardown — bypasses normal close path. Implementations
     * must not throw; any internal errors should be swallowed and logged.
     */
    public abstract void forceClose();
}

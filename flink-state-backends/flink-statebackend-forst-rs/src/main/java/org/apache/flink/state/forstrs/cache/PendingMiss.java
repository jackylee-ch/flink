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

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Per-(stateName, key) convoy of in-flight RMW inputs awaiting a single GET resolution. Umbrella
 * spec §15 (PendingMiss component).
 *
 * <p>Package-private — only accessed via {@link PendingMissTable}.
 *
 * @param <IN> the input type supplied by add() callers
 * @param <ACC> the accumulator type returned after folding
 */
class PendingMiss<IN, ACC> {

    /** FIFO queue of pending inputs that joined this convoy, in arrival order. */
    final Deque<IN> pendingInputs = new ArrayDeque<>();
}

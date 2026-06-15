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

import java.util.function.Consumer;

/**
 * Test-only public bridge to the package-private {@link BatchDrainPausePoint} seam, so tests in
 * other packages (e.g. {@code org.apache.flink.state.forstrs.exec}) can arm/disarm the worker-drain
 * pause-point used to deterministically force the q8 op-mix interleaving.
 */
public final class TestPausePointAccess {

    private TestPausePointAccess() {}

    /** Install the pause-point hook (receives the classifier about to be applied). */
    public static void arm(Consumer<VectorizedClassifier> hook) {
        BatchDrainPausePoint.arm(hook);
    }

    /** Remove the pause-point hook (production no-op state). */
    public static void disarm() {
        BatchDrainPausePoint.disarm();
    }
}

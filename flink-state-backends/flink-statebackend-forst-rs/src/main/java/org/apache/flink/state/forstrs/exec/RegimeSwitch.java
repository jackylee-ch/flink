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

import org.apache.flink.annotation.Internal;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two-regime signal (design doc 2026-06-11 §3): LIGHT iff no batch is outstanding.
 * {@link #batchDispatched()} is called ONLY on the mailbox thread (AEC dispatch is
 * mailbox-confined), so the L→H transition hook runs on the mailbox while nothing is in
 * flight — the spec's race-free flush point. {@link #batchSettled()} may be called from
 * worker threads; {@link #isLight()} reads are mailbox-side and see 0 only after the
 * settle, at which point no engine work is pending (safe to re-enable staging).
 */
@Internal
public final class RegimeSwitch {

    private final AtomicInteger outstanding = new AtomicInteger();
    private volatile Runnable onHeavyTransition = () -> {};

    public void setOnHeavyTransition(Runnable hook) {
        this.onHeavyTransition = hook;
    }

    /** True ⇔ pipeline empty ⇔ inline (cache-on) execution is safe. */
    public boolean isLight() {
        return outstanding.get() == 0;
    }

    /** Mailbox-side, before enqueueing the batch. Fires the L→H hook on 0→1. */
    public void batchDispatched() {
        if (outstanding.getAndIncrement() == 0) {
            onHeavyTransition.run();
        }
    }

    /** Any-thread, after the batch fully settles. */
    public void batchSettled() {
        outstanding.decrementAndGet();
    }

    /** For fullyLoaded()-style backpressure. */
    public int outstanding() {
        return outstanding.get();
    }
}

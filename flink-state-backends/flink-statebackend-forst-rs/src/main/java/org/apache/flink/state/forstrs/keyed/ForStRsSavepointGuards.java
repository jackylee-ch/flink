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

package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;

/**
 * E6-HIGH-1: shared savepoint-format guards used by BOTH the async {@code
 * ForStRsAsyncKeyedStateBackend} and the V1-sync {@code ForStRsAbstractKeyedStateBackend}.
 *
 * <p>Lifted out of the async backend so the V1-sync path no longer emits a non-portable ForSt-RS
 * incremental handle when an operator triggers {@code stop --savepoint} with the default {@link
 * SavepointFormatType#CANONICAL} format. Pre-fix the V1-sync {@code snapshot()} silently produced
 * such a handle and operators only discovered the breakage on restore.
 *
 * <p>The contract is intentionally minimal: detect the CANONICAL format at the request site and
 * throw {@link UnsupportedOperationException} with a remediation hint pointing at the {@code
 * --type native} CLI flag (or the programmatic {@link SavepointFormatType#NATIVE} equivalent). The
 * canonical-emit PR is still TODO; both backends fail loudly until then.
 */
@Internal
final class ForStRsSavepointGuards {

    private ForStRsSavepointGuards() {}

    /**
     * E6-HIGH-1: rejects CANONICAL savepoint requests at the snapshot entry point. No-op for
     * periodic checkpoints and for NATIVE savepoints.
     *
     * @param checkpointOptions the {@link CheckpointOptions} passed to {@code snapshot(...)} by
     *     Flink's checkpoint coordinator.
     * @throws UnsupportedOperationException if {@code checkpointOptions} carries a CANONICAL
     *     savepoint format that ForSt-RS cannot yet emit.
     */
    static void rejectCanonicalSavepoint(CheckpointOptions checkpointOptions) {
        SnapshotType ctype = checkpointOptions.getCheckpointType();
        if (!ctype.isSavepoint()) {
            return;
        }
        SavepointFormatType formatType = ((SavepointType) ctype).getFormatType();
        if (formatType == SavepointFormatType.CANONICAL) {
            throw new UnsupportedOperationException(
                    "Canonical savepoint format not yet supported by ForSt-RS backend;"
                            + " use --type native or set SavepointFormatType.NATIVE");
        }
    }
}

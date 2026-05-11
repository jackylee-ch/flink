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

import java.io.IOException;

/**
 * Module-internal "checkpoint restore failed" exception (B-Prod-P4 Task 4.2).
 *
 * <p>Flink's runtime does not currently ship a public {@code CheckpointRestoreException} —
 * the module-internal {@link org.apache.flink.runtime.state.BackendBuildingException} is the
 * closest public type, but it does not carry the failed-path / checkpoint-id pair we want for
 * strict-restore diagnostics. We therefore expose this dedicated subclass of {@link IOException}
 * so a missing SST surfaces with both fields and so callers can pattern-match without leaking
 * a runtime-internal type.
 *
 * <p>Used by {@link ForStRsRestoreOperation} when a downloaded checkpoint is missing one or more
 * SST files referenced by its manifest, or when manifest parsing itself fails.
 */
@Internal
public class ForStRsCheckpointRestoreException extends IOException {

    private static final long serialVersionUID = 1L;

    /** Path (local or logical) the restore expected to find but couldn't, or {@code null}. */
    private final String missingPath;

    /** Checkpoint id the restore was operating on. */
    private final long checkpointId;

    public ForStRsCheckpointRestoreException(String missingPath, long checkpointId, String message) {
        super(message);
        this.missingPath = missingPath;
        this.checkpointId = checkpointId;
    }

    public ForStRsCheckpointRestoreException(
            String missingPath, long checkpointId, String message, Throwable cause) {
        super(message, cause);
        this.missingPath = missingPath;
        this.checkpointId = checkpointId;
    }

    public String getMissingPath() {
        return missingPath;
    }

    public long getCheckpointId() {
        return checkpointId;
    }
}

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

package org.apache.flink.fs.s3presto;

import org.apache.flink.fs.s3.common.HAJobRunOnMinioS3StoreITCase;

import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

/** Runs the {@link HAJobRunOnMinioS3StoreITCase} on the Presto S3 file system. */
@DisabledForJreRange(
        min = JRE.JAVA_24,
        disabledReason =
                "FLINK-38280: HA blob store creation against s3:// fails on JDK 24+ — same"
                        + " S3A SSL / TrustManager surface as the Hadoop variant. Phase 4 follow-up.")
class HAJobRunOnPrestoS3FileSystemITCase extends HAJobRunOnMinioS3StoreITCase {}

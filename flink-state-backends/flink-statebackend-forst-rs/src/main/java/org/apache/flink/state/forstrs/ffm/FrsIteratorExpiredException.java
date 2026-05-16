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

package org.apache.flink.state.forstrs.ffm;

/**
 * Thrown when the iterator watchdog (umbrella spec §1 §b) drops the
 * native iterator's snapshot before the consumer finished. Wraps
 * FrsErrorCode.ITER_EXPIRED.
 *
 * <p>Per spec §4 Severe-2, the dispatch layer does NOT auto-reopen the
 * iterator: a new iterator would see a different snapshot, and the
 * consumer has already advanced past observed rows. The operator must
 * fail and Flink restart re-issues the iterator on the next attempt.
 */
public class FrsIteratorExpiredException extends FrsException {
    public FrsIteratorExpiredException(int rowIndex) {
        super(FrsErrorCode.ITER_EXPIRED, rowIndex, new byte[0]);
    }
}

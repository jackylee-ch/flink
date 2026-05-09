/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.forstdb;

/**
 * Minimal stub of upstream {@code org.forstdb.Status}. The community JNI
 * looks this class up via {@code RocksDBException(Status)} ctor. We declare
 * just enough surface (the {@code (BBLjava/lang/String;)V} ctor used by
 * {@code StatusJni}) so that throw-paths can resolve it.
 */
public class Status {
    @SuppressWarnings("unused")
    private byte code;

    @SuppressWarnings("unused")
    private byte subCode;

    private String state;

    public Status(byte code, byte subCode, String state) {
        this.code = code;
        this.subCode = subCode;
        this.state = state;
    }

    public String getState() {
        return state;
    }
}

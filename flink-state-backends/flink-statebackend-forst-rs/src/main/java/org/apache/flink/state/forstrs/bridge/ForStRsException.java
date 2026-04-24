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

/** Exception raised when a ForSt-RS native call returns a non-OK status. */
public class ForStRsException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int statusCode;

    public ForStRsException(int statusCode, String operation) {
        super("ForSt-RS '" + operation + "' failed: " + ForStRsStatus.name(statusCode));
        this.statusCode = statusCode;
    }

    public ForStRsException(int statusCode, String operation, Throwable cause) {
        super(
                "ForSt-RS '" + operation + "' failed: " + ForStRsStatus.name(statusCode),
                cause);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }

    public boolean isNotFound() {
        return statusCode == ForStRsStatus.NOT_FOUND;
    }

    public boolean isInvalidArgument() {
        return statusCode == ForStRsStatus.INVALID_ARGUMENT;
    }

    public boolean isPanic() {
        return statusCode == ForStRsStatus.PANIC;
    }
}

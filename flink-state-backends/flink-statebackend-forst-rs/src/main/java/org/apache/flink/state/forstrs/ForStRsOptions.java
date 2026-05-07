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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configuration options for {@link ForStRsStateBackend}. */
public final class ForStRsOptions {

    /** Optional override for the cdylib path (defaults to System.loadLibrary). */
    public static final ConfigOption<String> NATIVE_LIB_PATH = ConfigOptions
            .key("state.backend.forstrs.native-lib-path")
            .stringType()
            .noDefaultValue()
            .withDescription(
                    "Absolute path to libforst_rs_ffi.{dylib,so,dll}. "
                            + "If unset, java.library.path is used via System.loadLibrary.");

    private ForStRsOptions() {
        // utility class
    }
}

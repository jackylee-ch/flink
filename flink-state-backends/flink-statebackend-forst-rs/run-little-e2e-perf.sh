#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# B-Prod-followup-LittleE2E: drives org.apache.flink.state.forstrs.perf.LittleE2EPerfBench
# across the 4 backend variants and prints a comparison table.
#
#   1. rocksdb                                    — EmbeddedRocksDBStateBackendFactory
#   2. forst                                       — ForStStateBackendFactory (community libforstjni)
#   3. forst (libforstjni → libforst_rs_ffi)       — same factory, JNI lib symlink swap
#   4. forst-rs                                    — ForStRsStateBackendFactory (FFM path)
#
# Env overrides:
#   FORST_RS_CDYLIB    Path to libforst_rs_ffi.{so,dylib}; default infers from repo layout
#   EVENTS             Events per measured run (default 100000, recommend up to 1000000)
#   WARMUPS            Warmup runs per backend (default 1)
#   JAVA_HOME          JDK 25+ home (FFM stable on 25+, required for forst-rs variant)
#
# This script intentionally does NOT shell out to maven-dependency-plugin at runtime — that adds
# minutes of resolution latency that would dominate a CI loop. Instead it walks ~/.m2 once via
# Maven's dependency:build-classpath, caches the result, and reuses it across all 4 invocations.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
FLINK_ROOT="$(cd "$ROOT/../.." && pwd)"

# ----------------------------------------------------------------------
# Library + workload params
# ----------------------------------------------------------------------
EVENTS="${EVENTS:-100000}"
WARMUPS="${WARMUPS:-1}"

# Default-locate the cdylib next to this checkout. Same default the existing
# JMH driver script uses (run-jmh-3way.sh) so the two scripts share a
# convention.
DEFAULT_DYLIB_DARWIN="$ROOT/../../../ForSt/target/release/libforst_rs_ffi.dylib"
DEFAULT_DYLIB_LINUX="$ROOT/../../../ForSt/target/release/libforst_rs_ffi.so"
if [ -n "${FORST_RS_CDYLIB:-}" ]; then
    CDYLIB="$FORST_RS_CDYLIB"
elif [ -f "$DEFAULT_DYLIB_LINUX" ]; then
    CDYLIB="$DEFAULT_DYLIB_LINUX"
elif [ -f "$DEFAULT_DYLIB_DARWIN" ]; then
    CDYLIB="$DEFAULT_DYLIB_DARWIN"
else
    echo "FATAL: forst-rs cdylib not found; set FORST_RS_CDYLIB=/abs/path/libforst_rs_ffi.{so,dylib}" >&2
    exit 1
fi

if [ ! -f "$CDYLIB" ]; then
    echo "FATAL: cdylib not found at $CDYLIB" >&2
    exit 1
fi

# ----------------------------------------------------------------------
# JAVA_HOME — JDK 25 required for the FFM bridge in forst-rs.
# ----------------------------------------------------------------------
auto_detect_jdk25() {
    if [ -d /Library/Java/JavaVirtualMachines ]; then
        local found
        found="$(find /Library/Java/JavaVirtualMachines -maxdepth 1 -name 'zulu-25*' | head -1)"
        if [ -n "$found" ]; then
            echo "$found/Contents/Home"
            return
        fi
    fi
    # Linux / CI hint: setup-java@v4 sets JAVA_HOME directly.
    echo ""
}
needs_autodetect=1
if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
    jver="$("$JAVA_HOME/bin/java" -version 2>&1 | head -1 | awk -F'"' '{print $2}' | cut -d. -f1)"
    if [ "${jver:-0}" -ge 25 ] 2>/dev/null; then
        needs_autodetect=0
    fi
fi
if [ "$needs_autodetect" = 1 ]; then
    JAVA_HOME="$(auto_detect_jdk25)"
fi
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "$JAVA_HOME/bin/java" ]; then
    echo "FATAL: JDK 25 not found; set JAVA_HOME to a JDK 25+ install" >&2
    exit 1
fi

# ----------------------------------------------------------------------
# Build the module (test-classes) + sibling state-backend modules (rocksdb,
# forst) so their factory classes are on the classpath for variants 1/2/3.
# -am pulls the upstream multi-module dependency closure (flink-runtime,
# flink-streaming-java test-jars, etc.).
# ----------------------------------------------------------------------
echo "[build] mvn install (forst-rs + sibling backends, no tests)"
(
    cd "$FLINK_ROOT"
    mvn -q -B \
        -pl flink-state-backends/flink-statebackend-forst-rs,flink-state-backends/flink-statebackend-rocksdb,flink-state-backends/flink-statebackend-forst \
        -am \
        install \
        -DskipTests -Drat.skip=true \
        -Dforstrs.native.libpath="$CDYLIB"
)

# ----------------------------------------------------------------------
# Classpath: forst-rs main + test classes + rocksdb factory + forst factory
# + their transitive runtime deps. Resolve once via dependency:build-classpath
# and cache to /tmp/cp.txt (compiled artifact files only, no source jars).
# Use includeScope=test for the test-utils + JUnit deps the MiniCluster path
# needs.
# ----------------------------------------------------------------------
CP_FILE="/tmp/little-e2e-cp.txt"
echo "[classpath] resolving (cached at $CP_FILE)"
(
    cd "$FLINK_ROOT"
    mvn -q -B -pl flink-state-backends/flink-statebackend-forst-rs \
        dependency:build-classpath \
        -Dmdep.outputFile="$CP_FILE" \
        -DincludeScope=test \
        -Drat.skip=true \
        -Dforstrs.native.libpath="$CDYLIB"
)
DEP_CP="$(cat "$CP_FILE")"

# Append local sibling-module test-classes for the rocksdb + forst SPI
# implementations. The forst module ships the StateBackendFactory under its
# main jar, which mvn install above placed in ~/.m2 — DEP_CP already includes
# it. The rocksdb module is similar. So this is just a precaution for build
# layouts where ~/.m2 is empty.
ROCKSDB_JAR="$FLINK_ROOT/flink-state-backends/flink-statebackend-rocksdb/target/flink-statebackend-rocksdb-2.2.0.jar"
FORST_JAR="$FLINK_ROOT/flink-state-backends/flink-statebackend-forst/target/flink-statebackend-forst-2.2.0.jar"
CP="$ROOT/target/test-classes:$ROOT/target/classes:$DEP_CP"
[ -f "$ROCKSDB_JAR" ] && CP="$CP:$ROCKSDB_JAR"
[ -f "$FORST_JAR" ]   && CP="$CP:$FORST_JAR"

# ----------------------------------------------------------------------
# Stage the cdylib under the upstream JNI lib name. Variant 3 sets
# java.library.path so org.forstdb.RocksDB.loadLibrary() finds our cdylib
# instead of the community libforstjni — this proves the forst-rs cdylib is
# a drop-in replacement at the libforstjni surface.
#
# NOTE: this assumes libforst_rs_ffi exports the JNI symbols
# (Java_org_forstdb_RocksDB_*). The current G-A cdylib does — see
# /Users/lijunqing/Code/stczwd/ForSt/crates/forst-rs-ffi/src/jni_compat/.
# If those exports are absent in a given build, variant 3 will fail at
# UnsatisfiedLinkError; variants 1/2/4 still run.
# ----------------------------------------------------------------------
LIBSWAP_DIR="$(mktemp -d)"
case "$(uname -s)" in
    Darwin*) cp "$CDYLIB" "$LIBSWAP_DIR/libforstjni.dylib" ;;
    *)       cp "$CDYLIB" "$LIBSWAP_DIR/libforstjni.so" ;;
esac

# ----------------------------------------------------------------------
# Run sequence — one stdout block per variant, RESULT lines parsed by CI.
# --enable-native-access=ALL-UNNAMED is required for the forst-rs FFM
# linker; harmless for the JNI variants.
# ----------------------------------------------------------------------
echo ""
echo "=== LittleE2EPerfBench across 4 backend variants ==="
echo "Events: $EVENTS   Warmups: $WARMUPS"
echo "CDylib: $CDYLIB"
echo ""

# Per-variant runs are non-fatal: if variant 2 (community forstjni) fails
# due to an upstream API mismatch (e.g. com.ververica:forstjni:0.1.8's
# RocksDB.loadLibrary() not matching flink-statebackend-forst's expected
# signature), we still want variants 1, 3, 4 to produce numbers. set +e
# is scoped to the variant loop; the rest of the script remains strict.
set +e
run_variant() {
    local label="$1"
    local backend="$2"
    shift 2
    echo ""
    echo "--- $label ---"
    "$JAVA_HOME/bin/java" \
        --enable-native-access=ALL-UNNAMED \
        -Dforstrs.native.libpath="$CDYLIB" \
        "$@" \
        -cp "$CP" \
        org.apache.flink.state.forstrs.perf.LittleE2EPerfBench \
        --backend "$backend" --events "$EVENTS" --warmups "$WARMUPS"
    local rc=$?
    if [ $rc -ne 0 ]; then
        echo "VARIANT_FAILED label='$label' backend=$backend exit_code=$rc"
    fi
}

run_variant "rocksdb" rocksdb
run_variant "forst (community libforstjni)" forst
run_variant "forst (libforstjni -> libforst_rs_ffi swap)" forst \
    "-Djava.library.path=$LIBSWAP_DIR"
run_variant "forst-rs" forst-rs
set -e

echo ""
echo "=== done ==="

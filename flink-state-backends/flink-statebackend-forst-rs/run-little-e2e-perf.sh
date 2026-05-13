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
#   JAVA_HOME_25       JDK 25+ home (FFM stable on 25+, required for forst-rs variant)
#   JAVA_HOME_17       JDK 17+ home (for rocksdb + forst variants)
#   JAVA_HOME          Fallback: used as JDK 25 if JAVA_HOME_25 not set
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
EVENTS="${EVENTS:-1000000}"
WARMUPS="${WARMUPS:-1}"
PARALLELISM_SWEEP="${PARALLELISM_SWEEP:-2 4 8}"
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-0}"

# JFR profiling: set PROFILE=1 to attach Java Flight Recorder to forst-rs runs.
# The JFR file is written to /tmp/forst-rs-profile.jfr for post-hoc analysis.
if [ "${PROFILE:-}" = "1" ]; then
    PROFILE_ARGS="-XX:+FlightRecorder -XX:StartFlightRecording=duration=120s,filename=/tmp/forst-rs-profile.jfr"
else
    PROFILE_ARGS=""
fi

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
# JAVA_HOME / JAVA_HOME_25 / JAVA_HOME_17 — dual-JDK support.
#
# forst-rs requires JDK 25 (FFM). rocksdb + forst run on JDK 17.
# The GHA workflow sets JAVA_HOME_25 and JAVA_HOME_17 explicitly.
# For local dev, JAVA_HOME is used as the JDK 25 path and JDK 17 is
# auto-detected or defaults to JAVA_HOME (runs everything on JDK 25).
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
    echo ""
}
auto_detect_jdk17() {
    if [ -d /Library/Java/JavaVirtualMachines ]; then
        local found
        found="$(find /Library/Java/JavaVirtualMachines -maxdepth 1 -name '*17*' | head -1)"
        if [ -n "$found" ]; then
            echo "$found/Contents/Home"
            return
        fi
    fi
    echo ""
}

# Resolve JDK 25 (required for forst-rs + compilation)
if [ -n "${JAVA_HOME_25:-}" ] && [ -x "${JAVA_HOME_25}/bin/java" ]; then
    JDK25_HOME="$JAVA_HOME_25"
else
    needs_autodetect=1
    if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
        jver="$("$JAVA_HOME/bin/java" -version 2>&1 | head -1 | awk -F'"' '{print $2}' | cut -d. -f1)"
        if [ "${jver:-0}" -ge 25 ] 2>/dev/null; then
            needs_autodetect=0
            JDK25_HOME="$JAVA_HOME"
        fi
    fi
    if [ "$needs_autodetect" = 1 ]; then
        JDK25_HOME="$(auto_detect_jdk25)"
    fi
fi
if [ -z "${JDK25_HOME:-}" ] || [ ! -x "$JDK25_HOME/bin/java" ]; then
    echo "FATAL: JDK 25 not found; set JAVA_HOME or JAVA_HOME_25 to a JDK 25+ install" >&2
    exit 1
fi
export JAVA_HOME="$JDK25_HOME"

# Resolve JDK 17 (for rocksdb + forst variants)
if [ -n "${JAVA_HOME_17:-}" ] && [ -x "${JAVA_HOME_17}/bin/java" ]; then
    JDK17_HOME="$JAVA_HOME_17"
else
    JDK17_HOME="$(auto_detect_jdk17)"
    # Fall back to JDK 25 if no JDK 17 found (local dev: all variants on JDK 25)
    if [ -z "$JDK17_HOME" ] || [ ! -x "$JDK17_HOME/bin/java" ]; then
        JDK17_HOME="$JDK25_HOME"
    fi
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
# Compile the JDK-17-compatible bench class. This lives under
# src/test/java17/ and is compiled separately with --release 17 so it
# can run on JDK 17 for the rocksdb + forst variants.
# ----------------------------------------------------------------------
JDK17_BENCH_SRC="$ROOT/src/test/java17/org/apache/flink/state/forstrs/perf/LittleE2EPerfBenchJdk17.java"
JDK17_BENCH_OUT="$ROOT/target/test-classes-jdk17"
if [ -f "$JDK17_BENCH_SRC" ]; then
    echo "[compile] LittleE2EPerfBenchJdk17.java (--release 17)"
    mkdir -p "$JDK17_BENCH_OUT"
    "$JDK25_HOME/bin/javac" --release 17 -cp "$CP" -d "$JDK17_BENCH_OUT" "$JDK17_BENCH_SRC"
fi
# Prepend JDK17 bench classes to classpath (takes priority over test-classes for the Jdk17 class)
CP_JDK17="$JDK17_BENCH_OUT:$CP"

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
echo "Events: $EVENTS   Warmups: $WARMUPS   Parallelism sweep: $PARALLELISM_SWEEP"
echo "CDylib: $CDYLIB"
echo "JDK 25 (forst-rs): $JDK25_HOME"
echo "JDK 17 (rocksdb/forst): $JDK17_HOME"
echo ""

# Per-variant runs are non-fatal: if variant 2 (community forstjni) fails
# due to an upstream API mismatch (e.g. com.ververica:forstjni:0.1.8's
# RocksDB.loadLibrary() not matching flink-statebackend-forst's expected
# signature), we still want variants 1, 3, 4 to produce numbers. set +e
# is scoped to the variant loop; the rest of the script remains strict.
set +e

# run_variant_jdk17: runs rocksdb/forst variants on JDK 17 using LittleE2EPerfBenchJdk17
run_variant_jdk17() {
    local label="$1"
    local backend="$2"
    shift 2
    echo ""
    echo "--- $label ---"
    local java_bin="$JDK17_HOME/bin/java"
    # shellcheck disable=SC2086
    "$java_bin" \
        "$@" \
        -cp "$CP_JDK17" \
        org.apache.flink.state.forstrs.perf.LittleE2EPerfBenchJdk17 \
        --backend "$backend" --events "$EVENTS" --warmups "$WARMUPS" --parallelism "$PAR" \
        --checkpoint-interval "$CHECKPOINT_INTERVAL"
    local rc=$?
    if [ $rc -ne 0 ]; then
        echo "VARIANT_FAILED label='$label' backend=$backend parallelism=$PAR exit_code=$rc"
    fi
}

# run_variant_jdk25: runs forst-rs on JDK 25 using LittleE2EPerfBench (FFM + Vector API)
run_variant_jdk25() {
    local label="$1"
    local backend="$2"
    shift 2
    local extra_jvm_args=""
    # Attach JFR profiling to forst-rs variants when PROFILE=1
    if [ -n "$PROFILE_ARGS" ] && [ "$backend" = "forst-rs" ]; then
        extra_jvm_args="$PROFILE_ARGS"
    fi
    echo ""
    echo "--- $label ---"
    local java_bin="$JDK25_HOME/bin/java"
    local jvm_module_args="--enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector"
    # shellcheck disable=SC2086
    "$java_bin" \
        $jvm_module_args \
        -Dforstrs.native.libpath="$CDYLIB" \
        $extra_jvm_args \
        "$@" \
        -cp "$CP" \
        org.apache.flink.state.forstrs.perf.LittleE2EPerfBench \
        --backend "$backend" --events "$EVENTS" --warmups "$WARMUPS" --parallelism "$PAR" \
        --checkpoint-interval "$CHECKPOINT_INTERVAL"
    local rc=$?
    if [ $rc -ne 0 ]; then
        echo "VARIANT_FAILED label='$label' backend=$backend parallelism=$PAR exit_code=$rc"
    fi
}

for PAR in $PARALLELISM_SWEEP; do
    echo ""
    echo "====== Parallelism = $PAR ======"
    run_variant_jdk17 "rocksdb (p=$PAR)" rocksdb
    run_variant_jdk17 "forst (community libforstjni, p=$PAR)" forst
    run_variant_jdk17 "forst (libforstjni -> libforst_rs_ffi swap, p=$PAR)" forst \
        "-Djava.library.path=$LIBSWAP_DIR"
    run_variant_jdk25 "forst-rs (p=$PAR)" forst-rs
done

# Checkpoint variant: p=4, ckpt=5s — measures overhead of periodic snapshots.
if [ "${CHECKPOINT_INTERVAL:-0}" = "0" ]; then
    echo ""
    echo "====== Checkpoint variant: p=4, ckpt=5000ms ======"
    PAR=4
    CHECKPOINT_INTERVAL=5000
    run_variant_jdk17 "rocksdb (p=4, ckpt=5s)" rocksdb
    run_variant_jdk25 "forst-rs (p=4, ckpt=5s)" forst-rs
    CHECKPOINT_INTERVAL=0
fi
set -e

echo ""
echo "=== done ==="

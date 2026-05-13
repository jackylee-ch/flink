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
# Stateful E2E benchmark runner — starts MinIO, runs the bench across backends,
# and stops MinIO on exit.
#
# State size tiers:
#   100m  — 100K keys x 1KB   = ~100MB  (GHA-runnable, local dev)
#   1t    — 10M keys x 100KB  = ~1TB    (dedicated perf machines only)
#   10t   — 100M keys x 100KB = ~10TB   (dedicated perf machines only)
#
# Env overrides:
#   STATE_SIZE         100m | 1t | 10t (default: 100m)
#   MEASURE_SECONDS    Steady-state measurement duration (default: 30)
#   PARALLELISM        Flink parallelism (default: 4)
#   FORST_RS_CDYLIB    Path to libforst_rs_ffi.{so,dylib}
#   SKIP_MINIO         Set to 1 to skip MinIO start/stop (use existing instance)
#   MINIO_ENDPOINT     MinIO endpoint (default: http://localhost:9000)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
FLINK_ROOT="$(cd "$ROOT/../.." && pwd)"

STATE_SIZE="${STATE_SIZE:-100m}"
MEASURE_SECONDS="${MEASURE_SECONDS:-30}"
PARALLELISM="${PARALLELISM:-4}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_BUCKET="forst-rs-bench"
SKIP_MINIO="${SKIP_MINIO:-0}"

# --- Locate cdylib ---
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

echo "=== Stateful E2E Benchmark ==="
echo "State size: $STATE_SIZE"
echo "Measure seconds: $MEASURE_SECONDS"
echo "Parallelism: $PARALLELISM"
echo "CDylib: $CDYLIB"
echo ""

# --- Start MinIO (unless SKIP_MINIO=1) ---
cleanup_minio() {
    if [ "$SKIP_MINIO" = "0" ]; then
        echo "[cleanup] Stopping MinIO..."
        docker stop minio-stateful-bench 2>/dev/null || true
        docker rm minio-stateful-bench 2>/dev/null || true
    fi
}
trap cleanup_minio EXIT

if [ "$SKIP_MINIO" = "0" ]; then
    echo "[minio] Starting MinIO container..."
    docker rm -f minio-stateful-bench 2>/dev/null || true
    docker run -d --name minio-stateful-bench -p 9000:9000 \
        -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
        minio/minio:RELEASE.2024-08-17T01-24-54Z server /data
    echo "[minio] Waiting for MinIO to be ready..."
    for i in $(seq 1 30); do
        if curl -sf "$MINIO_ENDPOINT/minio/health/live" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    # Create bucket
    docker exec minio-stateful-bench mkdir -p /data/$MINIO_BUCKET
    echo "[minio] Bucket $MINIO_BUCKET created"
fi

# --- Build ---
echo "[build] mvn install (forst-rs + rocksdb, no tests)"
(
    cd "$FLINK_ROOT"
    mvn -q -B \
        -pl flink-state-backends/flink-statebackend-forst-rs,flink-state-backends/flink-statebackend-rocksdb \
        -am install -DskipTests -Drat.skip=true \
        -Dforstrs.native.libpath="$CDYLIB"
)

# --- Classpath ---
CP_FILE="/tmp/stateful-e2e-cp.txt"
echo "[classpath] Resolving..."
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
MODULE_DIR="$FLINK_ROOT/flink-state-backends/flink-statebackend-forst-rs"
ROCKSDB_JAR="$FLINK_ROOT/flink-state-backends/flink-statebackend-rocksdb/target/flink-statebackend-rocksdb-2.2.0.jar"
CP="$MODULE_DIR/target/test-classes:$MODULE_DIR/target/classes:$DEP_CP"
[ -f "$ROCKSDB_JAR" ] && CP="$CP:$ROCKSDB_JAR"

# --- Run benchmarks ---
echo ""
echo "=== Running benchmarks ==="
echo ""

set +e
for backend in rocksdb forst-rs; do
    for storage in local s3; do
        # rocksdb doesn't support S3
        if [ "$backend" = "rocksdb" ] && [ "$storage" = "s3" ]; then
            continue
        fi
        echo ""
        echo "--- $backend / $storage ---"
        STORAGE_ARGS=""
        if [ "$storage" = "s3" ]; then
            STORAGE_ARGS="--storage-uri s3://$MINIO_BUCKET/ --s3-endpoint $MINIO_ENDPOINT"
        fi

        JAVA_BIN="${JAVA_HOME:-/usr}/bin/java"
        # shellcheck disable=SC2086
        "$JAVA_BIN" \
            --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector \
            -Dforstrs.native.libpath="$CDYLIB" \
            -cp "$CP" \
            org.apache.flink.state.forstrs.perf.StatefulE2EBench \
            --backend "$backend" --state-size "$STATE_SIZE" \
            --measure-seconds "$MEASURE_SECONDS" --parallelism "$PARALLELISM" \
            $STORAGE_ARGS
        rc=$?
        if [ $rc -ne 0 ]; then
            echo "VARIANT_FAILED backend=$backend storage=$storage exit_code=$rc"
        fi
    done
done
set -e

echo ""
echo "=== done ==="

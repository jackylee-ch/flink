#!/usr/bin/env bash
# Drives the ForStCompareBenchmark harness against either the ForSt-RS shim
# cdylib or the community ForSt cdylib, using a freshly compiled classpath
# from the bench source — no Maven, no toolchain dance.
#
# Usage:  ./run-jmh-3way.sh (forst-rs|forst)
#
# Env overrides:
#   FORST_RS_LIB    cdylib for the "forst-rs" variant
#   FORST_LIB       cdylib for the "forst" (community) variant
#   JAVA_HOME       JDK 25+ home (auto-detected from /Library/Java/...)
#   BENCH_WARMUP_S  warmup duration per workload (default 6)
#   BENCH_MEASURE_S measurement duration per workload (default 25)
set -euo pipefail

VARIANT="${1:-forst-rs}"
case "$VARIANT" in
  forst-rs)
    LIB="${FORST_RS_LIB:-/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib}"
    ;;
  forst)
    LIB="${FORST_LIB:-/tmp/forstjni-community.dylib}"
    ;;
  *)
    echo "Usage: $0 (forst-rs|forst)" >&2
    exit 2
    ;;
esac

if [ ! -f "$LIB" ]; then
  echo "Library not found: $LIB" >&2
  exit 1
fi

# Resolve JAVA_HOME — JDK 25 required for FFM + --enable-native-access. Note we
# DO NOT trust an inherited JAVA_HOME unless its java reports >=25; many shells
# export JAVA_HOME pointing at JDK 17 by default and that would fail with
# "release version 21 not supported" at javac time.
auto_detect_jdk25() {
  local found
  found="$(find /Library/Java/JavaVirtualMachines -maxdepth 1 -name 'zulu-25*' | head -1)"
  if [ -n "$found" ]; then
    echo "$found/Contents/Home"
  fi
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
  echo "JDK 25 not found. Set JAVA_HOME to a JDK 25 install or place one under /Library/Java/JavaVirtualMachines/zulu-25*" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Each variant has its OWN sourceset and its OWN compiled output directory.
# This is necessary because both libraries claim the package + class name
# `org.forstdb.RocksDB`, but they expose different native method signatures —
# trying to put both stubs on the same classpath would either NoClassDefFound
# or pick up the wrong native symbol mangling. Separating the build output
# keeps the JNI symbol search clean per process.
case "$VARIANT" in
  forst-rs)
    SRC_DIR="$SCRIPT_DIR/src/test/java"
    OUT_DIR="$SCRIPT_DIR/target/jmh-classes-forst-rs"
    SRCS=(
      "$SRC_DIR/org/forstdb/RocksDB.java"
      "$SRC_DIR/org/forstdb/RocksDBException.java"
      "$SRC_DIR/org/apache/flink/state/forstrs/jmh/ForStCompareBenchmark.java"
    )
    MAIN_CLASS="org.apache.flink.state.forstrs.jmh.ForStCompareBenchmark"
    ;;
  forst)
    SRC_DIR="$SCRIPT_DIR/src/test/java-community"
    OUT_DIR="$SCRIPT_DIR/target/jmh-classes-community"
    SRCS=(
      "$SRC_DIR/org/forstdb/RocksDB.java"
      "$SRC_DIR/org/forstdb/Options.java"
      "$SRC_DIR/org/forstdb/FlushOptions.java"
      "$SRC_DIR/org/forstdb/Status.java"
      "$SRC_DIR/org/forstdb/RocksDBException.java"
      "$SRC_DIR/org/apache/flink/state/forstrs/jmh/ForStCommunityBenchmark.java"
    )
    MAIN_CLASS="org.apache.flink.state.forstrs.jmh.ForStCommunityBenchmark"
    ;;
esac

mkdir -p "$OUT_DIR"
echo "[compile] javac -> $OUT_DIR"
"$JAVA_HOME/bin/javac" --release 21 -d "$OUT_DIR" "${SRCS[@]}"

WARMUP="${BENCH_WARMUP_S:-6}"
MEASURE="${BENCH_MEASURE_S:-25}"

# JVM tuning per variant. The community cdylib was built against an older
# JDK and crashes inside G1's GC barrier when it receives compressed oops
# from JDK 25 — disable compressed oops + class pointers when running it.
# Our forst-rs shim has no such issue (it's pure Rust, doesn't touch JNI
# oop layout besides the standard JByteArray copy helpers).
JVM_EXTRA=()
case "$VARIANT" in
  forst)
    JVM_EXTRA=(
      -XX:-UseCompressedOops
      -XX:-UseCompressedClassPointers
      -XX:+UseSerialGC
    )
    ;;
esac

echo "[run] variant=$VARIANT lib=$LIB main=$MAIN_CLASS"
RESULT_FILE="/tmp/jmh-results-$VARIANT.txt"
"$JAVA_HOME/bin/java" \
  --enable-native-access=ALL-UNNAMED \
  ${JVM_EXTRA[@]+"${JVM_EXTRA[@]}"} \
  -Xms256m -Xmx2g \
  -Dorg.forstdb.libpath="$LIB" \
  -Dbench.warmup.s="$WARMUP" \
  -Dbench.measure.s="$MEASURE" \
  -cp "$OUT_DIR" \
  "$MAIN_CLASS" 2>&1 | tee "$RESULT_FILE"

echo
echo "[done] results captured at $RESULT_FILE"

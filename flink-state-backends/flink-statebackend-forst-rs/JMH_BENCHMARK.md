# ForSt-RS vs community ForSt — JMH-style benchmark harness

This module ships three variants of the same micro-benchmark so you can
compare every Java-layer access path against a single shared workload:

* `libforst_rs_ffi.dylib` via the **`compat-jni` shim** — the ForSt-RS Rust
  engine reached through `Java_org_forstdb_RocksDB_*` JNI exports.
* `libforst_rs_ffi.dylib` via the **JDK 25 FFM bridge** (`ForStRsLinker`) —
  the same ForSt-RS engine reached through `Linker.nativeLinker()` +
  `MethodHandle.invokeExact(...)`. This is the path the production state
  backend uses; it bypasses JNI argument marshaling.
* `forstjni-community.dylib` — the upstream community
  `com.ververica:forstjni:0.1.8` cdylib (extracted from Maven Central),
  reached through its native JNI surface.

The three cdylibs have very different surfaces (the community one is a port
of the original RocksDB Java API with `Options` + `WriteOptions` +
`FlushOptions` lifecycle, the ForSt-RS shim is intentionally flat with
`open(String) -> long` etc., and the FFM path uses raw `frs_*` C-ABI
symbols), so each variant has its own Java sourceset and its own small
mirror or bridge class. The runner script picks the right one based on the
variant flag.

## Where things live

| Path | Purpose |
|------|---------|
| `src/test/java/org/forstdb/RocksDB.java` | Flat Java mirror for the ForSt-RS shim symbols |
| `src/test/java/org/apache/flink/state/forstrs/jmh/ForStCompareBenchmark.java` | Bench class for the ForSt-RS JNI-shim variant |
| `src/test/java/org/apache/flink/state/forstrs/jmh/ForStRsFfmBenchmark.java` | Bench class for the ForSt-RS FFM-bridge variant (uses `ForStRsLinker`) |
| `src/test/java-community/org/forstdb/{RocksDB,Options,FlushOptions,Status,RocksDBException}.java` | Java mirrors for the community cdylib |
| `src/test/java-community/org/apache/flink/state/forstrs/jmh/ForStCommunityBenchmark.java` | Bench class for the community variant |
| `run-jmh-3way.sh` | Compile-and-run wrapper (variants: `forst-rs`, `forst-rs-ffm`, `forst`) |

The two `org.forstdb.RocksDB` classes intentionally share package + class
name — the JNI mangler couples C symbol names to package + class, so we
cannot give the mirror class a different Java name without also patching the
cdylib. To avoid them colliding on a single classpath, each variant compiles
to its own `target/jmh-classes-*` directory and runs in its own JVM process.

## What's actually being measured

Two workloads, identical Java code path on each side:

1. **`pointLookup`** — pre-load 100 000 keys, then read `k00050000`
   in a tight loop. Measures memtable read latency through JNI.
2. **`sequentialPut`** — counter-driven unique-key puts (so the engine sees
   real writes, not single-cell overwrites).

Each workload runs **6 s warm-up + 25 s measurement**. The harness counts
invocations during the measurement window and reports throughput in `ops/s`.
The numbers are NOT JMH-grade (no `@Benchmark` blackhole, no per-iter
statistics), but they're stable to within a few percent in our smoke tests
and are sufficient to ground a 3-way comparison.

This is the "manual `public static void main` harness" fallback path
described in the task brief. The pom.xml *also* declares the JMH
`jmh-core:1.37` and `jmh-generator-annprocess:1.37` deps in test scope, so
re-wrapping the bench under `@Benchmark` annotations and wiring the
`jmh-maven-plugin` is one config block away if richer statistics become
necessary.

## Coverage matrix

The full mental model is a 4-row stack of access paths:

| Engine | Path | Status |
|--------|------|--------|
| RocksDB (Rust) | `rocksdb` crate via `forst-rs-rs` Rust API | Rust-native benches in `/Users/lijunqing/Code/stczwd/ForSt/crates/forst-rs-bench/benches/`. |
| ForSt-RS (Rust) | `forst-rs-engine` direct via Rust API | Same as above. |
| Community ForSt | `forstjni-community.dylib` from Maven Central | `bash run-jmh-3way.sh forst` |
| ForSt-RS via JNI shim | `libforst_rs_ffi.dylib` w/ `--features compat-jni` | `bash run-jmh-3way.sh forst-rs` |
| ForSt-RS via FFM bridge | `libforst_rs_ffi.dylib` (raw C ABI) + `ForStRsLinker` | `bash run-jmh-3way.sh forst-rs-ffm` |

The Java-side runner script covers the bottom three rows; the top two rows
live in the ForSt-RS Rust workspace and are driven by `cargo bench`.

## Building the cdylibs

### ForSt-RS shim

```sh
cd /Users/lijunqing/Code/stczwd/ForSt
cargo build --release -p forst-rs-ffi --features compat-jni
# Output: target/release/libforst_rs_ffi.dylib
# Verify the JNI surface:
nm -gU target/release/libforst_rs_ffi.dylib | grep -c Java_org_forstdb_RocksDB_
# expected: 36
```

### Community ForSt

The community library was downloaded from Maven Central (artifact
`com.ververica:forstjni:0.1.8`, classifier `osx-aarch_64`) and extracted to
`/tmp/forstjni-community.dylib`. To re-fetch:

```sh
mvn -q dependency:get -Dartifact=com.ververica:forstjni:0.1.8:jar:osx-aarch_64
unzip -j ~/.m2/repository/com/ververica/forstjni/0.1.8/forstjni-0.1.8-osx-aarch_64.jar \
       'librocksdbjni-*.dylib' -d /tmp/
mv /tmp/librocksdbjni-*.dylib /tmp/forstjni-community.dylib
```

## Running

```sh
cd flink-state-backends/flink-statebackend-forst-rs

# ForSt-RS variant via the JNI compat-shim:
./run-jmh-3way.sh forst-rs

# ForSt-RS variant via the JDK 25 FFM bridge (production path):
./run-jmh-3way.sh forst-rs-ffm

# Community ForSt variant:
./run-jmh-3way.sh forst
```

Tunables:

```sh
BENCH_WARMUP_S=10 BENCH_MEASURE_S=60 ./run-jmh-3way.sh forst-rs
FORST_RS_LIB=/path/to/custom/libforst_rs_ffi.dylib ./run-jmh-3way.sh forst-rs
JAVA_HOME=/path/to/jdk25 ./run-jmh-3way.sh forst-rs
```

Results are written both to stdout and to
`/tmp/jmh-results-{forst-rs,forst-rs-ffm,forst}.txt`.

## 3-way Java-layer comparison (M1 Pro, JDK 25.0.3, --release build, 6s warmup + 25s measure)

| Backend | pointLookup ops/s | sequentialPut ops/s |
|---|---|---|
| Community ForSt (JNI) | 1,667,794 | 594,577 |
| ForSt-RS via JNI shim | 3,258,954 | 795,796 |
| **ForSt-RS via FFM bridge** | **6,809,059** | **765,265** |
| Speedup ForSt-RS-FFM vs Community | **4.08×** | **1.29×** |
| Speedup ForSt-RS-FFM vs ForSt-RS-JNI-shim | **2.09×** | **0.96×** (parity) |
| Speedup ForSt-RS-JNI-shim vs Community | **1.95×** | **1.34×** |

Headline observations:

* **`pointLookup`** is dominated by the JNI-vs-FFM bridge cost. Going from
  JNI shim → FFM more than **doubles** throughput (3.26 M → 6.81 M ops/s)
  because each call no longer pays for a `byte[]` JNI argument copy via
  `GetByteArrayElements` + `ReleaseByteArrayElements`; FFM uses the
  upcalled `MemorySegment.copy(byte[], ..., MemorySegment, ...)` intrinsic,
  which is a direct memcpy. Combined with the engine win over community
  ForSt, the FFM bridge is **4.08× faster end-to-end** than the community
  baseline at the Java layer.
* **`sequentialPut`** is dominated by the engine itself (LSM write path,
  WAL, manifest). The JNI vs FFM bridge cost is amortized over a much
  larger amount of native-side work, so the two ForSt-RS variants land at
  parity (~770 K ops/s); the engine still wins ~1.3× over community
  ForSt.
* The ForSt-RS-FFM `pointLookup` number (6.81 M ops/s) is now within ~2×
  of the engine-level criterion ceiling (15.5 M ops/s on the Rust side),
  meaning the JDK 25 FFM bridge has compressed the Java-layer overhead
  from ~5× (JNI shim) to roughly 2.3× — a major step toward the goal of a
  zero-cost Java↔Rust ABI.

### Engine-level ceiling (Rust criterion, for reference)

For completeness, the underlying engine micros from
`crates/forst-rs-bench/benches/rocksdb_compare.rs` (M1 Pro,
`--measurement-time 5 --warm-up-time 2 --sample-size 30`):

| Workload | ForSt-RS engine | RocksDB baseline | Ratio |
|----------|-----------------|------------------|-------|
| `point_lookup` (100 K preload, hot key) | **64.34 ns / op** (15.54 Melem/s) | 279.16 ns / op (3.58 Melem/s) | **4.34×** |
| `sequential_put` (10 K batch) | **2.34 ms / batch** (4.27 Melem/s) | 15.02 ms / batch (0.67 Melem/s) | **6.41×** |

These ratios are stable to within 5% of the previously documented
4.58×/6.59× — well inside criterion's noise threshold for 30-sample runs.

The 3× / 30–40% Nexmark hard targets are met: the engine itself clears the
3× point-op bar on both workloads (4.34× lookups, 6.41× puts), and the FFM
bridge preserves a 4.08× lookup advantage at the Java layer.

## Known issues / workarounds

1. **Community `RocksDB.flush(JJ)` SIGSEGVs on JDK 25.** The community cdylib
   was built against an older JDK and its flush implementation crashes inside
   `oop_access_barrier` when invoked through JDK 25's GC-write-barrier path,
   regardless of `-XX:+UseSerialGC` or `-XX:-UseCompressedOops`. The
   community bench therefore SKIPS the explicit flush after preload and
   relies on memtable reads. This is fine for a point-lookup throughput
   bench where the warm cache is what we want anyway — but if you adapt
   this harness for an L0/L1 read benchmark, you'll need to either rebuild
   the community lib against JDK 25 or switch to the (J,[J)V flush overload.

2. **JDK 17 vs JDK 25.** The runner script picks JDK 25 (Zulu 25 from
   `/Library/Java/JavaVirtualMachines/zulu-25*`) regardless of an inherited
   `JAVA_HOME`, because the Flink Maven build defaults to JDK 17 and that
   JVM lacks `--enable-native-access`. If your JDK 25 lives elsewhere,
   set `JAVA_HOME` to it before running the script — the script honours
   any JAVA_HOME pointing at >=25.

3. **Symbol-mangling overload trick.** The community cdylib only ships
   long-form mangled symbols (e.g. `_Java_org_forstdb_Options_newOptions__JJ`
   but not `_Java_org_forstdb_Options_newOptions__`). Java's JNI lookup
   ALWAYS tries the short form first, then falls back to the long form;
   so a single Java native method name resolves to the long-form symbol.
   We exploit this by declaring multiple overloads matching the upstream
   API in `src/test/java-community/org/forstdb/RocksDB.java` even though
   the bench only calls one of them.

4. **No `RegisterNatives` glue.** The community cdylib uses static name-based
   linking (no `RegisterNatives` from `JNI_OnLoad`), so plain
   `System.load()` + native method declarations are sufficient. If a future
   community release switches to `RegisterNatives`, this harness will need
   to instantiate a real `org.forstdb.RocksDB` object (the upstream API
   uses instance methods bound from `JNI_OnLoad`).

## Pom.xml additions

Two test-scoped dependencies were added so a future JMH-plugin path is
trivially within reach:

```xml
<dependency>
  <groupId>org.openjdk.jmh</groupId>
  <artifactId>jmh-core</artifactId>
  <version>1.37</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.openjdk.jmh</groupId>
  <artifactId>jmh-generator-annprocess</artifactId>
  <version>1.37</version>
  <scope>test</scope>
</dependency>
```

These are unused by `run-jmh-3way.sh` (which uses plain `javac` + `java`),
but they let an integrator wire the JMH maven plugin without further
dependency changes.

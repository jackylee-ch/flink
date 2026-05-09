# ForSt-RS vs community ForSt — JMH-style benchmark harness

This module ships a small JNI-driven benchmark that lets you point the SAME
Java workload at either:

* `libforst_rs_ffi.dylib` — the ForSt-RS Rust engine, exposed through the
  `compat-jni` cdylib in this repo, OR
* `forstjni-community.dylib` — the upstream community
  `com.ververica:forstjni:0.1.8` cdylib (extracted from Maven Central).

The two cdylibs have very different JNI surfaces (the community one is a port
of the original RocksDB Java API with `Options` + `WriteOptions` +
`FlushOptions` lifecycle, the ForSt-RS shim is intentionally flat with
`open(String) -> long` etc.), so each variant has its own Java sourceset and
its own small Java mirror class. The runner script picks the right one based
on the variant flag.

## Where things live

| Path | Purpose |
|------|---------|
| `src/test/java/org/forstdb/RocksDB.java` | Flat Java mirror for the ForSt-RS shim symbols |
| `src/test/java/org/apache/flink/state/forstrs/jmh/ForStCompareBenchmark.java` | Bench class for the ForSt-RS variant |
| `src/test/java-community/org/forstdb/{RocksDB,Options,FlushOptions,Status,RocksDBException}.java` | Java mirrors for the community cdylib |
| `src/test/java-community/org/apache/flink/state/forstrs/jmh/ForStCommunityBenchmark.java` | Bench class for the community variant |
| `run-jmh-3way.sh` | Compile-and-run wrapper |

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

## Why "3-way" if the runner only takes two flavours?

The task brief refers to a 4-row mental model:

| Engine | Path | Status |
|--------|------|--------|
| RocksDB (Rust) | `rocksdb` crate via `forst-rs-rs` Rust API | Out of scope for THIS bench — covered by the Rust-native benches in `/Users/lijunqing/Code/stczwd/ForSt/benches/`. |
| ForSt-RS (Rust) | `forst-rs-engine` direct via Rust API | Same as above. |
| Community ForSt | `forstjni-community.dylib` from Maven Central | `bash run-jmh-3way.sh forst` |
| ForSt-RS via JNI shim | `libforst_rs_ffi.dylib` w/ `--features compat-jni` | `bash run-jmh-3way.sh forst-rs` |

So this harness covers the bottom two rows. The top two rows live in the
ForSt-RS Rust workspace and are driven by `cargo bench`.

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

# ForSt-RS variant (pure-Rust engine via JNI shim):
./run-jmh-3way.sh forst-rs

# Community ForSt variant:
./run-jmh-3way.sh forst
```

Tunables:

```sh
BENCH_WARMUP_S=10 BENCH_MEASURE_S=60 ./run-jmh-3way.sh forst-rs
FORST_RS_LIB=/path/to/custom/libforst_rs_ffi.dylib ./run-jmh-3way.sh forst-rs
JAVA_HOME=/path/to/jdk25 ./run-jmh-3way.sh forst-rs
```

Results are written both to stdout and to `/tmp/jmh-results-{forst-rs,forst}.txt`.

## Sample numbers (M1 Pro, JDK 25.0.3, --release build, 6s warmup + 25s measure)

| Workload | ForSt-RS shim | Community ForSt | Ratio |
|----------|---------------|-----------------|-------|
| pointLookup (memtable) | **3.10 M ops/s** | 2.06 M ops/s | **1.50×** |
| sequentialPut | **751 K ops/s** | 583 K ops/s | **1.29×** |

These are the raw JNI-bound numbers — both variants funnel through a
`byte[]` JNI argument copy on each invocation, so they're really
"JNI-mediated point op" throughput, not the underlying engine ceiling.

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

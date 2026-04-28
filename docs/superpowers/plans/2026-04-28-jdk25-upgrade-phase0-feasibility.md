# JDK 25 Upgrade — Phase 0 Feasibility & Inventory — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a measured, dual-audience feasibility report for upgrading the Flink fork to JDK 25, grounded in repo scans + a real `mvn clean compile` attempt under JDK 25.

**Architecture:** Five parallel investigation lanes (L1 static API-risk, L2 build-config, L3 Scala footprint, L4 smoke-compile in throwaway worktree, L5 JDK changelog cross-ref) feed raw artifacts into a final consolidation step that writes the dual-audience report. No POM or source edits land on `jdk25_performance` — only the report and the raw artifacts.

**Tech Stack:** ripgrep, Maven 3.9.x, Zulu OpenJDK 25.0.3 (`/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home`), git worktrees, Anthropic Explore subagent.

**Reference spec:** `docs/superpowers/specs/2026-04-28-jdk25-upgrade-phase0-feasibility-design.md`

---

## Task 0: Verify environment and create artifacts directory

**Files:**
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/.gitkeep`

- [ ] **Step 1: Verify JDK 25 is installed and accessible**

Run:
```bash
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
"$JDK25/bin/java" -version
```
Expected: output contains `openjdk version "25.0.3"` (or any 25.x).

- [ ] **Step 2: Verify ripgrep is on PATH**

Run: `rg --version | head -1`
Expected: prints a `ripgrep` version string. If missing, install via `brew install ripgrep` before proceeding.

- [ ] **Step 3: Verify on the right branch**

Run: `git rev-parse --abbrev-ref HEAD`
Expected: `jdk25_performance`. If not, `git checkout jdk25_performance` first.

- [ ] **Step 4: Create artifacts directory with .gitkeep**

Run:
```bash
mkdir -p docs/superpowers/specs/artifacts/jdk25-phase0
touch docs/superpowers/specs/artifacts/jdk25-phase0/.gitkeep
```

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/artifacts/jdk25-phase0/.gitkeep \
        docs/superpowers/specs/2026-04-28-jdk25-upgrade-phase0-feasibility-design.md \
        docs/superpowers/plans/2026-04-28-jdk25-upgrade-phase0-feasibility.md
git commit -m "[chore][docs] Add JDK 25 Phase-0 feasibility design and plan scaffolding"
```

---

## Task 1: Create the L4 smoke-test worktree (one-time setup, used by L4 only)

**Files:**
- Create: `../flink-wt-jdk25-phase0/` (sibling worktree, branch `jdk25-phase0-smoketest`)
- Modify (in worktree only): `../flink-wt-jdk25-phase0/pom.xml` — bump `target.java.version` and `source.java.version` to 25

- [ ] **Step 1: Create the worktree**

Run:
```bash
git worktree add ../flink-wt-jdk25-phase0 -b jdk25-phase0-smoketest jdk25_performance
```
Expected: `Preparing worktree ... HEAD is now at <sha>`.

- [ ] **Step 2: Bump Java version in worktree's root pom.xml only**

In `../flink-wt-jdk25-phase0/pom.xml`, change:
```xml
<source.java.version>11</source.java.version>
<target.java.version>17</target.java.version>
```
to:
```xml
<source.java.version>25</source.java.version>
<target.java.version>25</target.java.version>
```

- [ ] **Step 3: Verify the change**

Run: `grep -E "source.java.version|target.java.version" ../flink-wt-jdk25-phase0/pom.xml | head -5`
Expected: both lines now show `25`.

- [ ] **Step 4: Commit inside the worktree (local-only, never pushed)**

Run:
```bash
git -C ../flink-wt-jdk25-phase0 add pom.xml
git -C ../flink-wt-jdk25-phase0 commit -m "[smoketest] bump java version to 25 — phase-0 throwaway"
```

---

## Task 2 (L1): Static API-risk inventory

**Files:**
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l1-api-risk.tsv`

This task runs **in parallel** with Tasks 3, 4, and 5 (L2, L3, L4). Dispatch via the Agent tool with subagent_type=Explore so it doesn't pollute main context.

- [ ] **Step 1: Dispatch the L1 Explore agent**

Send the following prompt to a new `Explore` subagent (description: "L1 JDK-25 API-risk inventory"):

```
Scan the entire Flink repo at /Users/lijunqing/Code/stczwd/flink for JDK-25-risky API usages. Produce a tab-separated file at docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l1-api-risk.tsv with columns:

category<TAB>file<TAB>line<TAB>excerpt

Categories to find (one row per match):
- unsafe          : sun.misc.Unsafe / jdk.internal.misc.Unsafe usage
- setAccessible   : .setAccessible(true) calls on Field/Method/Constructor
- privateLookup   : MethodHandles.privateLookupIn
- securityManager : SecurityManager subclass / System.setSecurityManager
- finalize        : protected void finalize() { overrides
- jaxb            : import javax.xml.bind.* (JAXB removed from JDK)
- comSun          : import com.sun.* or sun.* (excluding sun.misc.Unsafe which is in 'unsafe')
- threadStop      : Thread.stop() / Thread.suspend() / Thread.resume() calls
- illegalAccess   : --illegal-access flag references in any file (pom, scripts, docs)

Exclusions:
- Skip directories: .git, target, node_modules, .idea, build-target
- Skip generated sources: anything under target/generated-sources

Use ripgrep with -t java -t scala -t xml -t bash -t kotlin where appropriate. For each match, include just enough excerpt (max 120 chars, single line) to make the row understandable.

After producing the TSV, also write a one-line summary file inventory-l1-api-risk-summary.txt in the same directory with category counts in the form:

category<TAB>count

Do NOT modify any source file. Read-only scan.
```

- [ ] **Step 2: Verify L1 outputs exist and are non-empty**

Run:
```bash
wc -l docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l1-api-risk.tsv
cat docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l1-api-risk-summary.txt
```
Expected: TSV has >0 lines (likely thousands); summary lists per-category counts. The `unsafe` count is expected to be substantial; `securityManager` may be small but non-zero.

---

## Task 3 (L2): Build-config & dependency-version inventory

**Files:**
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l2-build-config.tsv`
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l2-deps.tsv`

This task runs **in parallel** with Tasks 2, 4, 5.

- [ ] **Step 1: Dispatch the L2 Explore agent**

Send the following prompt to a new `Explore` subagent (description: "L2 JDK-25 build-config inventory"):

```
Scan all pom.xml files in /Users/lijunqing/Code/stczwd/flink and extract two TSVs into docs/superpowers/specs/artifacts/jdk25-phase0/.

FILE 1: inventory-l2-build-config.tsv
Columns: pom_path<TAB>setting<TAB>value

For each pom.xml that diverges from root defaults, emit one row per:
- maven-compiler-plugin <source>, <target>, <release> (only if explicitly set in that POM)
- maven-surefire-plugin <argLine> entries containing --add-opens or --add-exports
- maven-failsafe-plugin same
- maven-enforcer-plugin <requireJavaVersion>
- properties: java.version, source.java.version, target.java.version, target.java.bytecode

Also emit one row per --add-opens / --add-exports flag found *anywhere* in any pom.xml or .mvn/jvm.config or scripts under tools/ (path<TAB>add-opens<TAB><module>/<package>=<targetmodule>).

FILE 2: inventory-l2-deps.tsv
Columns: dependency<TAB>current_version<TAB>used_in_pom

For each of the following dependencies, find the version pinned in any pom.xml (root or sub-module) and emit one row per (dep, version, pom):
- org.ow2.asm:asm and asm-* artifacts
- net.bytebuddy:byte-buddy
- org.javassist:javassist
- com.esotericsoftware.kryo:kryo / com.esotericsoftware:kryo / kryo-shaded
- io.netty:netty-all and netty-* core artifacts
- com.fasterxml.jackson.core:jackson-databind / jackson-core / jackson-annotations
- org.apache.calcite:calcite-core
- org.apache.pekko:pekko-* and com.typesafe.akka:akka-* (whichever is present)
- org.scala-lang:scala-library
- org.xerial.snappy:snappy-java
- org.codehaus.janino:janino
- commons-beanutils:commons-beanutils
- org.slf4j:slf4j-api
- org.apache.logging.log4j:log4j-core
- org.rocksdb:rocksdbjni and any forst-related jar coordinates

Use grep/rg over **/pom.xml. For shaded variants (e.g. flink-shaded-asm), include them with a 'shaded' suffix in the dependency column. Read-only.
```

- [ ] **Step 2: Verify L2 outputs**

Run:
```bash
wc -l docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l2-build-config.tsv \
      docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l2-deps.tsv
head -20 docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l2-deps.tsv
```
Expected: build-config TSV has >0 rows including some `--add-opens`; deps TSV includes ASM, Netty, Jackson, Pekko/Akka, Scala 2.12.20, Calcite at minimum.

---

## Task 4 (L3): Scala footprint inventory

**Files:**
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l3-scala.tsv`

This task runs **in parallel** with Tasks 2, 3, 5.

- [ ] **Step 1: Dispatch the L3 Explore agent**

Send the following prompt to a new `Explore` subagent (description: "L3 JDK-25 Scala footprint inventory"):

```
Scan /Users/lijunqing/Code/stczwd/flink for the Scala footprint. Produce docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l3-scala.tsv with columns:

module<TAB>scala_loc<TAB>scala_files<TAB>has_scala_dep<TAB>api_stability<TAB>deprecated_upstream

Method:
1. For every immediate sub-module directory containing a pom.xml, count .scala source files and total LOC under src/main/scala (and src/test/scala separately, optional column).
2. has_scala_dep = "yes" if the module's pom.xml depends on org.scala-lang:scala-library (directly or via parent), else "no".
3. api_stability = look at the module's primary public packages — if classes carry @Public or @PublicEvolving annotations, mark as "Public"; if @Internal predominates or no annotations, mark "Internal". One label per module — give the dominant one.
4. deprecated_upstream = check the module's README.md or top-level package-info.java for @Deprecated text or "deprecated" / "removed in Flink 2.x" markers; emit "yes" or "no".

Only include rows where scala_loc > 0 OR has_scala_dep = yes. Sort by scala_loc descending.

Also write inventory-l3-scala-summary.txt with totals: total Scala LOC, total Scala-bearing modules, Public-API Scala modules, Internal-only Scala modules.

Read-only scan.
```

- [ ] **Step 2: Verify L3 outputs**

Run:
```bash
cat docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l3-scala-summary.txt
head -20 docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l3-scala.tsv
```
Expected: at least the modules `flink-scala`, `flink-streaming-scala`, `flink-table/flink-table-api-scala*` appear if they still exist in this branch.

---

## Task 5 (L4): Smoke-test compile under JDK 25

**Files:**
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/compile-aggregated.log`
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/compile-error-buckets.tsv`
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/per-module-sweep.tsv`

This task runs **in parallel** with Tasks 2, 3, 4, but the *compile command itself* is the long-pole (~30–60 min). Launch the compile in the background early, then move on; bucket results once it finishes.

- [ ] **Step 1: Kick off the aggregated compile in the background (worktree from Task 1)**

Run (in background):
```bash
cd /Users/lijunqing/Code/stczwd/flink && \
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home/bin:$PATH \
../flink-wt-jdk25-phase0/mvnw -f ../flink-wt-jdk25-phase0/pom.xml clean compile -T 1C -fae --no-transfer-progress \
  > docs/superpowers/specs/artifacts/jdk25-phase0/compile-aggregated.log 2>&1
```
Use `run_in_background: true`. Capture the shell ID — you'll be notified on completion.

- [ ] **Step 2: While compile runs, prepare bucketing helper**

While step 1 runs, prepare the awk/grep one-liner that will bucket errors. No file written yet — just have it ready:

```bash
# Run AFTER compile finishes:
awk '/^\[ERROR\]/' docs/superpowers/specs/artifacts/jdk25-phase0/compile-aggregated.log | \
  awk -F: '{
    line=$0;
    if (line ~ /UnsupportedClassVersionError/) bucket="bytecode-version";
    else if (line ~ /sun\.misc\.Unsafe|jdk\.internal\.misc\.Unsafe/) bucket="unsafe-removed";
    else if (line ~ /InaccessibleObjectException|module .* does not "opens"/) bucket="sealed-package";
    else if (line ~ /scala|\.scala/) bucket="scala";
    else if (line ~ /cannot find symbol|package .* does not exist/) bucket="api-removed";
    else if (line ~ /preview|--enable-preview/) bucket="preview-feature";
    else if (line ~ /deprecation|deprecated/) bucket="deprecated-warning";
    else bucket="other";
    counts[bucket]++;
    if (!sample[bucket]) sample[bucket]=line;
  }
  END {
    for (b in counts) printf "%s\t%d\t%s\n", b, counts[b], substr(sample[b],1,160);
  }' | sort -t$'\t' -k2,2 -nr > docs/superpowers/specs/artifacts/jdk25-phase0/compile-error-buckets.tsv
```

- [ ] **Step 3: Wait for the background compile to finish**

You will be notified when the background shell exits. Inspect log size to confirm it produced output:
```bash
ls -lh docs/superpowers/specs/artifacts/jdk25-phase0/compile-aggregated.log
tail -50 docs/superpowers/specs/artifacts/jdk25-phase0/compile-aggregated.log
```
Expected: the log is non-trivial in size (likely 1–50 MB) and the tail contains either `BUILD FAILURE` (most likely) or `BUILD SUCCESS`. Either is fine — both produce data.

- [ ] **Step 4: Run the bucketing awk from Step 2**

Run the awk command from Step 2.

Verify:
```bash
cat docs/superpowers/specs/artifacts/jdk25-phase0/compile-error-buckets.tsv
```
Expected: at least one row, sorted by count descending. Rows like `scala<TAB>123<TAB><excerpt>` are expected.

- [ ] **Step 5: Per-module sweep for failed modules**

Extract the list of modules that failed in the aggregated compile, then attempt each independently with `-am` to distinguish primary failures from cascade failures. Run this script:

```bash
# Identify failed modules from the aggregated log
grep -E "BUILD FAILURE|FAILED|ERROR.* on project" docs/superpowers/specs/artifacts/jdk25-phase0/compile-aggregated.log | \
  grep -oE "on project [a-zA-Z0-9_-]+" | awk '{print $3}' | sort -u \
  > /tmp/failed-modules.txt

# For each, attempt isolated compile
> docs/superpowers/specs/artifacts/jdk25-phase0/per-module-sweep.tsv
echo -e "module\tresult\terror_signature" >> docs/superpowers/specs/artifacts/jdk25-phase0/per-module-sweep.tsv
while read mod; do
  out=$(JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home \
        ../flink-wt-jdk25-phase0/mvnw -f ../flink-wt-jdk25-phase0/pom.xml \
        -pl "$mod" -am clean compile -T 1C -q --no-transfer-progress 2>&1 | tail -20)
  if echo "$out" | grep -q "BUILD SUCCESS"; then
    echo -e "$mod\tPASS\t-" >> docs/superpowers/specs/artifacts/jdk25-phase0/per-module-sweep.tsv
  else
    sig=$(echo "$out" | grep -m1 -oE "(scala|UnsupportedClassVersion|cannot find symbol|sun\.misc\.Unsafe|InaccessibleObjectException|preview)" || echo "other")
    echo -e "$mod\tFAIL\t$sig" >> docs/superpowers/specs/artifacts/jdk25-phase0/per-module-sweep.tsv
  fi
done < /tmp/failed-modules.txt
```

This may take another 30–60 min depending on how many modules failed. Run with `run_in_background: true` and wait for notification.

Verify:
```bash
wc -l docs/superpowers/specs/artifacts/jdk25-phase0/per-module-sweep.tsv
cat docs/superpowers/specs/artifacts/jdk25-phase0/per-module-sweep.tsv
```
Expected: header row + one row per failed module. Mix of PASS/FAIL.

---

## Task 6 (L5): JDK 17→21→25 changelog cross-ref

**Files:**
- Create: `docs/superpowers/specs/artifacts/jdk25-phase0/risk-register.tsv`

This task runs **after** Tasks 2, 3, 5 complete (L1, L2, L4 outputs feed it so it can prune to actual footprint).

- [ ] **Step 1: Dispatch the L5 agent**

Send the following prompt to a new `general-purpose` subagent (description: "L5 JDK changelog cross-ref"). This is an Agent tool call (general-purpose, since it needs WebFetch + synthesis):

```
Build a risk register for the Flink JDK 25 upgrade. Inputs:

1. Read the L1 inventory at /Users/lijunqing/Code/stczwd/flink/docs/superpowers/specs/artifacts/jdk25-phase0/inventory-l1-api-risk.tsv (and -summary.txt)
2. Read the L2 inventory at .../inventory-l2-build-config.tsv and .../inventory-l2-deps.tsv
3. Read the L4 buckets at .../compile-error-buckets.tsv and .../per-module-sweep.tsv

Then cross-reference against JEP changes that landed in JDK 17, 21, and 25 that affect Flink-relevant areas:
- Module encapsulation (JEP 403 strong encapsulation, JEP 396)
- Removed APIs: SecurityManager (JEP 411), Thread.stop/suspend/resume, finalize() in JDK 18, --illegal-access removal in JDK 17
- Foreign Memory & Memory-Access API replacements for sun.misc.Unsafe (JEP 442/454, etc.)
- Virtual Threads (JEP 444 in JDK 21 GA)
- ZGC default / Generational ZGC (JEP 439)
- Pattern matching, sealed classes, records — note only if affects Flink's bytecode/reflection surface
- Class file version 69 (JDK 25) and ASM compatibility (≥ 9.7 for v69)

Use WebFetch on https://openjdk.org/jeps/<n> for any JEP you cite. Do NOT invent JEP numbers.

Output: docs/superpowers/specs/artifacts/jdk25-phase0/risk-register.tsv with columns:

risk_id<TAB>jdk_version<TAB>jep<TAB>category<TAB>description<TAB>flink_footprint<TAB>risk_level<TAB>blocking_subproject

Where:
- risk_id: R001, R002, ...
- jdk_version: 17 / 18 / 21 / 25
- jep: e.g. JEP-411 (or "-" if none)
- category: encapsulation / removed-api / unsafe / threading / gc / language / bytecode
- description: one-line explanation
- flink_footprint: count from L1/L4 evidence, or "-" if no footprint observed
- risk_level: 🔴 / 🟡 / 🟢
- blocking_subproject: A/B/C/D/E/F/G or - (sub-project IDs from the design doc §6)

Items with NO observed footprint go in a separate appendix section at the bottom of the TSV with risk_level=🟢 and a leading 'monitored<TAB>' marker. Read-only.
```

- [ ] **Step 2: Verify L5 output**

Run:
```bash
wc -l docs/superpowers/specs/artifacts/jdk25-phase0/risk-register.tsv
head -10 docs/superpowers/specs/artifacts/jdk25-phase0/risk-register.tsv
```
Expected: at least 10 rows of risks with footprint, sorted by severity.

---

## Task 7: Cross-cutting commit of all artifacts

**Files:**
- Modify: index of `docs/superpowers/specs/artifacts/jdk25-phase0/`

- [ ] **Step 1: Stage all artifacts**

Run:
```bash
git add docs/superpowers/specs/artifacts/jdk25-phase0/
git status
```
Verify: all six TSVs + the compile log + summary txts are staged. No source files outside the artifacts dir are staged.

- [ ] **Step 2: Commit**

```bash
git commit -m "[chore][docs] JDK 25 Phase-0 raw inventory artifacts (L1-L5)"
```

---

## Task 8: Consolidate into the dual-audience feasibility report

**Files:**
- Create: `docs/superpowers/specs/2026-04-28-jdk25-upgrade-phase0-feasibility-report.md`

The report **must** answer every question in the original Step-1 + Step-2.3 + Step-3 prompt sections, grounded in the artifacts from Tasks 2–6. No "TBD".

- [ ] **Step 1: Read all artifacts and draft the report**

Use Read on all six TSVs and the two summary txts. Write the report file with the following exact section structure:

```
# JDK 25 Upgrade — Phase 0 Feasibility & Inventory Report

> Date: 2026-04-28
> Branch: jdk25_performance (based on release-2.2.0)
> Audience: VP of Engineering (§1) + Engineers (§2–§6)
> Underlying artifacts: docs/superpowers/specs/artifacts/jdk25-phase0/

## 1. Executive Summary

### 1.1 Feasibility verdict
[1 paragraph: "Yes, phased over N weeks" / "Yes, but Scala migration is the gating dependency" / etc. — backed by smoke-compile pass-rate from per-module-sweep.tsv]

### 1.2 Top 5 risks
[Bulleted list, 1 sentence each, drawn from risk-register.tsv top 5 by severity]

### 1.3 Recommended sub-project ordering
[A→B→C→D→E→F→G with effort buckets S/M/L/XL per node, derived from inventory counts]

### 1.4 Scala go/no-go call
[Recommend Drop / Migrate-to-2.13 / Migrate-to-3.x with one-paragraph reasoning, citing inventory-l3-scala-summary.txt]

## 2. Measured Inventory (L1–L3)

### 2.1 API-risk surface (L1)
[Table: category | count | top-3 file paths | risk_level — pulled from inventory-l1-api-risk-summary.txt and -tsv]

### 2.2 Build-config surface (L2)
[Two tables: (a) explicit per-module Java version overrides; (b) current dependency versions vs. JDK-25 minimums]

### 2.3 Scala footprint (L3)
[Table from inventory-l3-scala.tsv top 10 modules by LOC, plus the summary totals]

## 3. Smoke-Compile Results (L4)

### 3.1 Aggregated compile outcome
[BUILD SUCCESS / FAILURE; total wall-clock time; total modules attempted; bucketed error counts table from compile-error-buckets.tsv with one representative excerpt per bucket]

### 3.2 Per-module pass/fail rate
["X of Y modules compile cleanly under JDK 25 with no source changes" — derived from per-module-sweep.tsv. Include a table of FAIL modules with their primary error signature.]

## 4. Risk Register (L5)

[Full risk-register.tsv rendered as a sortable markdown table, then the appendix of "monitored, no observed footprint" items]

## 5. Sub-Project Decomposition

### 5.1 DAG
[Sub-projects A–G as defined in the design doc §6, with measured effort buckets per node]

### 5.2 Phase-1 candidate
[Name the recommended Phase-1 sub-project. One paragraph: "what its brainstorm should cover" — explicit list of decisions Phase 1 must make.]

## 6. Reproduction Guide

### 6.1 Environment
- JDK 25: Zulu 25.0.3 LTS at /Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
- Maven: bundled mvnw (verify with ./mvnw --version)
- OS: macOS arm64 (Darwin 25.4.0)

### 6.2 Steps
1. git checkout jdk25_performance
2. git worktree add ../flink-wt-jdk25-phase0 -b jdk25-phase0-smoketest jdk25_performance
3. In that worktree, edit pom.xml: source.java.version=25, target.java.version=25
4. JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home ../flink-wt-jdk25-phase0/mvnw clean compile -T 1C -fae --no-transfer-progress > compile.log 2>&1
5. Apply the awk bucketer in plan Task 5 step 2 to compile.log
6. Per-module sweep per plan Task 5 step 5
7. After review, git worktree remove ../flink-wt-jdk25-phase0 --force; git branch -D jdk25-phase0-smoketest

### 6.3 Known gotchas
[Any encountered during this run — e.g., specific --add-opens that needed surefire injection, modules that hung, etc.]
```

- [ ] **Step 2: Self-review the report**

Re-read the report and check:
- Every number in §1–§4 is traceable to a row in one of the artifact TSVs.
- §1.4 (Scala call) is concrete: not "consider migrating" — pick one and defend it.
- §5.2 names the Phase-1 sub-project and lists the decisions its brainstorm must make.
- No "TBD", no "investigate further", no "depends on…" without a concrete dependency.

Fix any issue inline.

- [ ] **Step 3: Commit the report**

```bash
git add docs/superpowers/specs/2026-04-28-jdk25-upgrade-phase0-feasibility-report.md
git commit -m "[docs] JDK 25 Phase-0 feasibility & inventory report"
```

---

## Task 9: Tear down the smoketest worktree

The worktree was throwaway. Only the report and artifacts on `jdk25_performance` should survive.

- [ ] **Step 1: Confirm the worktree branch is not pushed and has no other dependents**

Run:
```bash
git -C ../flink-wt-jdk25-phase0 status
git branch --list jdk25-phase0-smoketest
git log --oneline jdk25-phase0-smoketest -5
```
Expected: branch only contains the one local "[smoketest] bump java version to 25" commit.

- [ ] **Step 2: Remove the worktree and delete the local branch**

Run:
```bash
git worktree remove ../flink-wt-jdk25-phase0 --force
git branch -D jdk25-phase0-smoketest
```
Expected: `Worktree ... removed` and `Deleted branch jdk25-phase0-smoketest`.

- [ ] **Step 3: Verify nothing leaked onto jdk25_performance**

Run:
```bash
grep -E "source.java.version|target.java.version" pom.xml | head -5
git status
```
Expected: root pom.xml still has `source.java.version=11` and `target.java.version=17`. Working tree clean (modulo unrelated untracked files we agreed to leave alone).

---

## Task 10: Final sanity check

- [ ] **Step 1: Verify all deliverables are committed**

Run:
```bash
git log --oneline jdk25_performance | head -5
ls docs/superpowers/specs/artifacts/jdk25-phase0/
ls docs/superpowers/specs/*.md
```
Expected:
- Recent commits include "[chore][docs] Add JDK 25 Phase-0 feasibility design and plan scaffolding", "[chore][docs] JDK 25 Phase-0 raw inventory artifacts", "[docs] JDK 25 Phase-0 feasibility & inventory report"
- Artifacts dir has: `compile-aggregated.log`, `compile-error-buckets.tsv`, `per-module-sweep.tsv`, `inventory-l1-api-risk.tsv`, `inventory-l1-api-risk-summary.txt`, `inventory-l2-build-config.tsv`, `inventory-l2-deps.tsv`, `inventory-l3-scala.tsv`, `inventory-l3-scala-summary.txt`, `risk-register.tsv`, `.gitkeep`
- Specs dir has: design doc + report

- [ ] **Step 2: Surface the Phase-1 candidate to the user**

Print the §5.2 paragraph from the report so the user can decide whether to immediately brainstorm Phase 1 or pause. Phase 0 is complete — Phase 1 is its own brainstorm.

---

## Parallelism note

Tasks 2 (L1), 3 (L2), 4 (L3), and the *kick-off* of Task 5 (L4 step 1) can launch in a single message with parallel tool calls. Task 5 step 5 (per-module sweep) depends on Task 5 step 4 (bucketing). Task 6 (L5) depends on Tasks 2, 3, 5 outputs being on disk. Task 7 (commit) depends on all of 2–6. Task 8 (report) depends on Task 7.

A reasonable execution timeline on this machine:
- T+0: launch Tasks 1, 2, 3, 4, 5-step1 in parallel.
- T+5min: Tasks 2, 3, 4 likely done (Explore agents are fast); Task 5 still compiling.
- T+45min: Task 5-step1 finishes. Run 5-step4 bucketing.
- T+45min: launch Task 5-step5 (per-module sweep, in background).
- T+45min: launch Task 6 (L5) in parallel with Task 5-step5.
- T+90min: Task 5-step5 and Task 6 both done.
- T+90min: Tasks 7, 8, 9, 10 sequentially. Total wall-clock ≈ 2 hours.

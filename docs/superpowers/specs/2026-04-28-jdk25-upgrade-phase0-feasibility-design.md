# Phase 0 — JDK 25 Upgrade Feasibility & Inventory (Design)

- **Date**: 2026-04-28
- **Branch**: `jdk25_performance` (based on `release-2.2.0`; no JDK-25 work yet)
- **Author**: brainstorm session
- **Status**: design — awaiting user review before plan

---

## 1. Background & motivation

The user requested a "full upgrade" of the Flink fork to JDK 25 — meaning: zero compile errors, all non-Docker UTs green, and no runtime semantic regressions on `jdk25_performance`. Initial repo inspection showed:

- The branch `jdk25_performance` currently has **zero unique commits** over master and no working-tree changes — the prompt's claim that performance optimizations are already applied does not match the actual repo state.
- Root POM uses `source.java.version=11`, `target.java.version=17`, with profile scaffolding for source/target 11, 17, 21, **and 25** already present (someone primed the build for this work).
- `scala.version=2.12.20`, `scala.binary.version=2.12` — Scala 2.12 has no JDK-25-compatible release at any patch level. This is a structural blocker, not a late-stage cleanup.
- JDK 25 (Zulu 25.0.3 LTS) is installed locally and is the system default.

A single brainstorm → spec → implementation cycle covering the entire upgrade would either be too vague to act on or too long for anyone to read. The work has been decomposed into seven sub-projects (A–G in §6); this spec covers only **Phase 0**, a read-only feasibility & inventory pass that produces measured data so each later sub-project can be planned with real numbers.

## 2. Goal

Produce **one dual-audience markdown report** that:

- Tells the VP of Engineering whether the JDK 25 upgrade is feasible, what it will cost, and in what order the sub-projects should run.
- Gives engineers measured per-module data (Unsafe sites, reflection sites, current bytecode-lib versions, Scala module footprint, surefire `--add-opens` matrix, smoke-compile error buckets) so they can size each sub-project without re-doing the discovery work.

Every claim in the report must be backed by either a repo measurement or a real `mvn clean compile` attempt under JDK 25 — no "TBD", no extrapolation from theoretical breaking-change checklists.

## 3. Non-goals (what Phase 0 explicitly does NOT do)

- No edits to `pom.xml` or any source file on `jdk25_performance`. All probing happens in a throwaway worktree.
- No test execution. Compile-only.
- No fixes proposed at the patch level. The report ranks and orders sub-projects; each subsequent sub-project gets its own brainstorm.
- No touching the existing `task_plan.md` / `findings.md` / `progress.md` / `forst-backport-feasibility-report.md` files (March 2026 ForSt task — unrelated, untracked, leave alone).

## 4. Architecture — 5 parallel investigation lanes

L1, L2, L3, L5 launch in parallel as Explore-style agents. L4 launches in parallel too but takes the longest (≈30–60 min compile + bucketing). Consolidation into the report happens after all five report back. Lanes do not share state; they only feed the writeup.

| Lane | Output | Mechanism |
|---|---|---|
| **L1: Static API-risk inventory** | Counts + file:line lists for every high-risk pattern: `sun.misc.Unsafe`, `setAccessible(true)`, `MethodHandles.privateLookupIn`, `SecurityManager` / `System.setSecurityManager`, `finalize()` overrides, `javax.xml.bind.*`, `com.sun.*` / `sun.*` imports, `Thread.stop` / `suspend` / `resume`, `--illegal-access` references | `Explore` agent driving `rg` |
| **L2: Build-config inventory** | All `<source>`, `<target>`, `<release>`, `<argLine>` `--add-opens` and `--add-exports` lines across root + every sub-module POM. Current versions of: ASM (and shaded variants), ByteBuddy, Javassist, Kryo / kryo-shaded, Netty, Jackson, Calcite, Pekko/Akka, Snappy, RocksDB, ForSt, Janino, Beanutils. | `Explore` agent driving `grep` over `**/pom.xml` |
| **L3: Scala footprint** | Every module containing `.scala` sources or a `scala-library` dependency; LOC per module; whether each module is `@Public`/`@PublicEvolving` or `@Internal`; whether the module is already deprecated upstream | `Explore` agent driving `find` + `wc` + grep on annotations |
| **L4: Smoke-test compile under JDK 25** | A `compile.log` (kept under `docs/superpowers/specs/artifacts/`) and a bucketed summary table classifying the first ~200 distinct error signatures into: Scala / bytecode-version / API-removed / sealed-package access / sun.misc.Unsafe / other. Plus per-module pass-fail count from a follow-up `-pl` sweep on the modules that fail in the aggregated build (so we get a "% of modules that compile cleanly under JDK 25 with no fixes" number). | `EnterWorktree` + `Bash` (compile takes ≈30–60 min) |
| **L5: JDK 17→21→25 changelog cross-ref** | Risk-tagged breaking-change table that the prompt's Step 1 calls for, but **pruned to only changes whose footprint actually shows up in L1+L2+L4**. Items with no repo footprint are listed in an appendix as "monitored, not blocking." | `WebFetch` against JEP pages + agent synthesis |

## 5. Smoke-compile methodology (L4 in detail)

This is the biggest single piece of new evidence the report introduces, so its method is pinned down here.

1. Create sibling worktree: `git worktree add ../flink-wt-jdk25-phase0 -b jdk25-phase0-smoketest jdk25_performance`
2. In the worktree, edit *only* root `pom.xml`: set `<target.java.version>25</target.java.version>` (and `<source.java.version>25</source.java.version>` if needed for `--release` consistency). Commit locally to the smoketest branch only — never push, never merge.
3. Set `JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home`, confirm `java -version` shows 25.
4. Run aggregated compile, capture log:
   ```bash
   ./mvnw clean compile -T 1C -fae --no-transfer-progress \
     2>&1 | tee compile-aggregated.log
   ```
   `-fae` so we get all module failures in one pass, not just the first.
5. Bucket errors by signature: extract `[ERROR]` lines from `compile-aggregated.log`, group by error-type prefix and classifier (Scala vs Java, error code, missing class), produce the summary table.
6. Per-module sweep: for each module that failed in step 4, attempt `./mvnw -pl <module> -am clean compile` to distinguish "module fails because of itself" from "module fails because its upstream failed." This produces the "% modules clean under JDK 25 with no fixes" number.
7. Archive `compile-aggregated.log` and the per-module sweep summary under `docs/superpowers/specs/artifacts/jdk25-phase0/`. The report references and quotes from them; the raw logs go into the artifacts dir on `jdk25_performance` (committed, since they are evidence).
8. Tear down worktree at end of Phase 0; only the report file and artifacts dir land on `jdk25_performance`.

## 6. Sub-project decomposition (the DAG the report will refine)

Phase 0's output names and orders these. Phase 0 itself does not execute any of them.

```
A. POM/toolchain bump (root + per-module overrides)
        |
        v
D. Bytecode-lib bump (ASM / ByteBuddy / Javassist / Kryo) ----+
                                                              |
B. Scala 2.12 strategy (drop modules vs. 2.13 vs. 3.x) -------+
                                                              |
                                                              v
C. sun.misc.Unsafe -> MemorySegment / Foreign Memory API in flink-core
                                                              |
                                                              v
E. Reflection / setAccessible -> MethodHandles + --add-opens audit
                                                              |
                                                              v
F. Per-module compile fixes + UT triage (parallel, many engineers)
                                                              |
                                                              v
G. Runtime validation (default GC change, virtual threads, Pekko)
```

The report will replace these arrows with measured effort buckets (S/M/L/XL) per node and identify the Phase-1 candidate (likely **A** if smoke-compile cleanly isolates Scala and Unsafe failures; otherwise **B** or a "B + A combined" if Scala issues block too much).

## 7. Deliverables

**Primary**: `docs/superpowers/specs/2026-04-28-jdk25-upgrade-phase0-feasibility-report.md`

Sections:
1. Executive summary (≤1 page, for VP) — feasibility verdict, top-5 risks, recommended sub-project ordering with effort buckets, Scala go/no-go call.
2. Measured inventory (for engineers) — actual numbers from L1/L2/L3 with file paths.
3. Smoke-compile results (for engineers) — bucketed error counts from L4 with representative excerpts; per-module pass/fail rate.
4. Risk register — L5's table, filtered to applicable items only; appendix lists monitored-but-not-blocking items.
5. Sub-project decomposition — DAG of A–G with dependency arrows, effort estimate per node, Phase-1 candidate identified with one-paragraph "what its brainstorm should cover" stub.
6. Reproduction guide — exact commands to recreate the smoke compile from a fresh checkout.

**Secondary**: `docs/superpowers/specs/artifacts/jdk25-phase0/`
- `compile-aggregated.log` — raw output from step 4
- `per-module-sweep.tsv` — table of `module → result` from step 6
- `inventory-l1-api-risk.tsv`, `inventory-l2-build-config.tsv`, `inventory-l3-scala.tsv` — raw lane outputs

Both the report and the artifacts dir are committed to `jdk25_performance` at the end of Phase 0. The smoketest worktree is removed; no JDK-25 source/POM changes land on `jdk25_performance`.

## 8. Success criteria

Phase 0 is "done" when:

- The report file exists, is committed to `jdk25_performance`, and answers every question in the original prompt's Step 1 + Step 2.3 with measured data.
- The smoke-compile log is archived and referenced from the report.
- Phase 1 is named, scoped, and has a one-paragraph "what its brainstorm should cover" stub embedded in §5 of the report.
- The smoketest worktree has been removed and no JDK-25 changes have leaked onto `jdk25_performance`.

## 9. Risks to Phase 0 itself

| Risk | Likelihood | Mitigation |
|---|---|---|
| Aggregated compile fails so early (e.g., Scala) that the bucketed-error summary is dominated by one root cause and we learn little | Medium | Per-module sweep in step 6 explicitly disentangles cascade failures from real failures |
| Smoke-compile takes longer than 60 min on this machine and times out individual command runs | Low | Run in `run_in_background` mode and poll log size; no single command needs to block |
| `--release 25` rejects sources targeting Java 11 internal APIs the report should still surface | Low | Compile-only without `--release` first; only add `--release` if errors are unclear |
| L5's JDK changelog work duplicates what L1/L2/L4 already measured | Medium | L5 runs *after* L1/L2/L4 finish so it can prune to footprint actually present |

## 10. Out of scope (deferred to later phases)

- Editing any POM or source file on `jdk25_performance`.
- Running any test (UT, IT, E2E).
- Choosing the Scala target version (2.13 vs 3.x) — Phase 0 only documents the size of the Scala footprint and presents the trade-offs; the decision belongs to sub-project B's brainstorm.
- Touching `task_plan.md` / `findings.md` / `progress.md` / `forst-backport-feasibility-report.md`.
- Anything related to Flink Kubernetes Operator (out of repo scope for this fork).

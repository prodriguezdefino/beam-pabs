<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Code-quality tooling for the Rust SDK

Two kinds of measurement:

- **Test quality**: how much each test adds. Use it to find redundant tests to
  merge or remove, and weak spots: code that tests run but do not check. It
  combines two independent signals:
  1. **Per-test coverage overlap**: which production lines each test executes.
  2. **Mutation testing**: which tests fail when a tool changes the production code.
- **Size and complexity**: a scorecard that compares the Rust, Go and Python SDKs
  by code size, cyclomatic complexity and test volume. Feature counts normalize
  the sizes (see [Complexity scorecard](#complexity-scorecard)).

The Gradle tasks in [`gradle/quality.gradle`](../../gradle/quality.gradle) are
the supported entry points; the commands are listed in
[docs/development.md](../../docs/development.md#code-quality).

## Where things run: collection = container only; analysis = anywhere

| Step | Script | Runs |
|---|---|---|
| Per-test coverage collection | `per_test_cov.sh` | **Only** in the `beam_rust_builder` container |
| Mutation runs | `cargo mutants`, through `run_mutants.sh` | **Only** in the `beam_rust_builder` container |
| Analysis | `analyze_overlap.py`, `kill_matrix.py`, `candidates.py` | Anywhere with Python 3.9+ (stdlib only) |
| Scorecard | `complexity_scorecard.py` | Anywhere with Python 3.9+, `scc` and `lizard` (the builder image pins both) |

Collection needs a pinned toolchain, `llvm-tools`, `cargo-llvm-cov`,
`cargo-nextest`, `cargo-mutants`, `mold`, GNU coreutils and a Linux Prism. The
builder image ([`container/Dockerfile.builder`](../../container/Dockerfile.builder))
has all of them. The image sets `BEAM_RUST_BUILDER=1`. `per_test_cov.sh` and
`run_mutants.sh` do not run without it (`builder_guard.sh`). On a Linux host that
has the same tools (for example a CI machine), `ALLOW_OUTSIDE_BUILDER=1` turns off
the check. There is no macOS host mode.

The Gradle tasks run the analysis scripts in the same container invocation as
the collection, so a task does not depend on the host's Python. To try a
different focus or test filter, run the task again with `-Pfocus` or
`-PtestFileRegex`. `analyze_overlap.py` maps the lcov paths (`/workspace/...`
inside the container) back to source files to drop `#[cfg(test)]` code.

## Files

| File | Purpose |
|---|---|
| `per_test_cov.sh` | Builds a crate's tests with coverage instrumentation, then runs **each test alone** (`<binary> --exact <test> --test-threads=1`) with its own `LLVM_PROFILE_FILE`; writes `tests.tsv` (status, wall time) and one lcov file per test, restricted to the crate's `src/`. |
| `analyze_overlap.py` | Over production lines only: union coverage, hits per line, identical-coverage groups, tests with zero unique lines, strict subsumption, greedy minimal cover, per test file and per source file tables. Writes `redundancy_<focus|all>.tsv`. |
| `kill_matrix.py` | Parses a cargo-mutants run made with `--test-tool nextest --cargo-test-arg=--no-fail-fast` into a test x mutant matrix: mutation score, killers per mutant, sole killers, greedy minimal killing set, missed mutants. |
| `candidates.py` | Joins the two: classifies each in-scope test as keep / keep (sole killer) / REMOVE/MERGE candidate / mutation-redundant only / review. |
| `complexity_scorecard.py` | Cross-SDK size and complexity scorecard (Markdown and JSON); also production vs test line counts per area and per SDK tree, with a per-crate Rust breakdown. |
| `run_coverage.sh`, `run_mutants.sh`, `run_report.sh` | What the Gradle tasks run inside the container: collection plus analysis, cargo-mutants exit-code handling plus `kill_matrix.py`, and `candidates.py` with its summary saved. |
| `builder_guard.sh` | The container-only check sourced by the collection scripts. |

## Method

### Coverage overlap

Each test runs in its own process with its own profile, so a test gets a line
only if that test executed it. From the per-test line sets:

- **unique lines**: lines that no other test executes. You can delete a test with
  zero unique lines and keep the line coverage. This holds for *one* test at a
  time: deleting two such tests together can lose lines.
- **identical groups / strict subsumption**: tests whose line set equals, or is
  a strict subset of, the line set of one other test.
- **greedy minimal cover**: a small set of tests that gets the same union
  coverage. It shows how concentrated the suite is. It is not a recommendation
  to keep only those tests.

### Mutation testing

`cargo-mutants` makes small changes to production code: it replaces a function
body with `Default::default()`, changes `<` to `<=`, deletes a `!`, and so on. It
runs the tests against each change. A mutant that no test catches is *missed*: the
code runs, but no test checks its behaviour. With `--no-fail-fast` and nextest,
every test runs against every mutant, and the log names each failing test. This
turns "caught or not" into a **kill matrix**.

### Why both are needed

Coverage says what a test *executes*; mutation says what it *checks*.

- Coverage alone gives false positives for removal. A test can execute only
  lines that other tests also execute, and be the only test that checks a
  result.
- Mutation alone misses tests that check code outside the mutated files. It
  also takes much time on a whole crate.

A test is a **REMOVE/MERGE candidate** only if all of these hold:

1. it covers **0 unique** production lines;
2. it is **not the sole killer** of any mutant;
3. its set of killed mutants is **equal to or a strict subset of** the set of
   another in-scope test.

The report marks a test that kills **no** mutant as **review**, never as a
candidate. It probably checks code that the run did not mutate.

A person reviews the candidates. The usual result merges several of them into
one table-driven (`rstest`) test, instead of deleting them.

## Caveats

- **Crate-local coverage.** `coveragePerTest` runs the tests of one crate.
  Tests in other crates (harness, runners, IO) also run its code. The true
  coverage is higher, and other crates test some "uncovered" code.
- **Line coverage only.** Branch coverage needs a nightly toolchain.
- **Equivalent mutants.** Some mutants do not change behaviour:
  a zero-sized `Default`, `Ok(())` for a unit type, a preallocation size,
  `|` vs `^` on disjoint bits. No test can catch them. They lower the score but do
  not show a gap. Review missed mutants before you write tests for them.
- **Mutants only of mutated files.** Kill sets, sole-killer status and the
  candidate rule apply only to the mutated files (`-Pfiles`). A test
  that looks redundant for those files may be the only check on other code.
  Scope `--test-file-regex` to the tests that target the mutated code, and mutate
  everything those tests target before removing any of them.
- **Join key.** Tests are named `<test binary>::<test>` in both analyses
  (library unit tests: `<crate_with_underscores>::<module path>`).
- **Flaky or slow tests** show up as `fail` in `tests.tsv` or as `Timeout`
  mutants. Timeouts count as caught. [`.config/nextest.toml`](../../.config/nextest.toml)
  stops a test of `apache-beam-core`, `apache-beam-harness` or
  `apache-beam-test-utils` after 10 s. A mutant that deadlocks one of these
  tests fails that test and counts as caught.

## Complexity scorecard

`complexity_scorecard.py` compares the Rust, Go and Python SDKs area by area
(engine, platform, io, ml, testing). It counts only production code: it leaves
out tests, generated code and local or legacy runners. It uses two tools:

- [`scc`](https://github.com/boyter/scc): code lines and branch-based complexity;
- [`lizard`](https://github.com/terryyin/lizard): per-function cyclomatic
  complexity (CCN): average, p90, functions over 15, function length, parameters.

The scorecard divides sizes by **feature points** from
[docs/sdk-parity.md](../../docs/sdk-parity.md) (supported = 1, partial = 0.5).
An SDK with more features does not get a worse score. The scorecard also counts
test code (non-blank, non-comment lines, no tools needed), per area with a
test/prod ratio and per SDK tree:

- Rust: `#[cfg(test)]` items inside sources, plus `tests/`, `benches/`,
  `runner-tests/`, `test-utils/` (and `testing/` crates in the tree totals);
- Go: `*_test.go`;
- Python: `*_test.py`, `test_*.py`, `tests/` (and `testing/` in the tree totals).

Java and TypeScript have no per-area mapping. They appear only in the whole-tree
test/prod totals, with `--with-java-ts`.

Options: `--repo-root`, `--out`, `--format md|json`,
`--json-out` (JSON in addition to the Markdown),
`--list-areas` (print the area to path mapping and exit; needs neither tool), `--with-java-ts`.

The builder image pins `scc` and `lizard` (with SHA-256 checks), so
`./gradlew :sdks:rust:complexityScorecard` gives the same numbers on any
machine.

## Which task runs which script

| Gradle task | Scripts |
|---|---|
| `coveragePerTest` | `run_coverage.sh` → `per_test_cov.sh`, `analyze_overlap.py` |
| `mutants` | `run_mutants.sh` → `cargo mutants`, `kill_matrix.py` |
| `testQualityReport` | `run_report.sh` → `candidates.py` |
| `complexityScorecard` | `complexity_scorecard.py` |
| `machete` | `cargo machete` (unused `Cargo.toml` dependencies) |

```bash
./gradlew :sdks:rust:coveragePerTest -Ppkg=apache-beam-core -Pfocus=/coders/
./gradlew :sdks:rust:mutants -Ppkg=apache-beam-core -Pfiles='beam/core/src/coders/**'
./gradlew :sdks:rust:testQualityReport -Ppkg=apache-beam-core -PtestFileRegex='^(coder_|pane_info)'
./gradlew :sdks:rust:complexityScorecard
./gradlew :sdks:rust:machete
```

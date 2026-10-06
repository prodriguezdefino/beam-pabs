#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Test x mutant kill matrix from a cargo-mutants run.

Needs ``cargo mutants --test-tool nextest --cargo-test-arg=--no-fail-fast`` so
every test runs against every mutant and each killing test logs a ``FAIL``
line. Reports the mutation score, killers per mutant, sole killers, a greedy
minimal killing set, top killers and missed mutants. ``--redundancy`` flags
coverage-redundant tests that are sole killers; ``--tsv`` writes one row per
mutant.

Test names match ``per_test_cov.sh`` (``<test binary>::<test name>``); library
unit tests are ``<crate_with_underscores>::<path>``.

Usage::

    python3 kill_matrix.py build/reports/test-quality/apache-beam-core/mutants \\
        [--redundancy .../coverage/redundancy_all.tsv] [--tsv kill_matrix.tsv]
"""

import argparse
import collections
import json
import os
import re
import sys

# One nextest result line, e.g.
#   FAIL [   0.009s] ( 25/441) apache-beam-core::co_group_by_key_test cogbk_groups
FAIL = re.compile(
    r"^\s*(?:FAIL|TIMEOUT|SIGSEGV|SIGABRT)\s+\[.*?\]\s+(?:\(\s*\d+/\d+\)\s+)?"
    r"\S+?::(\S+)\s+(\S+)")
# The same line for a library unit test: `<crate> <module::path::test>`.
FAIL_LIB = re.compile(
    r"^\s*(?:FAIL|TIMEOUT|SIGSEGV|SIGABRT)\s+\[.*?\]\s+(?:\(\s*\d+/\d+\)\s+)?"
    r"([^\s:]+)\s+(\S+)\s*$")

CAUGHT = ("CaughtMutant", "Timeout")


def mutants_out_dir(path):
    """Accepts either a ``mutants.out`` directory or its parent."""
    sub = os.path.join(path, "mutants.out")
    return sub if os.path.isdir(sub) else path


def describe(scenario):
    """Human-readable ``file:line: function :: replacement``; None for baseline."""
    m = scenario.get("Mutant") if isinstance(scenario, dict) else None
    if not m:
        return None
    span = m.get("span", {}).get("start", {})
    fn = m.get("function")
    fn_name = fn.get("function_name", "") if isinstance(fn, dict) else ""
    return (f"{m.get('file')}:{span.get('line')}: {fn_name} :: "
            f"{m.get('replacement', m.get('genre', ''))}")


def killing_tests(log_path, lib_tests=False):
    """Names of the tests that failed in one cargo-mutants log."""
    tests = set()
    with open(log_path, errors="ignore") as fh:
        for line in fh:
            m = FAIL.match(line)
            if m:
                tests.add(f"{m.group(1)}::{m.group(2)}")
                continue
            if lib_tests:
                m = FAIL_LIB.match(line)
                if m:
                    tests.add(f"{m.group(1).replace('-', '_')}::{m.group(2)}")
    return tests


def load_outcomes(path, label=None, lib_tests=False):
    """Parses one cargo-mutants output directory.

    Returns a list of dicts with keys ``key`` (unique per mutant, prefixed by
    ``label``), ``desc``, ``summary`` (cargo-mutants outcome) and ``tests``
    (killing tests; empty unless caught or timed out).
    """
    d = mutants_out_dir(path)
    label = label or os.path.basename(os.path.dirname(os.path.abspath(d))) or d
    with open(os.path.join(d, "outcomes.json")) as fh:
        oc = json.load(fh)
    rows = []
    for o in oc["outcomes"]:
        desc = describe(o["scenario"])
        if desc is None:
            continue
        tests = set()
        log = o.get("log_path") or ""
        if o["summary"] in CAUGHT:
            lp = log if os.path.isabs(log) else os.path.join(d, log)
            if not os.path.exists(lp):
                lp = os.path.join(d, "log", os.path.basename(log))
            tests = killing_tests(lp, lib_tests)
        rows.append({
            "key": f"{label}:{os.path.basename(log) or desc}",
            "desc": desc,
            "summary": o["summary"],
            "tests": tests,
        })
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mutants_dir", help="cargo-mutants -o directory or its mutants.out")
    ap.add_argument("--redundancy", help="redundancy_*.tsv from analyze_overlap.py")
    ap.add_argument("--tsv", help="write the kill matrix (one row per mutant) here")
    ap.add_argument("--lib-tests", action=argparse.BooleanOptionalAction, default=True,
                    help="attribute kills to library unit tests too (default: on)")
    ap.add_argument("--top", type=int, default=10, help="top killers to list")
    args = ap.parse_args(argv)

    d = mutants_out_dir(args.mutants_dir)
    rows = load_outcomes(d, lib_tests=args.lib_tests)
    summary = collections.Counter(r["summary"] for r in rows)
    kills = {r["key"]: r["tests"] for r in rows if r["summary"] in CAUGHT}
    desc = {r["key"]: r["desc"] for r in rows}
    missed = [r["desc"] for r in rows if r["summary"] == "MissedMutant"]

    viable = summary["CaughtMutant"] + summary["MissedMutant"] + summary["Timeout"]
    print(f"# Mutation report: {d}\n")
    print("outcomes:", dict(summary))
    print(f"mutation score: "
          f"{100 * (summary['CaughtMutant'] + summary['Timeout']) / max(viable, 1):.1f}%"
          f" of {viable} viable mutants\n")

    killers = collections.Counter(t for ts in kills.values() for t in ts)
    sole = collections.Counter(next(iter(ts)) for ts in kills.values() if len(ts) == 1)
    dist = collections.Counter(min(len(ts), 50) for ts in kills.values())
    print("mutants killed by N tests:", {k: dist[k] for k in sorted(dist)})
    if dist.get(0):
        print(f"  ({dist[0]} caught mutants have no attributed test: build "
              "failures, or timeouts without a FAIL line)")
    print(f"tests that kill >=1 mutant: {len(killers)}; tests that are the SOLE "
          f"killer of some mutant: {len(sole)}")

    # Greedy minimal killing set.
    remaining, chosen = set(kills), []
    tk = collections.defaultdict(set)
    for m, ts in kills.items():
        for t in ts:
            tk[t].add(m)
    while remaining:
        best = max(tk, key=lambda t: len(tk[t] & remaining), default=None)
        if best is None or not (tk[best] & remaining):
            break
        chosen.append(best)
        remaining -= tk[best]
    print(f"greedy minimal test set that kills every caught mutant: "
          f"{len(chosen)} tests\n")

    print("top killers:")
    for t, n in killers.most_common(args.top):
        print(f"  {n:>4}  {t}")

    print(f"\nmissed mutants ({len(missed)}):")
    for m in sorted(missed):
        print("  " + m)

    if args.redundancy:
        # Are 'coverage-redundant' tests also mutation-redundant?
        with open(args.redundancy) as fh:
            red = [r.split("\t") for r in fh.read().splitlines()[1:]]
        cov_redundant = {r[0] for r in red if r[2] == "0"}
        print("\ncoverage-redundant tests (0 unique lines) that are nevertheless "
              "SOLE killers of a mutant:")
        for t in sorted(set(sole) & cov_redundant):
            print(f"  {t}  (sole killer of {sole[t]})")

    if args.tsv:
        with open(args.tsv, "w") as fh:
            fh.write("mutant\toutcome\tkillers\ttests\n")
            for r in rows:
                fh.write(f"{r['desc']}\t{r['summary']}\t{len(r['tests'])}\t"
                         f"{','.join(sorted(r['tests']))}\n")
        print(f"\nkill matrix written to {args.tsv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

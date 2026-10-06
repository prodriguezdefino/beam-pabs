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
"""Cross-reference coverage redundancy with mutation kills -> remove/merge candidates.

Inputs are a ``redundancy_*.tsv`` from ``analyze_overlap.py`` and cargo-mutants
output directories run with ``--test-tool nextest --cargo-test-arg=--no-fail-fast``.
Only tests in the TSV are classified; ``--test-file-regex`` limits both the
classified tests and the tests that may subsume them.

Categories, applied in this order:

``review (kills no mutant)``
    May check unmutated code, so never a candidate.
``keep (sole killer)``
    The only test catching at least one mutant.
``REMOVE/MERGE candidate``
    0 unique covered lines, not a sole killer, and its killed set equals or is
    a strict subset of another in-scope test's.
``mutation-redundant only``
    Killed set is subsumed, but it covers unique lines.
``keep``
    Everything else.

Usage::

    python3 candidates.py --redundancy coverage/redundancy_all.tsv \\
        --mutants mutants_pane --mutants mutants_coders \\
        --test-file-regex '^(coder_|pane_info)' --out candidates.tsv
"""

import argparse
import collections
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kill_matrix import CAUGHT, load_outcomes  # noqa: E402

REVIEW = "review (kills no mutant)"
SOLE = "keep (sole killer)"
CANDIDATE = "REMOVE/MERGE candidate"
MUTATION_ONLY = "mutation-redundant only"
KEEP = "keep"


def classify(cov, sig, in_scope):
    """Returns rows (category, test, lines, unique, killed, sole, other, seconds).

    ``cov`` maps test -> redundancy TSV row, ``sig`` maps test -> set of
    killed mutant keys, ``in_scope`` is a predicate on test names.
    """
    killer_count = collections.Counter(m for s in sig.values() for m in s)
    out = []
    for t in sorted(x for x in cov if in_scope(x)):
        r = cov[t]
        cov_red = r[2] == "0"
        s = sig.get(t, set())
        sole = sum(1 for m in s if killer_count[m] == 1)
        subsumer = next((u for u in sig
                         if u != t and s and s < sig[u] and in_scope(u)), None)
        dup = next((u for u in sig
                    if u != t and s and s == sig[u] and in_scope(u)), None)
        if not s:
            cat = REVIEW
        elif sole:
            cat = SOLE
        elif cov_red and (dup or subsumer):
            cat = CANDIDATE
        elif dup or subsumer:
            cat = MUTATION_ONLY
        else:
            cat = KEEP
        seconds = r[6] if len(r) > 6 else ""
        out.append((cat, t, r[1], r[2], len(s), sole, dup or subsumer or "", seconds))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--redundancy", required=True,
                    help="redundancy_*.tsv from analyze_overlap.py")
    ap.add_argument("--mutants", action="append", required=True, metavar="DIR",
                    help="cargo-mutants output directory (repeatable)")
    ap.add_argument("--test-file-regex",
                    help="only consider tests whose '<binary>::<test>' name matches "
                    "(re.match), e.g. '^(coder_|pane_info)'")
    ap.add_argument("--lib-tests", action=argparse.BooleanOptionalAction, default=True,
                    help="attribute kills to library unit tests too (default: on)")
    ap.add_argument("--out", help="output TSV (default: candidates.tsv next to "
                    "the redundancy TSV)")
    args = ap.parse_args(argv)

    scope_re = re.compile(args.test_file_regex) if args.test_file_regex else None

    def in_scope(t):
        return scope_re is None or bool(scope_re.match(t))

    sig = collections.defaultdict(set)
    n_caught = 0
    for i, d in enumerate(args.mutants):
        label = f"{i}:{os.path.basename(os.path.normpath(d))}"
        for row in load_outcomes(d, label=label, lib_tests=args.lib_tests):
            if row["summary"] not in CAUGHT:
                continue
            n_caught += 1
            for t in row["tests"]:
                sig[t].add(row["key"])

    with open(args.redundancy) as fh:
        rows = [r.split("\t") for r in fh.read().splitlines()[1:]]
    cov = {r[0]: r for r in rows}

    out = classify(cov, sig, in_scope)
    cats = collections.Counter(c for c, *_ in out)

    print(f"# Test removal/merge candidates\n")
    print(f"redundancy: {args.redundancy}")
    print(f"mutant runs: {', '.join(args.mutants)} ({n_caught} caught mutants)")
    print(f"scope: {args.test_file_regex or 'all tests in the redundancy TSV'}\n")
    print(f"in-scope tests: {len(out)}")
    for c in (KEEP, SOLE, CANDIDATE, MUTATION_ONLY, REVIEW):
        print(f"  {c:28s} {cats[c]}")

    path = args.out or os.path.join(
        os.path.dirname(os.path.abspath(args.redundancy)), "candidates.tsv")
    with open(path, "w") as fh:
        fh.write("category\ttest\tlines\tunique_lines\tmutants_killed\tsole_kills\t"
                 "subsumed_by_or_dup_of\tseconds\n")
        for row in sorted(out):
            fh.write("\t".join(map(str, row)) + "\n")

    print("\nREMOVE/MERGE candidates per test file:")
    pf = collections.Counter(t.split("::")[0] for c, t, *_ in out if c == CANDIDATE)
    tot = collections.Counter(t.split("::")[0] for _, t, *_ in out)
    for f, n in pf.most_common():
        print(f"  {f:40s} {n}/{tot[f]}")
    secs = sum(float(r[7] or 0) for r in out if r[0] == CANDIDATE)
    print(f"\ncandidate test time: {secs:.1f}s")
    print(f"TSV written to {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

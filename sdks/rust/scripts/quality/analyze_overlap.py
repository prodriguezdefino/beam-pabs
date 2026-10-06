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
"""Per-test coverage overlap: redundancy, subsumption and greedy minimal cover.

Reads ``per_test_cov.sh`` output and reports over production lines only
(``#[cfg(test)]`` items excluded). Writes ``redundancy_<focus|all>.tsv``, one
row per test, for ``candidates.py``.

``--repo-root`` maps the container lcov paths (``/workspace/``) to the host so
``#[cfg(test)]`` ranges can be read from the sources.

Usage::

        python3 analyze_overlap.py build/reports/test-quality/apache-beam-core/coverage \\
                --repo-root ../.. [--focus /coders/]
"""

import argparse
import collections
import os
import sys


def inline_test_ranges(path):
    """Line ranges (1-based, inclusive) of ``#[cfg(test)]`` items in ``path``."""
    rng = []
    try:
        with open(path) as fh:
            lines = fh.read().splitlines()
    except OSError:
        return rng
    i = 0
    while i < len(lines):
        if lines[i].strip().startswith("#[cfg(test)]"):
            s, depth, started = i + 1, 0, False
            while i < len(lines):
                depth += lines[i].count("{") - lines[i].count("}")
                started |= "{" in lines[i]
                i += 1
                if started and depth <= 0:
                    break
            rng.append((s, i))
        else:
            i += 1
    return rng


class ProdFilter:
    """Decides whether an lcov (file, line) is production code."""

    def __init__(self, repo_root, container_root):
        self.repo_root = repo_root
        self.container_root = container_root
        self._excl = {}
        self.unreadable = set()

    def host_path(self, f):
        if self.repo_root and f.startswith(self.container_root):
            return os.path.join(self.repo_root, f[len(self.container_root):])
        return f

    def is_prod(self, f, ln):
        if f not in self._excl:
            hp = self.host_path(f)
            if not os.path.exists(hp):
                self.unreadable.add(hp)
            self._excl[f] = inline_test_ranges(hp)
        return not any(a <= ln <= b for a, b in self._excl[f])


def parse_lcov(path, prod):
    """Returns (covered, instrumented) sets of (file, line) production lines."""
    cov, inst, f = set(), set(), None
    with open(path) as fh:
        for line in fh:
            if line.startswith("SF:"):
                f = line[3:].strip()
            elif line.startswith("DA:"):
                ln, cnt = line[3:].split(",")[:2]
                ln = int(ln)
                if not prod.is_prod(f, ln):
                    continue
                key = (f, ln)
                inst.add(key)
                if int(cnt) > 0:
                    cov.add(key)
    return cov, inst


def main(argv=None):
    ap = argparse.ArgumentParser(
            description=__doc__.split("\n\n")[0],
            formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out_dir", help="per_test_cov.sh output directory")
    ap.add_argument(
            "--focus",
            help="only count lines whose path contains this substring, e.g. /coders/")
    ap.add_argument(
            "--repo-root",
            help="host path of the repository root, used to read sources and find "
            "#[cfg(test)] ranges (default: lcov paths are used as-is)")
    ap.add_argument(
            "--container-root",
            default="/workspace/",
            help="repository root as seen in the lcov paths (default: %(default)s)")
    ap.add_argument(
            "--tsv-out",
            help="where to write the per-test TSV "
            "(default: <out_dir>/redundancy_<focus|all>.tsv)")
    args = ap.parse_args(argv)

    out = args.out_dir
    focus = args.focus
    root = args.container_root
    if not root.endswith("/"):
        root += "/"
    prod = ProdFilter(args.repo_root, root)

    tests = {}
    meta = {}
    inst_all = set()
    with open(os.path.join(out, "tests.tsv")) as fh:
        rows = fh.read().splitlines()[1:]
    for row in rows:
        idx, b, t, st, sec = row.split("\t")
        meta[idx] = (b, t, st, float(sec))
        p = os.path.join(out, "lcov", idx + ".info")
        if os.path.exists(p):
            c, ins = parse_lcov(p, prod)
            inst_all |= ins
            if focus:
                c = {k for k in c if focus in k[0]}
            tests[idx] = frozenset(c)

    if focus:
        inst_all = {k for k in inst_all if focus in k[0]}

    def name(i):
        return f"{meta[i][0]}::{meta[i][1]}"

    union = frozenset().union(*tests.values()) if tests else frozenset()
    cnt = collections.Counter(k for s in tests.values() for k in s)

    print(f"# Per-test overlap report  (focus={focus or 'whole crate'})\n")
    if prod.unreadable:
        print(f"warning: {len(prod.unreadable)} source files not readable, their "
                    "#[cfg(test)] lines are counted as production (check --repo-root)\n")
    fails = [i for i in meta if meta[i][2] != "pass"]
    print(f"tests run: {len(meta)}  failed/timeout: {len(fails)}  "
                f"total test time: {sum(m[3] for m in meta.values()):.1f}s")
    if inst_all:
        print(f"instrumented prod lines: {len(inst_all)}  covered by union: "
                    f"{len(union & inst_all)} "
                    f"({100 * len(union & inst_all) / len(inst_all):.1f}%)")
    touching = {i: s for i, s in tests.items() if s}
    print(f"tests touching focus code: {len(touching)}")

    # Hit-count distribution: how many tests cover each line.
    hist = collections.Counter(min(v, 50) for v in cnt.values())
    buckets = [(1, 1), (2, 5), (6, 20), (21, 50)]
    print("\nlines covered by N tests:")
    for a, b in buckets:
        n = sum(hist[k] for k in range(a, b + 1))
        print(f"  {a:>2}-{b:<2}{'+' if b == 50 else ' '}: {n:>6} lines "
                    f"({100 * n / max(len(cnt), 1):.1f}%)")

    # Exact duplicates.
    groups = collections.defaultdict(list)
    for i, s in touching.items():
        groups[s].append(i)
    dups = [g for g in groups.values() if len(g) > 1]
    print(f"\nidentical-coverage groups: {len(dups)} (covering "
                f"{sum(len(g) for g in dups)} tests; "
                f"{sum(len(g) - 1 for g in dups)} removable without line loss)")
    for g in sorted(dups, key=len, reverse=True)[:15]:
        print(f"  [{len(g)}] " + ", ".join(name(i) for i in g[:6]) +
                    (" ..." if len(g) > 6 else ""))

    # Unique contribution.
    uniq = {i: sum(1 for k in s if cnt[k] == 1) for i, s in touching.items()}
    zero = [i for i, u in uniq.items() if u == 0]
    print(f"\ntests with zero unique lines: {len(zero)} / {len(touching)}")

    # Strict subsumption (s_i proper subset of some s_j).
    items = sorted(touching.items(), key=lambda kv: len(kv[1]))
    subsumed = {}
    for i, s in items:
        for j, t in touching.items():
            if i != j and len(t) > len(s) and s < t:
                subsumed[i] = j
                break
    print(f"tests strictly subsumed by another single test: {len(subsumed)}")

    # Greedy set cover.
    remaining, chosen = set(union), []
    pool = dict(touching)
    while remaining:
        best = max(pool, key=lambda i: len(pool[i] & remaining))
        gain = pool[best] & remaining
        if not gain:
            break
        chosen.append((best, len(gain)))
        remaining -= gain
        del pool[best]
    print(f"\ngreedy minimal cover: {len(chosen)} tests reach the same line "
                f"coverage as {len(touching)} "
                f"({100 * len(chosen) / max(len(touching), 1):.0f}%)")

    # Per-binary (test file) summary.
    print("\nper test file: tests | zero-unique | subsumed | in-min-cover | seconds")
    chosen_set = {c for c, _ in chosen}
    pb = collections.defaultdict(lambda: [0, 0, 0, 0, 0.0])
    for i in touching:
        b = meta[i][0]
        pb[b][0] += 1
        pb[b][1] += i in zero
        pb[b][2] += i in subsumed
        pb[b][3] += i in chosen_set
        pb[b][4] += meta[i][3]
    for b, v in sorted(pb.items(), key=lambda kv: -kv[1][0]):
        print(f"  {b:40s} {v[0]:>4} {v[1]:>5} {v[2]:>5} {v[3]:>5} {v[4]:>8.1f}")

    # Per-source-file coverage.
    if inst_all:
        print("\nper source file: covered/instrumented, mean tests per covered line")
        pf = collections.defaultdict(lambda: [0, 0, 0])
        for k in inst_all:
            pf[k[0]][1] += 1
            if k in union:
                pf[k[0]][0] += 1
                pf[k[0]][2] += cnt[k]
        for f, (c, n, h) in sorted(pf.items(),
                                                              key=lambda kv: kv[1][0] / max(kv[1][1], 1)):
            print(f"  {f.split('/sdks/rust/')[-1]:55s} {c:>5}/{n:<5} "
                        f"{100 * c / max(n, 1):5.1f}%  x{h / max(c, 1):.1f}")

    tsv = args.tsv_out or os.path.join(
            out, f"redundancy_{(focus or 'all').strip('/').replace('/', '_')}.tsv")
    with open(tsv, "w") as fh:
        fh.write("test\tlines\tunique\tidentical_group\tsubsumed_by\t"
                          "in_min_cover\tseconds\n")
        gid = {i: n for n, g in enumerate(dups) for i in g}
        for i in touching:
            fh.write(f"{name(i)}\t{len(touching[i])}\t{uniq[i]}\t{gid.get(i, '')}\t"
                              f"{name(subsumed[i]) if i in subsumed else ''}\t"
                              f"{i in chosen_set}\t{meta[i][3]}\n")
    print(f"\nper-test TSV written to {tsv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

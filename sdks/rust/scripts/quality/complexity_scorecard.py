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
"""Cross-SDK complexity scorecard: Rust vs Go vs Python.

Measures production code (tests, generated code and local/legacy runners
excluded) with ``scc`` (code lines, branch complexity) and ``lizard``
(per-function CCN), normalized by feature points from ``docs/sdk-parity.md``
(supported = 1, partial = 0.5). Test lines are counted without tools, per area
and per SDK tree.

Usage::

    pip install lizard && brew install scc
    python3 sdks/rust/scripts/quality/complexity_scorecard.py > scorecard.md
    python3 sdks/rust/scripts/quality/complexity_scorecard.py --out scorecard.md --json-out scorecard.json
    python3 sdks/rust/scripts/quality/complexity_scorecard.py --list-areas   # no scc/lizard needed

or, in the builder container, ``./gradlew :sdks:rust:complexityScorecard``.
"""

import argparse
import csv
import io
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

# Set by init_paths() from --repo-root.
REPO = RUST = GO = PY = None
AREAS = EXAMPLES = None

SCC = os.environ.get("SCC", "scc")
LIZARD = os.environ.get("LIZARD")
if not LIZARD:
    if shutil.which("lizard"):
        LIZARD = "lizard"
    elif (Path.home() / ".local/bin/lizard").exists():
        LIZARD = str(Path.home() / ".local/bin/lizard")
    else:
        LIZARD = "lizard"


def init_paths(repo):
    """Area mapping. Each area lists (root, include-prefixes, exclude-prefixes).

    Prefixes are relative to the SDK root; "" means everything under it.
    """
    global REPO, RUST, GO, PY, AREAS, EXAMPLES
    REPO = Path(repo).resolve()
    RUST = REPO / "sdks/rust"
    GO = REPO / "sdks/go/pkg/beam"
    PY = REPO / "sdks/python/apache_beam"

    AREAS = {
        "engine": {  # construction API + model + worker harness
            "rust": (RUST / "beam", ["core/", "harness/", "derive/", "fluent/", "model/", "src/"], []),
            "go": (GO, ["", ], ["runners/", "io/", "testing/", "model/", "transforms/xlang/",
                                 "transforms/sql/", "xlang.go", "external.go",
                                 "core/runtime/xlangx/", "x/debug/"]),
            "python": (PY, ["pipeline.py", "pvalue.py", "error.py", "coders/", "transforms/",
                            "typehints/", "runners/worker/", "runners/common.py",
                            "runners/pipeline_context.py", "runners/sdf_utils.py",
                            "portability/", "options/", "metrics/", "internal/", "utils/"],
                       ["portability/api/", "transforms/external.py",
                        "transforms/external_transform_provider.py", "transforms/sql.py",
                        "utils/subprocess_server.py"]),
        },
        "platform": {  # runner clients, cross-language, expansion service
            "rust": (RUST / "beam", ["runners/", "expansion/", "external/"], []),
            "go": (GO, ["runners/dataflow/", "runners/universal/", "runners/prism/prism.go",
                        "runners/flink/", "runners/spark/", "runners/flag.go", "xlang.go",
                        "external.go", "core/runtime/xlangx/", "transforms/xlang/",
                        "transforms/sql/"],
                   ["transforms/xlang/inference/"]),
            "python": (PY, ["runners/runner.py", "runners/dataflow/", "runners/portability/",
                            "runners/job/", "transforms/external.py",
                            "transforms/external_transform_provider.py", "transforms/sql.py",
                            "utils/subprocess_server.py"],
                       ["runners/dataflow/internal/clients/", "runners/portability/fn_api_runner/"]),
        },
        "io": {
            "rust": (RUST / "beam", ["io/"], []),
            "go": (GO, ["io/"], []),
            "python": (PY, ["io/"], ["io/gcp/internal/clients/", "io/source_test_utils.py"]),
        },
        "ml": {
            "rust": (RUST / "beam", ["ml/"], []),
            "go": (GO, ["transforms/xlang/inference/"], []),
            "python": (PY, ["ml/inference/"], []),
        },
        "testing": {
            "rust": (RUST / "beam", ["testing/"], []),
            "go": (GO, ["testing/"], []),
            "python": (PY, ["testing/util.py", "testing/test_stream.py", "testing/test_pipeline.py",
                            "testing/pipeline_verifiers.py", "testing/test_utils.py"], []),
        },
    }

    EXAMPLES = {
        "minimal_wordcount": (RUST / "examples/minimal_wordcount/src",
                              REPO / "sdks/go/examples/minimal_wordcount",
                              PY / "examples/wordcount_minimal.py"),
        "wordcount": (RUST / "examples/wordcount/src", REPO / "sdks/go/examples/wordcount",
                      PY / "examples/wordcount.py"),
        "windowed_wordcount": (RUST / "examples/windowed_wordcount/src",
                               REPO / "sdks/go/examples/windowed_wordcount",
                               PY / "examples/windowed_wordcount.py"),
        "leaderboard": (RUST / "examples/leaderboard/src", None,
                        PY / "examples/complete/game/leader_board.py"),
    }


# Legacy or out-of-scope code in the other SDKs; reported for context only.
EXCLUDED_CONTEXT = {
    "go": [("runners/direct/", "legacy direct runner"),
           ("runners/prism/internal/", "Prism runner itself (Rust only launches it)"),
           ("runners/dot/", "dot renderer"), ("runners/vet/", "vet runner")],
    "python": [("runners/direct/", "legacy direct runner"),
               ("runners/portability/fn_api_runner/", "local FnApiRunner"),
               ("runners/interactive/", "interactive runner"),
               ("runners/dask/", "dask runner"), ("dataframe/", "DataFrame API"),
               ("yaml/", "Beam YAML")],
}

# Section of sdk-parity.md -> area it is implemented in.
SECTION_AREA = {
    "Core model": "engine", "Windowing": "engine", "State and timers": "engine",
    "Splittable DoFn": "engine", "Schemas and types": "engine",
    "Portability and deployment": "platform", "Testing and observability": "testing",
    "I/O connectors": "io", "Ecosystem": "ml",
}
ECOSYSTEM_ML_ROWS = ("RunInference",)

EXT = {"rust": (".rs",), "go": (".go",), "python": (".py",)}
LANG = {"rust": "rust", "go": "go", "python": "python"}
COMMENT = {"rust": ("//",), "go": ("//",), "python": ("#",)}
SDKS = ("rust", "go", "python")

LEGACY_RE = re.compile(
    r"#\[deprecated|@deprecated|\bDeprecated:|\bdeprecated\b|\blegacy\b|backwards?[- ]compat"
    r"|for compatibility|compat(ibility)? (shim|layer|alias)|\bkept for\b|no longer used"
    r"|\bold (api|name|behaviou?r)\b|\bformerly\b|\bwas renamed\b",
    re.IGNORECASE)


# ---------------------------------------------------------------------------
# File selection and normalization
# ---------------------------------------------------------------------------
def is_generated(sdk, rel, path):
    name = path.name
    if sdk == "rust":
        return "target" in rel.split("/")
    if sdk == "go":
        if name.endswith(".pb.go"):
            return True
        with open(path, errors="ignore") as f:
            head = f.read(2000)
        return "Code generated" in head and "DO NOT EDIT" in head
    if sdk == "python":
        return "_pb2" in name
    return False


def is_test(sdk, rel, path):
    name = path.name
    if sdk == "rust":
        parts = rel.split("/")
        if any(p in ("tests", "benches", "runner-tests", "test-utils") for p in parts):
            return True
        return name in ("tests.rs", "test.rs") or name.endswith("_tests.rs") or name.startswith("test_")
    if sdk == "go":
        return name.endswith("_test.go")
    if sdk == "python":
        return (name.endswith("_test.py") or name.startswith("test_") and "testing/" not in rel
                or "/tests/" in "/" + rel)
    return False


def is_test_or_generated(sdk, rel, path):
    return is_test(sdk, rel, path) or is_generated(sdk, rel, path)


def candidates(sdk, area):
    """Files of `area` for `sdk` (generated code excluded), as (rel, path, is_test)."""
    root, incs, excs = AREAS[area][sdk]
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in EXT[sdk]:
            continue
        rel = str(path.relative_to(root))
        if not any(rel.startswith(i) for i in incs):
            continue
        if any(rel.startswith(e) for e in excs):
            continue
        if is_generated(sdk, rel, path):
            continue
        yield rel, path, is_test(sdk, rel, path)


def select(sdk, area):
    """Production files of an area."""
    return [(rel, path) for rel, path, t in candidates(sdk, area) if not t]


_STR_RE = re.compile(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])\'')


def strip_rust_cfg_test(src):
    """Remove `#[cfg(test)]` items (usually `mod tests { ... }`)."""
    lines = src.split("\n")
    out, i = [], 0
    while i < len(lines):
        if lines[i].strip().startswith("#[cfg(test)]"):
            j = i + 1
            while j < len(lines) and lines[j].strip().startswith("#["):
                j += 1
            depth, opened = 0, False
            while j < len(lines):
                clean = _STR_RE.sub("", lines[j].split("//")[0])
                if not opened and "{" not in clean and clean.rstrip().endswith(";"):
                    break
                depth += clean.count("{") - clean.count("}")
                opened = opened or "{" in clean
                if opened and depth <= 0:
                    break
                j += 1
            i = j + 1
            continue
        out.append(lines[i])
        i += 1
    return "\n".join(out)


def materialize(sdk, files, dest):
    for rel, path in files:
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        text = path.read_text(errors="ignore")
        if sdk == "rust":
            text = strip_rust_cfg_test(text)
        target.write_text(text)


# ---------------------------------------------------------------------------
# Test code (no external tools)
# ---------------------------------------------------------------------------
def code_line_count(text, comment_prefixes):
    """Non-blank lines that are not comment-only."""
    return sum(1 for l in text.split("\n") if l.strip() and not l.strip().startswith(comment_prefixes))


def area_test_code(sdk, area):
    """Production and test lines of one area, counted the same way so the ratio is fair."""
    r = {"prod_files": 0, "test_files": 0, "prod_lines": 0, "test_lines": 0}
    for rel, path, test in candidates(sdk, area):
        text = path.read_text(errors="ignore")
        n = code_line_count(text, COMMENT[sdk])
        if test:
            r["test_files"] += 1
            r["test_lines"] += n
            continue
        r["prod_files"] += 1
        if sdk == "rust":
            prod = code_line_count(strip_rust_cfg_test(text), COMMENT[sdk])
            r["prod_lines"] += prod
            r["test_lines"] += n - prod   # inline #[cfg(test)] items
        else:
            r["prod_lines"] += n
    return r


SKIP_DIRS = {"target", "build", "node_modules", ".git", "vendor", "__pycache__",
             "generated", "proto"}


def _walk(base, exts):
    for d, dirs, files in os.walk(base):
        dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
        for f in files:
            if f.endswith(exts):
                yield os.path.join(d, f)


def _rust_split(path):
    """(prod, test) lines of a Rust file over the whole tree; test crates count as test."""
    text = Path(path).read_text(errors="ignore")
    if (re.search(r"/(tests|benches)/", path) or "/runner-tests/" in path
            or "/test-utils/" in path or "/testing/" in path):
        return 0, code_line_count(text, ("//",))
    lines = text.split("\n")
    prod = test = 0
    i, n = 0, len(lines)
    while i < n:
        s = lines[i].strip()
        if s.startswith("#[cfg(test)]") or s.startswith("#[cfg(all(test"):
            depth, started = 0, False
            while i < n:
                l = lines[i]
                if l.strip() and not l.strip().startswith("//"):
                    test += 1
                depth += l.count("{") - l.count("}")
                started = started or "{" in l
                i += 1
                if started and depth <= 0:
                    break
                if not started and l.strip().endswith(";"):
                    break
            continue
        if s and not s.startswith("//"):
            prod += 1
        i += 1
    return prod, test


def _generic(base, exts, test_pred, comment):
    prod = test = 0
    for p in _walk(base, exts):
        c = code_line_count(Path(p).read_text(errors="ignore"), comment)
        if test_pred(p):
            test += c
        else:
            prod += c
    return prod, test


def sdk_test_totals(with_java_ts=False):
    """Whole-tree production vs test lines per SDK, plus a per-crate Rust breakdown."""
    rows = {}
    crates = defaultdict(lambda: [0, 0])
    for p in _walk(str(RUST), (".rs",)):
        rel = os.path.relpath(p, RUST).split(os.sep)
        key = ("/".join(rel[:3]) if rel[0] == "beam" and len(rel) > 2
               and rel[1] in ("runners", "io") else "/".join(rel[:2]))
        a, b = _rust_split(p)
        crates[key][0] += a
        crates[key][1] += b
    rows["rust"] = (sum(v[0] for v in crates.values()), sum(v[1] for v in crates.values()))
    rows["go"] = _generic(str(REPO / "sdks/go"), (".go",),
                          lambda p: p.endswith("_test.go") or "/test/" in p, ("//",))
    rows["python"] = _generic(
        str(PY), (".py",),
        lambda p: (os.path.basename(p).endswith("_test.py") or "/testing/" in p or "_it_test" in p),
        ("#",))
    if with_java_ts:
        for sub in ("sdks/java/core", "sdks/java/harness", "sdks/java/io", "runners"):
            rows["java:" + sub.split("/")[-1]] = _generic(
                str(REPO / sub), (".java",),
                lambda p: "/src/test/" in p or "/testing/" in p, ("//", "*", "/*"))
        rows["ts"] = _generic(str(REPO / "sdks/typescript/src"), (".ts",),
                              lambda p: "/test" in p or p.endswith(".test.ts"), ("//", "*", "/*"))
    return rows, {k: tuple(v) for k, v in crates.items()}


def ratio(test, prod):
    return test / prod if prod else None


# ---------------------------------------------------------------------------
# Measurement
# ---------------------------------------------------------------------------
def run_scc(d):
    res = subprocess.run([SCC, "--format", "json", "--no-cocomo", str(d)],
                         capture_output=True, text=True, check=True)
    code = cx = files = comment = 0
    for lang in json.loads(res.stdout or "[]"):
        if lang["Name"] in ("Rust", "Go", "Python"):
            code += lang["Code"]; cx += lang["Complexity"]
            files += lang["Count"]; comment += lang["Comment"]
    return {"code": code, "scc_cx": cx, "files": files, "comment": comment}


def run_lizard(d, sdk):
    res = subprocess.run([LIZARD, "--csv", "-l", LANG[sdk], str(d)],
                         capture_output=True, text=True)
    ccns, nlocs, params = [], [], []
    for row in csv.reader(io.StringIO(res.stdout)):
        if len(row) < 5:
            continue
        try:
            nlocs.append(int(row[0])); ccns.append(int(row[1])); params.append(int(row[3]))
        except ValueError:
            continue
    if not ccns:
        return {"funcs": 0, "ccn_avg": 0, "ccn_p90": 0, "ccn_gt15": 0, "fn_len": 0, "params": 0}
    ccns_sorted = sorted(ccns)
    return {
        "funcs": len(ccns),
        "ccn_avg": statistics.mean(ccns),
        "ccn_p90": ccns_sorted[int(0.9 * (len(ccns) - 1))],
        "ccn_gt15": sum(c > 15 for c in ccns),
        "fn_len": statistics.mean(nlocs),
        "params": statistics.mean(params),
    }


def public_items(sdk, d):
    pats = {
        "rust": re.compile(r"^\s*pub\s+(?:async\s+|unsafe\s+|const\s+)*(fn|struct|enum|trait|type)\s"),
        "go": re.compile(r"^(?:func\s+(?:\([^)]*\)\s*)?[A-Z]|type\s+[A-Z])"),
        "python": re.compile(r"^\s*(?:def|class)\s+[A-Za-z]"),
    }[sdk]
    n = 0
    for p in d.rglob("*"):
        if p.is_file():
            n += sum(1 for line in p.read_text(errors="ignore").split("\n") if pats.match(line))
    return n


def legacy_markers(d):
    hits = []
    for p in d.rglob("*"):
        if p.is_file():
            for no, line in enumerate(p.read_text(errors="ignore").split("\n"), 1):
                if LEGACY_RE.search(line):
                    hits.append((p, no, line.strip()))
    return hits


def rust_structure(d):
    """Over-abstraction signals: trait / impl ratio, generics, macros."""
    trait_re = re.compile(r"^\s*pub(?:\([^)]*\))?\s+(?:unsafe\s+)?trait\s+(\w+)|^\s*trait\s+(\w+)")
    text_all = []
    traits = {}
    for p in d.rglob("*.rs"):
        t = p.read_text(errors="ignore")
        text_all.append(t)
        for no, line in enumerate(t.split("\n"), 1):
            m = trait_re.match(line)
            if m:
                traits[m.group(1) or m.group(2)] = (p, no)
    blob = "\n".join(text_all)
    impls = {}
    for name in traits:
        impls[name] = len(re.findall(r"\bimpl\b[^{;]*?\b" + re.escape(name) + r"\b(?:<[^{;]*?>)?\s+for\b", blob))
    generic_fns = len(re.findall(r"\bfn\s+\w+\s*<", blob))
    all_fns = len(re.findall(r"\bfn\s+\w+", blob))
    return {
        "traits": len(traits),
        "traits_le1_impl": sorted((n, traits[n], impls[n]) for n in traits if impls[n] <= 1),
        "generic_fn_ratio": generic_fns / max(all_fns, 1),
        "macro_rules": len(re.findall(r"\bmacro_rules!\s*\w+", blob)),
        "where_clauses": len(re.findall(r"\bwhere\b", blob)),
        "box_dyn": len(re.findall(r"\bBox<dyn\b", blob)),
        "arc_mutex": len(re.findall(r"Arc<(?:Mutex|RwLock)", blob)),
    }


# ---------------------------------------------------------------------------
# Feature points from docs/sdk-parity.md
# ---------------------------------------------------------------------------
def feature_points():
    text = (RUST / "docs/sdk-parity.md").read_text()
    pts = defaultdict(lambda: defaultdict(float))
    xlang = defaultdict(lambda: defaultdict(int))
    section, cols = None, []
    for line in text.split("\n"):
        if line.startswith("## "):
            section, cols = line[3:].strip(), []
            continue
        if not line.startswith("|") or section not in SECTION_AREA:
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if not cols:
            cols = [c.replace("*", "").strip().lower() for c in cells]
            continue
        if set(cells[0]) <= set("-: "):
            continue
        feature_name = cells[0].replace("`", "").strip()
        if section == "Ecosystem" and not feature_name.startswith(ECOSYSTEM_ML_ROWS):
            continue
        area = SECTION_AREA[section]
        for sdk in ("go", "python", "rust"):
            if sdk not in cols:
                continue
            cell = cells[cols.index(sdk)]
            if cell.startswith("✅"):
                pts[area][sdk] += 1
            elif cell.startswith("🟡"):
                pts[area][sdk] += 0.5
            if "xlang" in cell:
                xlang[area][sdk] += 1
    return pts, xlang


# ---------------------------------------------------------------------------
# User-facing complexity: the same example in every SDK
# ---------------------------------------------------------------------------
def example_code(sdk, src, tmp):
    if src is None or not src.exists():
        return None
    d = tmp / f"ex_{sdk}_{src.name}"
    files = [src] if src.is_file() else [p for p in src.rglob("*") if p.suffix in EXT[sdk]]
    files = [p for p in files if not is_test_or_generated(sdk, p.name, p)]
    materialize(sdk, [(p.name, p) for p in files], d)
    return run_scc(d)["code"]


# ---------------------------------------------------------------------------
def fmt(x, nd=0):
    return "—" if x is None else (f"{x:,.{nd}f}" if isinstance(x, float) else f"{x:,}")


def fmt_ratio(x):
    return "—" if x is None else f"{x:.2f}"


def render_test_totals(p, totals, crates):
    p("## 1b. Test code (whole SDK trees)\n")
    p("Non-blank, non-comment lines, split by path and by `#[cfg(test)]` items; shared test "
      "infrastructure (Rust `testing/`, `runner-tests/`, `test-utils/`; Python `testing/`) counts "
      "as test code here.\n")
    p("| SDK | Prod lines | Test lines | Test/prod |")
    p("|---|--:|--:|--:|")
    for sdk, (prod, test) in totals.items():
        p(f"| {sdk} | {fmt(prod)} | {fmt(test)} | {fmt_ratio(ratio(test, prod))} |")
    p("\nRust per crate:\n")
    p("| Crate | Prod lines | Test lines | Test/prod |")
    p("|---|--:|--:|--:|")
    for k, (prod, test) in sorted(crates.items(), key=lambda kv: -(kv[1][0] + kv[1][1])):
        p(f"| `{k}` | {fmt(prod)} | {fmt(test)} | {fmt_ratio(ratio(test, prod))} |")
    p()


def list_areas(args, p, data):
    """Selection and test-code counts only: no scc, no lizard."""
    pts, _ = feature_points()
    p("# SDK area selection (no scc/lizard)\n")
    p("| Area | SDK | Prod files | Test files | Prod lines | Test lines | Test/prod | Feature pts |")
    p("|---|---|--:|--:|--:|--:|--:|--:|")
    for area in AREAS:
        for sdk in SDKS:
            t = area_test_code(sdk, area)
            data["areas"].setdefault(area, {})[sdk] = dict(t, feature_points=pts[area].get(sdk, 0))
            p(f"| {area} | {sdk} | {fmt(t['prod_files'])} | {fmt(t['test_files'])} | "
              f"{fmt(t['prod_lines'])} | {fmt(t['test_lines'])} | "
              f"{fmt_ratio(ratio(t['test_lines'], t['prod_lines']))} | {fmt(pts[area].get(sdk, 0), 1)} |")
    p()
    totals, crates = sdk_test_totals(args.with_java_ts)
    data["sdk_test_totals"] = {k: {"prod_lines": a, "test_lines": b} for k, (a, b) in totals.items()}
    data["rust_crates"] = {k: {"prod_lines": a, "test_lines": b} for k, (a, b) in crates.items()}
    render_test_totals(p, totals, crates)


def scorecard(args, p, data):
    for tool in (SCC, LIZARD):
        if not shutil.which(tool):
            sys.exit(f"missing tool: {tool} (set SCC / LIZARD env vars or install it)")
    sdks = SDKS
    tmp = Path(tempfile.mkdtemp(prefix="beam_scorecard_"))
    m = defaultdict(dict)
    legacy = {}
    for sdk in sdks:
        all_dir = tmp / sdk / "_all"
        for area in AREAS:
            d = tmp / sdk / area
            files = select(sdk, area)
            materialize(sdk, files, d)
            materialize(sdk, [(f"{area}/{r}", p_) for r, p_ in files], all_dir)
            r = run_scc(d)
            r.update(run_lizard(d, sdk))
            r["public"] = public_items(sdk, d)
            t = area_test_code(sdk, area)
            r["test_lines"] = t["test_lines"]
            r["test_ratio"] = ratio(t["test_lines"], t["prod_lines"])
            m[sdk][area] = r
        legacy[sdk] = legacy_markers(all_dir)
    pts, xlang = feature_points()
    data["areas"] = {a: {s: dict(m[s][a], feature_points=pts[a].get(s, 0)) for s in sdks} for a in AREAS}
    data["xlang"] = {a: dict(xlang[a]) for a in AREAS}

    p("# SDK Complexity Scorecard (Rust vs Go vs Python)\n")
    p("Production code only: tests, generated code and local/legacy runners excluded. "
      "Feature points from `docs/sdk-parity.md` (✅=1, 🟡=0.5). Test lines and test/prod "
      "are counted without tools (non-blank, non-comment lines), both sides the same way.\n")

    p("## 1. Size normalized by features\n")
    p("| Area | SDK | Code lines | Feature pts | Lines / feature | vs Go | vs Python | Test lines | Test/prod |")
    p("|---|---|--:|--:|--:|--:|--:|--:|--:|")
    for area in AREAS:
        per = {}
        for sdk in sdks:
            f = pts[area].get(sdk, 0)
            per[sdk] = m[sdk][area]["code"] / f if f else None
        for sdk in sdks:
            f = pts[area].get(sdk, 0)
            rel = lambda o: (f"{per[sdk] / per[o]:.2f}×" if per[sdk] and per[o] and sdk != o else "")
            r = m[sdk][area]
            p(f"| {area} | {sdk} | {fmt(r['code'])} | {fmt(f, 1) if f else '—'} "
              f"| {fmt(per[sdk])} | {rel('go')} | {rel('python')} "
              f"| {fmt(r['test_lines'])} | {fmt_ratio(r['test_ratio'])} |")
    tot = {s: sum(m[s][a]["code"] for a in AREAS) for s in sdks}
    totf = {s: sum(pts[a].get(s, 0) for a in AREAS) for s in sdks}
    tott = {s: sum(m[s][a]["test_lines"] for a in AREAS) for s in sdks}
    p(f"| **total** | " + " / ".join(f"{s}: {fmt(tot[s])} lines, {fmt(totf[s], 1)} pts, "
                                     f"**{fmt(tot[s] / totf[s])}**/pt, {fmt(tott[s])} test lines"
                                     for s in sdks) + " ||||||||\n")
    p("Cross-language (`xlang`) feature cells per area (cheap to implement, inflate points): " +
      "; ".join(f"{a}: " + ", ".join(f"{s}={xlang[a].get(s, 0)}" for s in sdks)
                for a in AREAS if any(xlang[a].values())) + "\n")

    totals, crates = sdk_test_totals(args.with_java_ts)
    data["sdk_test_totals"] = {k: {"prod_lines": a, "test_lines": b} for k, (a, b) in totals.items()}
    data["rust_crates"] = {k: {"prod_lines": a, "test_lines": b} for k, (a, b) in crates.items()}
    render_test_totals(p, totals, crates)

    p("## 2. Logic complexity (lizard, per function)\n")
    p("| Area | SDK | Functions | Avg CCN | p90 CCN | Fns CCN>15 | Avg fn length | Avg params | scc complexity / kLOC | Public items |")
    p("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|")
    for area in AREAS:
        for sdk in sdks:
            r = m[sdk][area]
            dens = r["scc_cx"] / r["code"] * 1000 if r["code"] else 0
            p(f"| {area} | {sdk} | {fmt(r['funcs'])} | {r['ccn_avg']:.2f} | {r['ccn_p90']} | "
              f"{r['ccn_gt15']} | {r['fn_len']:.1f} | {r['params']:.2f} | {dens:.0f} | {fmt(r['public'])} |")
    p()

    p("## 3. User-facing complexity (same example, code lines)\n")
    p("| Example | Rust | Go | Python |")
    p("|---|--:|--:|--:|")
    data["examples"] = {}
    for name, (rs, go, py) in EXAMPLES.items():
        vals = [example_code("rust", rs, tmp), example_code("go", go, tmp), example_code("python", py, tmp)]
        data["examples"][name] = dict(zip(sdks, vals))
        p(f"| {name} | " + " | ".join(fmt(v) for v in vals) + " |")
    p()

    p("## 4. Legacy markers (per kLOC of production code)\n")
    p("| SDK | Hits | Per kLOC |")
    p("|---|--:|--:|")
    for sdk in sdks:
        p(f"| {sdk} | {len(legacy[sdk])} | {len(legacy[sdk]) / tot[sdk] * 1000:.2f} |")
    p("\nRust hits (goal: zero):\n")
    data["legacy"] = {s: len(legacy[s]) for s in sdks}
    data["legacy_rust_hits"] = []
    for path, no, line in legacy["rust"]:
        rel = str(path.relative_to(tmp / "rust" / "_all"))
        data["legacy_rust_hits"].append({"file": rel, "line": no, "text": line[:140]})
        p(f"- `{rel}:{no}` — {line[:140]}")
    p()

    p("## 5. Code the other SDKs carry that Rust must never need\n")
    p("| SDK | Path | What | Code lines |")
    p("|---|---|---|--:|")
    data["excluded_context"] = []
    for sdk, items in EXCLUDED_CONTEXT.items():
        root = GO if sdk == "go" else PY
        for prefix, what in items:
            d = tmp / "ctx" / sdk / prefix.strip("/")
            files = [(str(f.relative_to(root)), f) for f in (root / prefix).rglob("*")
                     if f.is_file() and f.suffix in EXT[sdk]
                     and not is_test_or_generated(sdk, str(f.relative_to(root)), f)]
            materialize(sdk, files, d)
            code = run_scc(d)["code"] if files else 0
            data["excluded_context"].append({"sdk": sdk, "path": prefix, "what": what, "code": code})
            p(f"| {sdk} | `{prefix}` | {what} | {fmt(code)} |")
    p()

    p("## 6. Rust over-abstraction signals (engine + platform)\n")
    comb = tmp / "rust_struct"
    for area in ("engine", "platform", "io", "ml", "testing"):
        shutil.copytree(tmp / "rust" / area, comb / area, dirs_exist_ok=True)
    s = rust_structure(comb)
    p(f"- Traits declared: **{s['traits']}**, with ≤1 `impl ... for`: **{len(s['traits_le1_impl'])}**")
    p(f"- Generic fn ratio: {s['generic_fn_ratio']:.1%} · `where` clauses: {s['where_clauses']} · "
      f"`Box<dyn>`: {s['box_dyn']} · `Arc<Mutex|RwLock>`: {s['arc_mutex']} · `macro_rules!`: {s['macro_rules']}")
    p("\nTraits with ≤1 implementation (candidates to inline; blanket impls and "
      "user-implemented extension points are legitimate):\n")
    data["rust_structure"] = {k: v for k, v in s.items() if k != "traits_le1_impl"}
    data["rust_structure"]["traits_le1_impl"] = []
    for name, (path, no), n in s["traits_le1_impl"]:
        data["rust_structure"]["traits_le1_impl"].append(
            {"trait": name, "impls": n, "file": str(path.relative_to(comb)), "line": no})
        p(f"- `{name}` ({n} impl) — `{path.relative_to(comb)}:{no}`")

    shutil.rmtree(tmp, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=None,
                    help="Beam repository root (default: derived from this script's location)")
    ap.add_argument("--out", help="write the report here instead of stdout")
    ap.add_argument("--format", choices=("md", "json"), default="md",
                    help="format of --out / stdout (default: %(default)s)")
    ap.add_argument("--json-out", help="also write the JSON form here")
    ap.add_argument("--list-areas", action="store_true",
                    help="only file selection and test-code counts per area (no scc/lizard)")
    ap.add_argument("--with-java-ts", action="store_true",
                    help="add Java and TypeScript to the whole-tree test-code totals")
    args = ap.parse_args(argv)
    init_paths(args.repo_root or str(Path(__file__).resolve().parents[4]))

    md = []
    data = {"repo_root": str(REPO), "areas": {}}

    def p(s=""):
        md.append(s)

    if args.list_areas:
        list_areas(args, p, data)
    else:
        scorecard(args, p, data)

    text = json.dumps(data, indent=2, sort_keys=True, default=str) if args.format == "json" \
        else "\n".join(md) + "\n"
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text)
    else:
        sys.stdout.write(text)
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(json.dumps(data, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())

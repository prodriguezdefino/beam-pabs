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

"""
Runs the profile command of each variant (see report.py), usually the `profilePrism` task of
a checkout, for some rounds. Reads the summary.md of each run, and writes results.json and
report.md with the medians and the change against the baseline or an earlier results.json.
The launch command can hold the placeholders {run_dir} (the output directory of the run)
and {rows} (the rows of the input).

The rounds run one after the other. In each round the order of the variants alternates,
so a slow drift of the host does not always favor the same variant.
"""

import argparse
import datetime
import glob
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional, Sequence

import report

METRICS = [
    ("rust_cpu_s", "Rust CPU (s)", ",.2f"),
    ("prism_cpu_s", "Prism CPU (s)", ",.2f"),
    ("cpu_s_per_1m_rows", "Rust CPU s / 1M rows", ",.3f"),
]
SECTIONS = ["metrics", "hot_frames"]

NUMBERS = {
    "rows": r"\*\*Rows Processed\*\*: ([\d,]+)",
    "rust_cpu_s": r"\*\*Rust CPU Time\*\*: ([\d.,]+) s",
    "prism_cpu_s": r"\*\*Prism CPU Time\*\*: ([\d.,]+) s",
    "cpu_s_per_1m_rows": r"\*\*CPU Time / 1M rows \(Rust only\)\*\*: ([\d.,]+)",
}
HOT_FRAME = re.compile(r"^\| `(.+)` \| ([\d.]+)% \|$")


def parse_summary(text: str) -> Dict[str, Dict[str, float]]:
  """The CPU numbers and the hot frames (self CPU %) of a profilePrism summary.md."""
  metrics = {
      key: float(m.group(1).replace(",", ""))
      for key, pattern in NUMBERS.items() if (m := re.search(pattern, text))
  }
  _, _, hot = text.partition("Hot Frames")
  frames = {
      m.group(1): float(m.group(2))
      for line in hot.splitlines() if (m := HOT_FRAME.match(line.strip()))
  }
  return {"metrics": metrics, "hot_frames": frames}


def count_lines(pattern: str) -> int:
  """The lines of the files that match the glob: the rows that wordcount reads."""
  files = sorted(glob.glob(pattern))
  if not files:
    raise FileNotFoundError(f"No file matches --input={pattern}.")
  total = 0
  for path in files:
    with open(path, "rb") as f:
      total += sum(chunk.count(b"\n") for chunk in iter(lambda: f.read(1 << 20), b""))
  return total


def round_order(variants: Sequence[Any], round_number: int) -> List[Any]:
  return list(variants) if round_number % 2 else list(reversed(variants))


def profile_command(variant: report.Variant, run_dir: str, rows: Optional[int]) -> List[str]:
  """The argv of one run. Without the rows, the arguments that need {rows} are dropped."""
  kept = {**variant, "argv": [a for a in variant["argv"] if rows or "{rows}" not in a]}
  return report.expand(kept, {"run_dir": run_dir, "rows": rows})


def top_frames(all_medians: report.Medians, top: int) -> List[report.Column]:
  """The `top` hottest frames of any variant, hottest first."""
  heat: Dict[str, float] = {}
  for sections in all_medians.values():
    for frame, pct in sections.get("hot_frames", {}).items():
      heat[frame] = max(heat.get(frame, 0.0), pct)
  ranked = sorted(heat, key=heat.get, reverse=True)[:top]
  return [(frame, f"`{frame}`", ".1f") for frame in ranked]


def render(results: Dict[str, Any], saved: Optional[Dict[str, Any]], top: int) -> List[str]:
  all_medians = report.with_saved(results["medians"], saved)
  config = results["config"]
  rows = f"{config['rows']:,} rows" if config.get("rows") else "rows unknown"
  return [
      f"# Local profile comparison ({results['created']})", "",
      f"{config['describe']}. Input `{config.get('input') or 'none'}`, {rows}, "
      f"{config['runs']} rounds, `worker` Cargo profile, CPU mode.", "",
      "Variants: " + ", ".join(f"{k}={v}" for k, v in config["variants"].items()) + ".", "",
      "## Medians", "",
      *report.compare_table(all_medians, "metrics", METRICS), "", "## Runs", "",
      *report.runs_table(results["runs"], "metrics", METRICS), "",
      "## Hot frames (self CPU % of the Rust process)", "",
      *report.compare_table(all_medians, "hot_frames", top_frames(all_medians, top),
                            relative=False)
  ]


def run(args: argparse.Namespace) -> None:
  variants = report.load_variants(args.variants)
  rows = args.rows or (count_lines(args.input) if args.input and glob.glob(args.input) else None)
  runs = []
  for round_number in range(1, args.runs + 1):
    for variant in round_order(variants, round_number):
      label = variant["label"]
      run_dir = os.path.join(args.out_dir, f"{label}-r{round_number}")
      command = profile_command(variant, run_dir, rows)
      print(f"Round {round_number}, {label}: "
            f"{report.show(command, variant['cwd'], variant.get('env'))}", flush=True)
      if args.dry_run:
        continue
      os.makedirs(run_dir, exist_ok=True)
      log = os.path.join(run_dir, "gradle.log")
      with open(log, "w") as out:
        if subprocess.run(command, cwd=variant["cwd"], env=report.environment(variant),
                          stdout=out, stderr=subprocess.STDOUT).returncode:
          raise RuntimeError(f"The profile of {label} failed. See {log}.")
      with open(os.path.join(run_dir, "summary.md")) as f:
        runs.append({"round": round_number, "variant": label, **parse_summary(f.read())})
  if args.dry_run:
    return
  results = {
      "task": "profileCompare",
      "created": datetime.datetime.now().isoformat(timespec="seconds"),
      "config": {"describe": args.describe, "input": args.input, "rows": rows,
                 "runs": args.runs,
                 "variants": {v["label"]: v.get("describe", v["cwd"]) for v in variants}},
      "runs": runs,
      "medians": report.medians(runs, SECTIONS),
  }
  report.write(args.out_dir, results, render(results, report.load(args.compare_to), args.top))


def main(argv: Sequence[str]) -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--out-dir", required=True)
  parser.add_argument("--variants", required=True,
                      help="JSON file of the variants. Placeholders: {run_dir}, {rows}.")
  parser.add_argument("--input", help="Local glob of the input files, to count the rows.")
  parser.add_argument("--rows", type=int, help="Rows of the input. Default: its lines.")
  parser.add_argument("--runs", type=int, default=3)
  parser.add_argument("--describe", default="", help="A line for the report.")
  parser.add_argument("--top", type=int, default=10, help="Hot frames in the report.")
  parser.add_argument("--compare-to", help="An earlier results.json.")
  parser.add_argument("--dry-run", action="store_true", help="Print the commands only.")
  run(parser.parse_args(argv))


if __name__ == "__main__":
  main(sys.argv[1:])

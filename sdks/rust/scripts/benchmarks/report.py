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
Shared parts of the benchmark scripts: the variants under test, medians of the runs,
comparison tables in Markdown, and the results.json and report.md files.

A variant is a JSON object {"label", "cwd", "argv", "env", "describe", "prepare"}: the
command that runs it, in its working directory, with more environment variables. Each value
in "argv" can hold placeholders, such as {job_name} or {run_dir}, that the script sets for
each run. The optional "prepare" command runs once before the rounds, for example a build.
The variants come from a JSON file, so any launcher can be a variant: the Gradle task of a
git ref, or a pipeline of another SDK.

A run is a dict with "round", "variant" and one dict of numbers for each section, for
example "metrics" and "steps". The variant "current" is the checkout under test. The other
variants ("baseline", or "saved <variant>" from an earlier results.json) are references.
"""

import json
import os
import shlex
import statistics
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

CURRENT = "current"

# (key, title, format spec) of one value in a table.
Column = Tuple[str, str, str]

Run = Dict[str, Any]
Variant = Dict[str, Any]
Medians = Dict[str, Dict[str, Dict[str, float]]]


def load_variants(path: str) -> List[Variant]:
  """The variants of a JSON file. Each needs a unique "label", a "cwd" and an "argv"."""
  with open(path) as f:
    variants = json.load(f)
  labels = [v.get("label") for v in variants]
  if not variants or any(not v.get("label") or not v.get("cwd") or not v.get("argv")
                         for v in variants) or len(set(labels)) != len(labels):
    raise ValueError(f"{path} needs variants with a unique label, a cwd and an argv.")
  return variants


def expand(variant: Variant, values: Mapping[str, Any]) -> List[str]:
  """The argv of a variant with each {placeholder} replaced by its value."""
  def one(arg: str) -> str:
    for name, value in values.items():
      arg = arg.replace("{" + name + "}", str(value))
    return arg
  return [one(arg) for arg in variant["argv"]]


def environment(variant: Variant) -> Dict[str, str]:
  return {**os.environ, **variant.get("env", {})}


def show(command: Sequence[str], cwd: str, env: Optional[Mapping[str, str]] = None) -> str:
  prefix = "".join(f"{k}={shlex.quote(v)} " for k, v in (env or {}).items())
  return f"(cd {shlex.quote(cwd)} && {prefix}{shlex.join(command)})"


def median(values: Iterable[Optional[float]]) -> Optional[float]:
  present = [v for v in values if v is not None]
  return statistics.median(present) if present else None


def medians(runs: Sequence[Run], sections: Sequence[str]) -> Medians:
  """The median of each value of each section, for each variant."""
  out: Medians = {}
  for variant in dict.fromkeys(run["variant"] for run in runs):
    mine = [run for run in runs if run["variant"] == variant]
    out[variant] = {}
    for section in sections:
      keys = dict.fromkeys(k for run in mine for k in run.get(section, {}))
      values = {k: median(run.get(section, {}).get(k) for run in mine) for k in keys}
      out[variant][section] = {k: v for k, v in values.items() if v is not None}
  return out


def with_saved(current: Medians, saved: Optional[Dict[str, Any]]) -> Medians:
  """Adds the medians of an earlier results.json as the variants "saved <variant>"."""
  if not saved:
    return current
  return {**current, **{f"saved {v}": m for v, m in saved.get("medians", {}).items()}}


def fmt(value: Optional[float], spec: str) -> str:
  return "-" if value is None else format(value, spec)


def change(new: Optional[float], old: Optional[float], relative: bool) -> str:
  """The change from `old` to `new`: in percent, or in percentage points."""
  if new is None or old is None:
    return "-"
  if not relative:
    return f"{new - old:+.1f} pp"
  return f"{100.0 * (new - old) / old:+.1f}%" if old else "-"


def table(header: Sequence[str], rows: Iterable[Sequence[str]]) -> List[str]:
  cells = lambda row: "| " + " | ".join(str(c).replace("|", "\\|") for c in row) + " |"
  return [cells(header), "|" + "---|" * len(header), *(cells(row) for row in rows)]


def runs_table(runs: Sequence[Run], section: str, columns: Sequence[Column],
               extra: Sequence[Tuple[str, Any]] = ()) -> List[str]:
  """One row for each run. `extra` holds (title, function of the run) text columns."""
  header = ["Round", "Variant", *(title for _, title, _ in columns), *(t for t, _ in extra)]
  rows = ([
      str(run["round"]), run["variant"],
      *(fmt(run.get(section, {}).get(key), spec) for key, _, spec in columns),
      *(str(cell(run)) for _, cell in extra)
  ] for run in runs)
  return table(header, rows)


def compare_table(all_medians: Medians, section: str, columns: Sequence[Column],
                  relative: bool = True) -> List[str]:
  """One row for each value: the median of each variant, then the change of the current
  variant against each reference variant."""
  variants = list(all_medians)
  references = [v for v in variants if v != CURRENT] if CURRENT in variants else []
  value = lambda variant, key: all_medians[variant].get(section, {}).get(key)
  header = ["", *variants, *(f"current vs {r}" for r in references)]
  rows = ([
      title, *(fmt(value(v, key), spec) for v in variants),
      *(change(value(CURRENT, key), value(r, key), relative) for r in references)
  ] for key, title, spec in columns)
  return table(header, rows)


def load(path: Optional[str]) -> Optional[Dict[str, Any]]:
  if not path:
    return None
  with open(path) as f:
    return json.load(f)


def write(out_dir: str, results: Dict[str, Any], markdown: Sequence[str]) -> None:
  os.makedirs(out_dir, exist_ok=True)
  with open(os.path.join(out_dir, "results.json"), "w") as f:
    json.dump(results, f, indent=2, sort_keys=True)
  with open(os.path.join(out_dir, "report.md"), "w") as f:
    f.write("\n".join(markdown) + "\n")
  print("\n".join(markdown))
  print(f"\nResults written to: {out_dir}")

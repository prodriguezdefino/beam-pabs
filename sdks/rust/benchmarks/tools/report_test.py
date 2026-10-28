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

import contextlib
import io
import json
import os
import tempfile
import unittest
import report

RUNS = [
  {"round": 1, "variant": "baseline", "metrics": {"cpu": 10.0}},
  {"round": 1, "variant": "current", "metrics": {"cpu": 8.0, "only_current": 1.0}},
  {"round": 2, "variant": "baseline", "metrics": {"cpu": 12.0}},
  {"round": 2, "variant": "current", "metrics": {"cpu": 9.0}},
  {"round": 3, "variant": "baseline", "metrics": {"cpu": 11.0}},
  {"round": 3, "variant": "current", "metrics": {}},
]

class TestReport(unittest.TestCase):
  def write_variants(self, folder, variants):
    path = os.path.join(folder, "variants.json")
    with open(path, "w") as f:
      json.dump(variants, f)
    return path

  def test_load_variants(self):
    good = [{"label": "baseline", "cwd": "/b", "argv": ["x"]},
            {"label": "current", "cwd": "/a", "argv": ["y"], "env": {"A": "1"}}]
    with tempfile.TemporaryDirectory() as tmp:
      self.assertEqual(report.load_variants(self.write_variants(tmp, good)), good)
      for bad in ([], [{"label": "a", "cwd": "/a"}], [good[0], good[0]]):
        with self.assertRaises(ValueError):
          report.load_variants(self.write_variants(tmp, bad))

  def test_expand_replaces_each_placeholder(self):
    variant = {"argv": ["run", "--name={job_name}", "--out={run_dir}/x", "{json}", "--keep={other}"]}
    self.assertEqual(report.expand(variant, {"job_name": "j", "run_dir": "/o", "json": '{"a":1}'}),
                     ["run", "--name=j", "--out=/o/x", '{"a":1}', "--keep={other}"])

  def test_show_prefixes_the_environment(self):
    self.assertEqual(report.show(["a", "b c"], "/d", {"X": "1 2"}), "(cd /d && X='1 2' a 'b c')")

  def test_medians_skip_missing_values(self):
    medians = report.medians(RUNS, ["metrics", "steps"])
    self.assertEqual(list(medians), ["baseline", "current"])
    self.assertEqual(medians["baseline"]["metrics"], {"cpu": 11.0})
    self.assertEqual(medians["current"]["metrics"], {"cpu": 8.5, "only_current": 1.0})
    self.assertEqual(medians["current"]["steps"], {})

  def test_change(self):
    self.assertEqual(report.change(9.0, 10.0, True), "-10.0%")
    self.assertEqual(report.change(12.5, 10.0, False), "+2.5 pp")
    self.assertEqual(report.change(None, 10.0, True), "-")
    self.assertEqual(report.change(1.0, 0.0, True), "-")

  def test_compare_table_against_baseline_and_saved(self):
    saved = {"medians": {"current": {"metrics": {"cpu": 10.0}}}}
    medians = report.with_saved(report.medians(RUNS, ["metrics"]), saved)
    lines = report.compare_table(medians, "metrics", [("cpu", "CPU", ".1f")])
    self.assertEqual(lines[0], "|  | baseline | current | saved current | current vs baseline "
                               "| current vs saved current |")
    self.assertEqual(lines[2], "| CPU | 11.0 | 8.5 | 10.0 | -22.7% | -15.0% |")

  def test_compare_table_without_current_has_no_change_columns(self):
    medians = {"baseline": {"metrics": {"cpu": 1.0}}}
    lines = report.compare_table(medians, "metrics", [("cpu", "CPU", ".1f")])
    self.assertEqual(lines[0], "|  | baseline |")

  def test_compare_table_with_a_named_variant_under_test(self):
    medians = {"java": {"metrics": {"cpu": 10.0}}, "rust": {"metrics": {"cpu": 8.0}}}
    lines = report.compare_table(medians, "metrics", [("cpu", "CPU", ".1f")], current="rust")
    self.assertEqual(lines[0], "|  | java | rust | rust vs java |")
    self.assertEqual(lines[2], "| CPU | 10.0 | 8.0 | -20.0% |")

  def test_expand_splices_a_list_that_is_a_whole_argument(self):
    variant = {"argv": ["run", "{labels_args}", "--x={labels_args}", "--n={job_name}"]}
    values = {"labels_args": ["--labels=a=1", "--labels=b=2"], "job_name": "j"}
    self.assertEqual(report.expand(variant, values),
                     ["run", "--labels=a=1", "--labels=b=2", "--x={labels_args}", "--n=j"])
    self.assertEqual(report.expand(variant, {"labels_args": [], "job_name": "j"}),
                     ["run", "--x={labels_args}", "--n=j"])

  def test_table_escapes_pipes(self):
    self.assertEqual(report.table(["a"], [["x|y"]])[2], "| x\\|y |")

  def test_runs_table(self):
    lines = report.runs_table(RUNS[:2], "metrics", [("cpu", "CPU", ".0f")],
                              [("Note", lambda r: r["variant"][0])])
    self.assertEqual(lines[0], "| Round | Variant | CPU | Note |")
    self.assertEqual(lines[3], "| 1 | current | 8 | c |")

  def test_write_and_load(self):
    with tempfile.TemporaryDirectory() as out, contextlib.redirect_stdout(io.StringIO()):
      report.write(out, {"medians": {}}, ["# Title"])
      self.assertEqual(report.load(os.path.join(out, "results.json")), {"medians": {}})
      with open(os.path.join(out, "report.md")) as f:
        self.assertEqual(f.read(), "# Title\n")
    self.assertIsNone(report.load(None))

if __name__ == "__main__":
  unittest.main()

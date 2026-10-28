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

import os
import tempfile
import unittest
import local_profile
import report

SUMMARY = """## Top 15 Hot Frames (Self Time, Non-Idle)

| Frame | Self CPU % |
|---|---|
| `regex_automata::hybrid::search::find_fwd` | 32.1% |
| `<a as b>::c` | 11.3% |

## Profile Execution Metrics
- **Rows Processed**: 6,270,452
- **Rust CPU Time**: 18.05 s
- **Prism CPU Time**: 1,012.55 s
- **CPU Time / 1M rows (Rust only)**: 2.88 CPU seconds
"""

class TestLocalProfile(unittest.TestCase):
  def test_parse_summary(self):
    parsed = local_profile.parse_summary(SUMMARY)
    self.assertEqual(parsed["metrics"], {
      "rows": 6270452.0, "rust_cpu_s": 18.05, "prism_cpu_s": 1012.55,
      "cpu_s_per_1m_rows": 2.88})
    self.assertEqual(parsed["hot_frames"], {
      "regex_automata::hybrid::search::find_fwd": 32.1, "<a as b>::c": 11.3})

  def test_parse_summary_without_rows(self):
    self.assertEqual(local_profile.parse_summary("## CPU Breakdown by Stage\n"),
                     {"metrics": {}, "hot_frames": {}})

  def test_count_lines(self):
    with tempfile.TemporaryDirectory() as tmp:
      for name, text in (("a.json", "1\n2\n"), ("b.json", "3\n"), ("c.txt", "x\n")):
        with open(os.path.join(tmp, name), "w") as f:
          f.write(text)
      self.assertEqual(local_profile.count_lines(os.path.join(tmp, "*.json")), 3)
      with self.assertRaises(FileNotFoundError):
        local_profile.count_lines(os.path.join(tmp, "*.csv"))

  def test_round_order_alternates(self):
    self.assertEqual(local_profile.round_order(["b", "c"], 1), ["b", "c"])
    self.assertEqual(local_profile.round_order(["b", "c"], 2), ["c", "b"])

  def test_profile_command(self):
    variant = {"label": "current", "cwd": "/repo",
               "argv": ["/repo/gradlew", ":sdks:rust:profilePrism", "-Prows={rows}",
                        "-Pout={run_dir}", "-PextraArgs=--output={run_dir}/out/counts"]}
    self.assertEqual(local_profile.profile_command(variant, "/out/r1", 7), [
        "/repo/gradlew", ":sdks:rust:profilePrism", "-Prows=7", "-Pout=/out/r1",
        "-PextraArgs=--output=/out/r1/out/counts"])
    self.assertNotIn("-Prows=None", local_profile.profile_command(variant, "/out/r1", None))
    self.assertEqual(len(local_profile.profile_command(variant, "/out/r1", None)), 4)

  def test_top_frames_ranks_by_hottest_variant(self):
    medians = {"baseline": {"hot_frames": {"a": 5.0, "b": 1.0}},
               "current": {"hot_frames": {"b": 9.0, "c": 2.0}}}
    self.assertEqual([key for key, _, _ in local_profile.top_frames(medians, 2)], ["b", "a"])

  def test_render(self):
    runs = [{"round": 1, "variant": v, **local_profile.parse_summary(SUMMARY)}
            for v in ("baseline", "current")]
    results = {"created": "now", "runs": runs,
               "config": {"describe": "Example wordcount", "input": "x", "rows": 10, "runs": 1,
                          "variants": {"baseline": "HEAD~1", "current": "HEAD"}},
               "medians": report.medians(runs, local_profile.SECTIONS)}
    text = "\n".join(local_profile.render(results, None, 1))
    self.assertIn("Example wordcount. Input `x`, 10 rows", text)
    self.assertIn("Variants: baseline=HEAD~1, current=HEAD.", text)
    self.assertIn("| Rust CPU (s) | 18.05 | 18.05 | +0.0% |", text)
    self.assertIn("| `regex_automata::hybrid::search::find_fwd` | 32.1 | 32.1 | +0.0 pp |", text)

if __name__ == "__main__":
  unittest.main()

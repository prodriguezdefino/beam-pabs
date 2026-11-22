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

import unittest
from unittest import mock
import io_report

CFG = {"pipeline": "python", "namespace": "e2e", "rows": "rows", "latency": "latency_ms",
       "buckets": "latency_le_", "streaming": True}


def user(name, namespace="e2e", **value):
  return {"name": {"origin": "user", "name": name,
                   "context": {"namespace": namespace, "step": "s"}}, **value}


def series(*values):
  return [{"metric": {"labels": {}}, "points": [
      {"interval": {"endTime": f"2026-10-28T00:0{i}:00Z"}, "value": {"doubleValue": v}}
      for i, v in enumerate(values)]}]


METRICS = [
  user("rows", scalar=600),
  user("rows", scalar=400),
  user("rows", namespace="other", scalar=99),
  {"name": {"origin": "user", "name": "rows",
            "context": {"namespace": "e2e", "tentative": "true"}}, "scalar": 5000},
  user("latency_ms", distribution={"count": 4, "sum": 400, "min": 10, "max": 250}),
  user("latency_le_50", scalar=40),
  user("latency_le_100", scalar=50),
  user("latency_le_250", scalar=8),
  user("latency_le_inf", scalar=2),
]

MESSAGES = [
  {"time": "2026-10-28T00:01:00Z", "messageText": "Starting 4 workers in us-central1-b..."},
  {"time": "2026-10-28T00:03:00Z",
   "messageText": "All workers have finished the startup processes and began to receive work "
                  "requests."},
  {"time": "2026-10-28T00:13:30Z", "messageText": "Starting worker pool teardown."},
]

LOGS = [
  {"textPayload": "Received Control stream from SDK harness sdk-0-0 in environment "
                  "ref_Environment_default_environment_1"},
  {"jsonPayload": {"message": "Received Control stream from SDK harness sdk-1-0 in environment "
                              "beam:env:docker:v1-rust_1"}},
  {"textPayload": "Received Control stream from SDK harness sdk-2-0 in environment "
                  "_sibling_java_1"},
]


class TestIoReport(unittest.TestCase):
  def test_user_values(self):
    values = io_report.user_values(METRICS, CFG)
    self.assertEqual(values["rows"], 1000)
    self.assertEqual(values["latency_mean_ms"], 100)
    self.assertEqual(values["latency_max_ms"], 250)
    self.assertEqual((values["latency_p50_ms"], values["latency_p95_ms"],
                      values["latency_p99_ms"]), (100, 250, float("inf")))

  def test_percentiles_without_counts(self):
    self.assertEqual(io_report.percentiles([0.0] * 13), [None, None, None])

  def test_processing_seconds(self):
    self.assertEqual(io_report.processing_seconds(MESSAGES), 630.0)
    self.assertIsNone(io_report.processing_seconds(MESSAGES[:2]))
    stopping = {**MESSAGES[2], "messageText": "Stopping worker pool..."}
    self.assertEqual(io_report.processing_seconds(MESSAGES[:2] + [stopping]), 630.0)

  def test_environments(self):
    self.assertEqual(io_report.environments(LOGS, "python"),
                     {"sdk-0-0": "python", "sdk-1-0": "rust"})
    self.assertEqual(io_report.environments(LOGS[:1], "java"), {"sdk-0-0": "java"})
    self.assertEqual(io_report.sdk_of("beam:env:docker:v1", "python"), "java")

  def test_summarize(self):
    details = {"job": {}, "metrics": METRICS, "messages": MESSAGES,
               "io": {"logs": LOGS, "system_lag": series(3, 40, 7), "backlog": series(9, 0)}}
    out = io_report.summarize(details, CFG, 500.0)
    self.assertEqual(out["metrics"]["rows_per_vcpu_s"], 2.0)
    self.assertEqual(out["metrics"]["processing_s"], 630.0)
    self.assertEqual(out["metrics"]["peak_system_lag_s"], 40)
    self.assertEqual(out["metrics"]["last_backlog_elements"], 0)
    # The p99 bucket has no upper bound, so it has no value.
    self.assertNotIn("latency_p99_ms", out["metrics"])
    self.assertEqual(io_report.name_containers({"sdk-1-0": 300.0, "harness": 20.0},
                                               out["environments"]),
                     {"sdk-1-0 (rust)": 300.0, "harness": 20.0})

  def test_columns(self):
    keys = [key for key, _, _ in io_report.columns({"rows": "rows"})]
    self.assertEqual(keys, ["processing_s", "rows_per_vcpu_s"])
    self.assertIn("peak_system_lag_s", [key for key, _, _ in io_report.columns(CFG)])

  def test_collect_reads_monitoring_only_for_streaming_jobs(self):
    queries = []
    details = {"job": {"id": "j", "createTime": "a", "currentStateTime": "b"}}
    with mock.patch.object(io_report, "logs", return_value=[]):
      io_report.collect("p", "t", details, {**CFG, "streaming": False}, queries.append)
      self.assertEqual(queries, [])
      out = io_report.collect("p", "t", details, CFG, lambda q: queries.append(q) or [])
    self.assertEqual(sorted(out), ["backlog", "logs", "system_lag"])
    self.assertIn('job/system_lag" AND metric.labels.job_id="j"', queries[0]["filter"])


if __name__ == "__main__":
  unittest.main()

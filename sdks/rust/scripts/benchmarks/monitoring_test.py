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

import json
import os
import unittest
import urllib.error
import monitoring

# Recorded replies of a wordcount job on one n2-standard-4 worker (100 shards).
FIXTURE = os.path.join(os.path.dirname(__file__), "testdata", "wordcount_job.json")

def load_fixture():
  with open(FIXTURE) as f:
    return json.load(f)

def series(points, **labels):
  return {"metric": {"labels": labels}, "points": [
    {"interval": {"endTime": t}, "value": {"doubleValue": v}} for t, v in points]}

class TestMonitoring(unittest.TestCase):
  def setUp(self):
    self.recorded = load_fixture()

  def test_machine_vcpus(self):
    self.assertEqual(monitoring.machine_vcpus("n2-standard-4"), 4)
    self.assertEqual(monitoring.machine_vcpus("zones/x/machineTypes/n2-custom-8-32768"), 8)
    self.assertEqual(monitoring.machine_vcpus("e2-medium"), 2)
    self.assertIsNone(monitoring.machine_vcpus("odd"))
    self.assertIsNone(monitoring.machine_vcpus(None))

  def test_used_vcpu_seconds_of_recorded_job(self):
    cpu = self.recorded["cpu"]
    utilization = sum(p["value"]["doubleValue"] for p in cpu[0]["points"])
    used = monitoring.used_vcpu_seconds(cpu, [], 4)
    self.assertAlmostEqual(used, utilization * 4 * 60)
    # Without the machine type, a VM gets the vCPUs of the job: one VM with 4 vCPUs.
    vcpus = monitoring.total_points(self.recorded["vcpus"])
    self.assertAlmostEqual(monitoring.used_vcpu_seconds(cpu, vcpus, None), used)

  def test_used_vcpu_seconds_without_data(self):
    self.assertIsNone(monitoring.used_vcpu_seconds([], [], 4))
    cpu = [series([("2026-10-28T00:31:00Z", 0.5)])]
    self.assertIsNone(monitoring.used_vcpu_seconds(cpu, [], None))

  def test_container_peaks_of_recorded_job(self):
    peaks = monitoring.container_peaks(self.recorded["memory"])
    self.assertEqual(set(peaks), {"sdk-0-0", "harness"})
    self.assertGreater(peaks["sdk-0-0"], 100)

  def test_container_peaks_take_the_largest_worker(self):
    memory = [series([("2026-10-28T00:31:00Z", 2**20)], container="sdk-0-0"),
              series([("2026-10-28T00:31:00Z", 3 * 2**20)], container="sdk-0-0")]
    self.assertEqual(monitoring.container_peaks(memory), {"sdk-0-0": 3.0})

  def test_counter_rate_of_recorded_job(self):
    rate = monitoring.counter_rate(self.recorded["counter"])
    self.assertEqual(rate["counter_total"], 62762451)
    # 10% is first reached at 00:33 (20,021,914) and 90% at 00:36 (60,434,121).
    self.assertAlmostEqual(rate["counter_per_s"], (60434121 - 20021914) / 180)

  def test_counter_rate_needs_the_expected_total(self):
    counter = self.recorded["counter"]
    self.assertEqual(monitoring.counter_rate(counter, 62762451)["counter_total"], 62762451)
    # Monitoring got only the first minutes of the counter: no rate yet.
    partial = [{**counter[0], "points": counter[0]["points"][-2:]}]
    self.assertEqual(monitoring.counter_rate(partial, 62762451), {})
    self.assertFalse(monitoring.complete({"counter": partial}, "2026-10-28T00:33:00Z", 62762451))
    self.assertTrue(monitoring.complete({"counter": counter}, "2026-10-28T00:37:08Z", 62762451))
    # 60,434,121 at 00:36 is above 90% of the total, so the last minute is not needed.
    most = [{**counter[0], "points": counter[0]["points"][1:]}]
    self.assertAlmostEqual(monitoring.counter_rate(most, 62762451)["counter_per_s"],
                           (60434121 - 20021914) / 180)

  def test_summarize_marks_an_incomplete_counter(self):
    counter = self.recorded["counter"]
    raw = {k: self.recorded[k] for k in ("cpu", "memory", "vcpus")}
    raw["counter"] = [{**counter[0], "points": counter[0]["points"][-2:]}]
    summary = monitoring.summarize(raw, "n2-standard-4", 1510, 62762451)
    self.assertEqual(summary["monitoring_missing"], ["counter"])
    self.assertEqual(summary["metrics"]["counter_total"], 62762451)
    self.assertNotIn("counter_per_s", summary["metrics"])

  def test_counter_rate_adds_the_series_of_each_step(self):
    counter = [series([("2026-10-28T00:31:00Z", 10), ("2026-10-28T00:32:00Z", 100)], s="a"),
               series([("2026-10-28T00:32:00Z", 100)], s="b")]
    self.assertEqual(monitoring.counter_rate(counter)["counter_total"], 200)
    self.assertEqual(monitoring.counter_rate([]), {})

  def test_complete(self):
    end = self.recorded["job"]["currentStateTime"]
    self.assertTrue(monitoring.complete({"cpu": self.recorded["cpu"]}, end))
    self.assertFalse(monitoring.complete({"cpu": self.recorded["cpu"], "memory": []}, end))
    self.assertFalse(monitoring.complete({"cpu": self.recorded["cpu"]}, "2026-10-28T01:00:00Z"))

  def test_collect_retries_until_complete_and_then_stops(self):
    job = self.recorded["job"]
    replies = {"cpu": [[], self.recorded["cpu"]], "memory": [[], self.recorded["memory"]],
               "vcpus": [[], self.recorded["vcpus"]]}
    calls, sleeps = [], []

    def fetch(query):
      kinds = {"cpu/utilization": "cpu", "memory/bytes_used": "memory",
               "current_num_vcpus": "vcpus"}
      name = next(v for k, v in kinds.items() if k in query["filter"])
      calls.append(name)
      return replies[name][min(calls.count(name), 2) - 1]

    raw = monitoring.collect(fetch, job["id"], job["createTime"], job["currentStateTime"], None,
                             wait_seconds=600, sleep=sleeps.append)
    self.assertEqual(len(sleeps), 1)
    self.assertEqual(raw["cpu"], self.recorded["cpu"])

  def test_collect_gives_up_after_the_wait(self):
    job = self.recorded["job"]
    sleeps = []

    def fail(query):
      raise urllib.error.URLError("down")

    raw = monitoring.collect(fail, job["id"], job["createTime"], job["currentStateTime"],
                             "line_count", wait_seconds=60, sleep=sleeps.append)
    self.assertEqual(len(sleeps), 2)
    self.assertEqual(raw, {"cpu": [], "memory": [], "vcpus": [], "counter": []})
    self.assertEqual(monitoring.summarize(raw, "n2-standard-4", 1500)["monitoring_missing"],
                     ["cpu", "memory", "vcpus", "counter"])

  def test_summarize_recorded_job(self):
    raw = {k: self.recorded[k] for k in ("cpu", "memory", "vcpus", "counter")}
    summary = monitoring.summarize(raw, "n2-standard-4", 1510)
    values = summary["metrics"]
    self.assertLess(values["used_vcpu_ratio"], 1.2)
    self.assertEqual(values["peak_vcpus"], 4)
    self.assertEqual(values["peak_sdk_memory_mib"], summary["memory"]["sdk-0-0"])
    self.assertAlmostEqual(values["counter_per_used_vcpu_s"], 62762451 / values["used_vcpu_s"])
    self.assertEqual(summary["monitoring_missing"], [])

  def test_queries_filter_by_job(self):
    queries = monitoring.queries("j1", "rows")
    self.assertEqual(set(queries), {"cpu", "memory", "vcpus", "counter"})
    self.assertIn('metadata.user_labels."dataflow_job_id"="j1"', queries["cpu"]["filter"])
    self.assertIn('metric.labels.metric_name="rows"', queries["counter"]["filter"])
    self.assertNotIn("counter", monitoring.queries("j1", None))

if __name__ == "__main__":
  unittest.main()

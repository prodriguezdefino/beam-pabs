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

import argparse
import contextlib
import datetime
import io
import json
import os
import tempfile
import unittest
import dataflow_job
import monitoring
import report

FIXTURE = os.path.join(os.path.dirname(__file__), "testdata", "wordcount_job.json")

def load_fixture():
  with open(FIXTURE) as f:
    return json.load(f)

def metric(name, scalar, **context):
  return {"name": {"origin": "dataflow/v1b3", "name": name, "context": context}, "scalar": scalar}

def user_metric(name, scalar, **context):
  return {"name": {"origin": "user", "name": name, "context": context}, "scalar": scalar}

METRICS = [
  metric("TotalVcpuTime", 1500, original_name="Service-cpu_num_seconds"),
  metric("TotalVcpuTime", 9999, original_name="Service-cpu_num_seconds", tentative="true"),
  metric("TotalMemoryUsage", 6144000),
  metric("TotalPdUsage", 37500),
  metric("TotalShuffleDataProcessed", 2.5),
  metric("ExecutionTime_ProcessElement", 510000, step="CountWords/ExtractWords"),
  metric("ExecutionTime_ProcessElement", 190000, step="ReadLines/Read"),
  metric("ExecutionTime_ProcessElement", 300000, step="ReadLines/Read", tentative="true"),
  metric("ElementCount", 1000000000, output_user_name="CountWords/ExtractWords-out0"),
  metric("ElementCount", 62000000, output_user_name="ReadLines/Read-out0"),
  user_metric("line_count", 40, step="a", namespace="wordcount"),
  user_metric("line_count", 2, step="b"),
  user_metric("line_count", 99, step="b", tentative="true"),
  {"name": {"name": "ElementCount", "context": {}}, "distribution": {}},
]

MESSAGES = [
  {"time": "2026-10-27T23:21:40.1Z", "messageImportance": "JOB_MESSAGE_DETAILED",
   "messageText": "Autoscaling: Raised the number of workers to 4 so that the pipeline can catch up."},
  {"time": "2026-10-27T23:20:40.1Z", "messageImportance": "JOB_MESSAGE_BASIC",
   "messageText": "Starting 1 workers in us-central1-b..."},
  {"time": "2026-10-27T23:25:00.1Z", "messageImportance": "JOB_MESSAGE_DETAILED",
   "messageText": "Autoscaling: Resized worker pool from 4 to 2."},
  {"time": "2026-10-27T23:20:50.1Z", "messageImportance": "JOB_MESSAGE_WARNING",
   "messageText": "A worker was unable to start up. Error: ZONE_RESOURCE_POOL_EXHAUSTED."},
]

def run(round_number, variant, platforms, peak=1.0, **fields):
  return {"round": round_number, "variant": variant, "platforms": platforms, "state": "DONE",
          "metrics": {"peak_workers": peak}, **fields}

class TestDataflowJob(unittest.TestCase):
  def test_job_id_from_log(self):
    log = ("INFO dataflow::runner: Dataflow Console: https://console.cloud.google.com/dataflow/"
           "jobs/us-central1/2026-10-27_16_20_31-14502306326069792411?project=p\n")
    self.assertEqual(dataflow_job.job_id_from_log(log), "2026-10-27_16_20_31-14502306326069792411")
    self.assertIsNone(dataflow_job.job_id_from_log("BUILD FAILED"))

  def test_parse_metrics(self):
    parsed = dataflow_job.parse_metrics(METRICS, "ExtractWords")
    self.assertEqual(parsed["steps"], {"CountWords/ExtractWords": 510.0, "ReadLines/Read": 190.0})
    values = parsed["metrics"]
    self.assertEqual(values["total_vcpu_s"], 1500)
    self.assertEqual(values["elements"], 1e9)
    self.assertAlmostEqual(values["elements_per_vcpu_s"], 1e9 / 1500)
    self.assertEqual(values["memory_gb_s"], 6000)
    self.assertEqual(values["pd_gb_s"], 37500)
    self.assertEqual(values["shuffle_gb"], 2.5)
    self.assertEqual(values["step_cpu_s"], 700.0)
    self.assertAlmostEqual(values["step_cpu_s_per_1m_elements"], 0.7)

  def test_parse_metrics_adds_the_counter_of_each_step(self):
    self.assertEqual(dataflow_job.parse_metrics(METRICS, "", "line_count")["metrics"]["counter_total"], 42)
    self.assertNotIn("counter_total", dataflow_job.parse_metrics(METRICS, "")["metrics"])

  def test_parse_metrics_without_count_output_has_no_ratios(self):
    for count_output in ("NoSuchStep", ""):
      values = dataflow_job.parse_metrics(METRICS, count_output)["metrics"]
      self.assertNotIn("elements", values)
      self.assertNotIn("step_cpu_s_per_1m_elements", values)

  def test_parse_messages(self):
    events = dataflow_job.parse_messages(MESSAGES)
    self.assertEqual(events["peak_workers"], 4)
    self.assertTrue(events["stockout"])
    self.assertFalse(events["quota"])
    self.assertEqual(events["warnings"],
                     ["23:20:50 WARNING: A worker was unable to start up. "
                      "Error: ZONE_RESOURCE_POOL_EXHAUSTED."])

  def test_summarize(self):
    job = {"id": "j1", "name": "n1", "currentState": "JOB_STATE_DONE",
           "createTime": "2026-10-27T23:20:31.990549Z", "startTime": "2026-10-27T23:20:31.990549Z",
           "currentStateTime": "2026-10-27T23:30:31.990549123Z", "labels": {"round": "1"}}
    record = dataflow_job.summarize({"job": job, "metrics": METRICS, "messages": MESSAGES},
                                    "ExtractWords")
    self.assertEqual(record["state"], "DONE")
    self.assertEqual(record["labels"], {"round": "1"})
    self.assertEqual(record["metrics"]["wall_s"], 600.0)
    self.assertEqual(record["metrics"]["peak_workers"], 4)
    self.assertEqual(record["monitoring_missing"], [])
    self.assertTrue(record["stockout"])

  def test_summarize_with_recorded_monitoring(self):
    recorded = load_fixture()
    details = {"job": recorded["job"], "metrics": METRICS, "messages": [],
               "monitoring": {k: recorded[k] for k in ("cpu", "memory", "vcpus", "counter")}}
    values = dataflow_job.summarize(details, "ExtractWords")["metrics"]
    self.assertGreater(values["used_vcpu_s"], 0)
    self.assertAlmostEqual(values["used_vcpu_ratio"], values["used_vcpu_s"] / 1500)
    self.assertEqual(values["peak_vcpus"], 4)
    self.assertEqual(values["counter_total"], 62762451)

  def test_find_job_by_name(self):
    jobs = load_fixture()["jobs_list"]["jobs"]
    name = jobs[0]["name"]
    launched = monitoring.parse_time(jobs[0]["createTime"])
    self.assertEqual(dataflow_job.find_job(jobs, name, launched)["id"], jobs[0]["id"])
    self.assertIsNone(dataflow_job.find_job(jobs, "other", launched))
    later = launched + datetime.timedelta(minutes=5)
    self.assertIsNone(dataflow_job.find_job(jobs, name, later))
    cancelled = [{**jobs[0], "currentState": "JOB_STATE_CANCELLED"}]
    self.assertIsNone(dataflow_job.find_job(cancelled, name, launched))

  def test_find_job_takes_the_newest(self):
    old = {"name": "n", "id": "a", "currentState": "JOB_STATE_FAILED",
           "createTime": "2026-10-28T00:29:36Z"}
    new = {**old, "id": "b", "currentState": "JOB_STATE_RUNNING", "createTime": "2026-10-28T00:30:00Z"}
    launched = monitoring.parse_time("2026-10-28T00:29:40Z")
    self.assertEqual(dataflow_job.find_job([old, new], "n", launched)["id"], "b")

  def test_flags(self):
    runs = [
      run(1, "baseline", ["Intel Cascade Lake"]),
      run(1, "current", ["Intel Ice Lake"], peak=3.0),
      run(2, "baseline", ["Intel Ice Lake", "Intel Cascade Lake"], quota=True),
      run(2, "current", [], state="FAILED", monitoring_missing=["cpu"]),
    ]
    self.assertEqual(dataflow_job.flags(runs), [
      "Round 1 current: autoscaled to 3 workers",
      "Round 2 baseline: quota warning",
      "Round 2 baseline: mixed CPU platforms ['Intel Ice Lake', 'Intel Cascade Lake']",
      "Round 2 current: state FAILED",
      "Round 2 current: CPU platform not captured",
      "Round 2 current: no Monitoring data for cpu",
      "Round 1: CPU platforms differ: baseline=Intel Cascade Lake, current=Intel Ice Lake",
      "Round 2: CPU platforms differ: baseline=Intel Ice Lake, Intel Cascade Lake, current=?",
    ])

  def test_flags_jobs_of_a_round_that_start_apart(self):
    runs = [run(1, "baseline", ["A"], start="2026-10-28T01:26:27Z"),
            run(1, "current", ["A"], start="2026-10-28T01:18:15.5Z"),
            run(2, "baseline", ["A"], start="2026-10-28T01:40:00Z"),
            run(2, "current", ["A"], start="2026-10-28T01:40:59Z")]
    self.assertEqual(dataflow_job.flags(runs), ["Round 1: jobs started 492 s apart"])

  def test_prepare_runs_once_for_each_variant_with_a_command(self):
    with tempfile.TemporaryDirectory() as out:
      variants = [
        {"label": "baseline", "cwd": out, "argv": ["false"],
         "prepare": ["sh", "-c", "echo built > {out_dir}/baseline.txt"]},
        {"label": "current", "cwd": out, "argv": ["false"]},
      ]
      args = argparse.Namespace(out_dir=out, dry_run=False)
      with contextlib.redirect_stdout(io.StringIO()):
        dataflow_job.prepare(args, variants)
        with open(os.path.join(out, "baseline.txt")) as f:
          self.assertEqual(f.read(), "built\n")
        variants[0]["prepare"] = ["false"]
        with self.assertRaises(RuntimeError):
          dataflow_job.prepare(args, variants)

  def test_flags_of_clean_runs_are_empty(self):
    self.assertEqual(dataflow_job.flags([run(1, "baseline", ["A"]), run(1, "current", ["A"])]), [])

  def test_run_values(self):
    values = dataflow_job.run_values("wordcountBenchmark", "Base_Line", 2, 17)
    self.assertEqual(values["job_name"], "wordcount-benchmark-base-line-r2-17")
    self.assertEqual(values["labels"],
                     "benchmark=wordcount-benchmark,variant=base-line,round=2,run_id=17")
    self.assertEqual(json.loads(values["labels_json"])["variant"], "base-line")

  def test_launch_command_from_variant(self):
    variant = {"label": "current", "cwd": "/repo",
               "argv": ["/repo/gradlew", "-PextraArgs=--job_name={job_name} --labels={labels}"]}
    values = dataflow_job.run_values("t", "current", 1, 5)
    self.assertEqual(report.expand(variant, values)[1],
                     "-PextraArgs=--job_name=t-current-r1-5 "
                     "--labels=benchmark=t,variant=current,round=1,run_id=5")

  def test_render(self):
    runs = [{**run(1, "current", ["A"]), **dataflow_job.parse_metrics(METRICS, "ExtractWords"),
             "memory": {"sdk-0-0": 900.0}, "job_id": "j1"}]
    results = {"task": "wordcountBenchmark", "created": "now",
               "config": {"description": "100 shards", "count_name": "words",
                          "counter": "line_count", "variants": {"current": "abc"}},
               "runs": runs, "medians": report.medians(runs, dataflow_job.SECTIONS), "flags": []}
    text = "\n".join(dataflow_job.render(results, None))
    self.assertIn("| Step CPU s / 1M words | 0.700 |", text)
    self.assertIn("| Words per vCPU·s | 666,667 |", text)
    self.assertIn("| line_count/s (10-90%) | - |", text)
    self.assertIn("| CountWords/ExtractWords | 510.0 |", text)
    self.assertIn("| sdk-0-0 | 900 |", text)
    self.assertIn("## Flags\n\nNone.", text)

class PlatformAndCapacityTest(unittest.TestCase):

  def test_unknown_platform_is_skipped(self):
    self.assertEqual(
        dataflow_job.known_platforms(["Intel Cascade Lake", "Unknown CPU Platform", ""]),
        {"Intel Cascade Lake"})

  def test_startup_failure_counts_as_stockout_only(self):
    events = dataflow_job.parse_messages([{
        "time": "2026-10-28T00:30:00Z",
        "messageImportance": "JOB_MESSAGE_ERROR",
        "messageText": "Startup of the worker pool failed. This is likely a quota issue or a "
                       "Compute Engine stockout.",
    }])
    self.assertTrue(events["stockout"])
    self.assertFalse(events["quota"])


if __name__ == "__main__":
  unittest.main()

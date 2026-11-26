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
import datetime
import io
import json
import os
import subprocess
import unittest
import urllib.error
from unittest import mock

import dataflow_api
import monitoring

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


class TestDataflowApi(unittest.TestCase):
  def test_access_token_and_get(self):
    with mock.patch.object(dataflow_api.subprocess, "run") as run_cmd:
      run_cmd.return_value = subprocess.CompletedProcess([], 0, stdout="tok-1\n")
      self.assertEqual(dataflow_api.access_token(), "tok-1")
      run_cmd.assert_called_once_with(["gcloud", "auth", "print-access-token"], check=True,
                                      capture_output=True, text=True)

    def fake_urlopen(request):
      self.assertEqual(request.full_url, "https://example.test/jobs")
      self.assertEqual(request.get_header("Authorization"), "Bearer tok-1")
      return io.StringIO('{"jobs": [{"id": "j1"}]}')

    with mock.patch.object(dataflow_api.urllib.request, "urlopen", side_effect=fake_urlopen):
      self.assertEqual(dataflow_api.get("https://example.test/jobs", "tok-1"),
                       {"jobs": [{"id": "j1"}]})

  def test_list_jobs_and_get_job(self):
    base = "https://dataflow.googleapis.com/v1b3/projects/p/locations/us-central1/jobs"
    with mock.patch.object(dataflow_api, "access_token", return_value="tok"):
      with mock.patch.object(dataflow_api, "get",
                             return_value={"jobs": [{"id": "j1"}]}) as get_fn:
        self.assertEqual(dataflow_api.list_jobs("p", "us-central1", "bench-r1"), [{"id": "j1"}])
        get_fn.assert_called_once_with(f"{base}?filter=ALL&name=bench-r1&pageSize=50", "tok")
      with mock.patch.object(dataflow_api, "get", return_value={"id": "j1"}) as get_fn:
        self.assertEqual(dataflow_api.get_job("p", "us-central1", "j1"), {"id": "j1"})
        get_fn.assert_called_once_with(f"{base}/j1", "tok")
      with mock.patch.object(dataflow_api, "get", side_effect=urllib.error.URLError("down")):
        self.assertEqual(dataflow_api.list_jobs("p", "us-central1", "bench-r1"), [])
        self.assertIsNone(dataflow_api.get_job("p", "us-central1", "j1"))

  def test_fetch_reads_all_message_pages(self):
    base = "https://dataflow.googleapis.com/v1b3/projects/p/locations/us-central1/jobs/j1"
    replies = {
        f"{base}/messages?minimumImportance=JOB_MESSAGE_DETAILED&pageSize=500": {
            "jobMessages": [{"id": "m1"}], "nextPageToken": "p2"
        },
        f"{base}/messages?minimumImportance=JOB_MESSAGE_DETAILED&pageSize=500&pageToken=p2": {
            "jobMessages": [{"id": "m2"}]
        },
        f"{base}?view=JOB_VIEW_DESCRIPTION": {"id": "j1"},
        f"{base}/metrics": {"metrics": [{"scalar": 1}]},
    }
    with mock.patch.object(dataflow_api, "access_token", return_value="tok") as token_fn, \
        mock.patch.object(dataflow_api, "get", side_effect=lambda url, t: replies[url]):
      self.assertEqual(dataflow_api.fetch("p", "us-central1", "j1"), {
          "job": {"id": "j1"},
          "metrics": [{"scalar": 1}],
          "messages": [{"id": "m1"}, {"id": "m2"}],
      })
      token_fn.assert_called_once_with()

  def test_counter_value(self):
    with mock.patch.object(dataflow_api, "access_token", return_value="tok"):
      with mock.patch.object(dataflow_api, "get", return_value={"metrics": METRICS}):
        self.assertEqual(dataflow_api.counter_value("p", "r", "j1", "line_count", "wordcount"),
                         40.0)
        self.assertEqual(dataflow_api.counter_value("p", "r", "j1", "missing"), 0.0)
      with mock.patch.object(dataflow_api, "get", side_effect=urllib.error.URLError("down")):
        self.assertIsNone(dataflow_api.counter_value("p", "r", "j1", "line_count"))

  def test_drain_and_worker_vms(self):
    with mock.patch.object(dataflow_api.subprocess, "run") as run_cmd:
      dataflow_api.drain("p", "us-central1", "j1")
      run_cmd.assert_called_once_with(
          ["gcloud", "dataflow", "jobs", "drain", "j1", "--project=p", "--region=us-central1"],
          check=False)

    with mock.patch.object(dataflow_api.subprocess, "run") as run_cmd:
      run_cmd.return_value = subprocess.CompletedProcess(
          [], 0, stdout="Intel Ice Lake\nUnknown CPU Platform\n")
      self.assertEqual(dataflow_api.cpu_platforms("p", "bench-r1"), {"Intel Ice Lake"})
      run_cmd.assert_called_once_with(dataflow_api.platform_command("p", "bench-r1"),
                                      capture_output=True, text=True)

    with contextlib.redirect_stdout(io.StringIO()), \
        mock.patch.object(dataflow_api.subprocess, "run") as run_cmd:
      run_cmd.side_effect = [
          subprocess.CompletedProcess([], 0, stdout="worker-1 us-central1-b\n"),
          subprocess.CompletedProcess([], 0, stdout=""),
      ]
      dataflow_api.kill_worker("p", "j1")
      self.assertEqual(run_cmd.call_args_list[1], mock.call(
          ["gcloud", "compute", "instances", "delete", "worker-1", "--project=p",
           "--zone=us-central1-b", "--quiet"], check=False))

    with contextlib.redirect_stdout(io.StringIO()), \
        mock.patch.object(dataflow_api.subprocess, "run") as run_cmd:
      run_cmd.return_value = subprocess.CompletedProcess([], 0, stdout="")
      dataflow_api.kill_worker("p", "j1")
      self.assertEqual(run_cmd.call_count, 1)

  def test_find_job_by_name(self):
    jobs = load_fixture()["jobs_list"]["jobs"]
    name = jobs[0]["name"]
    launched = monitoring.parse_time(jobs[0]["createTime"])
    self.assertEqual(dataflow_api.find_job(jobs, name, launched)["id"], jobs[0]["id"])
    self.assertIsNone(dataflow_api.find_job(jobs, "other", launched))
    later = launched + datetime.timedelta(minutes=5)
    self.assertIsNone(dataflow_api.find_job(jobs, name, later))
    cancelled = [{**jobs[0], "currentState": "JOB_STATE_CANCELLED"}]
    self.assertIsNone(dataflow_api.find_job(cancelled, name, launched))

  def test_find_job_takes_the_newest(self):
    old = {"name": "n", "id": "a", "currentState": "JOB_STATE_FAILED",
           "createTime": "2026-10-28T00:29:36Z"}
    new = {**old, "id": "b", "currentState": "JOB_STATE_RUNNING",
           "createTime": "2026-10-28T00:30:00Z"}
    launched = monitoring.parse_time("2026-10-28T00:29:40Z")
    self.assertEqual(dataflow_api.find_job([old, new], "n", launched)["id"], "b")

  def test_job_id_from_log(self):
    log = ("INFO dataflow::runner: Dataflow Console: https://console.cloud.google.com/dataflow/"
           "jobs/us-central1/2026-10-27_16_20_31-14502306326069792411?project=p\n")
    self.assertEqual(dataflow_api.job_id_from_log(log), "2026-10-27_16_20_31-14502306326069792411")
    self.assertEqual(dataflow_api.job_id_from_log("info\nJOB_ID=2026-11-11_00_01\n"),
                     "2026-11-11_00_01")
    self.assertIsNone(dataflow_api.job_id_from_log("BUILD FAILED"))

  def test_parse_metrics(self):
    parsed = dataflow_api.parse_metrics(METRICS, "ExtractWords")
    self.assertEqual(parsed["steps"], {"CountWords/ExtractWords": 510.0, "ReadLines/Read": 190.0})
    values = parsed["metrics"]
    self.assertEqual(values["total_vcpu_s"], 1500)
    self.assertEqual(values["elements"], 1e9)
    self.assertAlmostEqual(values["elements_per_vcpu_s"], 1e9 / 1500)
    self.assertEqual(values["memory_gb_s"], 6000)
    self.assertEqual(values["pd_gb_s"], 37500)
    self.assertEqual(values["shuffle_gb"], 2.5)
    self.assertEqual(values["step_wall_s"], 700.0)
    self.assertAlmostEqual(values["step_wall_s_per_1m_elements"], 0.7)

  def test_parse_metrics_adds_the_counter_of_each_step(self):
    self.assertEqual(dataflow_api.parse_metrics(METRICS, "", "line_count")["metrics"]
                     ["counter_total"], 42)
    self.assertNotIn("counter_total", dataflow_api.parse_metrics(METRICS, "")["metrics"])

  def test_counter_total_counts_one_namespace(self):
    metrics = [user_metric("rows", 10, step="a", namespace="e2e"),
               user_metric("rows", 7, step="b", namespace="bigquery")]
    self.assertEqual(dataflow_api.parse_metrics(metrics, "", "rows", "e2e")["metrics"]
                     ["counter_total"], 10)
    self.assertEqual(dataflow_api.parse_metrics(metrics, "", "rows")["metrics"]
                     ["counter_total"], 17)

  def test_parse_metrics_without_count_output_has_no_ratios(self):
    for count_output in ("NoSuchStep", ""):
      values = dataflow_api.parse_metrics(METRICS, count_output)["metrics"]
      self.assertNotIn("elements", values)
      self.assertNotIn("step_wall_s_per_1m_elements", values)

  def test_parse_messages(self):
    events = dataflow_api.parse_messages(MESSAGES)
    self.assertEqual(events["peak_workers"], 4)
    self.assertTrue(events["stockout"])
    self.assertFalse(events["quota"])
    self.assertEqual(events["warnings"],
                     ["23:20:50 WARNING: A worker was unable to start up. "
                      "Error: ZONE_RESOURCE_POOL_EXHAUSTED."])

  def test_startup_failure_counts_as_stockout_only(self):
    events = dataflow_api.parse_messages([{
        "time": "2026-10-28T00:30:00Z",
        "messageImportance": "JOB_MESSAGE_ERROR",
        "messageText": "Startup of the worker pool failed. This is likely a quota issue or a "
                       "Compute Engine stockout.",
    }])
    self.assertTrue(events["stockout"])
    self.assertFalse(events["quota"])

  def test_unknown_platform_is_skipped(self):
    self.assertEqual(
        dataflow_api.known_platforms(["Intel Cascade Lake", "Unknown CPU Platform", ""]),
        {"Intel Cascade Lake"})

  def test_machine_type(self):
    job = {"environment": {"workerPools": [{}, {"machineType": "n2-standard-4"}]}}
    self.assertEqual(dataflow_api.machine_type(job), "n2-standard-4")
    self.assertIsNone(dataflow_api.machine_type({}))


if __name__ == "__main__":
  unittest.main()

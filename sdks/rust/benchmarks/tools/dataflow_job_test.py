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
import io
import json
import os
import tempfile
import unittest
from unittest import mock

import dataflow_api
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

  def test_collect_monitoring_passes_counter_total_from_api(self):
    details = {
        "job": {"id": "j1", "createTime": "t0", "currentStateTime": "t1"},
        "metrics": METRICS,
    }
    with mock.patch.object(dataflow_api, "access_token", return_value="tok"), \
        mock.patch.object(monitoring, "collect", return_value={"cpu": []}) as collect_fn:
      self.assertEqual(
          dataflow_job.collect_monitoring("p", details, "line_count", 60, "wordcount"),
          {"cpu": []})
      self.assertEqual(collect_fn.call_args[0][1:], ("j1", "t0", "t1", "line_count", 60, 40.0))

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
    self.assertEqual(dataflow_job.flags([run(1, "current", ["A"], state="DRAINED")]), [])

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

  def test_run_values_with_variant_labels_and_a_prefix(self):
    values = dataflow_job.run_values("bigQueryBenchmark", "java-native", 1, 9,
                                     {"shape": "per_key", "benchmark": "x"}, "bq-java-count")
    self.assertEqual(values["job_name"], "bq-java-count-java-native-r1-9")
    self.assertEqual(values["job_name_underscore"], "bq_java_count_java_native_r1_9")
    self.assertEqual(values["labels"], "shape=per-key,benchmark=big-query-benchmark,"
                                       "variant=java-native,round=1,run_id=9")
    self.assertEqual(values["labels_args"][0], "--labels=shape=per-key")
    self.assertEqual(json.loads(values["labels_json"])["shape"], "per-key")
    variant = {"argv": ["python", "{labels_args}", "--labels={labels_json}"]}
    self.assertEqual(len(report.expand(variant, values)), 1 + 5 + 1)

  def test_job_name_fits_63_characters_and_keeps_the_round(self):
    name = dataflow_job.run_values("t", "x" * 80, 2, 1761700000, prefix="bq-java-copy")["job_name"]
    self.assertEqual(len(name), 63)
    self.assertTrue(name.endswith("x-r2-1761700000"))

  def test_flags_take_the_workers_of_the_job(self):
    runs = [run(1, "rust", ["A"], peak=4.0), run(1, "java", ["A"], peak=5.0)]
    self.assertEqual(dataflow_job.flags(runs, 4), ["Round 1 java: autoscaled to 5 workers"])

  def test_render_with_io_metrics_and_a_named_variant(self):
    runs = [{**run(1, "java", ["A"]), "metrics": {"processing_s": 100.0}},
            {**run(1, "rust", ["A"]), "metrics": {"processing_s": 80.0}}]
    results = {"task": "bigQueryBenchmark", "created": "now",
               "config": {"description": "d", "current": "rust", "io": {"rows": "rows"},
                          "variants": {"java": "reader java", "rust": "reader rust"}},
               "runs": runs, "medians": report.medians(runs, dataflow_job.SECTIONS), "flags": []}
    text = "\n".join(dataflow_job.render(results, None))
    self.assertIn("| Processing time (s) | 100 | 80 | -20.0% |", text)
    self.assertIn("rust vs java", text)

  def test_summarize_with_io(self):
    job = {"id": "j1", "currentState": "JOB_STATE_DONE", "createTime": "2026-10-27T23:20:31Z",
           "currentStateTime": "2026-10-27T23:30:31Z"}
    io_cfg = {"pipeline": "python", "namespace": "wordcount", "rows": "line_count"}
    record = dataflow_job.summarize({"job": job, "metrics": METRICS, "messages": MESSAGES},
                                    "", None, io_cfg)
    self.assertAlmostEqual(record["metrics"]["rows_per_vcpu_s"], 40 / 1500)

  def test_render(self):
    runs = [{**run(1, "current", ["A"]), **dataflow_api.parse_metrics(METRICS, "ExtractWords"),
             "memory": {"sdk-0-0": 900.0}, "job_id": "j1"}]
    results = {"task": "wordcountBenchmark", "created": "now",
               "config": {"description": "100 shards", "count_name": "words",
                          "counter": "line_count", "variants": {"current": "abc"}},
               "runs": runs, "medians": report.medians(runs, dataflow_job.SECTIONS), "flags": []}
    text = "\n".join(dataflow_job.render(results, None))
    self.assertIn("| Step wall time (s) | 700.0 |", text)
    self.assertIn("| Step wall s / 1M words | 0.700 |", text)
    self.assertIn("| Words per vCPU·s | 666,667 |", text)
    self.assertIn("| line_count/s (10-90%) | - |", text)
    self.assertIn("| CountWords/ExtractWords | 510.0 |", text)
    self.assertIn("| sdk-0-0 | 900 |", text)
    self.assertIn("## Flags\n\nNone.", text)
    self.assertNotIn("## Variant settings", text)

  def test_render_variant_settings_table(self):
    runs = [run(1, "python", ["A"]), run(1, "rust", ["A"])]
    results = {"task": "bigQueryBenchmark", "created": "now",
               "config": {"description": "d", "current": "rust",
                          "variants": {"python": "reader python", "rust": "reader rust"},
                          "variant_settings": {
                              "python": {"pipeline": "python", "shape": "count", "output": "dict"},
                              "rust": {"pipeline": "python", "shape": "count", "output": "row",
                                       "format": "arrow"},
                          }},
               "runs": runs, "medians": report.medians(runs, dataflow_job.SECTIONS), "flags": []}
    text = "\n".join(dataflow_job.render(results, None))
    self.assertIn("## Variant settings", text)
    self.assertIn("| Setting | python | rust |", text)
    self.assertIn("| pipeline | python | python |", text)
    self.assertIn("| shape | count | count |", text)
    self.assertIn("| output | dict | row |", text)
    self.assertIn("| format | - | arrow |", text)

  def test_flags_fairness_mismatches_in_a_round(self):
    matching = [
        run(1, "java-native", ["A"], peak=4.0,
            settings={"shape": "count", "format": "arrow", "output": "row",
                      "scaling": "fixed", "workers": "4", "max_workers": "4",
                      "machine": "n2-standard-4"}),
        run(1, "rust", ["A"], peak=4.0,
            settings={"shape": "count", "format": "arrow", "output": "row",
                      "scaling": "fixed", "workers": "4", "max_workers": "4",
                      "machine": "n2-standard-4"}),
    ]
    self.assertEqual(dataflow_job.flags(matching, workers=4), [])

    mismatched = [
        run(1, "java", ["A"], peak=4.0,
            labels={"shape": "count", "scaling": "fixed"},
            settings={"shape": "count", "format": "avro", "output": "tablerow",
                      "flatten": "false", "scaling": "fixed", "workers": "4",
                      "max_workers": "4", "machine": "n2-standard-4"}),
        run(1, "rust", ["A"], peak=4.0,
            labels={"shape": "per_key", "scaling": "fixed"},
            settings={"shape": "per_key", "format": "arrow", "output": "row",
                      "flatten": "true", "scaling": "autoscaling", "workers": "1",
                      "max_workers": "10", "machine": "n2-standard-8"}),
    ]
    self.assertEqual(dataflow_job.flags(mismatched, workers=4), [
        "Round 1: shape differs: java=count, rust=per_key",
        "Round 1: format differs: java=avro, rust=arrow",
        "Round 1: output differs: java=tablerow, rust=row",
        "Round 1: flatten differs: java=false, rust=true",
        "Round 1: scaling differs: java=fixed, rust=autoscaling",
        "Round 1: workers differs: java=4, rust=1",
        "Round 1: max_workers differs: java=4, rust=10",
        "Round 1: machine differs: java=n2-standard-4, rust=n2-standard-8",
    ])

    partial = [
        run(1, "python", ["A"], settings={"shape": "count", "output": "dict"}),
        run(1, "rust", ["A"], settings={"shape": "count", "output": "row", "format": "arrow"}),
    ]
    self.assertEqual(dataflow_job.flags(partial), [
        "Round 1: output differs: python=dict, rust=row",
    ])

    from_labels = [
        run(1, "java", ["A"], labels={"shape": "load", "scaling": "fixed"}),
        run(1, "rust", ["A"], labels={"shape": "load", "scaling": "autoscaling"}),
    ]
    self.assertEqual(dataflow_job.flags(from_labels), [
        "Round 1: scaling differs: java=fixed, rust=autoscaling",
    ])


class CompanionTest(unittest.TestCase):
  """The companion producer of a round: errors, failed rounds and the shared mode."""

  def setUp(self):
    self.saved = (dataflow_job.launch, dataflow_job.launch_companion,
                  dataflow_job.SHARED_POLL_SECONDS)
    dataflow_job.SHARED_POLL_SECONDS = 0.01

  def tearDown(self):
    (dataflow_job.launch, dataflow_job.launch_companion,
     dataflow_job.SHARED_POLL_SECONDS) = self.saved

  def running_launch(self):
    def fake_launch(args, variant, round_number, run_id, on_running=lambda: None,
                    producer_done_at=lambda: None):
      on_running()
      while producer_done_at() is None:
        dataflow_job.time.sleep(0.005)
      return {"round": round_number, "variant": variant["label"]}
    return fake_launch

  def test_run_round_starts_producer_once_variants_run(self):
    events = []

    def fake_launch(args, variant, round_number, run_id, on_running=lambda: None,
                    producer_done_at=lambda: None):
      events.append(("start", variant["label"]))
      on_running()
      while producer_done_at() is None:
        dataflow_job.time.sleep(0.005)
      events.append(("after_producer", variant["label"]))
      return {"round": round_number, "variant": variant["label"]}

    def fake_companion(args, producer, round_number, run_id):
      events.append(("producer", producer["label"]))
      return {"round": round_number, "variant": producer["label"], "job_id": "p1",
              "rows": 10.0, "ended": dataflow_job.time.time()}

    dataflow_job.launch = fake_launch
    dataflow_job.launch_companion = fake_companion
    records, companion = dataflow_job.run_round(argparse.Namespace(), [{"label": "rust"}],
                                                {"label": "produce"}, 1, 99)
    self.assertEqual(events, [("start", "rust"), ("producer", "produce"),
                              ("after_producer", "rust")])
    self.assertEqual(records, [{"round": 1, "variant": "rust"}])
    self.assertEqual(companion["job_id"], "p1")

  def test_a_failed_companion_fails_the_round(self):
    dataflow_job.launch = self.running_launch()
    dataflow_job.launch_companion = lambda *a: {"round": 1, "variant": "producer",
                                                "ended": dataflow_job.time.time(),
                                                "error": "companion producer failed to launch"}
    records, companion = dataflow_job.run_round(argparse.Namespace(), [{"label": "rust"}],
                                                {"label": "producer"}, 1, 5)
    self.assertTrue(records[0]["round_failed"])
    self.assertEqual(records[0]["error"], "companion producer failed to launch")
    self.assertEqual(companion["variant"], "producer")
    self.assertEqual(dataflow_job.measured(records), [])
    self.assertIn("Round 1: failed, not in the medians", dataflow_job.flags(records))

  def test_an_exception_in_the_companion_ends_the_round(self):
    dataflow_job.launch = self.running_launch()

    def broken(*args):
      raise OSError("no java")

    dataflow_job.launch_companion = broken
    records, companion = dataflow_job.run_round(argparse.Namespace(), [{"label": "rust"}],
                                                {"label": "producer"}, 1, 5)
    self.assertEqual(companion["error"], "companion producer failed: no java")
    self.assertTrue(records[0]["round_failed"])

  def test_a_launch_that_fails_before_running_does_not_block_the_round(self):
    def failing(args, variant, round_number, run_id, on_running=lambda: None,
                producer_done_at=lambda: None):
      raise OSError("no gcloud")

    dataflow_job.launch = failing
    dataflow_job.launch_companion = lambda *a: self.fail("no job runs, so no companion")
    with self.assertRaises(OSError):
      dataflow_job.run_round(argparse.Namespace(), [{"label": "rust"}], {"label": "p"}, 1, 5)

  def test_shared_producer_launches_once_and_writes_its_record(self):
    with tempfile.TemporaryDirectory() as shared:
      args = argparse.Namespace(shared_producer=shared, shared_tasks=1)
      dataflow_job.launch_companion = lambda *a: {"round": 1, "variant": "producer",
                                                  "job_id": "p1", "ended": 100.0}
      record = dataflow_job.shared_companion(args, {"label": "producer"}, 1, 5)
      self.assertEqual(record["job_id"], "p1")
      with open(os.path.join(shared, "r1", "done.json")) as f:
        self.assertEqual(json.load(f)["job_id"], "p1")
      dataflow_job.leave_shared(shared, 1)
      self.assertFalse(os.path.exists(os.path.join(shared, "r1")))

  def test_shared_producer_member_reads_the_record_of_the_owner(self):
    with tempfile.TemporaryDirectory() as shared:
      root = os.path.join(shared, "r1")
      os.makedirs(os.path.join(root, "owner"))
      args = argparse.Namespace(shared_producer=shared, shared_tasks=2)
      dataflow_job.launch_companion = lambda *a: self.fail("only the owner launches")

      def owner_ends(seconds):
        with open(os.path.join(root, "done.json"), "w") as f:
          json.dump({"round": 1, "variant": "producer", "job_id": "p9", "ended": 50.0}, f)

      record = dataflow_job.shared_companion(args, {"label": "producer"}, 1, 5,
                                             sleep=owner_ends)
      self.assertEqual(record["job_id"], "p9")

  def test_shared_producer_that_ran_before_is_an_error(self):
    with tempfile.TemporaryDirectory() as shared:
      os.makedirs(os.path.join(shared, "r1", "members"))
      open(os.path.join(shared, "r1", "done.json"), "w").close()
      args = argparse.Namespace(shared_producer=shared, shared_tasks=2)
      record = dataflow_job.shared_companion(args, {"label": "producer"}, 1, 5)
      self.assertIn("use a new -PsharedProducer", record["error"])

  def test_shared_producer_fails_when_the_other_tasks_do_not_join(self):
    with tempfile.TemporaryDirectory() as shared:
      args = argparse.Namespace(shared_producer=shared, shared_tasks=2)
      clock = {"now": 0.0}

      def sleep(seconds):
        clock["now"] += dataflow_job.SHARED_JOIN_SECONDS

      dataflow_job.launch_companion = lambda *a: self.fail("one task never launches")
      record = dataflow_job.shared_companion(args, {"label": "producer"}, 1, 5, sleep=sleep,
                                             clock=lambda: clock["now"])
      self.assertEqual(record["error"], "1 of 2 tasks joined the shared producer")

  def test_render_lists_the_companion(self):
    runs = [run(1, "rust", ["A"])]
    results = {"task": "kafkaBenchmark", "created": "now",
               "config": {"description": "d", "variants": {"rust": "r"}},
               "runs": runs, "medians": report.medians(runs, dataflow_job.SECTIONS),
               "flags": [], "companions": [{"round": 1, "job_id": "p1", "state": "DRAINED",
                                            "rows": 1800000.0,
                                            "start": "2026-11-11T10:00:00Z",
                                            "end": "2026-11-11T10:30:00Z"}]}
    text = "\n".join(dataflow_job.render(results, None))
    self.assertIn("## Companion producer\n\n- Round 1: job p1, state DRAINED, 1,800,000 rows, "
                  "2026-11-11T10:00:00 to 2026-11-11T10:30:00", text)


class NamespaceAndDrainTest(unittest.TestCase):
  def test_summarize_takes_the_namespace_of_io(self):
    job = {"id": "j1", "currentState": "JOB_STATE_DONE", "createTime": "2026-10-27T23:20:31Z",
           "currentStateTime": "2026-10-27T23:30:31Z"}
    metrics = METRICS + [user_metric("line_count", 1000, step="c", namespace="other")]
    io_cfg = {"pipeline": "python", "namespace": "wordcount", "rows": "line_count"}
    record = dataflow_job.summarize({"job": job, "metrics": metrics, "messages": []},
                                    "", "line_count", io_cfg)
    self.assertEqual(record["metrics"]["counter_total"], 40)

  def test_poll_running_drain_waits_for_running_window_once(self):
    state = {"running_since": None, "drained": False}
    notified, drained = [], []
    pending = {"id": "j1", "currentState": "JOB_STATE_PENDING"}
    running = {"id": "j1", "currentState": "JOB_STATE_RUNNING"}

    dataflow_job.poll_running_drain(state, pending, 240, "n1", lambda: notified.append(1),
                                    drained.append, 100.0)
    self.assertIsNone(state["running_since"])
    self.assertEqual((notified, drained), ([], []))

    dataflow_job.poll_running_drain(state, running, 240, "n1", lambda: notified.append(1),
                                    drained.append, 120.0)
    self.assertEqual(state["running_since"], 120.0)
    self.assertEqual((notified, drained), ([1], []))

    dataflow_job.poll_running_drain(state, running, 240, "n1", lambda: notified.append(1),
                                    drained.append, 359.0)
    self.assertEqual(drained, [])

    dataflow_job.poll_running_drain(state, running, 240, "n1", lambda: notified.append(1),
                                    drained.append, 360.0)
    self.assertTrue(state["drained"])
    self.assertEqual((notified, drained), ([1], ["j1"]))

    dataflow_job.poll_running_drain(state, running, 240, "n1", lambda: notified.append(1),
                                    drained.append, 400.0)
    self.assertEqual(drained, ["j1"])


if __name__ == "__main__":
  unittest.main()

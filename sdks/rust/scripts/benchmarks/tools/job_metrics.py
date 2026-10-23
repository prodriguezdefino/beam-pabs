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
#
"""Prints the resource metrics of the BigQuery benchmark jobs as Markdown tables.

Usage: python tools/job_metrics.py --project=<project> [--rows=<n>] [--steps] JOB_ID [JOB_ID ...]

Processing time runs from the first worker that receives work to the start of the worker
pool teardown, so it leaves out VM startup and SDK container startup. Memory is the memory
that each SDK container uses, from Cloud Monitoring, as the peak and the average over the
job, for each worker.

``--steps`` also prints the execution time of each step, summed over all workers, from the
``ExecutionTime_*`` metrics that the SDK workers report. The Python worker counts the time
that it decodes elements from the data plane in the first step that receives them.

``--streaming`` multiplies the CPU utilization of each worker VM by its vCPUs. The vCPUs come
from ``--vcpus-per-worker``, else from the worker machine type of the job, else from the vCPUs
of the job divided by its VMs in each minute.
"""

import argparse
import collections
import datetime
import json
import re
import subprocess
import sys
import urllib.parse
import urllib.request

SERVICE_METRICS = {
    "TotalVcpuTime": "vCPU·s",
    "TotalPdUsage": "Disk (GB·s)",
}
WORK_STARTED = "began to receive work requests"
TEARDOWN = "Starting worker pool teardown"
RESIZE = re.compile(r"(?:Resizing worker pool from \d+ to|Starting) (\d+)(?: workers)?")
ENVIRONMENT = re.compile(r"SDK harness (sdk-\d+-\d+) in environment (\S+)")


def gcloud(*args):
  out = subprocess.run(["gcloud", *args, "--format=json"],
                       check=True,
                       capture_output=True,
                       text=True).stdout
  return json.loads(out)


def timestamp(value):
  return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def job_logs(project, job_id, query):
  return gcloud("logging", "read",
                f'resource.type="dataflow_step" AND resource.labels.job_id="{job_id}" '
                f"AND ({query})", f"--project={project}", "--limit=1000")


def message(entry):
  return entry.get("textPayload") or entry.get("jsonPayload", {}).get("message", "")


def processing(project, job_id):
  """Returns (processing seconds, peak workers) from the job messages."""
  entries = job_logs(
      project, job_id,
      f'textPayload:"{WORK_STARTED}" OR textPayload:"{TEARDOWN}" OR '
      'textPayload:"Resizing worker pool" OR textPayload:"workers in"')
  started = [timestamp(e["timestamp"]) for e in entries if WORK_STARTED in message(e)]
  teardown = [timestamp(e["timestamp"]) for e in entries if TEARDOWN in message(e)]
  sizes = [int(m.group(1)) for e in entries if (m := RESIZE.search(message(e)))]
  seconds = (min(teardown) - min(started)).total_seconds() if started and teardown else None
  return seconds, max(sizes + [1])


def environments(project, job_id, pipeline):
  """Maps each SDK container (sdk-N-0) to the short name of its environment.

  ``pipeline`` is the SDK of the pipeline, which owns the default environment.
  """
  entries = job_logs(
      project, job_id, 'textPayload:"Received Control stream from SDK harness" OR '
      'jsonPayload.message:"Received Control stream from SDK harness"')
  names = {}
  for entry in entries:
    match = ENVIRONMENT.search(message(entry))
    if match and "_sibling_" not in message(entry):
      environment = match.group(2)
      if "rust" in environment or pipeline == "rust":
        environment = "rust"
      elif pipeline == "java":
        environment = "java"
      elif "default_environment" in environment:
        environment = "python"
      elif "java" in environment or "beam:env" in environment:
        environment = "java"
      names[match.group(1)] = environment
  return names


def container_memory(project, job_id, start, end):
  """Returns {container: (peak bytes, mean bytes)} for the SDK containers of a job."""
  series = monitoring(
      project, {
          "filter": 'metric.type="dataflow.googleapis.com/worker/memory/bytes_used" '
                    f'AND resource.labels.job_id="{job_id}"',
          "interval.startTime": start,
          "interval.endTime": end,
      })
  values = collections.defaultdict(list)
  for ts in series:
    container = ts["metric"]["labels"].get("container", "")
    if container.startswith("sdk-"):
      values[container] += [point_value(p) for p in ts["points"]]
  return {c: (max(v), sum(v) / len(v)) for c, v in values.items() if v}


# Upper bounds (ms) of the e2e/latency_le_<bound> buckets. Keep in sync with
# LATENCY_BOUNDS_MS in KafkaBenchmark.java and kafka_benchmark.py.
LATENCY_BOUNDS_MS = (50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000,
                     300_000)


def monitoring(project, query):
  """Returns the time series of a Cloud Monitoring query (a dict of request parameters).

  Reads every page of the response.
  """
  token = subprocess.run(
      ["gcloud", "auth", "application-default", "print-access-token"],
      check=True,
      capture_output=True,
      text=True).stdout.strip()
  series, page_token = [], None
  while True:
    page_query = {**query, "pageToken": page_token} if page_token else query
    request = urllib.request.Request(
        f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries?"
        f"{urllib.parse.urlencode(page_query)}",
        headers={
            "Authorization": f"Bearer {token}",
            "x-goog-user-project": project
        })
    with urllib.request.urlopen(request) as response:
      page = json.load(response)
    series += page.get("timeSeries", [])
    page_token = page.get("nextPageToken")
    if not page_token:
      return series


def point_value(point):
  value = point["value"]
  return float(value.get("doubleValue", value.get("int64Value", 0)))


def points_of(ts):
  """The [(time, value)] of one time series, in time order."""
  return sorted((timestamp(p["interval"]["endTime"]), point_value(p)) for p in ts["points"])


def user_counter_series(project, job_id, start, end):
  """Returns {metric_name: [[(time, value)] of each series]} of the user counters of a job.

  A counter can have several series, for example one for each step. Each series is
  cumulative on its own, so the series stay apart and ``total_at`` adds them.
  """
  series = monitoring(
      project, {
          "filter": 'metric.type="dataflow.googleapis.com/job/user_counter" '
                    f'AND metric.labels.job_id="{job_id}"',
          "interval.startTime": start,
          "interval.endTime": end,
      })
  out = collections.defaultdict(list)
  for ts in series:
    out[ts["metric"]["labels"].get("metric_name", "")].append(points_of(ts))
  return dict(out)


def value_at(points, at):
  """The last value of one series at or before `at` (a counter is cumulative), or 0."""
  before = [v for t, v in points if t <= at]
  return before[-1] if before else 0.0


def total_at(series, at):
  """The sum of the last values of each series at or before `at`."""
  return sum(value_at(points, at) for points in series)


def total_points(series):
  """[(time, sum of the series at that time)] at each time of any series, in time order."""
  times = sorted({t for points in series for t, _ in points})
  return [(t, total_at(series, t)) for t in times]


def vcpu_series(project, job_id, start, end):
  """Returns [(time, vCPUs)] of a job, in time order. Adds the series at each time."""
  series = monitoring(
      project, {
          "filter": 'metric.type="dataflow.googleapis.com/job/current_num_vcpus" '
                    f'AND metric.labels.job_id="{job_id}"',
          "interval.startTime": start,
          "interval.endTime": end,
      })
  return total_points([points_of(ts) for ts in series])


# vCPUs of the shared-core machine types, whose names hold no vCPU count.
SHARED_CORE_VCPUS = {"e2-micro": 2, "e2-small": 2, "e2-medium": 2, "f1-micro": 1, "g1-small": 1}
MACHINE_VCPUS = re.compile(r"-(?:custom-)?(\d+)(?:-\d+)?$")


def machine_vcpus(machine_type):
  """vCPUs of a machine type such as ``n2-standard-4`` or ``n2-custom-8-32768``, or None."""
  if not machine_type:
    return None
  name = machine_type.rsplit("/", 1)[-1]
  if name in SHARED_CORE_VCPUS:
    return SHARED_CORE_VCPUS[name]
  match = MACHINE_VCPUS.search(name)
  return int(match.group(1)) if match else None


def worker_machine_type(job):
  """The worker machine type of a job from ``jobs describe --full``, or None."""
  environment = job.get("environment", {})
  pools = [p.get("machineType") for p in environment.get("workerPools", [])]
  options = environment.get("sdkPipelineOptions", {}).get("options", {})
  names = pools + [options.get(k) for k in ("workerMachineType", "machineType", "machine_type")]
  return next((name for name in names if name), None)


def used_vcpu_seconds(project, job_id, start, end, vcpus, vcpus_per_vm=None):
  """CPU time that the worker VMs used between `start` and `end`, in vCPU·s, or None.

  Sums the mean CPU utilization of each VM for each minute, times the vCPUs of a VM:
  ``vcpus_per_vm``, else the vCPU count of the job divided by its VMs in that minute. Returns
  None when neither is known.
  """
  series = monitoring(
      project, {
          "filter": 'metric.type="compute.googleapis.com/instance/cpu/utilization" '
                    'AND resource.type="gce_instance" '
                    f'AND metadata.user_labels."dataflow_job_id"="{job_id}"',
          "interval.startTime": start,
          "interval.endTime": end,
          "aggregation.alignmentPeriod": "60s",
          "aggregation.perSeriesAligner": "ALIGN_MEAN",
      })
  by_minute = collections.defaultdict(list)
  for ts in series:
    for p in ts["points"]:
      by_minute[timestamp(p["interval"]["endTime"])].append(point_value(p))
  total = 0.0
  for minute, utils in by_minute.items():
    per_vm = vcpus_per_vm or (value_at(vcpus, minute) / len(utils) if vcpus else None)
    if not per_vm:
      return None
    total += sum(utils) * per_vm * 60
  return total


def percentiles(counts, wanted=(0.5, 0.95, 0.99)):
  """Upper bounds (ms) of the buckets that hold each wanted percentile, or None."""
  bounds = list(LATENCY_BOUNDS_MS) + [float("inf")]
  total = sum(counts)
  if total <= 0:
    return [None] * len(wanted)
  out = []
  for q in wanted:
    seen = 0
    for bound, count in zip(bounds, counts):
      seen += count
      if seen >= q * total:
        out.append(bound)
        break
  return out


def streaming_row(project,
                  region,
                  job_id,
                  window_start=None,
                  window_end=None,
                  vcpus_per_worker=None):
  """Throughput, used CPU, vCPUs and latency percentiles of a streaming job, or None.

  Without a window, the window is the steady state: from the time `e2e/rows` reaches 10% of
  its final value to the time it reaches 90%. That leaves out worker startup and the tail.
  Returns None when the job counted no row.
  """
  job = gcloud("dataflow", "jobs", "describe", job_id, f"--project={project}",
               f"--region={region}", "--full")
  start, end = job["createTime"], job["currentStateTime"]
  counters = user_counter_series(project, job_id, start, end)
  rows = total_points(counters.get("rows", []))
  final = rows[-1][1] if rows else 0
  if window_start and window_end:
    w0, w1 = timestamp(window_start), timestamp(window_end)
  elif final > 0:
    w0 = next(t for t, v in rows if v >= 0.1 * final)
    w1 = next(t for t, v in rows if v >= 0.9 * final)
  else:
    return None
  # Counters arrive as one cumulative point per minute. Snap the window to the times of the
  # points that the deltas use, so rows, latency buckets and used CPU cover the same interval.
  point_times = [t for t, _ in rows]
  w0 = max((t for t in point_times if t <= w0), default=w0)
  w1 = max((t for t in point_times if t <= w1), default=w1)
  seconds = (w1 - w0).total_seconds()
  rows_series = counters.get("rows", [])
  rows_in_window = total_at(rows_series, w1) - total_at(rows_series, w0)
  vcpus = vcpu_series(project, job_id, start, end)
  in_window = [v for t, v in vcpus if w0 <= t <= w1] or [0]
  per_vm = vcpus_per_worker or machine_vcpus(worker_machine_type(job))
  used = used_vcpu_seconds(project, job_id, w0.isoformat(), w1.isoformat(), vcpus, per_vm)
  buckets = [f"latency_le_{b}" for b in LATENCY_BOUNDS_MS] + ["latency_le_inf"]
  counts = [
      total_at(counters.get(b, []), w1) - total_at(counters.get(b, []), w0) for b in buckets
  ]
  return {
      "window": f"{w0:%H:%M}–{w1:%H:%M} ({seconds:.0f} s)",
      "rows_per_s": rows_in_window / seconds if seconds > 0 else None,
      "used_vcpu_s": used,
      "rows_per_used_vcpu_s": rows_in_window / used if used else None,
      "peak_vcpus": max(v for _, v in vcpus) if vcpus else None,
      "mean_vcpus": sum(in_window) / len(in_window),
      "p50_p95_p99": percentiles(counts),
  }


def transform_label(labels):
  """``<reader>`` for a read job, ``<writer>`` for a load job, or ``<reader> to <writer>``."""
  reader, writer = labels.get("reader"), labels.get("writer")
  if not writer:
    return reader or "?"
  if not reader:
    return writer
  semantics = labels.get("semantics")
  return f"{reader} to {writer}" + (f" ({semantics})" if semantics else "")


def user_metrics(project, region, job_id):
  """Returns ``e2e/rows`` and ``e2e/latency_ms`` (mean, min, max) from user metrics."""
  metrics = gcloud("beta", "dataflow", "metrics", "list", job_id, f"--project={project}",
                   f"--region={region}")
  out = {}
  for metric in metrics:
    name = metric.get("name", {})
    context = name.get("context", {})
    if context.get("namespace") != "e2e" or context.get("tentative"):
      continue
    if name.get("name") == "rows" and "scalar" in metric:
      out["e2e_rows"] = int(float(metric["scalar"]))
    elif name.get("name") == "latency_ms" and "distribution" in metric:
      dist = metric["distribution"]
      count = float(dist.get("count", 0))
      if count > 0:
        out["latency_mean_ms"] = float(dist.get("sum", 0)) / count
        out["latency_min_ms"] = float(dist.get("min", 0))
        out["latency_max_ms"] = float(dist.get("max", 0))
  return out


def job_row(project, region, job_id):
  job = gcloud("dataflow", "jobs", "describe", job_id, f"--project={project}",
               f"--region={region}")
  labels = job.get("labels", {})
  metrics = gcloud("beta", "dataflow", "metrics", "list", job_id, f"--project={project}",
                   f"--region={region}", "--source=service")
  values = {}
  for metric in metrics:
    name = metric.get("name", {}).get("name")
    if name in SERVICE_METRICS and not metric["name"].get("context", {}).get("tentative"):
      values[name] = float(metric.get("scalar", 0))
  values.update(user_metrics(project, region, job_id))
  created, finished = job["createTime"], job["currentStateTime"]
  seconds, peak_workers = processing(project, job_id)
  # Jobs from before the pipeline label ran in a Python pipeline.
  pipeline = labels.get("pipeline", "python")
  names = environments(project, job_id, pipeline)
  usage = container_memory(project, job_id, created, finished)
  counts = collections.Counter(names.get(c, c) for c in usage)
  memory = {
      (names.get(c, c) if counts[names.get(c, c)] == 1 else f"{names.get(c, c)} ({c})"): u
      for c, u in usage.items()
  }
  return {
      "job": job_id,
      "pipeline": pipeline,
      # The transforms under test: the reader of a read job, the writer of a load job, or the
      # reader, the writer and the semantics of a copy job.
      "reader": transform_label(labels),
      "shape": labels.get("shape", "?"),
      "scaling": labels.get("scaling", "fixed"),
      "state": job.get("currentState", "").replace("JOB_STATE_", ""),
      "elapsed": (timestamp(finished) - timestamp(created)).total_seconds(),
      "processing": seconds,
      "workers": peak_workers,
      "memory": memory,
      **values,
  }


def step_times(project, region, job_id):
  """Returns [(step name, seconds)] of the steps that took time, longest first."""
  metrics = gcloud("beta", "dataflow", "metrics", "list", job_id, f"--project={project}",
                   f"--region={region}", "--source=service")
  seconds = collections.Counter()
  for metric in metrics:
    name = metric.get("name", {})
    context = name.get("context", {})
    if name.get("name", "").startswith("ExecutionTime_") and not context.get("tentative"):
      seconds[context.get("step", "?")] += float(metric.get("scalar", 0)) / 1000
  job = gcloud("dataflow", "jobs", "describe", job_id, f"--project={project}",
               f"--region={region}", "--full")
  names = {
      step["name"]: step.get("properties", {}).get("user_name", step["name"])
      for step in job.get("steps", [])
  }
  return [(names.get(step, step), s) for step, s in seconds.most_common() if s >= 1]


def mib(value):
  return f"{value / 2**20:,.0f}"


def main(argv):
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--project", required=True)
  parser.add_argument("--region", default="us-central1")
  parser.add_argument("--rows", type=int, help="Rows that each job read.")
  parser.add_argument(
      "--steps", action="store_true", help="Also print the execution time of each step.")
  parser.add_argument(
      "--streaming",
      action="store_true",
      help="Also print steady-state rows/s, used CPU, vCPUs and latency percentiles.")
  parser.add_argument("--window-start", help="Start of the --streaming window (RFC 3339).")
  parser.add_argument("--window-end", help="End of the --streaming window (RFC 3339).")
  parser.add_argument(
      "--vcpus-per-worker",
      type=int,
      help="vCPUs of a worker VM for --streaming. Default: from the worker machine type.")
  parser.add_argument("jobs", nargs="+")
  args = parser.parse_args(argv)

  rows = [job_row(args.project, args.region, job) for job in args.jobs]
  has_latency = any("latency_mean_ms" in r for r in rows)
  has_rows_col = bool(args.rows) or any("e2e_rows" in r for r in rows)
  header = [
      "Pipeline", "Transform", "Shape", "Scaling", "State", "Peak workers", "Wall time (s)",
      "Processing (s)"
  ] + list(SERVICE_METRICS.values())
  if has_rows_col:
    header.append("Rows per vCPU·s")
  if has_latency:
    header.append("Latency mean / min / max (ms)")
  print("| " + " | ".join(header) + " |")
  print("|" + "---|" * len(header))
  for row in rows:
    processing_time = row["processing"]
    cells = [
        row["pipeline"], row["reader"], row["shape"], row["scaling"], row["state"],
        str(row["workers"]), f"{row['elapsed']:.0f}",
        f"{processing_time:.0f}" if processing_time is not None else "?"
    ]
    cells += [
        f"{row[name]:,.0f}" if row.get(name) is not None else "?"
        for name in SERVICE_METRICS
    ]
    if has_rows_col:
      vcpu = row.get("TotalVcpuTime")
      row_count = args.rows or row.get("e2e_rows")
      cells.append(f"{row_count / vcpu:,.0f}" if vcpu and row_count else "?")
    if has_latency:
      if "latency_mean_ms" in row:
        cells.append(f"{row['latency_mean_ms']:,.0f} / {row['latency_min_ms']:,.0f} / "
                     f"{row['latency_max_ms']:,.0f}")
      else:
        cells.append("—")
    print("| " + " | ".join(cells) + " |")

  if args.streaming:
    print()
    print("Streaming: rows/s, used CPU and latency in the window (steady state by default):")
    print()
    print("| Pipeline | Transform | Scaling | Window | Rows/s | Used vCPU·s | "
          "Rows per used vCPU·s | Mean / peak vCPUs | Latency p50 / p95 / p99 (ms) |")
    print("|---|---|---|---|---|---|---|---|---|")

    def fmt(value):
      if value is None:
        return "?"
      return "∞" if value == float("inf") else f"≤ {value:,.0f}"

    def num(value):
      return "?" if value is None else f"{value:,.0f}"

    for row in rows:
      s = streaming_row(args.project, args.region, row["job"], args.window_start,
                        args.window_end, args.vcpus_per_worker)
      if not s:
        print(f"| {row['pipeline']} | {row['reader']} | {row['scaling']} | no e2e/rows "
              "| | | | | |")
        continue
      p50, p95, p99 = s["p50_p95_p99"]
      print(f"| {row['pipeline']} | {row['reader']} | {row['scaling']} | {s['window']} "
            f"| {num(s['rows_per_s'])} | {num(s['used_vcpu_s'])} "
            f"| {num(s['rows_per_used_vcpu_s'])} "
            f"| {num(s['mean_vcpus'])} / {num(s['peak_vcpus'])} "
            f"| {fmt(p50)} / {fmt(p95)} / {fmt(p99)} |")

  print()
  print("Memory of each SDK container, per worker (MiB):")
  print()
  print("| Pipeline | Transform | Scaling | Container | Peak | Average |")
  print("|---|---|---|---|---|---|")
  for row in rows:
    for container, (peak, mean) in sorted(row["memory"].items()):
      print(f"| {row['pipeline']} | {row['reader']} | {row['scaling']} | {container} "
            f"| {mib(peak)} | {mib(mean)} |")
  if args.steps:
    for row in rows:
      print()
      print(f"Execution time of each step, `{row['job']}` "
            f"({row['pipeline']}, {row['reader']}, {row['scaling']}):")
      print()
      print("| Step | Seconds |")
      print("|---|---|")
      for step, seconds in step_times(args.project, args.region, row["job"]):
        print(f"| {step} | {seconds:,.0f} |")
  print()
  print("Jobs: " + ", ".join(
      f"`{row['job']}` ({row['pipeline']}, {row['reader']}, {row['scaling']})" for row in rows))


if __name__ == "__main__":
  main(sys.argv[1:])

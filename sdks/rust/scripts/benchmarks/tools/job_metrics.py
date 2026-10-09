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
      if "rust" in environment:
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
  token = subprocess.run(
      ["gcloud", "auth", "application-default", "print-access-token"],
      check=True,
      capture_output=True,
      text=True).stdout.strip()
  query = urllib.parse.urlencode({
      "filter": 'metric.type="dataflow.googleapis.com/worker/memory/bytes_used" '
                f'AND resource.labels.job_id="{job_id}"',
      "interval.startTime": start,
      "interval.endTime": end,
  })
  request = urllib.request.Request(
      f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries?{query}",
      headers={
          "Authorization": f"Bearer {token}",
          "x-goog-user-project": project
      })
  with urllib.request.urlopen(request) as response:
    series = json.load(response).get("timeSeries", [])
  values = collections.defaultdict(list)
  for ts in series:
    container = ts["metric"]["labels"].get("container", "")
    if container.startswith("sdk-"):
      values[container] += [
          float(p["value"].get("int64Value", p["value"].get("doubleValue", 0)))
          for p in ts["points"]
      ]
  return {c: (max(v), sum(v) / len(v)) for c, v in values.items() if v}


def transform_label(labels):
  """``<reader>`` for a read job, ``<reader> to <writer> (<semantics>)`` for a copy job."""
  reader, writer = labels.get("reader"), labels.get("writer")
  if not writer:
    return reader or "?"
  semantics = labels.get("semantics")
  return f"{reader or '?'} to {writer}" + (f" ({semantics})" if semantics else "")


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
      # The transforms under test: the reader of a read job, or the reader, the writer and the
      # semantics of a copy job.
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
  parser.add_argument("jobs", nargs="+")
  args = parser.parse_args(argv)

  rows = [job_row(args.project, args.region, job) for job in args.jobs]
  header = [
      "Pipeline", "Transform", "Shape", "Scaling", "State", "Peak workers", "Wall time (s)",
      "Processing (s)"
  ] + list(SERVICE_METRICS.values())
  if args.rows:
    header.append("Rows per vCPU·s")
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
    if args.rows:
      vcpu = row.get("TotalVcpuTime")
      cells.append(f"{args.rows / vcpu:,.0f}" if vcpu else "?")
    print("| " + " | ".join(cells) + " |")

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

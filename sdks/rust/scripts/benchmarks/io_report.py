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
Metrics of the I/O benchmarks (scripts/benchmarks/<io>/) for the report of dataflow_job.py.

The configuration comes from the benchmark registration, as JSON in --io:
{"pipeline": the SDK of the pipeline, "namespace": the namespace of the user metrics,
"rows": the row counter, "latency": the latency distribution in ms, "buckets": the prefix of
the latency bucket counters, "streaming": true for a streaming job}.

- Processing time: from the first worker that receives work to the start of the worker pool
  teardown. It leaves out VM startup and SDK container startup.
- Rows per vCPU·s: the row counter over the total vCPU time of the job.
- Latency: the mean and the maximum of the distribution, and the upper bound of the bucket
  that holds the 50th, 95th and 99th percentile, over the whole job.
- Streaming jobs: the peak system lag and the last element backlog, from Cloud Monitoring.
- Environments: the SDK of each SDK container (sdk-N-M), from the worker logs. The report
  names the memory of each container with it.
"""

import json
import re
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import monitoring
import report

WORK_STARTED = "began to receive work requests"
# The job messages that start the teardown of the worker pool.
TEARDOWN = ("Starting worker pool teardown", "Stopping worker pool")
ENVIRONMENT = re.compile(r"SDK harness (sdk-\d+-\d+) in environment (\S+)")
CONTROL_STREAM = "Received Control stream from SDK harness"
LOGGING_URL = "https://logging.googleapis.com/v2/entries:list"
# Upper bounds (ms) of the <buckets><bound> counters. Keep in sync with LATENCY_BOUNDS_MS in
# KafkaBenchmark.java, kafka_benchmark.py and kafka/rust/src/lib.rs.
LATENCY_BOUNDS_MS = (50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000,
                     300_000)

Config = Dict[str, Any]


def columns(cfg: Config) -> List[report.Column]:
  latency = [
      ("latency_mean_ms", "Latency mean (ms)", ",.0f"),
      ("latency_max_ms", "Latency max (ms)", ",.0f"),
      ("latency_p50_ms", "Latency p50 ≤ (ms)", ",.0f"),
      ("latency_p95_ms", "Latency p95 ≤ (ms)", ",.0f"),
      ("latency_p99_ms", "Latency p99 ≤ (ms)", ",.0f"),
  ] if cfg.get("latency") else []
  streaming = [
      ("peak_system_lag_s", "Peak system lag (s)", ",.0f"),
      ("last_backlog_elements", "Last backlog (elements)", ",.0f"),
  ] if cfg.get("streaming") else []
  return [("processing_s", "Processing time (s)", ",.0f"),
          ("rows_per_vcpu_s", "Rows per vCPU·s", ",.0f"), *latency, *streaming]


def queries(job_id: str) -> Dict[str, Dict[str, str]]:
  """The Monitoring requests of a streaming job, without the interval."""
  metric = lambda name: {"filter": f'metric.type="dataflow.googleapis.com/job/{name}" '
                                   f'AND metric.labels.job_id="{job_id}"'}
  return {"system_lag": metric("system_lag"), "backlog": metric("backlog_elements")}


def logs(project: str, token: str, job_id: str) -> List[Dict[str, Any]]:
  """The worker log entries that name the environment of each SDK container."""
  body = {"resourceNames": [f"projects/{project}"], "pageSize": 1000,
          "filter": f'resource.type="dataflow_step" AND resource.labels.job_id="{job_id}" '
                    f'AND "{CONTROL_STREAM}"'}
  request = urllib.request.Request(LOGGING_URL, data=json.dumps(body).encode(), method="POST",
                                   headers={"Authorization": f"Bearer {token}",
                                            "Content-Type": "application/json"})
  with urllib.request.urlopen(request) as response:
    return json.load(response).get("entries", [])


def collect(project: str, token: str, details: Dict[str, Any], cfg: Config,
            fetch: Callable[[Dict[str, str]], monitoring.Series]) -> Dict[str, Any]:
  """The logs and Monitoring series of a job from dataflow_job.fetch. A failed request gives
  no data."""
  job = details["job"]
  out: Dict[str, Any] = {}
  try:
    out["logs"] = logs(project, token, job["id"])
  except (urllib.error.URLError, OSError):
    out["logs"] = []
  interval = {"interval.startTime": job["createTime"], "interval.endTime": job["currentStateTime"]}
  for name, query in (queries(job["id"]).items() if cfg.get("streaming") else []):
    try:
      out[name] = fetch({**query, **interval})
    except (urllib.error.URLError, OSError):
      out[name] = []
  return out


def processing_seconds(messages: Sequence[Dict[str, Any]]) -> Optional[float]:
  def times(*texts: str) -> List[Any]:
    return [monitoring.parse_time(m["time"]) for m in messages
            if any(t in m.get("messageText", "") for t in texts) and m.get("time")]
  started, teardown = times(WORK_STARTED), times(*TEARDOWN)
  return (min(teardown) - min(started)).total_seconds() if started and teardown else None


def message(entry: Dict[str, Any]) -> str:
  return entry.get("textPayload") or entry.get("jsonPayload", {}).get("message", "")


def sdk_of(environment: str, pipeline: str) -> str:
  """The SDK of an environment. `pipeline` owns the default environment."""
  if "rust" in environment or pipeline == "rust":
    return "rust"
  if pipeline == "java":
    return "java"
  if "default_environment" in environment:
    return "python"
  return "java" if "java" in environment or "beam:env" in environment else environment


def environments(entries: Iterable[Dict[str, Any]], pipeline: str) -> Dict[str, str]:
  """The SDK of each SDK container."""
  texts = [message(e) for e in entries]
  matches = [(ENVIRONMENT.search(t), t) for t in texts]
  return {m.group(1): sdk_of(m.group(2), pipeline) for m, t in matches
          if m and "_sibling_" not in t}


def user_values(metrics: Iterable[Dict[str, Any]], cfg: Config) -> Dict[str, Any]:
  """The rows, the latency distribution and the latency buckets of the user metrics."""
  rows, buckets, out = 0.0, {}, {}
  for metric in metrics:
    name, context = metric["name"]["name"], metric["name"].get("context", {})
    if (metric["name"].get("origin") != "user" or context.get("tentative") == "true"
        or context.get("namespace") != cfg.get("namespace")):
      continue
    scalar, dist = metric.get("scalar"), metric.get("distribution")
    number = isinstance(scalar, (int, float))
    if name == cfg.get("rows") and number:
      rows += scalar
    elif cfg.get("buckets") and name.startswith(cfg["buckets"]) and number:
      bucket = name[len(cfg["buckets"]):]
      buckets[bucket] = buckets.get(bucket, 0.0) + scalar
    elif name == cfg.get("latency") and isinstance(dist, dict) and float(dist.get("count", 0)):
      out.update(latency_mean_ms=float(dist.get("sum", 0)) / float(dist["count"]),
                 latency_max_ms=float(dist.get("max", 0)))
  counts = [buckets.get(str(b), 0.0) for b in LATENCY_BOUNDS_MS] + [buckets.get("inf", 0.0)]
  p50, p95, p99 = percentiles(counts)
  return {**out, "rows": rows or None, "latency_p50_ms": p50, "latency_p95_ms": p95,
          "latency_p99_ms": p99}


def percentiles(counts: Sequence[float], wanted=(0.5, 0.95, 0.99)) -> List[Optional[float]]:
  """The upper bound (ms) of the bucket that holds each percentile, or None."""
  total, bounds = sum(counts), [*LATENCY_BOUNDS_MS, float("inf")]
  if total <= 0:
    return [None] * len(wanted)
  cumulative = [sum(counts[:i + 1]) for i in range(len(counts))]
  return [next(b for b, seen in zip(bounds, cumulative) if seen >= q * total) for q in wanted]


def summarize(details: Dict[str, Any], cfg: Config,
              total_vcpu_s: Optional[float]) -> Dict[str, Any]:
  """The metrics of the report and the SDK of each container."""
  io = details.get("io", {})
  user = user_values(details["metrics"], cfg)
  lag = [v for _, v in monitoring.total_points(io.get("system_lag", []))]
  backlog = monitoring.total_points(io.get("backlog", []))
  values = {
      **{k: v for k, v in user.items() if k != "rows"},
      "processing_s": processing_seconds(details["messages"]),
      "rows_per_vcpu_s": user["rows"] / total_vcpu_s if user["rows"] and total_vcpu_s else None,
      "peak_system_lag_s": max(lag, default=None),
      "last_backlog_elements": backlog[-1][1] if backlog else None,
  }
  return {"metrics": {k: v for k, v in values.items() if v is not None and v != float("inf")},
          "environments": environments(io.get("logs", []), cfg.get("pipeline", ""))}


def name_containers(memory: Dict[str, float], sdks: Dict[str, str]) -> Dict[str, float]:
  """The memory of each container, with the SDK of each SDK container in its name."""
  return {(f"{c} ({sdks[c]})" if c in sdks else c): v for c, v in memory.items()}

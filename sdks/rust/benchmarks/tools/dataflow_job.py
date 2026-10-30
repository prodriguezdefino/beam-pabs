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
Runs Dataflow benchmark jobs and reports their metrics.

`run` starts the launch command of each variant (see report.py) for some rounds. The rounds
run one after the other. The jobs of one round start together, so they share the conditions
of the region. The script gives each job its name and labels through the placeholders
{job_name}, {labels} (`k=v,k=v`), {labels_json} and {labels_args} of the launch command (see
run_values). It finds the job by its name through the Dataflow API, so the launcher of any
SDK works. While the job runs, the script records the CPU platform of its worker VMs. After
the job, it reads the job, its metrics, its messages and its Cloud Monitoring series, and
writes results.json and report.md.

A variant can also have "labels" (more job labels), "wait" (what to do after a launcher that
does not wait for its job, see job_wait.py) and "after" (a command that runs after each job,
with the same placeholders, for example a check of the output). With --io, the report also
has the metrics of the I/O benchmarks (see io_report.py).

With --producer, each round starts a companion load job when the measured jobs run. With
--shared-producer, the runs of several tasks on this host share one companion. results.json
holds the record of each companion. When the companion fails, the round fails and the medians
leave it out.

`fetch` prints the same record of jobs that ran before, without the CPU platform.

The total vCPU time includes worker startup and idle autoscaled workers. The used vCPU time
and the CPU time of the steps per million elements compare the code of two builds better.
"""

import argparse
import concurrent.futures
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import io_report
import job_wait
import monitoring
import report

API = "https://dataflow.googleapis.com/v1b3/projects/{project}/locations/{region}/jobs"
JOB_ID = re.compile(
    r"(?:^JOB_ID=(\S+)|Dataflow Console: \S+/dataflow/jobs/[^/\s]+/([^?\s/]+))",
    re.MULTILINE,
)
WORKERS = re.compile(r"Starting (\d+) workers|number of workers to (\d+)|"
                     r"worker pool from \d+ to (\d+)")
STOCKOUT = re.compile(r"(?i)stockout|resource.pool.exhausted|does not have enough resources")
QUOTA = re.compile(r"(?i)quota")
NOTABLE = ("JOB_MESSAGE_WARNING", "JOB_MESSAGE_ERROR")
POLL_SECONDS = 30
UNKNOWN_PLATFORM = "Unknown CPU Platform"
# A job found by name must start at most this long before its launch command, for clock skew.
LAUNCH_SLACK = datetime.timedelta(seconds=60)
SECTIONS = ["metrics", "steps", "memory"]
# The jobs of a round share the conditions of the region only if they start together.
MAX_START_SPREAD_SECONDS = 60
# The longest job name, label value and VM label value.
MAX_NAME = 63


def columns(count_name: str, counter: Optional[str]) -> List[report.Column]:
  """The metrics of the tables. `count_name` names the counted elements, for example words."""
  per_counter = [
      ("counter_total", f"{counter} total", ",.0f"),
      ("counter_per_s", f"{counter}/s (10-90%)", ",.0f"),
      ("counter_per_used_vcpu_s", f"{counter} per used vCPU·s", ",.0f"),
  ] if counter else []
  return [
      ("total_vcpu_s", "Total vCPU·s", ",.0f"),
      ("used_vcpu_s", "Used vCPU·s", ",.0f"),
      ("used_vcpu_ratio", "Used / total vCPU", ".2f"),
      ("elements", count_name.capitalize(), ",.0f"),
      ("elements_per_vcpu_s", f"{count_name.capitalize()} per vCPU·s", ",.0f"),
      *per_counter,
      ("memory_gb_s", "Memory (GB·s)", ",.0f"),
      ("peak_sdk_memory_mib", "Peak SDK container (MiB)", ",.0f"),
      ("pd_gb_s", "PD (GB·s)", ",.0f"),
      ("shuffle_gb", "Shuffle (GB)", ",.2f"),
      ("step_cpu_s", "Step CPU total (s)", ",.1f"),
      ("step_cpu_s_per_1m_elements", f"Step CPU s / 1M {count_name}", ",.3f"),
      ("peak_workers", "Peak workers", ".0f"),
      ("peak_vcpus", "Peak vCPUs", ".0f"),
      ("wall_s", "Wall time (s)", ",.0f"),
  ]


def access_token() -> str:
  return subprocess.run(["gcloud", "auth", "print-access-token"], check=True,
                        capture_output=True, text=True).stdout.strip()


def get(url: str, token: str) -> Dict[str, Any]:
  request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
  with urllib.request.urlopen(request) as response:
    return json.load(response)


def list_jobs(project: str, region: str, name: str) -> List[Dict[str, Any]]:
  """The jobs of the region with this name. An API error gives no job."""
  try:
    reply = get(f"{API.format(project=project, region=region)}?filter=ALL&name={name}"
                "&pageSize=50", access_token())
  except (urllib.error.URLError, OSError, subprocess.CalledProcessError):
    return []
  return reply.get("jobs", [])


def find_job(jobs: Iterable[Dict[str, Any]], name: str,
             launched: datetime.datetime) -> Optional[Dict[str, Any]]:
  """The newest job with this name that started after the launch and was not cancelled."""
  matches = [
      job for job in jobs
      if job.get("name") == name and job.get("currentState") != "JOB_STATE_CANCELLED"
      and monitoring.parse_time(job["createTime"]) >= launched - LAUNCH_SLACK
  ]
  return max(matches, key=lambda job: job["createTime"], default=None)


def fetch(project: str, region: str, job_id: str) -> Dict[str, Any]:
  """The job (with its worker pools), its metrics and all pages of its messages."""
  token = access_token()
  base = f"{API.format(project=project, region=region)}/{job_id}"
  messages, page = [], ""
  while True:
    reply = get(f"{base}/messages?minimumImportance=JOB_MESSAGE_DETAILED&pageSize=500"
                + (f"&pageToken={page}" if page else ""), token)
    messages += reply.get("jobMessages", [])
    page = reply.get("nextPageToken")
    if not page:
      break
  return {
      "job": get(f"{base}?view=JOB_VIEW_DESCRIPTION", token),
      "metrics": get(f"{base}/metrics", token).get("metrics", []),
      "messages": messages,
  }


def io_namespace(io: Optional[io_report.Config]) -> Optional[str]:
  """The namespace of the user metrics of an I/O benchmark, or None."""
  return io.get("namespace") if io else None


def collect_monitoring(project: str, details: Dict[str, Any], counter: Optional[str],
                       wait_seconds: float, namespace: Optional[str] = None
                       ) -> Dict[str, monitoring.Series]:
  """The Monitoring series of a job from `fetch`. The counter total from the Dataflow API
  tells when the counter series is complete."""
  job, token = details["job"], access_token()
  expected = parse_metrics(details["metrics"], "", counter,
                           namespace)["metrics"].get("counter_total")
  return monitoring.collect(lambda query: monitoring.time_series(project, token, query),
                            job["id"], job["createTime"], job["currentStateTime"], counter,
                            wait_seconds, expected)


def collect_io(args: argparse.Namespace, details: Dict[str, Any]) -> Dict[str, Any]:
  """The logs and Monitoring series of io_report.py for a job from `fetch`."""
  token = access_token()
  return io_report.collect(args.project, token, details, args.io,
                           lambda query: monitoring.time_series(args.project, token, query))


def platform_command(project: str, job_name: str) -> List[str]:
  return ["gcloud", "compute", "instances", "list", f"--project={project}",
          f"--filter=labels.dataflow_job_name~^{job_name}", "--format=value(cpuPlatform)"]


def cpu_platforms(project: str, job_name: str) -> Set[str]:
  """The CPU platforms of the running worker VMs of a job. Empty when none runs.

  A VM that is still staging reports an unknown platform, so that value is skipped.
  """
  out = subprocess.run(platform_command(project, job_name), capture_output=True, text=True)
  return known_platforms(out.stdout.splitlines())


def known_platforms(lines: Iterable[str]) -> Set[str]:
  return {line.strip() for line in lines if line.strip() and line.strip() != UNKNOWN_PLATFORM}


def job_id_from_log(text: str) -> Optional[str]:
  match = JOB_ID.search(text)
  return (match.group(1) or match.group(2)) if match else None


def parse_metrics(metrics: Iterable[Dict[str, Any]], count_output: str,
                  counter: Optional[str] = None,
                  namespace: Optional[str] = None) -> Dict[str, Any]:
  """The job totals, the counted elements and the ProcessElement seconds of each step.

  The counted elements are the elements of the first output whose name holds `count_output`.
  The total of the user counter `counter` adds the values of all steps. With `namespace`, it
  adds only the counters of that namespace.
  """
  totals: Dict[str, float] = {}
  steps: Dict[str, float] = {}
  elements = 0.0
  counter_total = 0.0
  for metric in metrics:
    name, context = metric["name"]["name"], metric["name"].get("context", {})
    scalar = metric.get("scalar")
    if context.get("tentative") == "true" or not isinstance(scalar, (int, float)):
      continue
    if name.startswith("Total") and "step" not in context:
      totals[name] = float(scalar)
    elif name == "ExecutionTime_ProcessElement" and "step" in context:
      step = context["step"]
      steps[step] = steps.get(step, 0.0) + scalar / 1000.0
    elif (count_output and name == "ElementCount"
          and count_output in context.get("output_user_name", "")):
      elements = max(elements, float(scalar))
    elif (counter and metric["name"].get("origin") == "user" and name == counter
          and (not namespace or context.get("namespace") == namespace)):
      counter_total += float(scalar)
  vcpu, step_cpu = totals.get("TotalVcpuTime"), sum(steps.values())
  values = {
      "total_vcpu_s": vcpu,
      "elements": elements or None,
      "elements_per_vcpu_s": elements / vcpu if vcpu and elements else None,
      "memory_gb_s": totals["TotalMemoryUsage"] / 1024 if "TotalMemoryUsage" in totals else None,
      "pd_gb_s": totals.get("TotalPdUsage"),
      # The service reports the shuffle data in GB.
      "shuffle_gb": totals.get("TotalShuffleDataProcessed"),
      "step_cpu_s": step_cpu,
      "step_cpu_s_per_1m_elements": step_cpu / (elements / 1e6) if elements else None,
      "counter_total": counter_total or None,
  }
  return {"metrics": {k: v for k, v in values.items() if v is not None}, "steps": steps}


def parse_messages(messages: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
  """The peak worker count, the warnings and errors, and the stockout and quota flags."""
  ordered = sorted(messages, key=lambda m: m.get("time", ""))
  sizes = [
      int(next(g for g in match.groups() if g)) for m in ordered
      for match in WORKERS.finditer(m.get("messageText", ""))
  ]
  warnings = [
      f"{m.get('time', '')[11:19]} {m['messageImportance'][12:]}: {m.get('messageText', '')[:200]}"
      for m in ordered if m.get("messageImportance") in NOTABLE
  ]
  return {
      "peak_workers": max(sizes, default=None),
      "warnings": warnings,
      "stockout": any(STOCKOUT.search(w) for w in warnings),
      # One startup message names both causes; it counts as a stockout.
      "quota": any(QUOTA.search(w) and not STOCKOUT.search(w) for w in warnings),
  }


def machine_type(job: Dict[str, Any]) -> Optional[str]:
  pools = job.get("environment", {}).get("workerPools", [])
  return next((p["machineType"] for p in pools if p.get("machineType")), None)


def summarize(details: Dict[str, Any], count_output: str, counter: Optional[str] = None,
              io: Optional[io_report.Config] = None) -> Dict[str, Any]:
  """The record of one job from the reply of `fetch` and its "monitoring" series, if any.
  With `io`, the record also has the metrics of io_report.py."""
  job = details["job"]
  parsed = parse_metrics(details["metrics"], count_output, counter, io_namespace(io))
  events = parse_messages(details["messages"])
  watched = monitoring.summarize(details.get("monitoring", {}), machine_type(job),
                                 parsed["metrics"].get("total_vcpu_s"),
                                 parsed["metrics"].get("counter_total"))
  extra = io_report.summarize(details, io, parsed["metrics"].get("total_vcpu_s")) if io \
      else {"metrics": {}, "environments": {}}
  start, end = job.get("startTime") or job.get("createTime"), job.get("currentStateTime")
  wall = (monitoring.parse_time(end) - monitoring.parse_time(start)).total_seconds() \
      if start and end else None
  metrics = {**parsed["metrics"], **watched["metrics"], **extra["metrics"],
             "peak_workers": events["peak_workers"], "wall_s": wall}
  return {
      "job_id": job.get("id"),
      "job_name": job.get("name"),
      "labels": job.get("labels", {}),
      "state": job.get("currentState", "").replace("JOB_STATE_", ""),
      "start": start,
      "end": end,
      "metrics": {k: v for k, v in metrics.items() if v is not None},
      "steps": parsed["steps"],
      "memory": io_report.name_containers(watched["memory"], extra["environments"]),
      "monitoring_missing": watched["monitoring_missing"],
      **{k: events[k] for k in ("warnings", "stockout", "quota")},
  }


def flags(runs: Sequence[Dict[str, Any]], workers: int = 1) -> List[str]:
  """Conditions that make a run or a round a poor comparison. A job runs on `workers`."""
  out = []
  for run in runs:
    tag = f"Round {run['round']} {run['variant']}"
    peak = run.get("metrics", {}).get("peak_workers")
    platforms = run.get("platforms", [])
    missing = run.get("monitoring_missing", [])
    out += [f"{tag}: {run['error']}"] if run.get("error") else []
    # A drained streaming job ended as the benchmark asked.
    out += [f"{tag}: state {run['state']}"] if run.get("state") not in (None, "DONE", "DRAINED") \
        else []
    out += [f"{tag}: stockout warning"] if run.get("stockout") else []
    out += [f"{tag}: quota warning"] if run.get("quota") else []
    out += [f"{tag}: autoscaled to {peak:.0f} workers"] if peak and peak > workers else []
    out += [f"{tag}: mixed CPU platforms {platforms}"] if len(platforms) > 1 else []
    out += [f"{tag}: CPU platform not captured"] if not platforms else []
    out += [f"{tag}: no Monitoring data for {', '.join(missing)}"] if missing else []
  for round_number in sorted({run["round"] for run in runs}):
    pair = {run["variant"]: tuple(run.get("platforms", [])) for run in runs
            if run["round"] == round_number}
    if len(set(pair.values())) > 1:
      out.append(f"Round {round_number}: CPU platforms differ: " +
                 ", ".join(f"{v}={', '.join(p) or '?'}" for v, p in pair.items()))
    starts = [monitoring.parse_time(run["start"]) for run in runs
              if run["round"] == round_number and run.get("start")]
    apart = (max(starts) - min(starts)).total_seconds() if starts else 0.0
    if apart > MAX_START_SPREAD_SECONDS:
      out.append(f"Round {round_number}: jobs started {apart:.0f} s apart")
    if any(run.get("round_failed") for run in runs if run["round"] == round_number):
      out.append(f"Round {round_number}: failed, not in the medians")
  return out


def measured(runs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
  """The runs that the medians use: a round with no valid load is left out."""
  return [run for run in runs if not run.get("round_failed")]


def ranked_columns(all_medians: report.Medians, section: str, minimum: float,
                   spec: str) -> List[report.Column]:
  """The keys of a section with at least `minimum` in any variant, largest first."""
  largest: Dict[str, float] = {}
  for sections in all_medians.values():
    for key, value in sections.get(section, {}).items():
      largest[key] = max(largest.get(key, 0.0), value)
  ranked = sorted((k for k in largest if largest[k] >= minimum), key=largest.get, reverse=True)
  return [(key, key, spec) for key in ranked]


def render(results: Dict[str, Any], saved: Optional[Dict[str, Any]]) -> List[str]:
  all_medians = report.with_saved(results["medians"], saved)
  runs, config = results["runs"], results["config"]
  metrics = columns(config.get("count_name", "elements"), config.get("counter")) + \
      (io_report.columns(config["io"]) if config.get("io") else [])
  current = config.get("current", report.CURRENT)
  compare = lambda section, cols: report.compare_table(all_medians, section, cols, current=current)
  extra = [("CPU platform", lambda r: ", ".join(r.get("platforms", [])) or "?"),
           ("State", lambda r: r.get("state", "?")),
           ("Start", lambda r: (r.get("start") or "?")[:19]),
           ("End", lambda r: (r.get("end") or "?")[:19]),
           ("Job", lambda r: r.get("job_id") or "?")]
  warnings = [f"- Round {r['round']} {r['variant']}: {w}" for r in runs for w in r.get("warnings", [])]
  companions = [companion_line(c) for c in results.get("companions", [])]
  return [
      f"# {results['task']} ({results['created']})", "",
      config["description"], "",
      "Variants: " + ", ".join(f"{k}={v}" for k, v in config["variants"].items()) + ".", "",
      "## Flags", "", *([f"- {f}" for f in results["flags"]] or ["None."]), "",
      "## Medians", "", *compare("metrics", metrics), "",
      "## Runs", "", *report.runs_table(runs, "metrics", metrics, extra), "",
      *(["## Companion producer", "", *companions, ""] if companions else []),
      "## ProcessElement time of each step (s, all workers)", "",
      *compare("steps", ranked_columns(all_medians, "steps", 1.0, ",.1f")),
      "", "## Peak memory of each container (MiB, any worker)", "",
      *compare("memory", ranked_columns(all_medians, "memory", 0.0, ",.0f")),
      "", "## Warnings and errors", "", *(warnings or ["None."]),
  ]


def companion_line(companion: Dict[str, Any]) -> str:
  rows = companion.get("rows")
  return (f"- Round {companion['round']}: job {companion.get('job_id') or '?'}, "
          f"state {companion.get('state') or '?'}, "
          f"{report.fmt(rows, ',.0f')} rows, {(companion.get('start') or '?')[:19]} to "
          f"{(companion.get('end') or '?')[:19]}" +
          (f", error: {companion['error']}" if companion.get("error") else ""))


def label_value(text: str) -> str:
  """A valid Dataflow label value or job name part: lowercase letters, digits and hyphens."""
  return re.sub(r"[^a-z0-9-]", "-", re.sub(r"(?<=[a-z0-9])([A-Z])", r"-\1", text).lower())


def run_values(task: str, label: str, round_number: int, run_id: int,
               more_labels: Optional[Dict[str, Any]] = None,
               prefix: Optional[str] = None) -> Dict[str, Any]:
  """The placeholder values of one job: its name and its labels.

  The job name is `<prefix>-<variant>-r<round>-<run_id>`, at most 63 characters, so it fits
  the VM label of the job. `prefix` defaults to the task. The labels are the standard labels
  and `more_labels` (the "labels" of the variant). They have four forms: {labels} is
  `k=v,k=v`, which the Rust runner reads from one --labels flag; {labels_json} is a JSON
  object; {labels_args} is one `--labels=k=v` argument for each label, for the Python SDK.
  {job_name_underscore} is the job name with underscores, for example for a table name.
  """
  standard = {"benchmark": label_value(task), "variant": label_value(label),
              "round": str(round_number), "run_id": str(run_id)}
  labels = {**{k: label_value(str(v))[:MAX_NAME] for k, v in (more_labels or {}).items()},
            **standard}
  suffix = f"-r{round_number}-{run_id}"
  head = f"{label_value(prefix) if prefix else standard['benchmark']}-{standard['variant']}"
  name = head[:MAX_NAME - len(suffix)].rstrip("-") + suffix
  return {
      "job_name": name,
      "job_name_underscore": name.replace("-", "_"),
      "labels": ",".join(f"{k}={v}" for k, v in labels.items()),
      "labels_json": json.dumps(labels, separators=(",", ":")),
      "labels_args": [f"--labels={k}={v}" for k, v in labels.items()],
  }


def values_of(args: argparse.Namespace, variant: report.Variant, round_number: int,
              run_id: int) -> Dict[str, Any]:
  return {**run_values(args.task, variant["label"], round_number, run_id,
                       variant.get("labels"), args.name_prefix), "out_dir": args.out_dir}


def get_job(project: str, region: str, job_id: str) -> Optional[Dict[str, Any]]:
  try:
    return get(f"{API.format(project=project, region=region)}/{job_id}", access_token())
  except (urllib.error.URLError, OSError, subprocess.CalledProcessError):
    return None


def counter_value(project: str, region: str, job_id: str, counter: str,
                  namespace: Optional[str] = None) -> Optional[float]:
  """The total of a user counter: None on an API error, 0 when the job has no such counter."""
  try:
    reply = get(f"{API.format(project=project, region=region)}/{job_id}/metrics", access_token())
  except (urllib.error.URLError, OSError, subprocess.CalledProcessError, ValueError):
    return None
  return parse_metrics(reply.get("metrics", []), "", counter,
                       namespace)["metrics"].get("counter_total", 0.0)


def drain(project: str, region: str, job_id: str) -> None:
  subprocess.run(["gcloud", "dataflow", "jobs", "drain", job_id, f"--project={project}",
                  f"--region={region}"], check=False)


def kill_worker(project: str, job_id: str) -> None:
  """Deletes one worker VM of the job, for a fault test. Dataflow replaces it."""
  listed = subprocess.run(
      ["gcloud", "compute", "instances", "list", f"--project={project}",
       f"--filter=labels.dataflow_job_id={job_id}", "--format=value(name,zone.basename())"],
      check=False, capture_output=True, text=True).stdout.split()
  if len(listed) < 2:
    print(f"No worker VM of job {job_id} to delete", flush=True)
    return
  name, zone = listed[0], listed[1]
  print(f"Delete worker VM {name} in {zone}", flush=True)
  subprocess.run(["gcloud", "compute", "instances", "delete", name, f"--project={project}",
                  f"--zone={zone}", "--quiet"], check=False)


def after(variant: report.Variant, values: Dict[str, Any], base: str) -> Optional[str]:
  """Runs the "after" command of a variant, for example a check of the output. An error, or
  None."""
  if not variant.get("after"):
    return None
  with open(f"{base}-after.log", "w") as out:
    code = subprocess.run(report.expand({"argv": variant["after"]}, values), cwd=variant["cwd"],
                          env=report.environment(variant), stdout=out,
                          stderr=subprocess.STDOUT).returncode
  return f"the after command failed, see {base}-after.log" if code else None


def launch(args: argparse.Namespace, variant: report.Variant, round_number: int,
           run_id: int, on_running: Callable[[], None] = lambda: None,
           producer_done_at: Callable[[], Optional[float]] = lambda: None) -> Dict[str, Any]:
  """Runs one job to its end and returns its record. See job_wait.py for the "wait" field."""
  label, spec = variant["label"], variant.get("wait")
  values = values_of(args, variant, round_number, run_id)
  name = values["job_name"]
  base = os.path.join(args.out_dir, f"{label}-r{round_number}")
  platforms: Set[str] = set()
  job = None
  launched = datetime.datetime.now(datetime.timezone.utc)
  lookup = lambda: find_job(list_jobs(args.project, args.region, name), name, launched)
  with open(f"{base}.log", "w") as out:
    process = subprocess.Popen(report.expand(variant, values), cwd=variant["cwd"],
                               env=report.environment(variant), stdout=out,
                               stderr=subprocess.STDOUT)
    while process.poll() is None:
      platforms |= cpu_platforms(args.project, name)
      job = job or lookup()
      try:
        process.wait(timeout=POLL_SECONDS)
      except subprocess.TimeoutExpired:
        pass
  record = {"round": round_number, "variant": label, "job_name": name,
            "launcher_exit": process.returncode, "platforms": sorted(platforms)}
  with open(f"{base}.log") as f:
    logged_id = job_id_from_log(f.read())
  job = job or (get_job(args.project, args.region, logged_id) if logged_id else None) or lookup()
  waited: Dict[str, Any] = {}
  if spec is not None and not spec.get("detach") and process.returncode == 0:
    found = {"job": job}
    def poll() -> Optional[Dict[str, Any]]:
      platforms.update(cpu_platforms(args.project, name))
      known = found["job"]
      found["job"] = (get_job(args.project, args.region, known["id"]) if known else lookup()) \
          or known
      return found["job"]
    counter = spec.get("counter", "")
    namespace = spec.get("namespace") or io_namespace(args.io)
    waited = job_wait.wait(spec, poll,
                           lambda j: counter_value(args.project, args.region, j["id"], counter,
                                                   namespace),
                           lambda j: drain(args.project, args.region, j["id"]),
                           on_running=on_running,
                           producer_done_at=producer_done_at,
                           kill=lambda j: kill_worker(args.project, j["id"]))
    job = waited["job"] or job
    record["platforms"] = sorted(platforms)
  else:
    on_running()
  job_id = job["id"] if job else logged_id
  if not job_id:
    return {**record, "error": f"no job named {name}, see {base}.log", "metrics": {}}
  if spec is not None and spec.get("detach"):
    return {**record, "job_id": job_id, "state": "RUNNING", "error": "detached, no metrics",
            "metrics": {}}
  details = fetch(args.project, args.region, job_id)
  details["monitoring"] = collect_monitoring(args.project, details, args.counter,
                                             args.monitoring_wait, io_namespace(args.io))
  if args.io:
    details["io"] = collect_io(args, details)
  with open(f"{base}-job.json", "w") as f:
    json.dump(details, f, indent=2)
  errors = [e for e in (waited.get("error"), after(variant, values, base)) if e]
  return {**record, **summarize(details, args.count_output, args.counter, args.io),
          **({"error": "; ".join(errors)} if errors else {})}


def launch_companion(args: argparse.Namespace, producer: report.Variant, round_number: int,
                     run_id: int) -> Dict[str, Any]:
  """Starts the companion load job, waits for it to end and returns its record.

  The record has the job, its state, its start and end, and the `rows` of its counter. It has
  no metrics: the medians do not use it. "ended" is the time.time() of its end.
  """
  label = producer["label"]
  values = values_of(args, producer, round_number, run_id)
  name = values["job_name"]
  record: Dict[str, Any] = {"round": round_number, "variant": label, "job_name": name}
  base = os.path.join(args.out_dir, f"{label}-r{round_number}")
  launched = datetime.datetime.now(datetime.timezone.utc)
  lookup = lambda: find_job(list_jobs(args.project, args.region, name), name, launched)
  with open(f"{base}.log", "w") as out:
    code = subprocess.run(report.expand(producer, values), cwd=producer["cwd"],
                          env=report.environment(producer), stdout=out,
                          stderr=subprocess.STDOUT).returncode
  if code:
    return {**record, "ended": time.time(),
            "error": f"companion {label} failed to launch, see {base}.log"}
  with open(f"{base}.log") as f:
    logged_id = job_id_from_log(f.read())
  found = {"job": (get_job(args.project, args.region, logged_id) if logged_id else None) or lookup()}
  spec = producer.get("wait") or {}
  counter, namespace = spec.get("counter", ""), spec.get("namespace")

  def poll() -> Optional[Dict[str, Any]]:
    known = found["job"]
    found["job"] = (get_job(args.project, args.region, known["id"]) if known else lookup()) or known
    return found["job"]

  waited = job_wait.wait(spec, poll,
                         lambda j: counter_value(args.project, args.region, j["id"], counter,
                                                 namespace),
                         lambda j: drain(args.project, args.region, j["id"]))
  job = waited["job"] or found["job"]
  if job:
    record.update(job_id=job.get("id"), state=job.get("currentState", "").replace("JOB_STATE_", ""),
                  start=job.get("startTime") or job.get("createTime"),
                  end=job.get("currentStateTime"),
                  rows=counter_value(args.project, args.region, job["id"], counter, namespace))
  return {**record, "ended": time.time(),
          **({"error": f"companion {label}: {waited['error']}"} if waited.get("error") else {})}


# A task that shares a producer waits this long for the other tasks to join.
SHARED_JOIN_SECONDS = 1800
SHARED_POLL_SECONDS = 5


def shared_companion(args: argparse.Namespace, producer: report.Variant, round_number: int,
                     run_id: int, sleep: Callable[[float], None] = time.sleep,
                     clock: Callable[[], float] = time.time) -> Dict[str, Any]:
  """Joins the companion producer that the tasks of --shared-producer share in this round.

  Each task adds a member file when its jobs run. The first task launches the producer when
  --shared-tasks members are there, and writes done.json with its record when it ends. The
  other tasks read done.json. Each task then drains its jobs after its own idle time.
  """
  root = os.path.join(args.shared_producer, f"r{round_number}")
  members, done = os.path.join(root, "members"), os.path.join(root, "done.json")
  failed = lambda error: {"round": round_number, "variant": producer["label"], "ended": clock(),
                          "error": error}
  os.makedirs(members, exist_ok=True)
  if os.path.exists(done):
    return failed(f"shared producer {root} ran before: use a new -PsharedProducer")
  open(os.path.join(members, str(os.getpid())), "w").close()
  try:
    os.mkdir(os.path.join(root, "owner"))
  except FileExistsError:
    load = float((producer.get("wait") or {}).get("drain_after_seconds") or 0)
    deadline = clock() + 2 * SHARED_JOIN_SECONDS + load
    while not os.path.exists(done) and clock() < deadline:
      sleep(SHARED_POLL_SECONDS)
    if not os.path.exists(done):
      return failed(f"the shared producer did not end, see {root}")
    with open(done) as f:
      return json.load(f)
  record = failed("the shared producer stopped")
  try:
    deadline = clock() + SHARED_JOIN_SECONDS
    while len(os.listdir(members)) < args.shared_tasks and clock() < deadline:
      sleep(SHARED_POLL_SECONDS)
    joined = len(os.listdir(members))
    record = launch_companion(args, producer, round_number, run_id) \
        if joined >= args.shared_tasks else \
        failed(f"{joined} of {args.shared_tasks} tasks joined the shared producer")
  finally:
    with open(f"{done}.tmp", "w") as f:
      json.dump({**record, "ended": record.get("ended", clock())}, f)
    os.replace(f"{done}.tmp", done)
  return record


def leave_shared(shared_dir: str, round_number: int) -> None:
  """Removes the member file of this task. The last task to leave removes the round."""
  root = os.path.join(shared_dir, f"r{round_number}")
  members = os.path.join(root, "members")
  try:
    os.remove(os.path.join(members, str(os.getpid())))
    if not os.listdir(members):
      shutil.rmtree(root, ignore_errors=True)
  except FileNotFoundError:
    pass


def prepare(args: argparse.Namespace, variants: Sequence[report.Variant]) -> None:
  """Runs the "prepare" command of each variant, one after the other, before the rounds.

  The command builds what the launch command needs, so the launches of a round only submit
  their jobs and start together. The {out_dir} placeholder is the output directory.
  """
  for variant in (v for v in variants if v.get("prepare")):
    values = {
        **run_values(getattr(args, "task", "bench"), variant["label"], 0, 0,
                     variant.get("labels"), getattr(args, "name_prefix", None)),
        "out_dir": args.out_dir,
    }
    command = report.expand({"argv": variant["prepare"]}, values)
    print(f"Prepare {variant['label']}: {report.show(command, variant['cwd'], variant.get('env'))}",
          flush=True)
    if args.dry_run:
      continue
    os.makedirs(args.out_dir, exist_ok=True)
    log = os.path.join(args.out_dir, f"{variant['label']}-prepare.log")
    with open(log, "w") as out:
      if subprocess.run(command, cwd=variant["cwd"], env=report.environment(variant),
                        stdout=out, stderr=subprocess.STDOUT).returncode:
        raise RuntimeError(f"The prepare command of {variant['label']} failed. See {log}.")


def load_producer(path: Optional[str]) -> Optional[report.Variant]:
  if not path:
    return None
  with open(path) as f:
    return json.load(f)


def wait_running(ready: Sequence[threading.Event],
                 jobs: Sequence[concurrent.futures.Future]) -> None:
  """Waits until each measured job runs, or its launch ends (for example on an error)."""
  for event, job in zip(ready, jobs):
    while not event.wait(SHARED_POLL_SECONDS) and not job.done():
      pass


def start_companion(args: argparse.Namespace, producer: report.Variant,
                    jobs: Sequence[concurrent.futures.Future], round_number: int,
                    run_id: int) -> Optional[Dict[str, Any]]:
  """The record of the companion of this round, or None when no measured job still runs."""
  try:
    if getattr(args, "shared_producer", None):
      return shared_companion(args, producer, round_number, run_id)
    if any(not job.done() for job in jobs):
      return launch_companion(args, producer, round_number, run_id)
    return None
  except Exception as error:  # pylint: disable=broad-except
    # The measured jobs wait for the end of the companion, so an error must end it too.
    return {"round": round_number, "variant": producer["label"], "ended": time.time(),
            "error": f"companion {producer['label']} failed: {error}"}


def run_round(args: argparse.Namespace, variants: Sequence[report.Variant],
              producer: Optional[report.Variant], round_number: int,
              run_id: int) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
  """Runs all measured variants of one round, plus the optional companion load job.

  Returns the records of the measured jobs and the record of the companion. When the
  companion fails, the round fails and the medians leave it out.
  """
  ready = [threading.Event() for _ in variants]
  producer_done: Dict[str, Optional[float]] = {"at": None}
  companion = None
  with concurrent.futures.ThreadPoolExecutor(len(variants)) as pool:
    jobs = [
        pool.submit(launch, args, variant, round_number, run_id, ready[index].set,
                    lambda: producer_done["at"])
        for index, variant in enumerate(variants)
    ]
    if producer:
      wait_running(ready, jobs)
      companion = start_companion(args, producer, jobs, round_number, run_id)
      # "ended" can come from another task of a shared producer.
      ended = (companion or {}).get("ended", time.time())
      producer_done["at"] = time.monotonic() - max(0.0, time.time() - ended)
    records = [job.result() for job in jobs]
  if producer and getattr(args, "shared_producer", None):
    leave_shared(args.shared_producer, round_number)
  companion_error = (companion or {}).get("error")
  if companion_error:
    for record in records:
      record["error"] = "; ".join(e for e in (record.get("error"), companion_error) if e)
      record["round_failed"] = True
  return records, companion


def run(args: argparse.Namespace) -> None:
  variants = report.load_variants(args.variants)
  producer = load_producer(getattr(args, "producer", None))
  prepare(args, list(variants) + ([producer] if producer else []))
  if args.prepare_only:
    return
  runs, companions = [], []
  for round_number in range(1, args.runs + 1):
    run_id = int(time.time())
    for variant in variants:
      values = values_of(args, variant, round_number, run_id)
      command = report.expand(variant, values)
      after_command = report.expand({"argv": variant["after"]}, values) \
          if variant.get("after") else None
      print(f"Round {round_number}, {variant['label']}: "
            f"{report.show(command, variant['cwd'], variant.get('env'))}\n"
            f"  while it runs: {' '.join(platform_command(args.project, values['job_name']))}" +
            (f"\n  wait: {json.dumps(variant['wait'])}" if variant.get("wait") is not None else "") +
            (f"\n  after: {report.show(after_command, variant['cwd'])}" if after_command else ""),
            flush=True)
    if producer:
      p_values = values_of(args, producer, round_number, run_id)
      p_cmd = report.expand(producer, p_values)
      print(f"Round {round_number}, companion {producer['label']}: "
            f"{report.show(p_cmd, producer['cwd'], producer.get('env'))}" +
            (f"\n  wait: {json.dumps(producer['wait'])}" if producer.get("wait") is not None else ""),
            flush=True)
    if args.dry_run:
      continue
    os.makedirs(args.out_dir, exist_ok=True)
    records, companion = run_round(args, variants, producer, round_number, run_id)
    runs += records
    companions += [companion] if companion else []
  if args.dry_run:
    return
  results = {
      "task": args.task,
      "created": datetime.datetime.now().isoformat(timespec="seconds"),
      "config": {"description": args.describe, "runs": args.runs,
                 "count_name": args.count_name, "counter": args.counter,
                 "current": args.current, "io": args.io,
                 "variants": {v["label"]: v.get("describe", v["cwd"]) for v in variants}},
      "runs": runs,
      "companions": companions,
      "medians": report.medians(measured(runs), SECTIONS),
      "flags": flags(runs, args.workers),
  }
  report.write(args.out_dir, results, render(results, report.load(args.compare_to)))


def main(argv: Sequence[str]) -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  commands = parser.add_subparsers(dest="command", required=True)
  for name in ("run", "fetch"):
    sub = commands.add_parser(name)
    sub.add_argument("--project", required=True)
    sub.add_argument("--region", default="us-central1")
    sub.add_argument("--count-output", default="",
                     help="Part of the name of the output whose elements the report counts.")
    sub.add_argument("--counter", help="A user counter whose rate the report gives.")
    sub.add_argument("--monitoring-wait", type=float, default=360,
                     help="Seconds to wait for Cloud Monitoring data after the job.")
    sub.add_argument("--io", type=json.loads, help="JSON configuration of io_report.py.")
  run_parser, fetch_parser = commands.choices["run"], commands.choices["fetch"]
  run_parser.add_argument("--task", required=True, help="The benchmark, for names and labels.")
  run_parser.add_argument("--name-prefix", help="The start of the job names. Default: the task.")
  run_parser.add_argument("--out-dir", required=True)
  run_parser.add_argument("--variants", required=True,
                          help="JSON file of the variants. The jobs of a round start together.")
  run_parser.add_argument("--producer",
                          help="JSON file of an optional companion load job started per round.")
  run_parser.add_argument("--shared-producer",
                          help="Directory of a companion producer that several runs share.")
  run_parser.add_argument("--shared-tasks", type=int, default=2,
                          help="The runs that share --shared-producer.")
  run_parser.add_argument("--runs", type=int, default=3)
  run_parser.add_argument("--current", default=report.CURRENT,
                          help="The variant that the report compares with the others.")
  run_parser.add_argument("--workers", type=int, default=1,
                          help="The workers of a job. The report flags a job with more.")
  run_parser.add_argument("--count-name", default="elements", help="For example: words.")
  run_parser.add_argument("--describe", default="", help="A line for the report.")
  run_parser.add_argument("--compare-to", help="An earlier results.json.")
  run_parser.add_argument("--dry-run", action="store_true", help="Print the commands only.")
  run_parser.add_argument("--prepare-only", action="store_true",
                          help="Run the prepare commands and stop before the rounds.")
  fetch_parser.add_argument("jobs", nargs="+")
  args = parser.parse_args(argv)
  if args.command == "run":
    run(args)
    return
  records = []
  for job_id in args.jobs:
    details = fetch(args.project, args.region, job_id)
    details["monitoring"] = collect_monitoring(args.project, details, args.counter, 0,
                                               io_namespace(args.io))
    if args.io:
      details["io"] = collect_io(args, details)
    records.append(summarize(details, args.count_output, args.counter, args.io))
  print(json.dumps(records, indent=2))


if __name__ == "__main__":
  main(sys.argv[1:])

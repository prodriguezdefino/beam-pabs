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
{job_name}, {labels} (`k=v,k=v`) and {labels_json} of the launch command. It finds the job by
its name through the Dataflow API, so the launcher of any SDK works. While the job runs, the
script records the CPU platform of its worker VMs. After the job, it reads the job, its
metrics, its messages and its Cloud Monitoring series, and writes results.json and report.md.

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
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import monitoring
import report

API = "https://dataflow.googleapis.com/v1b3/projects/{project}/locations/{region}/jobs"
JOB_ID = re.compile(r"Dataflow Console: \S+/dataflow/jobs/[^/\s]+/([^?\s/]+)")
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


def collect_monitoring(project: str, details: Dict[str, Any], counter: Optional[str],
                       wait_seconds: float) -> Dict[str, monitoring.Series]:
  """The Monitoring series of a job from `fetch`. The counter total from the Dataflow API
  tells when the counter series is complete."""
  job, token = details["job"], access_token()
  expected = parse_metrics(details["metrics"], "", counter)["metrics"].get("counter_total")
  return monitoring.collect(lambda query: monitoring.time_series(project, token, query),
                            job["id"], job["createTime"], job["currentStateTime"], counter,
                            wait_seconds, expected)


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
  return match.group(1) if match else None


def parse_metrics(metrics: Iterable[Dict[str, Any]], count_output: str,
                  counter: Optional[str] = None) -> Dict[str, Any]:
  """The job totals, the counted elements and the ProcessElement seconds of each step.

  The counted elements are the elements of the first output whose name holds `count_output`.
  The total of the user counter `counter` adds the values of all steps.
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
    elif counter and metric["name"].get("origin") == "user" and name == counter:
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


def summarize(details: Dict[str, Any], count_output: str,
              counter: Optional[str] = None) -> Dict[str, Any]:
  """The record of one job from the reply of `fetch` and its "monitoring" series, if any."""
  job = details["job"]
  parsed = parse_metrics(details["metrics"], count_output, counter)
  events = parse_messages(details["messages"])
  watched = monitoring.summarize(details.get("monitoring", {}), machine_type(job),
                                 parsed["metrics"].get("total_vcpu_s"),
                                 parsed["metrics"].get("counter_total"))
  start, end = job.get("startTime") or job.get("createTime"), job.get("currentStateTime")
  wall = (monitoring.parse_time(end) - monitoring.parse_time(start)).total_seconds() \
      if start and end else None
  metrics = {**parsed["metrics"], **watched["metrics"], "peak_workers": events["peak_workers"],
             "wall_s": wall}
  return {
      "job_id": job.get("id"),
      "job_name": job.get("name"),
      "labels": job.get("labels", {}),
      "state": job.get("currentState", "").replace("JOB_STATE_", ""),
      "start": start,
      "end": end,
      "metrics": {k: v for k, v in metrics.items() if v is not None},
      "steps": parsed["steps"],
      "memory": watched["memory"],
      "monitoring_missing": watched["monitoring_missing"],
      **{k: events[k] for k in ("warnings", "stockout", "quota")},
  }


def flags(runs: Sequence[Dict[str, Any]]) -> List[str]:
  """Conditions that make a run or a round a poor comparison."""
  out = []
  for run in runs:
    tag = f"Round {run['round']} {run['variant']}"
    peak = run.get("metrics", {}).get("peak_workers")
    platforms = run.get("platforms", [])
    missing = run.get("monitoring_missing", [])
    out += [f"{tag}: {run['error']}"] if run.get("error") else []
    out += [f"{tag}: state {run['state']}"] if run.get("state") not in (None, "DONE") else []
    out += [f"{tag}: stockout warning"] if run.get("stockout") else []
    out += [f"{tag}: quota warning"] if run.get("quota") else []
    out += [f"{tag}: autoscaled to {peak:.0f} workers"] if peak and peak > 1 else []
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
  return out


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
  metrics = columns(config.get("count_name", "elements"), config.get("counter"))
  extra = [("CPU platform", lambda r: ", ".join(r.get("platforms", [])) or "?"),
           ("State", lambda r: r.get("state", "?")),
           ("Start", lambda r: (r.get("start") or "?")[:19]),
           ("End", lambda r: (r.get("end") or "?")[:19]),
           ("Job", lambda r: r.get("job_id") or "?")]
  warnings = [f"- Round {r['round']} {r['variant']}: {w}" for r in runs for w in r.get("warnings", [])]
  return [
      f"# {results['task']} ({results['created']})", "",
      config["description"], "",
      "Variants: " + ", ".join(f"{k}={v}" for k, v in config["variants"].items()) + ".", "",
      "## Flags", "", *([f"- {f}" for f in results["flags"]] or ["None."]), "",
      "## Medians", "", *report.compare_table(all_medians, "metrics", metrics), "",
      "## Runs", "", *report.runs_table(runs, "metrics", metrics, extra), "",
      "## ProcessElement time of each step (s, all workers)", "",
      *report.compare_table(all_medians, "steps", ranked_columns(all_medians, "steps", 1.0, ",.1f")),
      "", "## Peak memory of each container (MiB, any worker)", "",
      *report.compare_table(all_medians, "memory", ranked_columns(all_medians, "memory", 0.0, ",.0f")),
      "", "## Warnings and errors", "", *(warnings or ["None."]),
  ]


def label_value(text: str) -> str:
  """A valid Dataflow label value or job name part: lowercase letters, digits and hyphens."""
  return re.sub(r"[^a-z0-9-]", "-", re.sub(r"(?<=[a-z0-9])([A-Z])", r"-\1", text).lower())


def run_values(task: str, label: str, round_number: int, run_id: int) -> Dict[str, str]:
  """The placeholder values of one job: its name and its standard labels.

  The labels have two forms: {labels} is `k=v,k=v`, which the Rust runner reads from one
  --labels flag, and {labels_json} is a JSON object. A launcher that takes another form
  needs a new placeholder here.
  """
  labels = {"benchmark": label_value(task), "variant": label_value(label),
            "round": str(round_number), "run_id": str(run_id)}
  return {
      "job_name": f"{labels['benchmark']}-{labels['variant']}-r{round_number}-{run_id}",
      "labels": ",".join(f"{k}={v}" for k, v in labels.items()),
      "labels_json": json.dumps(labels, separators=(",", ":")),
  }


def launch(args: argparse.Namespace, variant: report.Variant, round_number: int,
           run_id: int) -> Dict[str, Any]:
  """Runs one job to its end and returns its record."""
  label = variant["label"]
  values = run_values(args.task, label, round_number, run_id)
  name = values["job_name"]
  base = os.path.join(args.out_dir, f"{label}-r{round_number}")
  platforms: Set[str] = set()
  job = None
  launched = datetime.datetime.now(datetime.timezone.utc)
  with open(f"{base}.log", "w") as out:
    process = subprocess.Popen(report.expand(variant, values), cwd=variant["cwd"],
                               env=report.environment(variant), stdout=out,
                               stderr=subprocess.STDOUT)
    while process.poll() is None:
      platforms |= cpu_platforms(args.project, name)
      job = job or find_job(list_jobs(args.project, args.region, name), name, launched)
      try:
        process.wait(timeout=POLL_SECONDS)
      except subprocess.TimeoutExpired:
        pass
  record = {"round": round_number, "variant": label, "job_name": name,
            "launcher_exit": process.returncode, "platforms": sorted(platforms)}
  job = job or find_job(list_jobs(args.project, args.region, name), name, launched)
  with open(f"{base}.log") as f:
    job_id = job["id"] if job else job_id_from_log(f.read())
  if not job_id:
    return {**record, "error": f"no job named {name}, see {base}.log", "metrics": {}}
  details = fetch(args.project, args.region, job_id)
  details["monitoring"] = collect_monitoring(args.project, details, args.counter,
                                             args.monitoring_wait)
  with open(f"{base}-job.json", "w") as f:
    json.dump(details, f, indent=2)
  return {**record, **summarize(details, args.count_output, args.counter)}


def prepare(args: argparse.Namespace, variants: Sequence[report.Variant]) -> None:
  """Runs the "prepare" command of each variant, one after the other, before the rounds.

  The command builds what the launch command needs, so the launches of a round only submit
  their jobs and start together. The {out_dir} placeholder is the output directory.
  """
  for variant in (v for v in variants if v.get("prepare")):
    command = report.expand({"argv": variant["prepare"]}, {"out_dir": args.out_dir})
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


def run(args: argparse.Namespace) -> None:
  variants = report.load_variants(args.variants)
  prepare(args, variants)
  if args.prepare_only:
    return
  runs = []
  for round_number in range(1, args.runs + 1):
    run_id = int(time.time())
    for variant in variants:
      values = run_values(args.task, variant["label"], round_number, run_id)
      command = report.expand(variant, values)
      print(f"Round {round_number}, {variant['label']}: "
            f"{report.show(command, variant['cwd'], variant.get('env'))}\n"
            f"  while it runs: {' '.join(platform_command(args.project, values['job_name']))}",
            flush=True)
    if args.dry_run:
      continue
    os.makedirs(args.out_dir, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(len(variants)) as pool:
      jobs = [pool.submit(launch, args, variant, round_number, run_id) for variant in variants]
      runs += [job.result() for job in jobs]
  if args.dry_run:
    return
  results = {
      "task": args.task,
      "created": datetime.datetime.now().isoformat(timespec="seconds"),
      "config": {"description": args.describe, "runs": args.runs,
                 "count_name": args.count_name, "counter": args.counter,
                 "variants": {v["label"]: v.get("describe", v["cwd"]) for v in variants}},
      "runs": runs,
      "medians": report.medians(runs, SECTIONS),
      "flags": flags(runs),
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
  run_parser, fetch_parser = commands.choices["run"], commands.choices["fetch"]
  run_parser.add_argument("--task", required=True, help="The benchmark, for names and labels.")
  run_parser.add_argument("--out-dir", required=True)
  run_parser.add_argument("--variants", required=True,
                          help="JSON file of the variants. The jobs of a round start together.")
  run_parser.add_argument("--runs", type=int, default=3)
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
    details["monitoring"] = collect_monitoring(args.project, details, args.counter, 0)
    records.append(summarize(details, args.count_output, args.counter))
  print(json.dumps(records, indent=2))


if __name__ == "__main__":
  main(sys.argv[1:])

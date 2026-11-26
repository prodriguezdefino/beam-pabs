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
Google Cloud Dataflow v1b3 REST calls, worker VM helpers and JSON reply parsers for
dataflow_job.py.
"""

import datetime
import json
import re
import subprocess
import urllib.error
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Set

import monitoring

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
UNKNOWN_PLATFORM = "Unknown CPU Platform"
# A job found by name must start at most this long before its launch command, for clock skew.
LAUNCH_SLACK = datetime.timedelta(seconds=60)


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
  """The newest non-cancelled job with this name that started after the launch."""
  matches = [
      job for job in jobs
      if job.get("name") == name and job.get("currentState") != "JOB_STATE_CANCELLED"
      and monitoring.parse_time(job["createTime"]) >= launched - LAUNCH_SLACK
  ]
  return max(matches, key=lambda job: job["createTime"], default=None)


def get_job(project: str, region: str, job_id: str) -> Optional[Dict[str, Any]]:
  try:
    return get(f"{API.format(project=project, region=region)}/{job_id}", access_token())
  except (urllib.error.URLError, OSError, subprocess.CalledProcessError):
    return None


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


def platform_command(project: str, job_name: str) -> List[str]:
  return ["gcloud", "compute", "instances", "list", f"--project={project}",
          f"--filter=labels.dataflow_job_name~^{job_name}", "--format=value(cpuPlatform)"]


def cpu_platforms(project: str, job_name: str) -> Set[str]:
  """The CPU platforms of the running worker VMs of a job. Empty when none runs.

  A staging VM reports an unknown platform, so `known_platforms` skips that value.
  """
  out = subprocess.run(platform_command(project, job_name), capture_output=True, text=True)
  return known_platforms(out.stdout.splitlines())


def known_platforms(lines: Iterable[str]) -> Set[str]:
  return {line.strip() for line in lines if line.strip() and line.strip() != UNKNOWN_PLATFORM}


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


def job_id_from_log(text: str) -> Optional[str]:
  match = JOB_ID.search(text)
  return (match.group(1) or match.group(2)) if match else None


def parse_metrics(metrics: Iterable[Dict[str, Any]], count_output: str,
                  counter: Optional[str] = None,
                  namespace: Optional[str] = None) -> Dict[str, Any]:
  """The job totals, the counted elements and the ProcessElement seconds of each step.

  The counted elements are the elements of the outputs whose name holds `count_output`.
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
  vcpu, step_wall = totals.get("TotalVcpuTime"), sum(steps.values())
  values = {
      "total_vcpu_s": vcpu,
      "elements": elements or None,
      "elements_per_vcpu_s": elements / vcpu if vcpu and elements else None,
      "memory_gb_s": totals["TotalMemoryUsage"] / 1024 if "TotalMemoryUsage" in totals else None,
      "pd_gb_s": totals.get("TotalPdUsage"),
      # The service reports the shuffle data in GB.
      "shuffle_gb": totals.get("TotalShuffleDataProcessed"),
      "step_wall_s": step_wall,
      "step_wall_s_per_1m_elements": step_wall / (elements / 1e6) if elements else None,
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

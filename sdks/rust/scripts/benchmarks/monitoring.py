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
Cloud Monitoring data of one Dataflow job: the CPU time that the worker VMs used, the peak
memory of each container, the vCPUs of the job over time, and the rate of a user counter.

The used vCPU·s is the CPU utilization of each worker VM, times its vCPUs, for each minute.
Unlike the total vCPU·s of the job, it leaves out the idle time of VMs that start, wait for
work or stop. Monitoring gets the data one to three minutes after the job ends, so `collect`
reads again until the data covers the end of the job or the wait stops.
"""

import datetime
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

URL = "https://monitoring.googleapis.com/v3/projects/{project}/timeSeries"
RETRY_SECONDS = 30
# The last point of a complete series is at most this many seconds before the job end. The
# worker VMs report each minute and stop shortly before the job ends.
COMPLETE_WITHIN_SECONDS = 150

# vCPUs of the shared-core machine types, whose names hold no vCPU count.
SHARED_CORE_VCPUS = {"e2-micro": 2, "e2-small": 2, "e2-medium": 2, "f1-micro": 1, "g1-small": 1}
MACHINE_VCPUS = re.compile(r"-(?:custom-)?(\d+)(?:-\d+)?$")

Series = List[Dict[str, Any]]
Points = List[Tuple[datetime.datetime, float]]


def queries(job_id: str, counter: Optional[str]) -> Dict[str, Dict[str, str]]:
  """The request parameters of each series, without the interval."""
  out = {
      "cpu": {
          "filter": 'metric.type="compute.googleapis.com/instance/cpu/utilization" '
                    'AND resource.type="gce_instance" '
                    f'AND metadata.user_labels."dataflow_job_id"="{job_id}"',
          "aggregation.alignmentPeriod": "60s",
          "aggregation.perSeriesAligner": "ALIGN_MEAN",
      },
      "memory": {
          "filter": 'metric.type="dataflow.googleapis.com/worker/memory/bytes_used" '
                    f'AND resource.labels.job_id="{job_id}"'
      },
      "vcpus": {
          "filter": 'metric.type="dataflow.googleapis.com/job/current_num_vcpus" '
                    f'AND metric.labels.job_id="{job_id}"'
      },
  }
  if counter:
    out["counter"] = {
        "filter": 'metric.type="dataflow.googleapis.com/job/user_counter" '
                  f'AND metric.labels.job_id="{job_id}" '
                  f'AND metric.labels.metric_name="{counter}"'
    }
  return out


def time_series(project: str, token: str, query: Dict[str, str]) -> Series:
  """All pages of the time series of one query."""
  series, page = [], None
  while True:
    params = {**query, "pageToken": page} if page else query
    request = urllib.request.Request(
        f"{URL.format(project=project)}?{urllib.parse.urlencode(params)}",
        headers={"Authorization": f"Bearer {token}", "x-goog-user-project": project})
    with urllib.request.urlopen(request) as response:
      reply = json.load(response)
    series += reply.get("timeSeries", [])
    page = reply.get("nextPageToken")
    if not page:
      return series


def parse_time(value: str) -> datetime.datetime:
  # Python before 3.11 parses at most six digits of a fraction.
  return datetime.datetime.fromisoformat(
      re.sub(r"(\.\d{6})\d*", r"\1", value).replace("Z", "+00:00"))


def point_value(point: Dict[str, Any]) -> float:
  value = point["value"]
  return float(value.get("doubleValue", value.get("int64Value", 0)))


def points_of(one: Dict[str, Any]) -> Points:
  """The points of one series, in time order."""
  return sorted((parse_time(p["interval"]["endTime"]), point_value(p)) for p in one["points"])


def value_at(points: Points, at: datetime.datetime) -> float:
  """The last value at or before `at`, or 0."""
  before = [v for t, v in points if t <= at]
  return before[-1] if before else 0.0


def total_points(series: Series) -> Points:
  """The sum of all series at each time of any series. Each series keeps its last value."""
  each = [points_of(s) for s in series]
  times = sorted({t for points in each for t, _ in points})
  return [(t, sum(value_at(points, t) for points in each)) for t in times]


def machine_vcpus(machine_type: Optional[str]) -> Optional[int]:
  """vCPUs of a machine type such as `n2-standard-4` or `n2-custom-8-32768`, or None."""
  if not machine_type:
    return None
  name = machine_type.rsplit("/", 1)[-1]
  match = MACHINE_VCPUS.search(name)
  return SHARED_CORE_VCPUS.get(name) or (int(match.group(1)) if match else None)


def used_vcpu_seconds(cpu: Series, vcpus: Points, vcpus_per_vm: Optional[int]) -> Optional[float]:
  """CPU seconds that the worker VMs used, or None without data.

  Without `vcpus_per_vm`, a VM gets the vCPUs of the job in that minute divided by its VMs.
  """
  by_minute: Dict[datetime.datetime, List[float]] = {}
  for one in cpu:
    for t, utilization in points_of(one):
      by_minute.setdefault(t, []).append(utilization)
  if not by_minute:
    return None
  total = 0.0
  for minute, utilizations in by_minute.items():
    # The CPU and the vCPU series align their points to different seconds of the minute.
    nearest = min(vcpus, key=lambda p: abs((p[0] - minute).total_seconds()), default=None)
    per_vm = vcpus_per_vm or (nearest[1] / len(utilizations) if nearest else None)
    if not per_vm:
      return None
    total += sum(utilizations) * per_vm * 60
  return total


def container_peaks(memory: Series) -> Dict[str, float]:
  """The peak memory of each container on any worker, in MiB."""
  peaks: Dict[str, float] = {}
  for one in memory:
    container = one["metric"]["labels"].get("container", "?")
    peak = max((v for _, v in points_of(one)), default=0.0) / 2**20
    peaks[container] = max(peaks.get(container, 0.0), peak)
  return peaks


def counter_rate(counter: Series, expected: Optional[float] = None) -> Dict[str, float]:
  """The final value of a counter and its rate between 10% and 90% of that value.

  The window leaves out worker startup and the tail of the job. `expected` is the final value
  from the Dataflow API. Monitoring gets the last minutes of a counter late, so the rate
  needs only the points up to 90% of that value.
  """
  points = total_points(counter)
  final = expected or (points[-1][1] if points else 0.0)
  if final <= 0 or not points or points[-1][1] < 0.9 * final:
    return {}
  start = next(p for p in points if p[1] >= 0.1 * final)
  end = next(p for p in points if p[1] >= 0.9 * final)
  seconds = (end[0] - start[0]).total_seconds()
  rate = {"counter_per_s": (end[1] - start[1]) / seconds} if seconds > 0 else {}
  return {"counter_total": final, **rate}


def complete(raw: Dict[str, Series], end: str, expected_counter: Optional[float] = None) -> bool:
  """Whether every series has data up to the job end, and the counter its expected value."""
  limit = parse_time(end) - datetime.timedelta(seconds=COMPLETE_WITHIN_SECONDS)
  latest = lambda series: max((t for s in series for t, _ in points_of(s)), default=None)
  counter_ok = "counter" not in raw or not expected_counter or bool(
      counter_rate(raw["counter"], expected_counter))
  return counter_ok and all(
      (last := latest(series)) is not None and last >= limit for series in raw.values())


def collect(fetch: Callable[[Dict[str, str]], Series], job_id: str, start: str, end: str,
            counter: Optional[str], wait_seconds: float, expected_counter: Optional[float] = None,
            sleep: Callable[[float], None] = time.sleep) -> Dict[str, Series]:
  """Reads each series of a job until all are complete, or until `wait_seconds` pass.

  A query that fails gives no series, so its values are missing in the report.
  """
  interval = {"interval.startTime": start,
              "interval.endTime": datetime.datetime.now(datetime.timezone.utc).isoformat()}
  waited = 0.0
  while True:
    raw = {}
    for name, query in queries(job_id, counter).items():
      try:
        raw[name] = fetch({**query, **interval})
      except (urllib.error.URLError, OSError):
        raw[name] = []
    if complete(raw, end, expected_counter) or waited >= wait_seconds:
      return raw
    sleep(RETRY_SECONDS)
    waited += RETRY_SECONDS
    interval["interval.endTime"] = datetime.datetime.now(datetime.timezone.utc).isoformat()


def summarize(raw: Dict[str, Series], machine_type: Optional[str], total_vcpu_s: Optional[float],
              expected_counter: Optional[float] = None) -> Dict[str, Any]:
  """The metrics and the container memory of a job from the series of `collect`.

  `expected_counter` is the final counter value from the Dataflow API. A counter series that
  has not reached it counts as missing.
  """
  vcpus = total_points(raw.get("vcpus", []))
  used = used_vcpu_seconds(raw.get("cpu", []), vcpus, machine_vcpus(machine_type))
  memory = container_peaks(raw.get("memory", []))
  sdk = [mib for container, mib in memory.items() if container.startswith("sdk")]
  rate = counter_rate(raw.get("counter", []), expected_counter)
  total = expected_counter or rate.get("counter_total")
  values = {
      "used_vcpu_s": used,
      "used_vcpu_ratio": used / total_vcpu_s if used and total_vcpu_s else None,
      "peak_vcpus": max((v for _, v in vcpus), default=None),
      "peak_sdk_memory_mib": max(sdk, default=None),
      "counter_total": total,
      "counter_per_s": rate.get("counter_per_s"),
      "counter_per_used_vcpu_s": total / used if used and total else None,
  }
  missing = [name for name in ("cpu", "memory", "vcpus", "counter")
             if name in raw and (not raw[name] or (name == "counter" and not rate))]
  return {"metrics": {k: v for k, v in values.items() if v is not None},
          "memory": memory, "monitoring_missing": missing}

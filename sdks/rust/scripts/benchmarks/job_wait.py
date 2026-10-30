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
Waits for a Dataflow job after its launcher stops.

The "wait" field of a variant (see report.py) tells dataflow_job.py what its launcher does:

- "async": the launcher stops when the job starts, for example a Rust pipeline with
  --async_job. The script polls the job state until the job ends.
- "counter", "expected_rows", "drain_after_seconds": the script drains a streaming job when
  the user counter "counter" reaches "expected_rows", or after "drain_after_seconds" from
  the end of the launcher. A job that ends with fewer rows than "expected_rows" fails.
- "detach": the job keeps running after the round. The script records the job and reads no
  metrics, so a load job can run at the same time for a latency run.

A launcher without "async" waits for its job itself, for example the Python pipelines with
their wait_for_streaming_job.
"""

import time
from typing import Any, Callable, Dict, Optional

DONE = ("JOB_STATE_DONE", "JOB_STATE_DRAINED", "JOB_STATE_CANCELLED")
TERMINAL = DONE + ("JOB_STATE_FAILED", "JOB_STATE_UPDATED")
POLL_SECONDS = 30
# The job of a launcher that stopped must appear in the job list within this time.
FIND_SECONDS = 600

Job = Dict[str, Any]
Spec = Dict[str, Any]


def drain_due(spec: Spec, state: str, rows: Optional[float], elapsed: float) -> bool:
  """Whether a running job reached the drain condition of `spec`."""
  expected, seconds = spec.get("expected_rows"), spec.get("drain_after_seconds")
  return state == "JOB_STATE_RUNNING" and (
      (expected is not None and rows is not None and rows >= float(expected))
      or (seconds is not None and elapsed >= float(seconds)))


def outcome(spec: Spec, job: Job, rows: Optional[float]) -> Optional[str]:
  """An error for a job that ended, or None."""
  state, expected = job.get("currentState", ""), spec.get("expected_rows")
  if state not in DONE:
    return f"job ended in {state}"
  if expected is not None and (rows or 0) < float(expected):
    return f"job ended with {rows or 0:.0f} rows, expected {expected}"
  return None


def wait(spec: Spec, get_job: Callable[[], Optional[Job]],
         rows: Callable[[Job], Optional[float]], drain: Callable[[Job], None],
         on_poll: Callable[[], None] = lambda: None,
         sleep: Callable[[float], None] = time.sleep,
         clock: Callable[[], float] = time.monotonic) -> Dict[str, Any]:
  """Polls the job until it ends and drains it at the condition of `spec`.

  Returns {"job": the last job or None, "error": None or a reason}. `get_job` gives the job
  or None while it is not in the job list, and `rows` the value of the counter.
  """
  start, drained, job = clock(), False, None
  while True:
    on_poll()
    job = get_job() or job
    elapsed = clock() - start
    if job is None and elapsed >= FIND_SECONDS:
      return {"job": None, "error": "the job did not appear"}
    state = job.get("currentState", "") if job else ""
    counted = rows(job) if job and spec.get("expected_rows") is not None else None
    if state in TERMINAL:
      return {"job": job, "error": outcome(spec, job, counted)}
    # A batch job ends on its own, so "expected_rows" only checks it.
    streaming = job is not None and job.get("type") != "JOB_TYPE_BATCH"
    if streaming and not drained and drain_due(spec, state, counted, elapsed):
      at = f"{counted:.0f} rows" if counted is not None else f"{elapsed:.0f} s"
      print(f"Drain {job.get('name')} at {at}", flush=True)
      drain(job)
      drained = True
    sleep(POLL_SECONDS)

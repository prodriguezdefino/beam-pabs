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
Waits for a Dataflow job after its launcher submits the job and exits.

The "wait" field of a variant (see report.py) configures how dataflow_job.py waits for,
drains and checks a submitted job across all SDKs:

- "counter", "expected_rows", "drain_after_seconds": drains a streaming job when the user
  counter "counter" reaches "expected_rows", or after "drain_after_seconds" from launcher
  exit. A batch job must end with "expected_rows" exact rows; a streaming job must end with
  at least "expected_rows" rows. A counter that can not be read is an error, not 0 rows.
- "namespace": the namespace of "counter". Default: the namespace of --io.
- "drain_after_producer_seconds": drains a streaming job this long after the round's
  companion producer job finishes.
- "kill_worker_after_seconds": deletes one worker VM of a running streaming job at that time,
  for a fault test.
- "detach": leaves the job running after submission without waiting or collecting metrics.
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


def drain_due(spec: Spec, state: str, rows: Optional[float], elapsed: float,
              producer_elapsed: Optional[float] = None) -> bool:
  """Whether a running job reached the drain condition of `spec`."""
  expected, seconds = spec.get("expected_rows"), spec.get("drain_after_seconds")
  after_producer = spec.get("drain_after_producer_seconds")
  return state == "JOB_STATE_RUNNING" and (
      (expected is not None and rows is not None and rows >= float(expected))
      or (seconds is not None and elapsed >= float(seconds))
      or (after_producer is not None and producer_elapsed is not None
          and producer_elapsed >= float(after_producer)))


def outcome(spec: Spec, job: Job, rows: Optional[float]) -> Optional[str]:
  """An error for a job that ended, or None. `rows` is None when the counter is unknown."""
  state, expected = job.get("currentState", ""), spec.get("expected_rows")
  if state not in DONE:
    return f"job ended in {state}"
  if expected is not None:
    if rows is None:
      return f"job ended, but its row counter could not be read (expected {expected})"
    batch = job.get("type") == "JOB_TYPE_BATCH"
    if (rows != float(expected)) if batch else (rows < float(expected)):
      return f"job ended with {rows:.0f} rows, expected {expected}"
  return None


def wait(spec: Spec, get_job: Callable[[], Optional[Job]],
         rows: Callable[[Job], Optional[float]], drain: Callable[[Job], None],
         on_poll: Callable[[], None] = lambda: None,
         on_running: Callable[[], None] = lambda: None,
         producer_done_at: Callable[[], Optional[float]] = lambda: None,
         kill: Callable[[Job], None] = lambda job: None,
         sleep: Callable[[float], None] = time.sleep,
         clock: Callable[[], float] = time.monotonic) -> Dict[str, Any]:
  """Polls the job until it ends and drains it at the condition of `spec`.

  Returns {"job": the last job or None, "error": None or a reason}. `get_job` gives the job
  or None while it is not in the job list, and `rows` the value of the counter. `on_running`
  runs once when the job enters RUNNING or a terminal state. `kill` deletes one worker VM.
  """
  start, drained, killed, notified, job = clock(), False, False, False, None
  while True:
    on_poll()
    job = get_job() or job
    now = clock()
    elapsed = now - start
    if job is None and elapsed >= FIND_SECONDS:
      if not notified:
        on_running()
      return {"job": None, "error": "the job did not appear"}
    state = job.get("currentState", "") if job else ""
    if not notified and (state == "JOB_STATE_RUNNING" or state in TERMINAL):
      on_running()
      notified = True
    counted = rows(job) if job and spec.get("expected_rows") is not None else None
    if state in TERMINAL:
      # A read can fail on an API error, so read once more before the check.
      if counted is None and spec.get("expected_rows") is not None:
        counted = rows(job)
      return {"job": job, "error": outcome(spec, job, counted)}
    # A batch job ends on its own, so "expected_rows" only checks it.
    streaming = job is not None and job.get("type") != "JOB_TYPE_BATCH"
    kill_after = spec.get("kill_worker_after_seconds")
    if (streaming and not killed and not drained and state == "JOB_STATE_RUNNING"
        and kill_after is not None and elapsed >= float(kill_after)):
      print(f"Kill one worker of {job.get('name')} at {elapsed:.0f} s", flush=True)
      kill(job)
      killed = True
    producer_done = producer_done_at()
    producer_elapsed = (now - producer_done) if producer_done is not None else None
    if streaming and not drained and drain_due(spec, state, counted, elapsed, producer_elapsed):
      at = f"{counted:.0f} rows" if counted is not None else f"{elapsed:.0f} s"
      print(f"Drain {job.get('name')} at {at}", flush=True)
      drain(job)
      drained = True
    sleep(POLL_SECONDS)

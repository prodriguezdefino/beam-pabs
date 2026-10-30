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

import contextlib
import io
import unittest
import job_wait


def job(state, name="j"):
  return {"id": "1", "name": name, "currentState": f"JOB_STATE_{state}"}


class Clock:
  """A clock that each sleep moves forward."""

  def __init__(self):
    self.now = 0.0

  def sleep(self, seconds):
    self.now += seconds


def run_wait(spec, states, rows=lambda j: None):
  """Waits through `states` (one for each poll) and returns the result and the drains."""
  clock, drains, polls = Clock(), [], iter(states)
  with contextlib.redirect_stdout(io.StringIO()):
    result = job_wait.wait(spec, lambda: next(polls), rows, drains.append, sleep=clock.sleep,
                           clock=lambda: clock.now)
  return result, drains, clock.now


class TestJobWait(unittest.TestCase):
  def test_drain_due(self):
    self.assertTrue(job_wait.drain_due({"expected_rows": 10}, "JOB_STATE_RUNNING", 10, 0))
    self.assertFalse(job_wait.drain_due({"expected_rows": 10}, "JOB_STATE_RUNNING", 9, 0))
    self.assertFalse(job_wait.drain_due({"expected_rows": 10}, "JOB_STATE_RUNNING", None, 0))
    self.assertTrue(job_wait.drain_due({"drain_after_seconds": 60}, "JOB_STATE_RUNNING", None, 60))
    self.assertFalse(job_wait.drain_due({"drain_after_seconds": 60}, "JOB_STATE_PENDING", 0, 99))
    self.assertFalse(job_wait.drain_due({}, "JOB_STATE_RUNNING", 99, 99))

  def test_batch_job_waits_until_done(self):
    result, drains, waited = run_wait({"async": True},
                                      [None, job("PENDING"), job("RUNNING"), job("DONE")])
    self.assertEqual(result, {"job": job("DONE"), "error": None})
    self.assertEqual(drains, [])
    self.assertEqual(waited, 3 * job_wait.POLL_SECONDS)

  def test_streaming_job_drains_once_at_the_expected_rows(self):
    rows = iter([5, 20, 20, 20])
    result, drains, _ = run_wait({"async": True, "expected_rows": 20},
                                 [job("RUNNING"), job("RUNNING"), job("DRAINING"),
                                  job("DRAINED")], lambda j: next(rows))
    self.assertEqual(len(drains), 1)
    self.assertIsNone(result["error"])

  def test_streaming_job_drains_after_the_time(self):
    states = [job("RUNNING")] * 4 + [job("DRAINED")]
    _, drains, _ = run_wait({"async": True, "drain_after_seconds": 90}, states)
    self.assertEqual(len(drains), 1)

  def test_errors(self):
    result, _, _ = run_wait({"async": True}, [job("FAILED")])
    self.assertEqual(result["error"], "job ended in JOB_STATE_FAILED")
    result, _, _ = run_wait({"async": True, "expected_rows": 20}, [job("CANCELLED")],
                            lambda j: 3)
    self.assertEqual(result["error"], "job ended with 3 rows, expected 20")
    result, _, waited = run_wait({"async": True}, iter(lambda: None, 1))
    self.assertEqual(result, {"job": None, "error": "the job did not appear"})
    self.assertEqual(waited, job_wait.FIND_SECONDS)

  def test_a_lost_lookup_keeps_the_last_job(self):
    result, _, _ = run_wait({"async": True}, [job("RUNNING"), None, job("DONE")])
    self.assertEqual(result["job"], job("DONE"))

  def test_batch_job_is_checked_but_not_drained(self):
    batch = lambda state: {**job(state), "type": "JOB_TYPE_BATCH"}
    result, drains, _ = run_wait({"async": True, "expected_rows": 10},
                                 [batch("RUNNING"), batch("DONE")], lambda j: 10)
    self.assertEqual((drains, result["error"]), ([], None))


if __name__ == "__main__":
  unittest.main()

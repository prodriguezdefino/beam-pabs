/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Drives `monitor_streaming_job` against a scripted job handle.

use std::sync::Mutex;
use std::time::Duration;

use beam::metrics::{MetricKey, MetricPhase, MetricReading, MetricResults, MetricValue};
use beam::runners::{JobHandle, JobState, PipelineResult, RunnerError};
use nexmark::launcher::{
    ELEMENTS, EVENTS, LauncherError, METRIC_NAMESPACE, MonitorTiming, StopReason, StopRules,
    metric_name, monitor_streaming_job, with_publisher,
};

const TIMING: MonitorTiming = MonitorTiming {
    perf_delay: Duration::from_millis(1),
    done_delay: Duration::from_millis(20),
    stuck_warning: Duration::from_secs(3600),
    stuck_terminate: Duration::from_secs(3600),
    min_steady_window: Duration::ZERO,
};

const RULES: StopRules = StopRules {
    done_delay: Duration::from_millis(20),
    stuck_warning: Duration::from_secs(3600),
    stuck_terminate: Duration::from_secs(3600),
    stream_timeout: Duration::from_secs(3600),
    expected_events: Some(100),
};

/// A job whose event count follows `events`, one entry per poll, then keeps the last entry.
struct ScriptedJob {
    events: Vec<i64>,
    fatal: i64,
    can_drain: bool,
    /// Poll after which the job ends by itself in `DONE`.
    ends_after: Option<usize>,
    polls: Mutex<usize>,
    state: Mutex<JobState>,
    calls: Mutex<Vec<&'static str>>,
}

impl ScriptedJob {
    fn new(events: &[i64]) -> Self {
        Self {
            events: events.to_vec(),
            fatal: 0,
            can_drain: true,
            ends_after: None,
            polls: Mutex::new(0),
            state: Mutex::new(JobState::Running),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().expect("calls lock").clone()
    }

    fn stop(&self, call: &'static str, state: JobState) {
        self.calls.lock().expect("calls lock").push(call);
        *self.state.lock().expect("state lock") = state;
    }
}

fn reading(namespace: &str, name: String, value: i64) -> MetricReading {
    MetricReading {
        key: MetricKey::new("Step", namespace, name),
        phase: MetricPhase::Committed,
        value: MetricValue::Counter(value),
    }
}

#[async_trait::async_trait]
impl JobHandle for ScriptedJob {
    fn job_id(&self) -> &str {
        "scripted"
    }

    async fn state(&self) -> Result<JobState, RunnerError> {
        let polls = *self.polls.lock().expect("polls lock");
        let state = *self.state.lock().expect("state lock");
        Ok(match self.ends_after {
            Some(last) if polls >= last => JobState::Done,
            Some(_) | None => state,
        })
    }

    async fn metrics(&self) -> Result<MetricResults, RunnerError> {
        let poll = {
            let mut polls = self.polls.lock().expect("polls lock");
            *polls += 1;
            *polls - 1
        };
        let events = self.events[poll.min(self.events.len() - 1)];
        let counters = [
            reading(METRIC_NAMESPACE, metric_name(EVENTS, ELEMENTS), events),
            reading("Query3", "fatal".to_owned(), self.fatal),
        ];
        Ok(counters.into_iter().collect())
    }

    async fn cancel(&self) -> Result<(), RunnerError> {
        self.stop("cancel", JobState::Cancelled);
        Ok(())
    }

    async fn drain(&self) -> Result<(), RunnerError> {
        match self.can_drain {
            true => {
                self.stop("drain", JobState::Drained);
                Ok(())
            }
            false => {
                self.calls.lock().expect("calls lock").push("drain");
                Err(RunnerError::Unsupported {
                    runner: "scripted",
                    operation: "drain",
                })
            }
        }
    }

    /// Gives the current state. With a timeout, a job that has not ended waits for it first.
    async fn wait_until_finish(
        &self,
        timeout: Option<Duration>,
    ) -> Result<PipelineResult, RunnerError> {
        let state = self.state().await?;
        if let (Some(timeout), false) = (timeout, state.is_terminal()) {
            tokio::time::sleep(timeout).await;
        }
        Ok(PipelineResult::new("scripted", state))
    }
}

async fn monitor(
    job: &ScriptedJob,
    rules: StopRules,
    cancel: bool,
) -> Result<StopReason, LauncherError> {
    monitor_streaming_job(job, &TIMING, rules, cancel, 0)
        .await
        .map(|run| run.stop)
}

#[tokio::test]
async fn done_rule_drains_the_job() {
    let job = ScriptedJob::new(&[10, 50, 100]);
    assert_eq!(
        monitor(&job, RULES, false).await.ok(),
        Some(StopReason::Finished)
    );
    assert_eq!(job.calls(), ["drain"]);
}

#[tokio::test]
async fn done_rule_cancels_when_drain_is_unsupported_or_cancel_is_asked() {
    let no_drain = ScriptedJob {
        can_drain: false,
        ..ScriptedJob::new(&[100])
    };
    assert_eq!(
        monitor(&no_drain, RULES, false).await.ok(),
        Some(StopReason::Finished)
    );
    assert_eq!(no_drain.calls(), ["drain", "cancel"]);

    let cancel = ScriptedJob::new(&[100]);
    assert_eq!(
        monitor(&cancel, RULES, true).await.ok(),
        Some(StopReason::Finished)
    );
    assert_eq!(cancel.calls(), ["cancel"]);
}

#[tokio::test]
async fn fatal_errors_and_stuck_jobs_are_cancelled_and_fail() {
    let fatal = ScriptedJob {
        fatal: 2,
        ..ScriptedJob::new(&[5])
    };
    let err = monitor(&fatal, RULES, false)
        .await
        .expect_err("fatal fails");
    assert!(err.to_string().contains("2 fatal errors"), "{err}");
    assert_eq!(fatal.calls(), ["cancel"]);

    let stuck = ScriptedJob::new(&[5]);
    let rules = StopRules {
        stuck_terminate: Duration::from_millis(20),
        ..RULES
    };
    let err = monitor(&stuck, rules, false)
        .await
        .expect_err("stuck fails");
    assert!(err.to_string().contains("no progress"), "{err}");
    assert_eq!(stuck.calls(), ["cancel"]);
}

#[tokio::test]
async fn timeout_cancels_an_endless_job() {
    let growing: Vec<i64> = (1..=10_000).collect();
    let job = ScriptedJob::new(&growing);
    let rules = StopRules {
        stream_timeout: Duration::from_millis(30),
        expected_events: None,
        ..RULES
    };
    assert_eq!(
        monitor(&job, rules, false).await.ok(),
        Some(StopReason::TimedOut)
    );
    assert_eq!(job.calls(), ["cancel"]);
}

#[tokio::test]
async fn job_that_ends_by_itself_needs_no_stop() {
    let job = ScriptedJob {
        ends_after: Some(3),
        ..ScriptedJob::new(&[10, 20, 30])
    };
    let run = monitor_streaming_job(&job, &TIMING, RULES, false, 0)
        .await
        .expect("finished run");
    assert_eq!(run.stop, StopReason::Finished);
    assert_eq!(run.snapshots.len(), 3);
    assert!(job.calls().is_empty());
}

#[tokio::test]
async fn combined_run_cancels_the_publisher_when_the_query_ends() {
    let publisher = ScriptedJob::new(&[0]);
    let query = ScriptedJob {
        ends_after: Some(2),
        ..ScriptedJob::new(&[10, 100])
    };
    let run = with_publisher(&publisher, monitor(&query, RULES, false)).await;
    assert_eq!(run.ok(), Some(StopReason::Finished));
    assert_eq!(publisher.calls(), ["cancel"]);
    assert!(query.calls().is_empty());

    let failing_publisher = ScriptedJob::new(&[0]);
    let fatal_query = ScriptedJob {
        fatal: 1,
        ..ScriptedJob::new(&[10])
    };
    let run = with_publisher(&failing_publisher, monitor(&fatal_query, RULES, false)).await;
    assert!(run.is_err());
    assert_eq!(failing_publisher.calls(), ["cancel"]);
    assert_eq!(fatal_query.calls(), ["cancel"]);
}

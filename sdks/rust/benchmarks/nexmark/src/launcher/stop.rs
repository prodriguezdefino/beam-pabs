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

//! Stop rules for a streaming job: a pure state machine over metric observations.

use std::time::Duration;

use crate::config::NexmarkConfig;

/// Poll interval and the delays of the streaming stop rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MonitorTiming {
    /// Interval between two reads of the job state and metrics.
    pub perf_delay: Duration,
    /// Quiet time, after all expected events, that ends a streaming run.
    pub done_delay: Duration,
    /// Quiet time that logs a warning at each poll.
    pub stuck_warning: Duration,
    /// Quiet time that cancels the job and fails the run.
    pub stuck_terminate: Duration,
    /// Time after the submission that cancels a job without events and fails the run.
    pub start_limit: Duration,
    /// Shortest sample span that gives a steady-state event rate.
    pub min_steady_window: Duration,
}

impl Default for MonitorTiming {
    fn default() -> Self {
        Self {
            perf_delay: Duration::from_secs(15),
            done_delay: Duration::from_secs(60),
            stuck_warning: Duration::from_secs(10 * 60),
            stuck_terminate: Duration::from_secs(60 * 60),
            start_limit: Duration::from_secs(10 * 60),
            min_steady_window: Duration::from_secs(2 * 60),
        }
    }
}

/// Thresholds that [`decide`] applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StopRules {
    pub done_delay: Duration,
    pub stuck_warning: Duration,
    pub stuck_terminate: Duration,
    /// Time after the submission that ends a run without events.
    pub start_limit: Duration,
    /// Longest run after the first event.
    pub stream_timeout: Duration,
    /// Events in the whole input, or `None` for an endless input.
    pub expected_events: Option<i64>,
    /// Time after the submission when the run ends, from [`running_time_limit`].
    pub running_time: Option<Duration>,
}

impl StopRules {
    /// Takes the delays from `timing` and the input size and timeout from `config`.
    pub fn new(timing: &MonitorTiming, config: &NexmarkConfig) -> Self {
        Self {
            done_delay: timing.done_delay,
            stuck_warning: timing.stuck_warning,
            stuck_terminate: timing.stuck_terminate,
            start_limit: timing.start_limit,
            stream_timeout: Duration::from_secs(u64::try_from(config.stream_timeout).unwrap_or(0)),
            expected_events: (config.num_events > 0).then_some(config.num_events),
            running_time: None,
        }
    }

    /// Ends the run `running_time` after the submission.
    pub fn with_running_time(self, running_time: Option<Duration>) -> Self {
        Self {
            running_time,
            ..self
        }
    }
}

/// Run-time limit of a job: `minutes` less `preload_seconds`, or `None` without `minutes`.
///
/// A limit below zero gives zero, so the first poll ends the run.
pub fn running_time_limit(minutes: Option<i64>, preload_seconds: i32) -> Option<Duration> {
    minutes.map(|minutes| {
        let secs = minutes
            .saturating_mul(60)
            .saturating_sub(i64::from(preload_seconds));
        Duration::from_secs(u64::try_from(secs).unwrap_or(0))
    })
}

/// One read of the job metrics. `at` is the time since the job submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Observation {
    pub at: Duration,
    pub num_events: Option<i64>,
    pub num_results: Option<i64>,
    /// Sum of the `fatal` counters of all namespaces.
    pub fatal: i64,
}

/// What the monitor knows from the earlier observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MonitorState {
    pub rules: StopRules,
    /// Event and result counts of the last observation.
    pub counts: (Option<i64>, Option<i64>),
    /// Time of the last change of the counts. Before the first change, the submission.
    pub last_activity: Duration,
    /// Time of the first observation with events.
    pub first_event: Option<Duration>,
}

impl MonitorState {
    /// State before the first observation.
    pub fn new(rules: StopRules) -> Self {
        Self {
            rules,
            counts: (None, None),
            last_activity: Duration::ZERO,
            first_event: None,
        }
    }

    /// Adds `observation` to the state.
    pub fn observe(self, observation: &Observation) -> Self {
        let counts = (observation.num_events, observation.num_results);
        let last_activity = if counts == self.counts {
            self.last_activity
        } else {
            observation.at
        };
        let first_event = self.first_event.or_else(|| {
            observation
                .num_events
                .filter(|&events| events > 0)
                .map(|_| observation.at)
        });
        Self {
            counts,
            last_activity,
            first_event,
            ..self
        }
    }
}

/// What the monitor does after an observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Poll again.
    Continue,
    /// The run reached its run-time limit: cancel the job, the run is complete.
    RunningTimeReached,
    /// Log that the counts did not change for `quiet`, then poll again.
    Warn { quiet: Duration },
    /// All expected events passed and the job is quiet: stop the job, the run is complete.
    Done,
    /// The run reached its timeout: cancel the job and report the figures so far.
    TimedOut,
    /// The pipeline counted fatal errors: cancel the job and fail the run.
    Fatal { count: i64 },
    /// The counts did not change for `quiet`: cancel the job and fail the run.
    Stuck { quiet: Duration },
    /// The job reported no event `waited` after the submission: cancel the job and fail the run.
    NotStarted { waited: Duration },
}

/// Applies the stop rules, in priority order, to `observation` after it is added to `state`.
///
/// The run-time limit comes first: Java ends the test at that time and checks nothing else.
/// The other rules, from the highest priority: fatal errors, done, not started, stuck, timed
/// out, warning. A job without events cannot time out, because the timeout counts from the
/// first event, so the start limit stops such a job.
pub fn decide(state: &MonitorState, observation: &Observation) -> Decision {
    let rules = &state.rules;
    if rules
        .running_time
        .is_some_and(|limit| observation.at > limit)
    {
        return Decision::RunningTimeReached;
    }
    let quiet = observation.at.saturating_sub(state.last_activity);
    let all_events = rules
        .expected_events
        .zip(observation.num_events)
        .is_some_and(|(expected, seen)| seen >= expected);
    let timed_out = state
        .first_event
        .is_some_and(|first| observation.at.saturating_sub(first) >= rules.stream_timeout);
    match (
        observation.fatal > 0,
        all_events && quiet >= rules.done_delay,
        state.first_event.is_none() && observation.at >= rules.start_limit,
        quiet >= rules.stuck_terminate,
        timed_out,
        quiet >= rules.stuck_warning,
    ) {
        (true, ..) => Decision::Fatal {
            count: observation.fatal,
        },
        (false, true, ..) => Decision::Done,
        (false, false, true, ..) => Decision::NotStarted {
            waited: observation.at,
        },
        (false, false, false, true, ..) => Decision::Stuck { quiet },
        (false, false, false, false, true, _) => Decision::TimedOut,
        (false, false, false, false, false, true) => Decision::Warn { quiet },
        (false, false, false, false, false, false) => Decision::Continue,
    }
}

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

use std::time::Duration;

use nexmark::config::NexmarkConfig;
use nexmark::launcher::{
    Decision, MonitorState, MonitorTiming, Observation, StopRules, decide, running_time_limit,
};

const fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

const RULES: StopRules = StopRules {
    done_delay: secs(10),
    stuck_warning: secs(60),
    stuck_terminate: secs(120),
    stream_timeout: secs(300),
    expected_events: Some(100),
    running_time: None,
};

fn observation(at: u64, events: Option<i64>) -> Observation {
    Observation {
        at: secs(at),
        num_events: events,
        num_results: None,
        fatal: 0,
    }
}

/// Adds the observations in order and gives the decision for the last one.
fn decide_after(rules: StopRules, observations: &[Observation]) -> Decision {
    let (state, last) = observations.iter().fold(
        (MonitorState::new(rules), observations[0]),
        |(state, _), observation| (state.observe(observation), *observation),
    );
    decide(&state, &last)
}

#[test]
fn rules_take_delays_from_timing_and_limits_from_config() {
    let timing = MonitorTiming::default();
    assert_eq!(timing.perf_delay, secs(15));
    let finite = StopRules::new(
        &timing,
        &NexmarkConfig {
            num_events: 500,
            stream_timeout: 30,
            ..NexmarkConfig::default()
        },
    );
    assert_eq!(finite.expected_events, Some(500));
    assert_eq!(finite.stream_timeout, secs(30));
    assert_eq!(finite.done_delay, timing.done_delay);

    let endless = StopRules::new(
        &timing,
        &NexmarkConfig {
            num_events: 0,
            ..NexmarkConfig::default()
        },
    );
    assert_eq!(endless.expected_events, None);
}

#[test]
fn observe_tracks_activity_and_first_event() {
    let state = MonitorState::new(RULES)
        .observe(&observation(1, None))
        .observe(&observation(2, Some(0)))
        .observe(&observation(3, Some(0)));
    assert_eq!(state.last_activity, secs(2));
    assert_eq!(state.first_event, None);

    let state = state
        .observe(&observation(4, Some(7)))
        .observe(&observation(9, Some(7)));
    assert_eq!(state.last_activity, secs(4));
    assert_eq!(state.first_event, Some(secs(4)));
    assert_eq!(state.counts, (Some(7), None));
}

#[test]
fn done_needs_all_expected_events_and_the_done_delay() {
    let reached = [observation(5, Some(50)), observation(20, Some(100))];
    assert_eq!(decide_after(RULES, &reached), Decision::Continue);

    let quiet_short = [reached[0], reached[1], observation(29, Some(100))];
    assert_eq!(decide_after(RULES, &quiet_short), Decision::Continue);

    let quiet_enough = [reached[0], reached[1], observation(30, Some(100))];
    assert_eq!(decide_after(RULES, &quiet_enough), Decision::Done);

    let endless = StopRules {
        expected_events: None,
        ..RULES
    };
    assert_eq!(decide_after(endless, &quiet_enough), Decision::Continue);
}

#[test]
fn quiet_job_warns_then_fails_as_stuck() {
    let start = observation(5, Some(10));
    assert_eq!(
        decide_after(RULES, &[start, observation(70, Some(10))]),
        Decision::Warn { quiet: secs(65) }
    );
    assert_eq!(
        decide_after(RULES, &[start, observation(125, Some(10))]),
        Decision::Stuck { quiet: secs(120) }
    );
    // Before the first change, the quiet time counts from the submission.
    assert_eq!(
        decide_after(RULES, &[observation(130, None)]),
        Decision::Stuck { quiet: secs(130) }
    );
}

#[test]
fn timeout_counts_from_the_first_event() {
    let first = observation(100, Some(1));
    assert_eq!(
        decide_after(RULES, &[first, observation(399, Some(90))]),
        Decision::Continue
    );
    assert_eq!(
        decide_after(RULES, &[first, observation(400, Some(91))]),
        Decision::TimedOut
    );
}

#[test]
fn fatal_errors_win_over_every_other_rule() {
    let fatal = Observation {
        fatal: 3,
        ..observation(500, Some(100))
    };
    assert_eq!(
        decide_after(RULES, &[observation(1, Some(100)), fatal]),
        Decision::Fatal { count: 3 }
    );
}

#[test]
fn running_time_limit_subtracts_the_preload() {
    assert_eq!(running_time_limit(None, 30), None);
    assert_eq!(running_time_limit(Some(2), 0), Some(secs(120)));
    assert_eq!(running_time_limit(Some(2), 30), Some(secs(90)));
    assert_eq!(running_time_limit(Some(1), 600), Some(Duration::ZERO));
}

#[test]
fn running_time_ends_the_run_before_any_other_rule() {
    let rules = RULES.with_running_time(Some(secs(100)));
    let fatal = Observation {
        fatal: 3,
        ..observation(101, Some(10))
    };
    assert_eq!(
        decide_after(rules, &[observation(5, Some(10)), fatal]),
        Decision::RunningTimeReached
    );
    assert_eq!(
        decide_after(
            rules,
            &[observation(5, Some(10)), observation(100, Some(20))]
        ),
        Decision::Continue
    );
    assert_eq!(
        decide_after(
            RULES,
            &[observation(5, Some(10)), observation(101, Some(20))]
        ),
        Decision::Continue
    );
}

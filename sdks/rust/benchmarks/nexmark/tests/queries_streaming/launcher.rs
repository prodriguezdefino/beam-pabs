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

//! Streaming launcher runs on Prism: the poll loop, snapshots and `stream_timeout`.

use std::time::Duration;

use beam::options::PipelineOptions;
use beam::testing::test_pipeline_options;
use nexmark::config::{NexmarkQueryName, SinkType};
use nexmark::launcher::{
    MonitorTiming, NexmarkArgs, NexmarkPerf, NexmarkRunOutcome, ProgressSnapshot, StopReason,
    run_nexmark,
};

/// Polls ten times a second. The stuck rule stops a run that never shows progress.
const TIMING: MonitorTiming = MonitorTiming {
    perf_delay: Duration::from_millis(100),
    done_delay: Duration::from_secs(1),
    stuck_warning: Duration::from_secs(10),
    stuck_terminate: Duration::from_secs(30),
    min_steady_window: Duration::from_millis(300),
};

fn streaming_options() -> PipelineOptions {
    let mut options = test_pipeline_options();
    options.streaming = true;
    options
}

fn rate_limited_args(num_events: i64, rate: i32) -> NexmarkArgs {
    NexmarkArgs {
        query: Some(NexmarkQueryName::Passthrough),
        sink_type: Some(SinkType::CountOnly),
        num_events: Some(num_events),
        num_event_generators: Some(1),
        is_rate_limited: Some(true),
        first_event_rate: Some(rate),
        next_event_rate: Some(rate),
        ..NexmarkArgs::default()
    }
}

/// Runs one query and returns its executed outcome.
async fn run_one(args: &NexmarkArgs) -> (NexmarkPerf, StopReason, Vec<ProgressSnapshot>) {
    let summaries = run_nexmark(&streaming_options(), args, &TIMING)
        .await
        .expect("streaming run");
    match summaries.as_slice() {
        [summary] => match &summary.outcome {
            NexmarkRunOutcome::Executed {
                perf,
                stop,
                snapshots,
            } => (*perf, *stop, snapshots.clone()),
            outcome => panic!("unexpected outcome {outcome:?}"),
        },
        other => panic!("expected one summary, got {other:?}"),
    }
}

#[tokio::test]
async fn rate_limited_finite_stream_finishes_and_records_snapshots() {
    let (perf, stop, snapshots) = run_one(&rate_limited_args(400, 200)).await;
    assert_eq!(stop, StopReason::Finished);
    assert_eq!(perf.num_events, Some(400));
    assert!(snapshots.len() >= 2, "{snapshots:?}");
    assert!(
        snapshots
            .windows(2)
            .all(|pair| pair[0].seconds_since_start <= pair[1].seconds_since_start),
        "{snapshots:?}"
    );
    assert!(
        perf.events_per_sec.is_some_and(|rate| rate > 0.0),
        "{perf:?}"
    );
}

#[tokio::test]
async fn endless_stream_is_cancelled_at_stream_timeout() {
    let args = NexmarkArgs {
        stream_timeout: Some(2),
        ..rate_limited_args(0, 100)
    };
    let (perf, stop, snapshots) = run_one(&args).await;
    assert_eq!(stop, StopReason::TimedOut);
    assert!(perf.num_events.is_some_and(|events| events > 0), "{perf:?}");
    let last = snapshots.last().expect("at least one poll");
    assert!(last.seconds_since_start >= 2.0, "{snapshots:?}");
}

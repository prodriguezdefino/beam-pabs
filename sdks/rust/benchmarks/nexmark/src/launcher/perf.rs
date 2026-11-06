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

//! Performance figures of one query run, from the [`Monitor`](super::Monitor) metrics.

use std::time::Duration;

use beam::metrics::MetricResults;

use super::monitor::{
    BYTES, ELEMENTS, EVENT_TIMESTAMP_MS, EVENTS, METRIC_NAMESPACE, PROCESSING_TIME_MS, RESULTS,
    metric_name,
};

/// Performance of one query run. `None` marks a value that the metrics cannot give.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct NexmarkPerf {
    /// Seconds from the first event to the last event or result, whichever is later.
    pub runtime_sec: Option<f64>,
    pub num_events: Option<i64>,
    pub events_per_sec: Option<f64>,
    pub event_bytes_per_sec: Option<f64>,
    pub num_results: Option<i64>,
    pub results_per_sec: Option<f64>,
    pub result_bytes_per_sec: Option<f64>,
    /// Seconds from the job submission to the first event.
    pub startup_delay_sec: Option<f64>,
    /// Seconds from the first event to the first result.
    pub processing_delay_sec: Option<f64>,
    /// Event-time span of the results divided by the runtime.
    pub time_dilation: Option<f64>,
}

impl NexmarkPerf {
    /// Computes the figures from the final metrics of a job submitted at `submit_ms`.
    ///
    /// `submit_ms` is wall-clock milliseconds since the Unix epoch, the same clock that
    /// [`PROCESSING_TIME_MS`] uses. A rate needs a runtime above zero.
    pub fn from_metrics(metrics: &MetricResults, submit_ms: i64) -> Self {
        let counter =
            |prefix, metric| metrics.counter(METRIC_NAMESPACE, &metric_name(prefix, metric));
        let span = |prefix, metric| {
            metrics
                .distribution(METRIC_NAMESPACE, &metric_name(prefix, metric))
                .filter(|dist| dist.count > 0)
                .map(|dist| (dist.min, dist.max))
        };
        let num_events = counter(EVENTS, ELEMENTS);
        let num_results = counter(RESULTS, ELEMENTS);
        let event_times = span(EVENTS, PROCESSING_TIME_MS);
        let result_times = span(RESULTS, PROCESSING_TIME_MS);
        let event_start = event_times.map(|(start, _)| start);
        let result_start = result_times.map(|(start, _)| start);
        // `None` orders before `Some`, so this is the later end of the two that exist.
        let end = event_times
            .map(|(_, end)| end)
            .max(result_times.map(|(_, end)| end));

        let runtime_sec = event_start
            .zip(end)
            .filter(|(start, end)| end >= start)
            .map(|(start, end)| seconds(end - start));
        let positive_runtime = runtime_sec.filter(|&secs| secs > 0.0);
        let rate = |count: Option<i64>| {
            count
                .zip(positive_runtime)
                .map(|(count, secs)| count as f64 / secs)
        };

        Self {
            runtime_sec,
            num_events,
            events_per_sec: rate(num_events),
            event_bytes_per_sec: rate(counter(EVENTS, BYTES)),
            num_results,
            results_per_sec: rate(num_results),
            result_bytes_per_sec: rate(counter(RESULTS, BYTES)),
            startup_delay_sec: event_start.map(|start| seconds(start - submit_ms)),
            processing_delay_sec: result_start
                .zip(event_start)
                .filter(|(result, event)| result >= event)
                .map(|(result, event)| seconds(result - event)),
            time_dilation: span(RESULTS, EVENT_TIMESTAMP_MS)
                .zip(positive_runtime)
                .map(|((first, last), secs)| seconds(last - first) / secs),
        }
    }
}

fn seconds(millis: i64) -> f64 {
    millis as f64 / 1000.0
}

/// Counts that one poll of a streaming job read.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ProgressSnapshot {
    /// Seconds from the job submission to the poll.
    pub seconds_since_start: f64,
    /// [`NexmarkPerf::runtime_sec`] at the poll.
    pub runtime_sec: Option<f64>,
    pub events: Option<i64>,
    pub results: Option<i64>,
}

/// Fewest samples that give a steady-state rate.
const MIN_STEADY_SAMPLES: usize = 9;

/// Event rate fitted by least squares over the middle third of the snapshots with progress.
///
/// The fit uses the runtime, not the poll time, so delays in polling do not change it.
/// Gives `None` with fewer than 9 samples or a middle third shorter than `min_window`.
pub fn steady_state_events_per_sec(
    snapshots: &[ProgressSnapshot],
    min_window: Duration,
) -> Option<f64> {
    let start = snapshots.iter().position(|s| s.events.is_some())?;
    let changed = |i: usize| {
        let (now, before) = (&snapshots[i], &snapshots[i - 1]);
        (now.events, now.results) != (before.events, before.results)
    };
    let end = (start + 1..snapshots.len())
        .rev()
        .find(|&i| changed(i))
        .unwrap_or(start);
    let samples = end - start + 1;
    if samples < MIN_STEADY_SAMPLES {
        return None;
    }
    let middle = &snapshots[start + samples / 3..=end - samples / 3];
    let span = middle.last()?.seconds_since_start - middle.first()?.seconds_since_start;
    if span < min_window.as_secs_f64() {
        return None;
    }
    let (sum_xx, sum_xy, _) = middle.iter().fold(
        (0.0, 0.0, None),
        |(sum_xx, sum_xy, previous), snapshot| match (snapshot.events, snapshot.runtime_sec) {
            // A sample without new events adds no data.
            (Some(events), Some(x)) if previous != Some(events) => {
                (sum_xx + x * x, sum_xy + x * events as f64, Some(events))
            }
            _ => (sum_xx, sum_xy, previous),
        },
    );
    (sum_xx > 0.0).then(|| sum_xy / sum_xx)
}

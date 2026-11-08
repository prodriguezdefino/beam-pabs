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

use beam::metrics::{
    DistributionValue, MetricKey, MetricPhase, MetricReading, MetricResults, MetricValue,
};
use nexmark::launcher::{
    BYTES, ELEMENTS, EVENT_TIMESTAMP_MS, EVENTS, METRIC_NAMESPACE, NexmarkPerf, PROCESSING_TIME_MS,
    ProgressSnapshot, RESULTS, metric_name, steady_state_events_per_sec,
};

const SUBMIT_MS: i64 = 1_000_000;

fn reading(prefix: &str, metric: &str, value: MetricValue) -> MetricReading {
    MetricReading {
        key: MetricKey::new("MonitorStep", METRIC_NAMESPACE, metric_name(prefix, metric)),
        phase: MetricPhase::Committed,
        value,
    }
}

fn counter(prefix: &str, metric: &str, value: i64) -> MetricReading {
    reading(prefix, metric, MetricValue::Counter(value))
}

fn span(prefix: &str, metric: &str, min: i64, max: i64) -> MetricReading {
    reading(
        prefix,
        metric,
        MetricValue::Distribution(DistributionValue {
            count: 2,
            sum: min + max,
            min,
            max,
        }),
    )
}

fn assert_close(actual: Option<f64>, expected: f64) {
    let value = actual.unwrap_or_else(|| panic!("expected {expected}, got None"));
    assert!(
        (value - expected).abs() < 1e-9,
        "expected {expected}, got {value}"
    );
}

#[test]
fn computes_rates_delays_and_dilation_from_monitor_metrics() {
    let metrics: MetricResults = [
        counter(EVENTS, ELEMENTS, 1_000),
        counter(EVENTS, BYTES, 100_000),
        counter(RESULTS, ELEMENTS, 50),
        counter(RESULTS, BYTES, 2_000),
        span(
            EVENTS,
            PROCESSING_TIME_MS,
            SUBMIT_MS + 3_000,
            SUBMIT_MS + 5_000,
        ),
        span(
            RESULTS,
            PROCESSING_TIME_MS,
            SUBMIT_MS + 3_500,
            SUBMIT_MS + 7_000,
        ),
        span(RESULTS, EVENT_TIMESTAMP_MS, 0, 8_000),
    ]
    .into_iter()
    .collect();

    let perf = NexmarkPerf::from_metrics(&metrics, SUBMIT_MS);

    assert_close(perf.runtime_sec, 4.0);
    assert_eq!(perf.num_events, Some(1_000));
    assert_close(perf.events_per_sec, 250.0);
    assert_close(perf.event_bytes_per_sec, 25_000.0);
    assert_eq!(perf.num_results, Some(50));
    assert_close(perf.results_per_sec, 12.5);
    assert_close(perf.result_bytes_per_sec, 500.0);
    assert_close(perf.startup_delay_sec, 3.0);
    assert_close(perf.processing_delay_sec, 0.5);
    assert_close(perf.time_dilation, 2.0);
}

#[test]
fn zero_runtime_gives_counts_but_no_rates() {
    let metrics: MetricResults = [
        counter(EVENTS, ELEMENTS, 10),
        counter(EVENTS, BYTES, 1_000),
        counter(RESULTS, ELEMENTS, 10),
        span(EVENTS, PROCESSING_TIME_MS, SUBMIT_MS + 200, SUBMIT_MS + 200),
        span(
            RESULTS,
            PROCESSING_TIME_MS,
            SUBMIT_MS + 200,
            SUBMIT_MS + 200,
        ),
        span(RESULTS, EVENT_TIMESTAMP_MS, 0, 1_000),
    ]
    .into_iter()
    .collect();

    let perf = NexmarkPerf::from_metrics(&metrics, SUBMIT_MS);

    assert_close(perf.runtime_sec, 0.0);
    assert_eq!(perf.num_events, Some(10));
    assert_eq!(perf.num_results, Some(10));
    assert_eq!(perf.events_per_sec, None);
    assert_eq!(perf.event_bytes_per_sec, None);
    assert_eq!(perf.results_per_sec, None);
    assert_eq!(perf.time_dilation, None);
    assert_close(perf.startup_delay_sec, 0.2);
    assert_close(perf.processing_delay_sec, 0.0);
}

#[test]
fn missing_distributions_leave_timing_values_empty() {
    let metrics: MetricResults = [counter(EVENTS, ELEMENTS, 10)].into_iter().collect();
    assert_eq!(
        NexmarkPerf::from_metrics(&metrics, SUBMIT_MS),
        NexmarkPerf {
            num_events: Some(10),
            ..NexmarkPerf::default()
        }
    );

    assert_eq!(
        NexmarkPerf::from_metrics(&MetricResults::default(), SUBMIT_MS),
        NexmarkPerf::default()
    );
}

#[test]
fn events_without_results_use_the_last_event_as_the_end() {
    let metrics: MetricResults = [
        counter(EVENTS, ELEMENTS, 100),
        span(
            EVENTS,
            PROCESSING_TIME_MS,
            SUBMIT_MS + 1_000,
            SUBMIT_MS + 3_000,
        ),
    ]
    .into_iter()
    .collect();

    let perf = NexmarkPerf::from_metrics(&metrics, SUBMIT_MS);

    assert_close(perf.runtime_sec, 2.0);
    assert_close(perf.events_per_sec, 50.0);
    assert_eq!(perf.num_results, None);
    assert_eq!(perf.processing_delay_sec, None);
    assert_eq!(perf.time_dilation, None);
}

/// Snapshots one second apart with `rate` events per second of runtime, then `flat` polls
/// without progress.
fn steady_snapshots(count: usize, rate: i64, flat: usize) -> Vec<ProgressSnapshot> {
    let moving = (0..count).map(|i| ProgressSnapshot {
        seconds_since_start: (i + 5) as f64,
        runtime_sec: Some(i as f64),
        events: Some(rate * i as i64),
        results: None,
    });
    let last = count.saturating_sub(1);
    let idle = (1..=flat).map(move |j| ProgressSnapshot {
        seconds_since_start: (last + 5 + j) as f64,
        runtime_sec: Some(last as f64),
        events: Some(rate * last as i64),
        results: None,
    });
    moving.chain(idle).collect()
}

#[test]
fn steady_state_fits_the_rate_of_the_middle_third() {
    let snapshots = steady_snapshots(12, 250, 4);
    assert_close(
        steady_state_events_per_sec(&snapshots, Duration::from_secs(2)),
        250.0,
    );

    let without_counts = [ProgressSnapshot {
        seconds_since_start: 1.0,
        runtime_sec: None,
        events: None,
        results: None,
    }; 3];
    let late_start: Vec<ProgressSnapshot> = without_counts
        .into_iter()
        .chain(steady_snapshots(12, 40, 0))
        .collect();
    assert_close(
        steady_state_events_per_sec(&late_start, Duration::from_secs(2)),
        40.0,
    );
}

#[test]
fn steady_state_needs_enough_samples_and_time() {
    assert_eq!(
        steady_state_events_per_sec(&steady_snapshots(8, 100, 10), Duration::ZERO),
        None
    );
    assert_eq!(
        steady_state_events_per_sec(&steady_snapshots(12, 100, 0), Duration::from_secs(30)),
        None
    );
    assert_eq!(steady_state_events_per_sec(&[], Duration::ZERO), None);
}

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

//! User metrics reported through [`PipelineResult::metrics`].

use std::fmt::Debug;

use beam::metrics::{MetricFilter, MetricResult};
use beam::prelude::*;
use beam::testing::{TestPipeline, TestPipelineError, passert};

use crate::RunnerKind;

/// Builds a pipeline that updates counter, distribution, gauge and string-set metrics.
/// See [`check_user_metrics`] for the outcome checks.
pub fn build_user_metrics(p: &TestPipeline) {
    let out = p
        .apply("Create", Create::new(vec![10i64, 20, 30]))
        .map("RecordMetrics", |x: i64| {
            Metrics::counter("vr_metrics", "elements").inc();
            Metrics::counter("vr_metrics", "total").inc_by(x);
            Metrics::distribution("vr_metrics", "values").update(x);
            Metrics::gauge("vr_metrics", "last_value").set(x);
            Metrics::string_set("vr_metrics", "buckets").add(if x < 25 { "low" } else { "high" });
            x * 2
        });

    passert::that("AssertMetricsOutput", &out).contains_in_any_order([20, 40, 60]);
}

/// Checks the user metrics of [`build_user_metrics`] after a successful run.
///
/// Covers `MetricsTest`: the `testAttempted*Metrics` and `testCommitted*Metrics` cases for
/// counters, distributions, gauges and string sets. See [`Phases::settled`] for the phases
/// that each runner reports. Dataflow returns no gauges, so there the gauge is optional.
pub fn check_user_metrics(
    runner: RunnerKind,
    result: std::result::Result<PipelineResult, TestPipelineError>,
) {
    let result = result.expect("the pipeline should succeed");
    let metrics = result.metrics().expect("runner reports metrics");
    let counter = |name| *phases(metrics.query_counters(&filter(name))).settled(name, runner);
    assert_eq!(counter("elements"), 3);
    assert_eq!(counter("total"), 60);
    let values = phases(metrics.query_distributions(&filter("values")))
        .map(|d| (d.count, d.sum, d.min, d.max));
    assert_eq!(values.settled("values", runner), &(3, 60, 10, 30));
    let buckets = phases(metrics.query_string_sets(&filter("buckets")));
    let buckets: Vec<&str> = buckets
        .settled("buckets", runner)
        .values()
        .iter()
        .map(String::as_str)
        .collect();
    assert_eq!(buckets, ["high", "low"]);

    let gauges = metrics.query_gauges(&filter("last_value"));
    match (runner, gauges.is_empty()) {
        (RunnerKind::Dataflow, true) => {}
        (RunnerKind::Prism, _) | (RunnerKind::Dataflow, false) => {
            let gauge = phases(gauges).map(|gauge| gauge.value);
            let latest = gauge.settled("last_value", runner);
            // The gauge keeps the latest value, and the elements have no order.
            assert!([10, 20, 30].contains(latest), "last_value: {latest}");
        }
    }
}

fn filter(name: &str) -> MetricFilter {
    MetricFilter::default()
        .with_namespace("vr_metrics")
        .with_name(name)
}

/// The attempted and the committed value of one metric cell.
struct Phases<T> {
    attempted: Option<T>,
    committed: Option<T>,
}

impl<T> Phases<T> {
    fn map<U>(self, f: impl Fn(T) -> U) -> Phases<U> {
        Phases {
            attempted: self.attempted.map(&f),
            committed: self.committed.map(&f),
        }
    }
}

impl<T: PartialEq + Debug> Phases<T> {
    /// The final value of the cell on `runner`, after a check of the other phase.
    ///
    /// Prism reports the bundle results as committed, and the last progress report of a
    /// bundle as attempted. That report can be partial or absent, so it is not checked.
    /// Dataflow reports values that retries can change as attempted, and in streaming no
    /// committed values. A committed value must match the attempted one.
    fn settled(&self, name: &str, runner: RunnerKind) -> &T {
        let Self {
            attempted,
            committed,
        } = self;
        let phases = format!("{name}: attempted {attempted:?}, committed {committed:?}");
        match (runner, attempted, committed) {
            (RunnerKind::Prism, _, Some(committed)) => committed,
            (RunnerKind::Dataflow, Some(attempted), None) => attempted,
            (RunnerKind::Dataflow, Some(attempted), Some(committed)) => {
                assert_eq!(attempted, committed, "{phases}");
                attempted
            }
            (RunnerKind::Prism, _, None) | (RunnerKind::Dataflow, None, _) => {
                panic!("{phases}: the final value is missing")
            }
        }
    }
}

/// The phases of the one cell in `cells`. Only `RecordMetrics` updates the metrics.
fn phases<T: Debug>(cells: Vec<MetricResult<T>>) -> Phases<T> {
    let [cell]: [MetricResult<T>; 1] = cells
        .try_into()
        .unwrap_or_else(|cells| panic!("expected one metric cell, got {cells:?}"));
    Phases {
        attempted: cell.attempted,
        committed: cell.committed,
    }
}

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

//! End-to-end tests for metrics that a `DoFn` records. The pipeline runs on Prism, and
//! `PipelineResult` must report exactly the recorded values, attributed to the `DoFn`.

use std::collections::BTreeSet;

use beam::metrics::{MetricFilter, MetricResult, MetricResults};
use beam::prelude::*;
use testing::{TestPipeline, passert};

/// Records counter, distribution, and gauge metrics for each element.
fn measure(p: &TestPipeline, values: Vec<i64>) -> PCollection<i64> {
    p.apply(Create::new("Values", values))
        .apply(ParDo::from_fn("Measure", |x: i64, ctx| {
            Metrics::counter("app", "elements").inc();
            Metrics::counter("app", "sum").inc_by(x);
            Metrics::distribution("app", "sizes").update(x);
            Metrics::gauge("app", "gauge").set(42);
            ctx.emit(x)
        }))
}

fn transforms<T>(results: Vec<MetricResult<T>>) -> BTreeSet<String> {
    results.into_iter().map(|r| r.key.transform_id).collect()
}

async fn run_and_collect_metrics(p: &TestPipeline) -> MetricResults {
    let result = p.run().await.expect("pipeline and assertions");
    result
        .metrics()
        .expect("the runner reports metrics")
        .clone()
}

#[tokio::test]
async fn dofn_metrics_are_reported_in_the_pipeline_result() {
    let p = TestPipeline::new();
    let out = measure(&p, vec![3, 5, 10]);
    // Metric recording must not alter pipeline elements.
    passert::that("AssertOut", &out).contains_in_any_order([3i64, 5, 10]);
    let metrics = run_and_collect_metrics(&p).await;

    assert_eq!(metrics.counter("app", "elements"), Some(3));
    assert_eq!(metrics.counter("app", "sum"), Some(18));

    let sizes = metrics.distribution("app", "sizes").expect("sizes");
    assert_eq!(
        (sizes.count, sizes.sum, sizes.min, sizes.max),
        (3, 18, 3, 10)
    );

    assert_eq!(metrics.gauge("app", "gauge").map(|g| g.value), Some(42));
    assert!(metrics.gauge("app", "gauge").unwrap().timestamp_ms > 0);

    assert_eq!(metrics.counter("app", "never_recorded"), None);
}

#[tokio::test]
async fn every_metric_kind_is_attributed_to_the_recording_transform() {
    let p = TestPipeline::new();
    let out = measure(&p, vec![1, 2]);
    passert::that("AssertOut", &out).has_count(2);
    let metrics = run_and_collect_metrics(&p).await;

    let app = MetricFilter::empty().with_namespace("app");
    let counter_transforms = transforms(metrics.query_counters(&app));
    let distribution_transforms = transforms(metrics.query_distributions(&app));
    let gauge_transforms = transforms(metrics.query_gauges(&app));

    // Each accessor attributes the metric to one transform, whatever ID the runner gives it.
    assert_eq!(counter_transforms.len(), 1, "{counter_transforms:?}");
    let transform = counter_transforms.iter().next().unwrap().clone();
    assert!(transform.contains("Measure"), "{transform}");
    assert_eq!(distribution_transforms, counter_transforms);
    assert_eq!(gauge_transforms, counter_transforms);

    assert_eq!(
        metrics.counter_for_transform(&transform, "app", "sum"),
        Some(3)
    );
}

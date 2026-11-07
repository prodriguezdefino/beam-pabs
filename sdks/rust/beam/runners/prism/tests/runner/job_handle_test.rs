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

//! Tests for the job handle that `PrismRunner::submit` returns.

use std::time::Duration;

use beam::metrics::Metrics;
use beam::pipeline::Pipeline;
use beam::runners::{JobHandle, JobState, PipelineResult, PipelineRunner, RunnerError};
use beam::transforms::PeriodicImpulse;
use fluent::prelude::*;
use prism::{PrismRunner, PrismRunnerOptions};

/// Longest time a test waits for Prism to start a job or to report a value.
const PATIENCE: Duration = Duration::from_secs(60);

fn runner(job_name: &str) -> PrismRunner {
    PrismRunner::with_options(PrismRunnerOptions {
        job_name: Some(job_name.to_string()),
        ..Default::default()
    })
}

/// An unbounded pipeline that counts each tick in `handle/ticks`. It does not end on its own.
fn ticking_pipeline() -> Pipeline {
    let p = Pipeline::new();
    let _ = p
        .apply("Ticks", PeriodicImpulse::new(Duration::from_millis(20)))
        .map("CountTicks", |tick: i64| {
            Metrics::counter("handle", "ticks").inc();
            tick
        });
    p
}

/// A bounded pipeline that counts five elements in `handle/elements`.
fn counting_pipeline() -> Pipeline {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new((1i64..=5).collect::<Vec<_>>()))
        .map("CountElements", |x: i64| {
            Metrics::counter("handle", "elements").inc();
            x
        });
    p
}

/// Polls `probe` every 100 ms until it gives `Some`, and fails the test after [`PATIENCE`].
async fn eventually<T, F: Future<Output = Option<T>>>(what: &str, probe: impl Fn() -> F) -> T {
    tokio::time::timeout(PATIENCE, async {
        loop {
            if let Some(value) = probe().await {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

#[tokio::test]
async fn test_streaming_job_runs_reports_live_metrics_and_cancels() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let handle = runner("prism_handle_streaming")
        .submit(&ticking_pipeline())
        .await
        .unwrap();
    let job: &dyn JobHandle = &*handle;
    assert!(job.job_id().starts_with("job-"), "{}", job.job_id());

    eventually("the Running state", || async move {
        (job.state().await.unwrap() == JobState::Running).then_some(())
    })
    .await;

    let ticks = eventually("a tick counter above zero", || async move {
        let metrics = job.metrics().await.unwrap();
        metrics.counter("handle", "ticks").filter(|&n| n > 0)
    })
    .await;
    assert!(ticks > 0);

    let pending = job
        .wait_until_finish(Some(Duration::from_millis(300)))
        .await
        .unwrap();
    assert_eq!(pending.job_id, job.job_id());
    assert!(!pending.state.is_terminal(), "{:?}", pending.state);

    assert!(
        matches!(
            job.drain().await,
            Err(RunnerError::Unsupported {
                runner: "prism",
                operation: "drain"
            })
        ),
        "Prism cannot drain a job"
    );

    job.cancel().await.unwrap();
    let finished = job.wait_until_finish(Some(PATIENCE)).await.unwrap();
    assert_eq!(finished.state, JobState::Cancelled);
    assert_eq!(job.state().await.unwrap(), JobState::Cancelled);
}

#[tokio::test]
async fn test_bounded_job_submit_and_wait_matches_run() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let ran = runner("prism_handle_run")
        .run(&counting_pipeline())
        .await
        .unwrap();

    let job = runner("prism_handle_submit")
        .submit(&counting_pipeline())
        .await
        .unwrap();
    let waited = job.wait_until_finish(None).await.unwrap();

    assert_eq!(waited.state, JobState::Done);
    assert_eq!(waited.job_id, job.job_id());
    let count = |result: &PipelineResult| result.metrics().unwrap().counter("handle", "elements");
    assert_eq!(count(&ran), Some(5));
    assert_eq!(count(&waited), count(&ran));
    assert_eq!(
        job.metrics().await.unwrap().counter("handle", "elements"),
        Some(5)
    );
}

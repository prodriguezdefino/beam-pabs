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

//! Streaming Prism integration tests for `UnboundedEventSource`, `Query10`, and Nexmark queries.

mod queries;
mod query10;
mod source;

use std::fmt::Debug;

use beam::coders::DefaultCoder;
use beam::prelude::*;
use beam::testing::{TestPipeline, TestPipelineError, passert, test_pipeline_options};
use nexmark::config::NexmarkConfig;
use nexmark::model::Event;
use nexmark::source::UnboundedEventSource;

pub(crate) fn streaming_pipeline() -> TestPipeline {
    let mut options = test_pipeline_options();
    options.streaming = true;
    TestPipeline::with_options(options)
}

pub(crate) fn base_streaming_config() -> NexmarkConfig {
    NexmarkConfig {
        is_rate_limited: false,
        prob_delayed_event: 0.0,
        ..NexmarkConfig::default()
    }
}

pub(crate) fn stamp<T: DefaultCoder>(pcoll: &PCollection<T>) -> PCollection<(T, Instant)> {
    pcoll.par_do_fn("Stamp", |elem: T, ctx| {
        let ts = ctx.timestamp();
        ctx.emit((elem, ts))
    })
}

pub(crate) async fn assert_streaming_query_matches_model<Q, Out>(
    label: &str,
    config: &NexmarkConfig,
    batch_size: usize,
    query: Q,
    expected: Vec<(Out, Instant)>,
) -> std::result::Result<(), TestPipelineError>
where
    Q: PTransform<PCollection<Event>, Output = PCollection<Out>>,
    Out: DefaultCoder + Clone + PartialEq + Debug + Send + Sync + 'static,
{
    assert!(
        !expected.is_empty(),
        "{label} reference model produced zero outputs"
    );
    let p = streaming_pipeline();
    let stamped = stamp(
        &p.apply(
            "ReadUnboundedEvents",
            UnboundedEventSource::new(config).with_batch_size(batch_size),
        )
        .apply(label, query),
    );
    passert::that(format!("{label}StreamingMatchesModel"), &stamped)
        .contains_in_any_order(expected);
    p.run().await?;
    Ok(())
}

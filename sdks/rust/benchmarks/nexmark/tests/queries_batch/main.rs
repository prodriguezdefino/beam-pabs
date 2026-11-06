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

//! Batch Prism integration tests for `BoundedEventSource` and `Query0`.

use beam::coders::{Coder, Context, DefaultCoder};
use beam::prelude::*;
use beam::testing::{TestPipeline, TestPipelineError, passert};
use nexmark::config::{NexmarkConfig, NexmarkQueryName, RateShape, RateUnit};
use nexmark::model::EventCoder;
use nexmark::models;
use nexmark::queries::Query0;
use nexmark::source::BoundedEventSource;

fn stamp<T: DefaultCoder>(pcoll: &PCollection<T>) -> PCollection<(T, Instant)> {
    pcoll.par_do_fn("Stamp", |elem: T, ctx| {
        let ts = ctx.timestamp();
        ctx.emit((elem, ts))
    })
}

fn expected_whole_stream_bytes(config: &NexmarkConfig) -> i64 {
    models::query0(config)
        .into_iter()
        .fold((Vec::new(), 0_i64), |(mut buf, total), (event, _)| {
            buf.clear();
            EventCoder
                .encode(&event, &mut buf, Context::WholeStream)
                .expect("encode event");
            let added = buf.len() as i64;
            (buf, total + added)
        })
        .1
}

#[tokio::test]
async fn query0_matches_model_single_generator() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::Passthrough),
        num_events: 500,
        num_event_generators: 1,
        ..NexmarkConfig::default()
    };
    let expected = models::query0(&config);
    let expected_bytes = expected_whole_stream_bytes(&config);

    let p = TestPipeline::new();
    let stamped = stamp(
        &p.apply("ReadEvents", BoundedEventSource::new(&config))
            .apply("Query0", Query0),
    );
    passert::that("Query0MatchesModelSingleGen", &stamped).contains_in_any_order(expected);

    let result = p.run().await?;
    let metrics = result.metrics().expect("Prism reports pipeline metrics");
    assert_eq!(metrics.counter("Query0", "events"), Some(500));
    assert_eq!(metrics.counter("Query0", "bytes"), Some(expected_bytes));
    Ok(())
}

#[tokio::test]
async fn query0_matches_model_multiple_generators() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::Passthrough),
        num_events: 500,
        num_event_generators: 4,
        rate_shape: RateShape::Sine,
        first_event_rate: 50,
        next_event_rate: 150,
        rate_unit: RateUnit::PerSecond,
        rate_period_sec: 20,
        out_of_order_group_size: 5,
        ..NexmarkConfig::default()
    };
    let expected = models::query0(&config);
    let expected_bytes = expected_whole_stream_bytes(&config);

    let p = TestPipeline::new();
    let stamped = stamp(
        &p.apply("ReadEvents", BoundedEventSource::new(&config))
            .apply("Query0", Query0),
    );
    passert::that("Query0MatchesModelMultiGen", &stamped).contains_in_any_order(expected);

    let result = p.run().await?;
    let metrics = result.metrics().expect("Prism reports pipeline metrics");
    assert_eq!(metrics.counter("Query0", "events"), Some(500));
    assert_eq!(metrics.counter("Query0", "bytes"), Some(expected_bytes));
    Ok(())
}

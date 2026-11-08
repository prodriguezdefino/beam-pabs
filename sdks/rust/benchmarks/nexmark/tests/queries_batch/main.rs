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

//! Batch Prism integration tests for `BoundedEventSource` and Nexmark queries.

mod stateless;
mod utility_and_side_inputs;
mod windowed;
mod winning_bids;

use std::collections::BTreeMap;
use std::fmt::Debug;

use beam::coders::{Coder, Context, DefaultCoder};
use beam::prelude::*;
use beam::testing::{TestPipeline, TestPipelineError};
use nexmark::config::{NexmarkConfig, NexmarkQueryName, RateShape, RateUnit};
use nexmark::correctness::assert_timestamped;
use nexmark::model::{BidsPerSession, Event, EventCoder};
use nexmark::models;
use nexmark::queries::Query0;
use nexmark::source::BoundedEventSource;

pub(crate) async fn assert_query_matches_model<Q, Out>(
    label: &str,
    config: &NexmarkConfig,
    query: Q,
    expected: Vec<(Out, Instant)>,
) -> std::result::Result<(), TestPipelineError>
where
    Q: PTransform<PCollection<Event>, Output = PCollection<Out>>,
    Out: DefaultCoder + PartialEq + Debug + Send + Sync + 'static,
{
    assert!(
        !expected.is_empty(),
        "{label} reference model produced zero outputs"
    );
    let p = TestPipeline::new();
    let out = p
        .apply("ReadEvents", BoundedEventSource::new(config))
        .apply(label, query);
    assert_timestamped(&format!("{label}MatchesModel"), &out, expected);
    p.run().await?;
    Ok(())
}

pub(crate) fn expected_whole_stream_bytes(config: &NexmarkConfig) -> i64 {
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

pub(crate) fn expected_bid_counts_by_bidder(config: &NexmarkConfig) -> BTreeMap<i64, i64> {
    models::query1(config)
        .into_iter()
        .fold(BTreeMap::new(), |mut counts, (bid, _)| {
            *counts.entry(bid.bidder).or_insert(0) += 1;
            counts
        })
}

pub(crate) fn check_session_totals(
    actual: &[BidsPerSession],
    expected_totals: &BTreeMap<i64, i64>,
) -> Result {
    if actual.is_empty() {
        return Err("expected non-empty BidsPerSession output".into());
    }
    if let Some(invalid) = actual.iter().find(|r| r.bids_per_session <= 0) {
        return Err(format!("non-positive session count: {invalid:?}").into());
    }
    let summed = actual.iter().fold(BTreeMap::new(), |mut acc, record| {
        *acc.entry(record.person_id).or_insert(0) += record.bids_per_session;
        acc
    });
    if &summed == expected_totals {
        Ok(())
    } else {
        Err(format!("summed bidder counts {summed:?} != expected {expected_totals:?}").into())
    }
}

async fn assert_query0_matches_model_and_counts_bytes(
    label: &str,
    config: &NexmarkConfig,
) -> std::result::Result<(), TestPipelineError> {
    let p = TestPipeline::new();
    let out = p
        .apply("ReadEvents", BoundedEventSource::new(config))
        .apply("Query0", Query0);
    assert_timestamped(label, &out, models::query0(config));

    let result = p.run().await?;
    let metrics = result.metrics().expect("Prism reports pipeline metrics");
    assert_eq!(metrics.counter("Query0", "events"), Some(config.num_events));
    assert_eq!(
        metrics.counter("Query0", "bytes"),
        Some(expected_whole_stream_bytes(config))
    );
    Ok(())
}

#[tokio::test]
async fn query0_matches_model_single_generator() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::Passthrough),
        num_events: 500,
        num_event_generators: 1,
        ..NexmarkConfig::default()
    };
    assert_query0_matches_model_and_counts_bytes("Query0MatchesModelSingleGen", &config).await
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
    assert_query0_matches_model_and_counts_bytes("Query0MatchesModelMultiGen", &config).await
}

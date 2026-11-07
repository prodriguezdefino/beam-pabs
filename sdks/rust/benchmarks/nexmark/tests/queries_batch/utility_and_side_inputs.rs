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

//! Batch Prism tests for `Query11`–`Query14`, `BoundedSideInputJoin`, and `SessionSideInputJoin`.

use beam::prelude::*;
use beam::testing::{TestPipeline, TestPipelineError, passert};
use nexmark::config::{NexmarkConfig, NexmarkQueryName};
use nexmark::models;
use nexmark::queries::{
    BoundedSideInputJoin, Query11, Query12, Query13, Query14, SessionSideInputJoin,
};
use nexmark::source::BoundedEventSource;

use crate::{
    assert_query_matches_model, check_session_totals, expected_bid_counts_by_bidder,
    expected_whole_stream_bytes,
};

#[test]
fn query11_builds_session_count_trigger_graph() {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::UserSessions),
        num_events: 100,
        window_size_sec: 2,
        max_log_events: 3,
        occasional_delay_sec: 6,
        ..NexmarkConfig::default()
    };
    let p = Pipeline::new();
    let _out = p
        .apply("ReadEvents", BoundedEventSource::new(&config))
        .apply("Query11", Query11::from_config(&config));
    p.validate().expect("Query11 pipeline graph must validate");

    let components = p
        .to_proto()
        .components
        .expect("validated pipeline has components");
    let expected_trigger = Trigger::repeatedly(Trigger::after_count(3));
    let has_session_count_strategy = components.windowing_strategies.values().any(|ws| {
        let is_session = ws
            .window_fn
            .as_ref()
            .is_some_and(|wf| wf.urn == "beam:window_fn:session_windows:v1");
        let has_trigger = ws
            .trigger
            .as_ref()
            .and_then(|t| Trigger::try_from(t).ok())
            .is_some_and(|t| t == expected_trigger);
        is_session && has_trigger && ws.allowed_lateness == 3_000
    });
    assert!(
        has_session_count_strategy,
        "Query11 must register a session windowing strategy with Repeat(AfterCount(3)) and 3000ms lateness"
    );
}

#[tokio::test]
#[ignore = "Prism rejects stateful count triggers on merging session windows (apache/beam#31438)"]
async fn query11_conserves_per_bidder_counts() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::UserSessions),
        num_events: 400,
        num_event_generators: 2,
        first_event_rate: 80,
        next_event_rate: 80,
        num_active_people: 25,
        window_size_sec: 2,
        max_log_events: 2,
        occasional_delay_sec: 4,
        ..NexmarkConfig::default()
    };
    let expected_totals = expected_bid_counts_by_bidder(&config);

    let p = TestPipeline::new();
    let out = p
        .apply("ReadEvents", BoundedEventSource::new(&config))
        .apply("Query11", Query11::from_config(&config));
    passert::that("Query11Totals", &out)
        .satisfies(move |actual| check_session_totals(actual, &expected_totals));
    p.run().await?;
    Ok(())
}

#[tokio::test]
async fn query12_conserves_per_bidder_counts() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::ProcessingTimeWindows),
        num_events: 400,
        num_event_generators: 2,
        num_active_people: 25,
        window_size_sec: 5,
        ..NexmarkConfig::default()
    };
    let expected_totals = expected_bid_counts_by_bidder(&config);

    let p = TestPipeline::new();
    let out = p
        .apply("ReadEvents", BoundedEventSource::new(&config))
        .apply("Query12", Query12::from_config(&config));
    passert::that("Query12Totals", &out)
        .satisfies(move |actual| check_session_totals(actual, &expected_totals));
    p.run().await?;
    Ok(())
}

#[tokio::test]
async fn query13_preserves_events_and_records_serde_bytes()
-> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::PortabilityBatch),
        num_events: 400,
        num_event_generators: 2,
        num_key_buckets: 16,
        pardo_cpu_factor: 1.0,
        ..NexmarkConfig::default()
    };
    let expected_events: Vec<_> = models::query0(&config)
        .into_iter()
        .map(|(event, _)| event)
        .collect();
    let expected_bytes = expected_whole_stream_bytes(&config);

    let p = TestPipeline::new();
    let out = p
        .apply("ReadEvents", BoundedEventSource::new(&config))
        .apply("Query13", Query13::from_config(&config));
    passert::that("Query13Events", &out).contains_in_any_order(expected_events);

    let result = p.run().await?;
    let metrics = result.metrics().expect("Prism reports pipeline metrics");
    assert_eq!(
        metrics.counter("Query13", "serde-bytes"),
        Some(expected_bytes)
    );
    Ok(())
}

#[tokio::test]
async fn query14_matches_passthrough_model() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::Reshuffle),
        num_events: 400,
        num_event_generators: 3,
        out_of_order_group_size: 5,
        num_key_buckets: 7,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "Query14",
        &config,
        Query14::from_config(&config),
        models::query0(&config),
    )
    .await
}

#[tokio::test]
async fn bounded_side_input_join_matches_model() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::BoundedSideInputJoin),
        num_events: 400,
        num_event_generators: 2,
        side_input_row_count: 37,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "BoundedSideInputJoin",
        &config,
        BoundedSideInputJoin::from_config(&config),
        models::bounded_side_input_join(&config),
    )
    .await
}

#[tokio::test]
async fn session_side_input_join_matches_model() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::SessionSideInputJoin),
        num_events: 500,
        num_event_generators: 2,
        first_event_rate: 50,
        next_event_rate: 50,
        num_active_people: 20,
        side_input_row_count: 29,
        session_gap_sec: 1,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "SessionSideInputJoin",
        &config,
        SessionSideInputJoin::from_config(&config),
        models::session_side_input_join(&config),
    )
    .await
}

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

//! Streaming Prism parity tests for Nexmark queries `Query0`–`Query9`, `Query11`–`Query14`, and side-input joins.

use std::collections::BTreeMap;

use beam::prelude::*;
use beam::testing::{TestPipelineError, passert};
use nexmark::config::{NexmarkConfig, NexmarkQueryName};
use nexmark::model::{BidsPerSession, SellerPrice};
use nexmark::models;
use nexmark::queries::{
    BoundedSideInputJoin, Query0, Query1, Query2, Query3, Query4, Query5, Query6, Query7, Query8,
    Query9, Query11, Query12, Query13, Query14, SessionSideInputJoin,
};
use nexmark::source::UnboundedEventSource;

use crate::{assert_streaming_query_matches_model, base_streaming_config, streaming_pipeline};

fn check_latest_seller_panes(actual: &[(SellerPrice, i64)], expected: &[SellerPrice]) -> Result {
    if actual.is_empty() {
        return Err("expected non-empty SellerPrice panes".into());
    }
    let latest = actual.iter().fold(
        BTreeMap::<i64, (i64, SellerPrice)>::new(),
        |mut acc, &(sp, pane_idx)| {
            match acc.get(&sp.seller) {
                Some(&(best_idx, _)) if pane_idx < best_idx => {}
                Some(_) | None => {
                    acc.insert(sp.seller, (pane_idx, sp));
                }
            }
            acc
        },
    );
    let final_prices: Vec<SellerPrice> = latest.into_values().map(|(_, sp)| sp).collect();
    if final_prices == expected {
        Ok(())
    } else {
        Err(format!("final seller panes {final_prices:?} != expected {expected:?}").into())
    }
}

fn check_session_totals(actual: &[BidsPerSession], expected_totals: &BTreeMap<i64, i64>) -> Result {
    if actual.is_empty() {
        return Err("expected non-empty BidsPerSession output".into());
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

#[tokio::test]
async fn streaming_stateless_and_stateful_queries_match_models()
-> std::result::Result<(), TestPipelineError> {
    let base = NexmarkConfig {
        num_events: 400,
        num_event_generators: 2,
        auction_skip: 5,
        ..base_streaming_config()
    };
    assert_streaming_query_matches_model("Query0", &base, 40, Query0, models::query0(&base))
        .await?;
    assert_streaming_query_matches_model("Query1", &base, 40, Query1, models::query1(&base))
        .await?;
    assert_streaming_query_matches_model(
        "Query2",
        &base,
        40,
        Query2::from_config(&base),
        models::query2(&base),
    )
    .await?;

    let q3_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::LocalItemSuggestion),
        num_events: 5_000,
        num_event_generators: 1,
        hot_sellers_ratio: 2,
        out_of_order_group_size: 12,
        ..base_streaming_config()
    };
    assert_streaming_query_matches_model(
        "Query3",
        &q3_cfg,
        250,
        Query3::from_config(&q3_cfg),
        models::query3(&q3_cfg),
    )
    .await
}

/// SMOKE Query 5 with its default delayed-event settings. An unpaced source must not hold
/// events back, or the first window loses late bids and ties (10 results instead of 12).
/// Small calls make the source report its watermark often, so held-back events turn late.
#[tokio::test]
async fn streaming_query5_with_default_delays_matches_batch_model()
-> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::HotItems),
        num_events: 100_000,
        ..NexmarkConfig::default()
    };
    assert!(config.prob_delayed_event > 0.0 && !config.is_rate_limited);
    let expected = models::query5(&config);
    assert_eq!(expected.len(), 12);
    assert_streaming_query_matches_model(
        "Query5Smoke",
        &config,
        128,
        Query5::from_config(&config),
        expected,
    )
    .await
}

#[tokio::test]
async fn streaming_windowed_queries_match_models() -> std::result::Result<(), TestPipelineError> {
    let q5_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::HotItems),
        num_events: 500,
        num_event_generators: 2,
        first_event_rate: 100,
        next_event_rate: 100,
        window_size_sec: 4,
        window_period_sec: 2,
        fanout: 2,
        ..base_streaming_config()
    };
    assert_streaming_query_matches_model(
        "Query5",
        &q5_cfg,
        40,
        Query5::from_config(&q5_cfg),
        models::query5(&q5_cfg),
    )
    .await?;

    let q7_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::HighestBid),
        window_size_sec: 2,
        ..q5_cfg
    };
    assert_streaming_query_matches_model(
        "Query7",
        &q7_cfg,
        40,
        Query7::from_config(&q7_cfg),
        models::query7(&q7_cfg),
    )
    .await?;

    let q8_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::MonitorNewUsers),
        num_events: 1_200,
        first_event_rate: 150,
        next_event_rate: 150,
        window_size_sec: 4,
        ..base_streaming_config()
    };
    assert_streaming_query_matches_model(
        "Query8",
        &q8_cfg,
        60,
        Query8::from_config(&q8_cfg),
        models::query8(&q8_cfg),
    )
    .await
}

#[tokio::test]
async fn streaming_winning_bids_queries_match_models() -> std::result::Result<(), TestPipelineError>
{
    let q4_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::AveragePriceForCategory),
        num_events: 1_200,
        num_event_generators: 2,
        first_event_rate: 200,
        next_event_rate: 200,
        num_in_flight_auctions: 20,
        window_size_sec: 4,
        window_period_sec: 2,
        fanout: 2,
        ..base_streaming_config()
    };
    assert_streaming_query_matches_model(
        "Query4",
        &q4_cfg,
        80,
        Query4::from_config(&q4_cfg),
        models::query4(&q4_cfg),
    )
    .await?;

    let q9_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::WinningBids),
        num_event_generators: 1,
        ..q4_cfg
    };
    assert_streaming_query_matches_model(
        "Query9",
        &q9_cfg,
        80,
        Query9::from_config(&q9_cfg),
        models::query9(&q9_cfg),
    )
    .await?;

    let q6_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::AverageSellingPriceBySeller),
        num_events: 2_000,
        num_event_generators: 1,
        num_in_flight_auctions: 15,
        hot_sellers_ratio: 4,
        ..base_streaming_config()
    };
    let expected_q6 = models::query6(&q6_cfg);
    assert!(!expected_q6.is_empty());

    let p = streaming_pipeline();
    let panes = p
        .apply(
            "ReadUnboundedQ6",
            UnboundedEventSource::new(&q6_cfg).with_batch_size(100),
        )
        .apply("Query6", Query6::from_config(&q6_cfg))
        .par_do_fn("WithPaneIdx", |sp: SellerPrice, ctx| {
            let idx = ctx
                .pane()
                .index()
                .and_then(|i| i64::try_from(i).ok())
                .unwrap_or(0);
            ctx.emit((sp, idx))
        });
    passert::that("Query6StreamingFinalPanes", &panes)
        .satisfies(move |actual| check_latest_seller_panes(actual, &expected_q6));
    p.run().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Prism rejects stateful count triggers on merging session windows (apache/beam#31438)"]
async fn streaming_query11_conserves_per_bidder_counts()
-> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::UserSessions),
        num_events: 300,
        num_event_generators: 2,
        first_event_rate: 80,
        next_event_rate: 80,
        num_active_people: 25,
        window_size_sec: 2,
        max_log_events: 2,
        ..base_streaming_config()
    };
    let expected_totals =
        models::query1(&config)
            .into_iter()
            .fold(BTreeMap::new(), |mut counts, (bid, _)| {
                *counts.entry(bid.bidder).or_insert(0) += 1;
                counts
            });
    let p = streaming_pipeline();
    let out = p
        .apply(
            "ReadUnboundedQ11",
            UnboundedEventSource::new(&config).with_batch_size(40),
        )
        .apply("Query11", Query11::from_config(&config));
    passert::that("Query11StreamingTotals", &out)
        .satisfies(move |actual| check_session_totals(actual, &expected_totals));
    p.run().await?;
    Ok(())
}

#[tokio::test]
async fn streaming_utility_and_side_input_queries_match_models()
-> std::result::Result<(), TestPipelineError> {
    let q12_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::ProcessingTimeWindows),
        num_events: 300,
        num_event_generators: 2,
        num_active_people: 25,
        window_size_sec: 5,
        ..base_streaming_config()
    };
    let expected_totals =
        models::query1(&q12_cfg)
            .into_iter()
            .fold(BTreeMap::new(), |mut counts, (bid, _)| {
                *counts.entry(bid.bidder).or_insert(0) += 1;
                counts
            });
    let p12 = streaming_pipeline();
    let out12 = p12
        .apply(
            "ReadUnboundedQ12",
            UnboundedEventSource::new(&q12_cfg).with_batch_size(40),
        )
        .apply("Query12", Query12::from_config(&q12_cfg));
    passert::that("Query12StreamingTotals", &out12)
        .satisfies(move |actual| check_session_totals(actual, &expected_totals));
    p12.run().await?;

    let q13_cfg = NexmarkConfig {
        query: Some(NexmarkQueryName::PortabilityBatch),
        num_events: 300,
        num_event_generators: 2,
        num_key_buckets: 16,
        ..base_streaming_config()
    };
    let expected_events: Vec<_> = models::query0(&q13_cfg)
        .into_iter()
        .map(|(ev, _)| ev)
        .collect();
    let p13 = streaming_pipeline();
    let out13 = p13
        .apply(
            "ReadUnboundedQ13",
            UnboundedEventSource::new(&q13_cfg).with_batch_size(40),
        )
        .apply("Query13", Query13::from_config(&q13_cfg));
    passert::that("Query13StreamingEvents", &out13).contains_in_any_order(expected_events);
    p13.run().await?;

    assert_streaming_query_matches_model(
        "Query14",
        &q13_cfg,
        40,
        Query14::from_config(&q13_cfg),
        models::query0(&q13_cfg),
    )
    .await?;

    let side_cfg = NexmarkConfig {
        num_events: 350,
        num_event_generators: 2,
        first_event_rate: 50,
        next_event_rate: 50,
        num_active_people: 20,
        side_input_row_count: 29,
        session_gap_sec: 1,
        watermark_holdback_sec: 10,
        ..base_streaming_config()
    };
    assert_streaming_query_matches_model(
        "BoundedSideInputJoin",
        &side_cfg,
        40,
        BoundedSideInputJoin::from_config(&side_cfg),
        models::bounded_side_input_join(&side_cfg),
    )
    .await?;
    assert_streaming_query_matches_model(
        "SessionSideInputJoin",
        &side_cfg,
        40,
        SessionSideInputJoin::from_config(&side_cfg),
        models::session_side_input_join(&side_cfg),
    )
    .await
}

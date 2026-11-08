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

//! Batch Prism tests for windowed queries `Query5`, `Query7`, and `Query8`.

use beam::prelude::*;
use beam::testing::{TestPipeline, TestPipelineError, passert};
use nexmark::config::{NexmarkConfig, NexmarkQueryName};
use nexmark::model::{AuctionCount, Bid, Event};
use nexmark::models;
use nexmark::queries::{Query5, Query7, Query8};

use crate::assert_query_matches_model;

#[tokio::test]
async fn query5_matches_model_without_and_with_fanout() -> std::result::Result<(), TestPipelineError>
{
    let base = NexmarkConfig {
        query: Some(NexmarkQueryName::HotItems),
        num_events: 600,
        num_event_generators: 2,
        first_event_rate: 100,
        next_event_rate: 100,
        window_size_sec: 4,
        window_period_sec: 2,
        fanout: 1,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "Query5NoFanout",
        &base,
        Query5::from_config(&base),
        models::query5(&base),
    )
    .await?;

    let sharded = NexmarkConfig { fanout: 4, ..base };
    assert_query_matches_model(
        "Query5WithFanout",
        &sharded,
        Query5::from_config(&sharded),
        models::query5(&sharded),
    )
    .await
}

#[tokio::test]
async fn query5_emits_all_tied_top_auctions() -> std::result::Result<(), TestPipelineError> {
    let ts = Instant::from_millis(1_000);
    let mk_bid = |auction: i64, bidder: i64| {
        (
            Event::Bid(Bid {
                auction,
                bidder,
                price: 100,
                date_time: ts,
                extra: String::new(),
            }),
            ts,
        )
    };
    let events = vec![
        mk_bid(10, 1),
        mk_bid(10, 2),
        mk_bid(20, 3),
        mk_bid(20, 4),
        mk_bid(30, 5),
    ];

    let p = TestPipeline::new();
    let out = p
        .apply("CreateTies", Create::new(events))
        .par_do_fn("AssignTs", |(event, event_ts): (Event, Instant), ctx| {
            ctx.output(event).at(event_ts).emit()
        })
        .apply("Query5", Query5::new(4, 4).with_fanout(2));
    passert::that("AllTiedWinners", &out).contains_in_any_order([
        AuctionCount {
            auction: 10,
            num: 2,
        },
        AuctionCount {
            auction: 20,
            num: 2,
        },
    ]);
    p.run().await?;
    Ok(())
}

#[tokio::test]
async fn query7_matches_model_without_and_with_fanout() -> std::result::Result<(), TestPipelineError>
{
    let base = NexmarkConfig {
        query: Some(NexmarkQueryName::HighestBid),
        num_events: 600,
        num_event_generators: 2,
        first_event_rate: 100,
        next_event_rate: 100,
        window_size_sec: 2,
        fanout: 1,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "Query7NoFanout",
        &base,
        Query7::from_config(&base),
        models::query7(&base),
    )
    .await?;

    let sharded = NexmarkConfig { fanout: 3, ..base };
    assert_query_matches_model(
        "Query7WithFanout",
        &sharded,
        Query7::from_config(&sharded),
        models::query7(&sharded),
    )
    .await
}

#[tokio::test]
async fn query8_matches_model() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::MonitorNewUsers),
        num_events: 1_500,
        num_event_generators: 3,
        first_event_rate: 150,
        next_event_rate: 150,
        window_size_sec: 4,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "Query8",
        &config,
        Query8::from_config(&config),
        models::query8(&config),
    )
    .await
}

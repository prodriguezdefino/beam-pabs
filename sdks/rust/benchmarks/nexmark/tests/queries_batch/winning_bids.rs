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

//! Batch Prism tests for `WinningBids`, `Query4`, `Query6`, and `Query9`.

use beam::prelude::*;
use beam::testing::{TestPipeline, TestPipelineError};
use nexmark::config::{BASE_TIME_MS, NexmarkConfig, NexmarkQueryName};
use nexmark::correctness::{assert_final_seller_prices, assert_timestamped};
use nexmark::model::{Auction, AuctionBid, Bid, Event, SellerPrice};
use nexmark::models;
use nexmark::queries::{Query4, Query6, Query9, WinningBids};
use nexmark::source::BoundedEventSource;

use crate::assert_query_matches_model;

fn mk_auction(id: i64, seller: i64, reserve: i64, open_ms: i64, expires_ms: i64) -> Auction {
    Auction {
        id,
        item_name: format!("item-{id}"),
        description: String::new(),
        initial_bid: 10,
        reserve,
        date_time: Instant::from_millis(BASE_TIME_MS + open_ms),
        expires: Instant::from_millis(BASE_TIME_MS + expires_ms),
        seller,
        category: 10,
        extra: String::new(),
    }
}

fn mk_bid(auction: i64, bidder: i64, price: i64, offset_ms: i64) -> Bid {
    Bid {
        auction,
        bidder,
        price,
        date_time: Instant::from_millis(BASE_TIME_MS + offset_ms),
        extra: String::new(),
    }
}

#[tokio::test]
async fn query9_matches_model_single_and_multi_generator()
-> std::result::Result<(), TestPipelineError> {
    let single = NexmarkConfig {
        query: Some(NexmarkQueryName::WinningBids),
        num_events: 1_500,
        num_event_generators: 1,
        num_in_flight_auctions: 20,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "Query9Single",
        &single,
        Query9::from_config(&single),
        models::query9(&single),
    )
    .await?;

    let multi = NexmarkConfig {
        num_event_generators: 3,
        out_of_order_group_size: 8,
        ..single
    };
    assert_query_matches_model(
        "WinningBidsMulti",
        &multi,
        WinningBids::from_config(&multi),
        models::winning_bids(&multi),
    )
    .await
}

#[tokio::test]
async fn winning_bids_handles_early_bids_reserve_ties_and_orphans()
-> std::result::Result<(), TestPipelineError> {
    let auction_10 = mk_auction(10, 100, 100, 200, 600);
    let auction_20 = mk_auction(20, 100, 300, 200, 500);
    let early_winner = mk_bid(10, 1, 250, 100);
    let later_tie = mk_bid(10, 2, 250, 300);
    let low_bid_10 = mk_bid(10, 3, 80, 250);
    let expired_bid_10 = mk_bid(10, 4, 999, 600);
    let low_bid_20 = mk_bid(20, 5, 150, 250);
    let orphan_bid = mk_bid(99, 6, 500, 150);

    let timed_auction = |auction: Auction| {
        let ts = auction.date_time;
        (Event::Auction(auction), ts)
    };
    let timed_bid = |bid: Bid| {
        let ts = bid.date_time;
        (Event::Bid(bid), ts)
    };
    let seeded = vec![
        timed_bid(early_winner.clone()),
        timed_bid(orphan_bid),
        timed_auction(auction_10.clone()),
        timed_auction(auction_20),
        timed_bid(low_bid_10),
        timed_bid(later_tie),
        timed_bid(expired_bid_10),
        timed_bid(low_bid_20),
    ];

    let p = TestPipeline::new();
    let out = p
        .apply("Seed", Create::new(seeded))
        .par_do_fn("WithTs", |(event, ts): (Event, Instant), ctx| {
            ctx.output(event).at(ts).emit()
        })
        .apply("WinningBids", WinningBids::new(500));
    assert_timestamped(
        "EdgeCaseWinners",
        &out,
        vec![(
            AuctionBid {
                auction: auction_10,
                bid: early_winner,
            },
            Instant::from_millis(BASE_TIME_MS + 599),
        )],
    );

    let result = p.run().await?;
    let metrics = result.metrics().expect("Prism reports pipeline metrics");
    assert_eq!(metrics.counter("WinningBids", "underReserve"), Some(2));
    assert_eq!(metrics.counter("WinningBids", "noValidBids"), Some(1));
    assert_eq!(metrics.counter("WinningBids", "noAuction"), Some(1));
    Ok(())
}

#[tokio::test]
async fn query4_matches_model_without_and_with_fanout() -> std::result::Result<(), TestPipelineError>
{
    let base = NexmarkConfig {
        query: Some(NexmarkQueryName::AveragePriceForCategory),
        num_events: 1_500,
        num_event_generators: 2,
        first_event_rate: 200,
        next_event_rate: 200,
        num_in_flight_auctions: 20,
        window_size_sec: 4,
        window_period_sec: 2,
        fanout: 1,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "Query4NoFanout",
        &base,
        Query4::from_config(&base),
        models::query4(&base),
    )
    .await?;

    let sharded = NexmarkConfig { fanout: 4, ..base };
    assert_query_matches_model(
        "Query4WithFanout",
        &sharded,
        Query4::from_config(&sharded),
        models::query4(&sharded),
    )
    .await
}

#[tokio::test]
async fn query6_final_panes_match_model_and_evict_past_ten()
-> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::AverageSellingPriceBySeller),
        num_events: 2_500,
        num_event_generators: 1,
        num_in_flight_auctions: 15,
        hot_sellers_ratio: 4,
        ..NexmarkConfig::default()
    };
    let expected = models::query6(&config);
    assert!(!expected.is_empty());

    let p = TestPipeline::new();
    let out = p
        .apply("ReadEvents", BoundedEventSource::new(&config))
        .apply("Query6", Query6::from_config(&config));
    assert_final_seller_prices("Query6FinalPanes", &out, expected);
    p.run().await?;

    let twelve_auctions: Vec<(Event, Instant)> = (1_i64..=12)
        .flat_map(|i| {
            let auction = mk_auction(100 + i, 77, 5, i * 10, i * 10 + 50);
            let bid = mk_bid(100 + i, 200 + i, i * 10, i * 10 + 20);
            let a_ts = auction.date_time;
            let b_ts = bid.date_time;
            [(Event::Auction(auction), a_ts), (Event::Bid(bid), b_ts)]
        })
        .collect();

    let p_evict = TestPipeline::new();
    let evict_out = p_evict
        .apply("SeedTwelve", Create::new(twelve_auctions))
        .par_do_fn("WithTs", |(event, ts): (Event, Instant), ctx| {
            ctx.output(event).at(ts).emit()
        })
        .apply("Query6Evict", Query6::new(500));
    assert_final_seller_prices(
        "Query6EvictsOldestTwo",
        &evict_out,
        vec![SellerPrice {
            seller: 77,
            price: 75,
        }],
    );
    p_evict.run().await?;
    Ok(())
}

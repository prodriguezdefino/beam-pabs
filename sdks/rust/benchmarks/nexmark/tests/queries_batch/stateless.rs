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

//! Batch Prism tests for `Query1`, `Query2`, and stateful `Query3`.

use beam::prelude::*;
use beam::testing::{TestPipeline, TestPipelineError};
use nexmark::config::{BASE_TIME_MS, NexmarkConfig, NexmarkQueryName};
use nexmark::correctness::assert_timestamped;
use nexmark::model::{Auction, Event, NameCityStateId, Person};
use nexmark::models;
use nexmark::queries::{Query1, Query2, Query3};
use nexmark::source::BoundedEventSource;

use crate::assert_query_matches_model;

#[tokio::test]
async fn query1_matches_model_single_and_multi_generator()
-> std::result::Result<(), TestPipelineError> {
    let single = NexmarkConfig {
        query: Some(NexmarkQueryName::CurrencyConversion),
        num_events: 500,
        num_event_generators: 1,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model("Query1Single", &single, Query1, models::query1(&single)).await?;

    let multi = NexmarkConfig {
        num_event_generators: 4,
        out_of_order_group_size: 4,
        ..single
    };
    assert_query_matches_model("Query1Multi", &multi, Query1, models::query1(&multi)).await
}

#[tokio::test]
async fn query2_matches_model() -> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::Selection),
        num_events: 600,
        num_event_generators: 2,
        auction_skip: 5,
        ..NexmarkConfig::default()
    };
    assert_query_matches_model(
        "Query2",
        &config,
        Query2::from_config(&config),
        models::query2(&config),
    )
    .await
}

#[tokio::test]
async fn query3_matches_model_and_records_state_metrics()
-> std::result::Result<(), TestPipelineError> {
    let config = NexmarkConfig {
        query: Some(NexmarkQueryName::LocalItemSuggestion),
        num_events: 5_000,
        num_event_generators: 1,
        hot_sellers_ratio: 2,
        out_of_order_group_size: 12,
        ..NexmarkConfig::default()
    };
    let expected = models::query3(&config);
    assert!(!expected.is_empty());
    let expected_len = expected.len() as i64;

    let p = TestPipeline::new();
    let out = p
        .apply("ReadEvents", BoundedEventSource::new(&config))
        .apply("Query3", Query3::from_config(&config));
    assert_timestamped("Query3MatchesModel", &out, expected);

    let result = p.run().await?;
    let metrics = result.metrics().expect("Prism reports pipeline metrics");
    let new_old = metrics.counter("Query3", "newOldOutput").unwrap_or(0);
    let old_new = metrics.counter("Query3", "oldNewOutput").unwrap_or(0);
    assert_eq!(new_old + old_new, expected_len);
    assert!(metrics.counter("Query3", "newPerson").unwrap_or(0) > 0);
    assert!(metrics.counter("Query3", "newAuction").unwrap_or(0) > 0);
    assert_eq!(metrics.counter("Query3", "fatal"), None);
    Ok(())
}

#[tokio::test]
async fn query3_joins_auctions_arriving_before_and_after_seller()
-> std::result::Result<(), TestPipelineError> {
    let t0 = Instant::from_millis(BASE_TIME_MS);
    let t1 = Instant::from_millis(BASE_TIME_MS + 100);
    let t2 = Instant::from_millis(BASE_TIME_MS + 200);

    let early_auction = Event::Auction(Auction {
        id: 2001,
        item_name: "kayak".to_owned(),
        description: "sea kayak".to_owned(),
        initial_bid: 100,
        reserve: 200,
        date_time: t0,
        expires: Instant::from_millis(BASE_TIME_MS + 10_000),
        seller: 42,
        category: 10,
        extra: String::new(),
    });
    let seller = Event::Person(Person {
        id: 42,
        name: "Ada Lovelace".to_owned(),
        email_address: "ada@example.com".to_owned(),
        credit_card: "0000 1111 2222 3333".to_owned(),
        city: "Portland".to_owned(),
        state: "OR".to_owned(),
        date_time: t1,
        extra: String::new(),
    });
    let late_auction = Event::Auction(Auction {
        id: 2002,
        item_name: "paddle".to_owned(),
        description: "carbon paddle".to_owned(),
        initial_bid: 50,
        reserve: 80,
        date_time: t2,
        expires: Instant::from_millis(BASE_TIME_MS + 10_000),
        seller: 42,
        category: 10,
        extra: String::new(),
    });
    let ignored_state_seller = Event::Person(Person {
        id: 99,
        name: "Ignored Seller".to_owned(),
        email_address: "wy@example.com".to_owned(),
        credit_card: "9999 8888 7777 6666".to_owned(),
        city: "Cheyenne".to_owned(),
        state: "WY".to_owned(),
        date_time: t0,
        extra: String::new(),
    });

    let p = TestPipeline::new();
    let events = p
        .apply(
            "SeedEvents",
            Create::new(vec![
                (early_auction, t0),
                (ignored_state_seller, t0),
                (seller, t1),
                (late_auction, t2),
            ]),
        )
        .par_do_fn("WithTimestamps", |(event, ts): (Event, Instant), ctx| {
            ctx.output(event).at(ts).emit()
        });
    let out = events.apply("Query3Synthetic", Query3::new(600));

    assert_timestamped(
        "Query3SyntheticMatches",
        &out,
        vec![
            (
                NameCityStateId {
                    name: "Ada Lovelace".to_owned(),
                    city: "Portland".to_owned(),
                    state: "OR".to_owned(),
                    id: 2001,
                },
                t1,
            ),
            (
                NameCityStateId {
                    name: "Ada Lovelace".to_owned(),
                    city: "Portland".to_owned(),
                    state: "OR".to_owned(),
                    id: 2002,
                },
                t2,
            ),
        ],
    );
    p.run().await?;
    Ok(())
}

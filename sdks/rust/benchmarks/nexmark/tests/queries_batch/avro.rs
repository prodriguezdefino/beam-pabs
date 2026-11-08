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

//! Avro event files and the `ROW` coder strategy on Prism.

use beam::io::avro::avroio;
use beam::options::PipelineOptions;
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use nexmark::config::{CoderStrategy, NexmarkQueryName, SinkType, SourceType};
use nexmark::launcher::{
    AVRO_AUCTION_FILES, AVRO_BID_FILES, AVRO_EVENT_FILES, AVRO_PERSON_FILES, MonitorTiming,
    NexmarkArgs, NexmarkRunOutcome, build_nexmark_pipeline, run_nexmark,
};
use nexmark::model::{Auction, AuctionRow, Bid, BidRow, Event, EventRow, Person, PersonRow};
use nexmark::models;

fn base_args() -> NexmarkArgs {
    NexmarkArgs {
        query: Some(NexmarkQueryName::Passthrough),
        num_events: Some(500),
        num_event_generators: Some(2),
        window_size_sec: Some(4),
        window_period_sec: Some(2),
        ..NexmarkArgs::default()
    }
}

/// Runs `args` and asserts that every query row executed.
async fn assert_all_executed(args: &NexmarkArgs) {
    let summaries = run_nexmark(&PipelineOptions::default(), args, &MonitorTiming::default())
        .await
        .expect("suite run");
    for summary in &summaries {
        assert!(
            matches!(summary.outcome, NexmarkRunOutcome::Executed { .. }),
            "{}: {:?}",
            summary.query,
            summary.outcome
        );
    }
}

#[tokio::test]
async fn avro_sink_files_hold_the_generated_events_and_feed_the_avro_source() -> Result {
    let dir = tempfile::tempdir()?;
    let output_path = dir.path().to_string_lossy().into_owned();
    let writer = NexmarkArgs {
        sink_type: Some(SinkType::Avro),
        output_path: Some(output_path.clone()),
        ..base_args()
    };
    let config = writer.resolve_configs().remove(0);
    build_nexmark_pipeline(&PipelineOptions::default(), &writer, &config)?
        .run()
        .await?;

    let events: Vec<Event> = models::query0(&config)
        .into_iter()
        .map(|(event, _)| event)
        .collect();
    let bids: Vec<Bid> = events
        .iter()
        .filter_map(|event| match event {
            Event::Bid(bid) => Some(bid.clone()),
            Event::Person(_) | Event::Auction(_) => None,
        })
        .collect();
    let auctions: Vec<Auction> = events
        .iter()
        .filter_map(|event| match event {
            Event::Auction(auction) => Some(auction.clone()),
            Event::Person(_) | Event::Bid(_) => None,
        })
        .collect();
    let persons: Vec<Person> = events
        .iter()
        .filter_map(|event| match event {
            Event::Person(person) => Some(person.clone()),
            Event::Auction(_) | Event::Bid(_) => None,
        })
        .collect();

    let files = |name: &str| format!("{output_path}/{name}*.avro");
    let p = TestPipeline::new();
    let read_events = p
        .apply(
            "ReadEvents",
            avroio::Read::<EventRow>::new(files(AVRO_EVENT_FILES)),
        )
        .flat_map("ToEvents", |row: EventRow| Event::try_from(row).ok());
    passert::that("AvroEvents", &read_events).contains_in_any_order(events);
    let read_bids = p
        .apply(
            "ReadBids",
            avroio::Read::<BidRow>::new(files(AVRO_BID_FILES)),
        )
        .map("ToBids", Bid::from);
    passert::that("AvroBids", &read_bids).contains_in_any_order(bids);
    let read_auctions = p
        .apply(
            "ReadAuctions",
            avroio::Read::<AuctionRow>::new(files(AVRO_AUCTION_FILES)),
        )
        .map("ToAuctions", Auction::from);
    passert::that("AvroAuctions", &read_auctions).contains_in_any_order(auctions);
    let read_persons = p
        .apply(
            "ReadPersons",
            avroio::Read::<PersonRow>::new(files(AVRO_PERSON_FILES)),
        )
        .map("ToPersons", Person::from);
    passert::that("AvroPersons", &read_persons).contains_in_any_order(persons);
    p.run().await?;

    for query in [
        NexmarkQueryName::Passthrough,
        NexmarkQueryName::Selection,
        NexmarkQueryName::HotItems,
        NexmarkQueryName::WinningBids,
    ] {
        let reader = NexmarkArgs {
            query: Some(query),
            source_type: Some(SourceType::Avro),
            input_path: Some(format!("{output_path}/{AVRO_EVENT_FILES}")),
            sink_type: Some(SinkType::CountOnly),
            assert_correctness: true,
            ..base_args()
        };
        assert_all_executed(&reader).await;
    }
    Ok(())
}

#[tokio::test]
async fn row_coder_strategy_keeps_query_output_equal_to_the_models() {
    for query in [
        NexmarkQueryName::Passthrough,
        NexmarkQueryName::LocalItemSuggestion,
        NexmarkQueryName::HotItems,
        NexmarkQueryName::AverageSellingPriceBySeller,
    ] {
        let args = NexmarkArgs {
            query: Some(query),
            coder_strategy: Some(CoderStrategy::Row),
            sink_type: Some(SinkType::CountOnly),
            assert_correctness: true,
            ..base_args()
        };
        assert_all_executed(&args).await;
    }
}

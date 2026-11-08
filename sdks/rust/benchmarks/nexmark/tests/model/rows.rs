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

//! Row forms of the events, and the Avro and coder strategy flags.

use beam::options::try_parse_from;
use beam::schema::BeamRow;
use beam_test_utils::xlang::describe;
use nexmark::config::{CoderStrategy, NexmarkConfig, SinkType, SourceType};
use nexmark::generator::{Generator, GeneratorConfig};
use nexmark::launcher::NexmarkArgs;
use nexmark::model::{
    Auction, AuctionRow, Bid, BidRow, Event, EventRow, InvalidEventRow, Person, PersonRow,
};

fn generated_events() -> Vec<Event> {
    let config = GeneratorConfig::from_nexmark_config(&NexmarkConfig {
        num_events: 200,
        ..NexmarkConfig::default()
    });
    Generator::new(config).map(|next| next.event).collect()
}

fn field_names<T: BeamRow>() -> Vec<String> {
    describe(&T::beam_schema())
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

#[test]
fn every_event_round_trips_through_its_row_form() {
    let events = generated_events();
    assert!(events.iter().any(|e| matches!(e, Event::Person(_))));
    assert!(events.iter().any(|e| matches!(e, Event::Auction(_))));
    events.into_iter().for_each(|event| {
        let row = EventRow::from(event.clone());
        let set = [
            row.new_person.is_some(),
            row.new_auction.is_some(),
            row.bid.is_some(),
        ];
        assert_eq!(set.iter().filter(|set| **set).count(), 1, "{row:?}");
        let back = Event::try_from(row).expect("one event is set");
        assert_eq!(back, event);
        match event {
            Event::Person(person) => {
                assert_eq!(Person::from(PersonRow::from(person.clone())), person);
            }
            Event::Auction(auction) => {
                assert_eq!(Auction::from(AuctionRow::from(auction.clone())), auction);
            }
            Event::Bid(bid) => assert_eq!(Bid::from(BidRow::from(bid.clone())), bid),
        }
    });
}

#[test]
fn event_rows_without_exactly_one_event_are_rejected() {
    let empty = EventRow {
        new_person: None,
        new_auction: None,
        bid: None,
    };
    assert_eq!(
        Event::try_from(empty.clone()),
        Err(InvalidEventRow { set_fields: 0 })
    );

    let events = generated_events();
    let bid = events.iter().find_map(|event| match event {
        Event::Bid(bid) => Some(BidRow::from(bid.clone())),
        Event::Person(_) | Event::Auction(_) => None,
    });
    let person = events.iter().find_map(|event| match event {
        Event::Person(person) => Some(PersonRow::from(person.clone())),
        Event::Auction(_) | Event::Bid(_) => None,
    });
    let two = EventRow {
        new_person: person,
        bid,
        ..empty
    };
    let err = Event::try_from(two).expect_err("two events are set");
    assert_eq!(err, InvalidEventRow { set_fields: 2 });
    assert!(err.to_string().contains("exactly one"), "{err}");
}

#[test]
fn row_schemas_use_the_java_field_names() {
    assert_eq!(
        field_names::<EventRow>(),
        ["newPerson", "newAuction", "bid"]
    );
    assert_eq!(
        field_names::<PersonRow>(),
        [
            "id",
            "name",
            "emailAddress",
            "creditCard",
            "city",
            "state",
            "dateTime",
            "extra"
        ]
    );
    assert_eq!(
        field_names::<AuctionRow>(),
        [
            "id",
            "itemName",
            "description",
            "initialBid",
            "reserve",
            "dateTime",
            "expires",
            "seller",
            "category",
            "extra"
        ]
    );
    assert_eq!(
        field_names::<BidRow>(),
        ["auction", "bidder", "price", "dateTime", "extra"]
    );
}

#[test]
fn avro_and_coder_strategy_flags_parse() {
    let (_, args) = try_parse_from::<NexmarkArgs, _, _>([
        "nexmark",
        "--source_type=AVRO",
        "--input_path=/tmp/nexmark/event",
        "--sink_type=AVRO",
        "--output_path=/tmp/nexmark",
        "--coder_strategy=ROW",
    ])
    .expect("valid flags");
    let config = args.resolve_configs().remove(0);
    assert_eq!(config.source_type, SourceType::Avro);
    assert_eq!(config.sink_type, SinkType::Avro);
    assert_eq!(config.coder_strategy, CoderStrategy::Row);
    assert_eq!(args.input_path.as_deref(), Some("/tmp/nexmark/event"));

    let defaults = NexmarkArgs::default().resolve_configs().remove(0);
    assert_eq!(defaults.coder_strategy, CoderStrategy::Hand);
    assert_eq!("hand".parse::<CoderStrategy>(), Ok(CoderStrategy::Hand));
    assert!("AVRO".parse::<CoderStrategy>().is_err());
}

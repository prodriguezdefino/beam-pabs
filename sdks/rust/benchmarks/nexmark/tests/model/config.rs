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

use std::cmp::Ordering;

use beam::Instant;
use nexmark::config::{
    NexmarkConfig, NexmarkQueryName, NexmarkSuite, RateShape, RateUnit, SinkType, SourceType,
};
use nexmark::model::{Bid, Event};

#[test]
fn annotations_and_bid_ordering_behave_deterministically() {
    let event = Event::Bid(Bid {
        auction: 1_000,
        bidder: 1_001,
        price: 500,
        date_time: Instant::from_millis(1_000),
        extra: "payload".to_owned(),
    });
    assert!(!event.has_annotation("LATE"));
    let annotated = event.clone().with_annotation("LATE");
    assert!(annotated.has_annotation("LATE"));
    assert_eq!(annotated.without_annotation("LATE"), event);

    let earlier_bid = Bid {
        auction: 1_000,
        bidder: 1_001,
        price: 500,
        date_time: Instant::from_millis(1_000),
        extra: String::new(),
    };
    let later_same_price = Bid {
        date_time: Instant::from_millis(2_000),
        ..earlier_bid.clone()
    };
    let higher_price = Bid {
        price: 600,
        date_time: Instant::from_millis(3_000),
        ..earlier_bid.clone()
    };

    assert_eq!(
        earlier_bid.price_then_descending_time(&later_same_price),
        Ordering::Greater
    );
    assert_eq!(
        earlier_bid.price_then_descending_time(&higher_price),
        Ordering::Less
    );
    assert_eq!(
        earlier_bid.ascending_time_then_price(&later_same_price),
        Ordering::Less
    );
}

#[test]
fn suites_query_names_and_rate_shapes_round_trip() {
    for &query in NexmarkQueryName::ALL {
        assert_eq!(query.as_str().parse(), Ok(query));
        assert_eq!(query.number().to_string().parse(), Ok(query));
    }

    assert_eq!("direct".parse(), Ok(SourceType::Direct));
    assert_eq!("count_only".parse(), Ok(SinkType::CountOnly));
    assert_eq!("devnull".parse(), Ok(SinkType::Devnull));
    assert_eq!("text".parse(), Ok(SinkType::Text));
    assert_eq!("smoke".parse(), Ok(NexmarkSuite::Smoke));

    let smoke = NexmarkSuite::Smoke.configs();
    assert_eq!(smoke.len(), NexmarkQueryName::ALL.len());
    let q4 = smoke
        .iter()
        .find(|c| c.query == Some(NexmarkQueryName::AveragePriceForCategory))
        .expect("query 4 in smoke");
    assert_eq!(q4.num_events, 10_000);

    assert_eq!(
        RateShape::Square.inter_event_delay_us(100, 200, RateUnit::PerSecond, 2),
        vec![20_000, 10_000]
    );
    assert_eq!(RateShape::Square.step_length_sec(15), 8);
    assert_eq!(NexmarkConfig::default().num_events, 100_000);
}

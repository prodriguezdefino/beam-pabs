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

//! Deterministic synthesis of [`Auction`] events and auction identifiers.

use beam::Instant;

use super::person::{last_base0_person_id, next_base0_person_id};
use super::strings::{next_extra, next_long, next_price, next_string};
use super::{GeneratorConfig, JavaRandom};
use crate::model::Auction;

const NUM_CATEGORIES: i32 = 5;
const AUCTION_ID_LEAD: i64 = 10;
const HOT_SELLER_QUANTUM: i64 = 100;
const AUCTION_END_REM: i64 =
    GeneratorConfig::PERSON_PROPORTION + GeneratorConfig::AUCTION_PROPORTION;

/// Synthesizes the [`Auction`] for `event_id` at `timestamp_ms`.
pub(crate) fn next_auction(
    events_count_so_far: i64,
    event_id: i64,
    random: &mut JavaRandom,
    timestamp_ms: i64,
    config: &GeneratorConfig,
) -> Auction {
    let id = last_base0_auction_id(event_id) + GeneratorConfig::FIRST_AUCTION_ID;
    let seller_base = if random.next_int_bound(config.configuration.hot_sellers_ratio) > 0 {
        (last_base0_person_id(event_id) / HOT_SELLER_QUANTUM) * HOT_SELLER_QUANTUM
    } else {
        next_base0_person_id(event_id, random, config)
    };
    let seller = seller_base + GeneratorConfig::FIRST_PERSON_ID;
    let category =
        GeneratorConfig::FIRST_CATEGORY_ID + i64::from(random.next_int_bound(NUM_CATEGORIES));
    let initial_bid = next_price(random);
    let expires_ms =
        timestamp_ms + next_auction_length_ms(events_count_so_far, random, timestamp_ms, config);
    let item_name = next_string(random, 20);
    let description = next_string(random, 100);
    let reserve = initial_bid + next_price(random);
    let current_size = 8 + (item_name.len() + description.len()) as i32 + 8 * 5;
    let extra = next_extra(
        random,
        current_size,
        config.configuration.avg_auction_byte_size,
    );

    Auction {
        id,
        item_name,
        description,
        initial_bid,
        reserve,
        date_time: Instant::from_millis(timestamp_ms),
        expires: Instant::from_millis(expires_ms),
        seller,
        category,
        extra,
    }
}

/// Samples a zero-based in-flight auction ID for a bid.
pub(crate) fn next_base0_auction_id(
    next_event_id: i64,
    random: &mut JavaRandom,
    config: &GeneratorConfig,
) -> i64 {
    let max_auction = last_base0_auction_id(next_event_id);
    let min_auction = (max_auction - i64::from(config.configuration.num_in_flight_auctions)).max(0);
    min_auction + next_long(random, max_auction - min_auction + 1 + AUCTION_ID_LEAD)
}

/// Returns the last zero-based auction ID generated at or before `event_id`.
pub(crate) const fn last_base0_auction_id(event_id: i64) -> i64 {
    let epoch = event_id / GeneratorConfig::PROPORTION_DENOMINATOR;
    let rem = event_id % GeneratorConfig::PROPORTION_DENOMINATOR;
    let (adj_epoch, offset) = match rem {
        ..GeneratorConfig::PERSON_PROPORTION => {
            (epoch - 1, GeneratorConfig::AUCTION_PROPORTION - 1)
        }
        GeneratorConfig::PERSON_PROPORTION..AUCTION_END_REM => {
            (epoch, rem - GeneratorConfig::PERSON_PROPORTION)
        }
        _ => (epoch, GeneratorConfig::AUCTION_PROPORTION - 1),
    };
    adj_epoch * GeneratorConfig::AUCTION_PROPORTION + offset
}

/// Samples an auction duration in milliseconds calibrated to keep `num_in_flight_auctions` open.
pub(crate) fn next_auction_length_ms(
    events_count_so_far: i64,
    random: &mut JavaRandom,
    timestamp_ms: i64,
    config: &GeneratorConfig,
) -> i64 {
    let current_event_number = config.next_adjusted_event_number(events_count_so_far);
    let num_events_for_auctions = (i64::from(config.configuration.num_in_flight_auctions)
        * GeneratorConfig::PROPORTION_DENOMINATOR)
        / GeneratorConfig::AUCTION_PROPORTION;
    let future_auction_ms = config
        .timestamp_and_inter_event_delay_us_for_event(
            current_event_number + num_events_for_auctions,
        )
        .0;
    let horizon_ms = future_auction_ms - timestamp_ms;
    1 + next_long(random, (horizon_ms * 2).max(1))
}

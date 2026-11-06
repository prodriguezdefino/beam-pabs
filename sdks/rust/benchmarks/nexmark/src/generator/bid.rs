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

//! Deterministic synthesis of [`Bid`] events.

use beam::Instant;

use super::auction::{last_base0_auction_id, next_base0_auction_id};
use super::person::{last_base0_person_id, next_base0_person_id};
use super::strings::{next_extra, next_price};
use super::{GeneratorConfig, JavaRandom};
use crate::model::Bid;

const HOT_AUCTION_QUANTUM: i64 = 100;
const HOT_BIDDER_QUANTUM: i64 = 100;

/// Synthesizes the [`Bid`] for `event_id` at `timestamp_ms`.
pub(crate) fn next_bid(
    event_id: i64,
    random: &mut JavaRandom,
    timestamp_ms: i64,
    config: &GeneratorConfig,
) -> Bid {
    let auction_base = if random.next_int_bound(config.configuration.hot_auction_ratio) > 0 {
        (last_base0_auction_id(event_id) / HOT_AUCTION_QUANTUM) * HOT_AUCTION_QUANTUM
    } else {
        next_base0_auction_id(event_id, random, config)
    };
    let bidder_base = if random.next_int_bound(config.configuration.hot_bidders_ratio) > 0 {
        (last_base0_person_id(event_id) / HOT_BIDDER_QUANTUM) * HOT_BIDDER_QUANTUM + 1
    } else {
        next_base0_person_id(event_id, random, config)
    };
    let price = next_price(random);
    let extra = next_extra(random, 8 * 4, config.configuration.avg_bid_byte_size);

    Bid {
        auction: auction_base + GeneratorConfig::FIRST_AUCTION_ID,
        bidder: bidder_base + GeneratorConfig::FIRST_PERSON_ID,
        price,
        date_time: Instant::from_millis(timestamp_ms),
        extra,
    }
}

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

//! Query 6 (`AVERAGE_SELLING_PRICE_BY_SELLER`): moving mean over each seller's last 10 closed auctions.

use std::time::Duration;

use beam::prelude::*;
use serde::{Deserialize, Serialize};

use super::winning_bids::{WinningBids, expected_auction_duration_ms};
use crate::config::{NexmarkConfig, round_half_up};
use crate::model::{Bid, Event, SellerPrice};

pub(crate) const MAX_MOVING_BIDS: usize = 10;

/// Emits the rounded moving-average selling price over the last 10 winning bids per seller.
#[derive(Clone, Copy, Debug)]
pub struct Query6 {
    expected_auction_duration_ms: i64,
}

impl Query6 {
    /// Creates a seller moving-average transform using `expected_auction_duration_ms` for [`WinningBids`].
    pub fn new(expected_auction_duration_ms: i64) -> Self {
        Self {
            expected_auction_duration_ms: expected_auction_duration_ms.max(1),
        }
    }

    /// Creates a seller moving-average transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(expected_auction_duration_ms(config))
    }
}

impl PTransform<PCollection<Event>> for Query6 {
    type Output = PCollection<SellerPrice>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<SellerPrice> {
        input
            .apply(
                "WinningBids",
                WinningBids::new(self.expected_auction_duration_ms),
            )
            .map("Rekey", |ab| (ab.auction.seller, ab.bid))
            .apply(
                "Window",
                WindowInto::new(GlobalWindows)
                    .with_trigger(Trigger::repeatedly(Trigger::after_count(1)))
                    .with_accumulation_mode(AccumulationMode::Accumulating)
                    .with_allowed_lateness(Duration::ZERO),
            )
            .combine_per_key(
                "MovingMeanSellingPrice",
                MovingMeanSellingPrice {
                    max_num_bids: MAX_MOVING_BIDS,
                },
            )
            .map("Select", |(seller, price)| SellerPrice { seller, price })
    }
}

pub(crate) fn retain_latest_bids(mut bids: Vec<Bid>, max_num_bids: usize) -> Vec<Bid> {
    // Each closed auction has a unique ID; deduplicating guards accumulating count-trigger replays.
    bids.sort_by_key(|a| a.auction);
    bids.dedup_by(|a, b| a.auction == b.auction);
    bids.sort_by(Bid::ascending_time_then_price);
    if bids.len() > max_num_bids {
        bids.drain(..bids.len() - max_num_bids);
    }
    bids
}

pub(crate) fn mean_bid_price(bids: &[Bid]) -> i64 {
    if bids.is_empty() {
        0
    } else {
        let sum: i64 = bids.iter().map(|b| b.price).sum();
        round_half_up((sum as f64) / (bids.len() as f64))
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Register)]
struct MovingMeanSellingPrice {
    max_num_bids: usize,
}

impl CombineFn for MovingMeanSellingPrice {
    type Input = Bid;
    type Accum = Vec<Bid>;
    type Output = i64;

    fn create_accumulator(&self) -> Result<Vec<Bid>> {
        Ok(Vec::new())
    }

    fn add_input(&self, mut acc: Vec<Bid>, input: Bid) -> Result<Vec<Bid>> {
        acc.push(input);
        Ok(retain_latest_bids(acc, self.max_num_bids))
    }

    fn merge_accumulators(&self, accumulators: Vec<Vec<Bid>>) -> Result<Vec<Bid>> {
        let merged: Vec<Bid> = accumulators.into_iter().flatten().collect();
        Ok(retain_latest_bids(merged, self.max_num_bids))
    }

    fn extract_output(&self, acc: Vec<Bid>) -> Result<i64> {
        Ok(mean_bid_price(&acc))
    }
}

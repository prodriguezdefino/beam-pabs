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

//! Query 5 (`HOT_ITEMS`): auctions with the highest bid count per sliding window.

use std::cmp::Ordering;
use std::time::Duration;

use beam::prelude::*;

use super::just_bids;
use crate::config::NexmarkConfig;
use crate::model::{AuctionCount, Event};

/// Emits every auction tied for the maximum bid count in each sliding window.
#[derive(Clone, Copy, Debug)]
pub struct Query5 {
    window_size_sec: i64,
    window_period_sec: i64,
    fanout: i32,
}

impl Query5 {
    /// Creates a hot-items transform over sliding windows of `window_size_sec` every `window_period_sec`.
    pub fn new(window_size_sec: i64, window_period_sec: i64) -> Self {
        Self {
            window_size_sec: window_size_sec.max(1),
            window_period_sec: window_period_sec.max(1),
            fanout: 1,
        }
    }

    /// Configures pre-aggregation shard count before the global top-count merge (`<= 1` disables sharding).
    pub fn with_fanout(mut self, fanout: i32) -> Self {
        self.fanout = fanout;
        self
    }

    /// Creates a hot-items transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.window_size_sec, config.window_period_sec).with_fanout(config.fanout)
    }
}

impl PTransform<PCollection<Event>> for Query5 {
    type Output = PCollection<AuctionCount>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<AuctionCount> {
        let size = Duration::from_secs(u64::try_from(self.window_size_sec).unwrap_or(1));
        let period = Duration::from_secs(u64::try_from(self.window_period_sec).unwrap_or(1));
        let counts = just_bids(input)
            .window_into("WindowBids", SlidingWindows::of(size).every(period))
            .map("BidToAuction", |bid| bid.auction)
            .count_per_element("CountPerAuction")
            .map("ToAuctionCount", |(auction, num)| AuctionCount {
                auction,
                num,
            });

        let fanout = usize::try_from(self.fanout).unwrap_or(1);
        let top_per_window = counts.apply(
            "TopAuctions",
            CombineGlobally::new(FoldCombineFn::new(Vec::new(), fold_top, merge_top))
                .without_defaults()
                .with_fanout(fanout),
        );

        top_per_window.flat_map("Select", |top| top)
    }
}

fn fold_top(mut top: Vec<AuctionCount>, item: AuctionCount) -> Vec<AuctionCount> {
    let current_max = top.first().map_or(0, |ac| ac.num);
    match item.num.cmp(&current_max) {
        Ordering::Greater => {
            top.clear();
            top.push(item);
        }
        Ordering::Equal if item.num > 0 => top.push(item),
        Ordering::Equal | Ordering::Less => {}
    }
    top
}

fn merge_top(mut left: Vec<AuctionCount>, right: Vec<AuctionCount>) -> Vec<AuctionCount> {
    let left_max = left.first().map_or(0, |ac| ac.num);
    let right_max = right.first().map_or(0, |ac| ac.num);
    match right_max.cmp(&left_max) {
        Ordering::Greater => right,
        Ordering::Equal if right_max > 0 => {
            left.extend(right);
            left
        }
        Ordering::Equal | Ordering::Less => left,
    }
}

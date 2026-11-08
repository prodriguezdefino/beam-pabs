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

//! Reference model for [`Query5`](crate::queries::Query5).

use std::collections::BTreeMap;
use std::time::Duration;

use beam::Instant;
use beam::coders::IntervalWindow;
use beam::windowing::SlidingWindows;

use super::{assign_intervals, bid_stream};
use crate::config::NexmarkConfig;
use crate::model::AuctionCount;

/// Computes expected `(AuctionCount, Instant)` outputs for Query 5 (`HOT_ITEMS`).
pub fn query5(config: &NexmarkConfig) -> Vec<(AuctionCount, Instant)> {
    let size = Duration::from_secs(u64::try_from(config.window_size_sec.max(1)).unwrap_or(1));
    let period = Duration::from_secs(u64::try_from(config.window_period_sec.max(1)).unwrap_or(1));
    let windows = SlidingWindows::of(size).every(period);

    let by_window = bid_stream(config).fold(
        BTreeMap::<IntervalWindow, BTreeMap<i64, i64>>::new(),
        |mut acc, (bid, ts)| {
            for window in assign_intervals(&windows, ts) {
                *acc.entry(window)
                    .or_default()
                    .entry(bid.auction)
                    .or_insert(0) += 1;
            }
            acc
        },
    );

    by_window
        .into_iter()
        .flat_map(|(window, counts)| {
            let max_num = counts.values().copied().max().unwrap_or(0);
            let emit_ts = window.max_timestamp();
            counts
                .into_iter()
                .filter(move |&(_, num)| num == max_num && max_num > 0)
                .map(move |(auction, num)| (AuctionCount { auction, num }, emit_ts))
        })
        .collect()
}

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

//! Reference model for [`Query4`](crate::queries::Query4).

use std::collections::BTreeMap;
use std::time::Duration;

use beam::Instant;
use beam::coders::IntervalWindow;
use beam::windowing::SlidingWindows;

use super::{assign_intervals, winning_bids};
use crate::config::{NexmarkConfig, round_half_up};
use crate::model::CategoryPrice;

/// Computes expected `(CategoryPrice, Instant)` outputs for Query 4 (`AVERAGE_PRICE_FOR_CATEGORY`).
pub fn query4(config: &NexmarkConfig) -> Vec<(CategoryPrice, Instant)> {
    let size = Duration::from_secs(u64::try_from(config.window_size_sec.max(1)).unwrap_or(1));
    let period = Duration::from_secs(u64::try_from(config.window_period_sec.max(1)).unwrap_or(1));
    let windows = SlidingWindows::of(size).every(period);

    let totals = winning_bids(config).into_iter().fold(
        BTreeMap::<(IntervalWindow, i64), (i64, i64)>::new(),
        |mut acc, (ab, ts)| {
            for window in assign_intervals(&windows, ts) {
                let entry = acc.entry((window, ab.auction.category)).or_insert((0, 0));
                entry.0 += ab.bid.price;
                entry.1 += 1;
            }
            acc
        },
    );

    totals
        .into_iter()
        .map(|((window, category), (sum, count))| {
            let price = round_half_up((sum as f64) / (count as f64));
            (
                CategoryPrice {
                    category,
                    price,
                    is_last: true,
                },
                window.max_timestamp(),
            )
        })
        .collect()
}

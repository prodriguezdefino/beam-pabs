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

//! Reference model for [`SessionSideInputJoin`](crate::queries::SessionSideInputJoin).

use std::collections::BTreeMap;
use std::time::Duration;

use beam::Instant;
use beam::coders::IntervalWindow;
use beam::windowing::Sessions;

use super::{assign_intervals, bid_stream};
use crate::config::NexmarkConfig;
use crate::model::Bid;

/// Computes expected `(Bid, Instant)` outputs for `SESSION_SIDE_INPUT_JOIN`.
pub fn session_side_input_join(config: &NexmarkConfig) -> Vec<(Bid, Instant)> {
    let row_count = i64::from(config.side_input_row_count.max(1));
    let gap = Duration::from_secs(u64::try_from(config.session_gap_sec.max(1)).unwrap_or(1));
    let sessions = Sessions::with_gap_duration(gap);

    let per_bidder = bid_stream(config).fold(
        BTreeMap::<i64, Vec<(IntervalWindow, Bid)>>::new(),
        |mut acc, (bid, ts)| {
            if let Some(window) = assign_intervals(&sessions, ts).next() {
                acc.entry(bid.bidder).or_default().push((window, bid));
            }
            acc
        },
    );

    per_bidder
        .into_iter()
        .flat_map(|(bidder, mut items)| {
            items.sort_by_key(|(window, _)| window.start);
            let merged = items.into_iter().fold(
                Vec::<(IntervalWindow, Vec<Bid>)>::new(),
                |mut groups, (window, bid)| {
                    match groups.last_mut() {
                        Some((active, bids)) if window.start <= active.end => {
                            *active = active.span(&window);
                            bids.push(bid);
                        }
                        Some(_) | None => groups.push((window, vec![bid])),
                    }
                    groups
                },
            );
            let side_id = bidder.rem_euclid(row_count);
            merged.into_iter().flat_map(move |(window, bids)| {
                let enriched = format!("{side_id}:{}:{}", window.start, window.end);
                let emit_ts = window.max_timestamp();
                bids.into_iter().map(move |bid| {
                    // Each bid in the merged session carries its own formatted label.
                    (
                        Bid {
                            extra: enriched.clone(),
                            ..bid
                        },
                        emit_ts,
                    )
                })
            })
        })
        .collect()
}

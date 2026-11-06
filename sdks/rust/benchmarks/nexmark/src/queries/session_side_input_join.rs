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

//! `SESSION_SIDE_INPUT_JOIN`: session-windowed bidder enrichment from a bounded side input.

use std::time::Duration;

use beam::prelude::*;

use super::just_bids;
use super::side_input::{SideInput, side_input_rows};
use crate::config::NexmarkConfig;
use crate::model::{Bid, Event};

/// Groups [`Bid`] events into per-bidder sessions and enriches `extra` with side-input and session bounds.
#[derive(Clone, Debug)]
pub struct SessionSideInputJoin {
    side_input_row_count: i32,
    session_gap_sec: i64,
    side_input: SideInput,
}

impl SessionSideInputJoin {
    /// Creates a session side-input join over `side_input_row_count` rows and `session_gap_sec` gap.
    pub fn new(side_input_row_count: i32, session_gap_sec: i64) -> Self {
        Self {
            side_input_row_count: side_input_row_count.max(1),
            session_gap_sec: session_gap_sec.max(1),
            side_input: SideInput::Direct,
        }
    }

    /// Creates a session side-input join configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.side_input_row_count, config.session_gap_sec)
    }

    /// Reads the lookup table from `side_input` instead of building it in the pipeline.
    pub fn with_side_input(mut self, side_input: SideInput) -> Self {
        self.side_input = side_input;
        self
    }
}

impl PTransform<PCollection<Event>> for SessionSideInputJoin {
    type Output = PCollection<Bid>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<Bid> {
        let row_count = i64::from(self.side_input_row_count);
        let gap = Duration::from_secs(u64::try_from(self.session_gap_sec).unwrap_or(1));
        let side_input = side_input_rows(
            input.pipeline(),
            self.side_input_row_count,
            &self.side_input,
        );

        just_bids(input)
            .key_by("ByBidder", |bid| bid.bidder)
            .window_into("Sessionize", Sessions::with_gap_duration(gap))
            .group_by_key("GroupSessions")
            .par_do_fn_with_side_inputs(
                "JoinToFiles",
                side_input.as_multimap(),
                move |Windowed {
                          value: (bidder, bids),
                          window,
                      }: Windowed<(i64, BeamIterable<Bid>)>,
                      side_map,
                      ctx| {
                    let interval = window
                        .interval()
                        .ok_or("session grouping requires an IntervalWindow")?;
                    let side_id = bidder.rem_euclid(row_count);
                    let label = side_map
                        .get(&side_id)?
                        .ok_or_else(|| format!("missing side input for key {side_id}"))?;
                    let enriched = format!("{label}:{}:{}", interval.start, interval.end);
                    bids.into_iter().try_for_each(|bid| {
                        let bid = bid?;
                        // Every bid in the session receives its own copy of the formatted session label.
                        ctx.emit(Bid {
                            extra: enriched.clone(),
                            ..bid
                        })
                    })
                },
            )
    }
}

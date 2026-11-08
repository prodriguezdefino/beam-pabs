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

//! Query 11 (`USER_SESSIONS`): bidder session counts capped by a pane element-count trigger.

use std::time::Duration;

use beam::prelude::*;

use super::just_bids;
use crate::config::NexmarkConfig;
use crate::model::{BidsPerSession, Event};

/// Groups bids by bidder into session windows with repeated count triggers and discarding panes.
#[derive(Clone, Copy, Debug)]
pub struct Query11 {
    window_size_sec: i64,
    max_log_events: i32,
    occasional_delay_sec: i64,
}

impl Query11 {
    /// Creates a user-sessions transform with `window_size_sec` gap and `max_log_events` pane cap.
    pub fn new(window_size_sec: i64, max_log_events: i32) -> Self {
        Self {
            window_size_sec: window_size_sec.max(1),
            max_log_events: max_log_events.max(1),
            occasional_delay_sec: 0,
        }
    }

    /// Sets `occasional_delay_sec` (`occasional_delay_sec / 2` becomes allowed lateness).
    pub fn with_occasional_delay_sec(mut self, occasional_delay_sec: i64) -> Self {
        self.occasional_delay_sec = occasional_delay_sec.max(0);
        self
    }

    /// Creates a user-sessions transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.window_size_sec, config.max_log_events)
            .with_occasional_delay_sec(config.occasional_delay_sec)
    }
}

impl PTransform<PCollection<Event>> for Query11 {
    type Output = PCollection<BidsPerSession>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<BidsPerSession> {
        let gap = Duration::from_secs(u64::try_from(self.window_size_sec).unwrap_or(1));
        let lateness =
            Duration::from_secs(u64::try_from((self.occasional_delay_sec / 2).max(0)).unwrap_or(0));

        just_bids(input)
            .map("Rekey", |bid| bid.bidder)
            .apply(
                "Sessionize",
                WindowInto::new(Sessions::with_gap_duration(gap))
                    .with_trigger(Trigger::repeatedly(Trigger::after_count(
                        self.max_log_events,
                    )))
                    .with_accumulation_mode(AccumulationMode::Discarding)
                    .with_allowed_lateness(lateness),
            )
            .count_per_element("CountPerBidder")
            .map("ToResult", |(person_id, bids_per_session)| BidsPerSession {
                person_id,
                bids_per_session,
            })
    }
}

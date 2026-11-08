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

//! Query 12 (`PROCESSING_TIME_WINDOWS`): bidder counts emitted on processing-time triggers.

use std::time::Duration;

use beam::prelude::*;

use super::just_bids;
use crate::config::NexmarkConfig;
use crate::model::{BidsPerSession, Event};

/// Counts bids per bidder in the global window under a repeated processing-time trigger.
#[derive(Clone, Copy, Debug)]
pub struct Query12 {
    window_size_sec: i64,
}

impl Query12 {
    /// Creates a processing-time window transform firing `window_size_sec` after each pane's first element.
    pub fn new(window_size_sec: i64) -> Self {
        Self {
            window_size_sec: window_size_sec.max(1),
        }
    }

    /// Creates a processing-time window transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.window_size_sec)
    }
}

impl PTransform<PCollection<Event>> for Query12 {
    type Output = PCollection<BidsPerSession>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<BidsPerSession> {
        let delay = Duration::from_secs(u64::try_from(self.window_size_sec).unwrap_or(1));

        just_bids(input)
            .map("ExtractBidder", |bid| bid.bidder)
            .apply(
                "WindowProcessingTime",
                WindowInto::new(GlobalWindows)
                    .with_trigger(Trigger::repeatedly(Trigger::after_processing_time(delay)))
                    .with_accumulation_mode(AccumulationMode::Discarding)
                    .with_allowed_lateness(Duration::ZERO),
            )
            .count_per_element("CountPerBidder")
            .map("ToResult", |(person_id, bids_per_session)| BidsPerSession {
                person_id,
                bids_per_session,
            })
    }
}

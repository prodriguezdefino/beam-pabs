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

//! `BOUNDED_SIDE_INPUT_JOIN`: stream enrichment against a bounded multimap side input.

use beam::prelude::*;

use super::{just_bids, side_input_rows};
use crate::config::NexmarkConfig;
use crate::model::{Bid, Event};

/// Enriches each [`Bid`] `extra` field from a bounded `(id, label)` multimap side input.
#[derive(Clone, Copy, Debug)]
pub struct BoundedSideInputJoin {
    side_input_row_count: i32,
}

impl BoundedSideInputJoin {
    /// Creates a bounded side-input join over `side_input_row_count` synthetic lookup rows.
    pub fn new(side_input_row_count: i32) -> Self {
        Self {
            side_input_row_count: side_input_row_count.max(1),
        }
    }

    /// Creates a bounded side-input join configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.side_input_row_count)
    }
}

impl PTransform<PCollection<Event>> for BoundedSideInputJoin {
    type Output = PCollection<Bid>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<Bid> {
        let row_count = i64::from(self.side_input_row_count);
        let side_input = side_input_rows(input.pipeline(), self.side_input_row_count);

        just_bids(input).map_with_side_inputs(
            "JoinToFiles",
            side_input.as_multimap(),
            move |bid, side_map| {
                let side_id = bid.bidder.rem_euclid(row_count);
                let extra = side_map
                    .get(&side_id)?
                    .ok_or_else(|| format!("missing side input for key {side_id}"))?;
                Ok(Bid { extra, ..bid })
            },
        )
    }
}

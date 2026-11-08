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

//! Reference model for [`BoundedSideInputJoin`](crate::queries::BoundedSideInputJoin).

use beam::Instant;

use super::bid_stream;
use crate::config::NexmarkConfig;
use crate::model::Bid;

/// Computes expected `(Bid, Instant)` outputs for `BOUNDED_SIDE_INPUT_JOIN`.
pub fn bounded_side_input_join(config: &NexmarkConfig) -> Vec<(Bid, Instant)> {
    let row_count = i64::from(config.side_input_row_count.max(1));
    bid_stream(config)
        .map(|(bid, ts)| {
            let extra = bid.bidder.rem_euclid(row_count).to_string();
            (Bid { extra, ..bid }, ts)
        })
        .collect()
}

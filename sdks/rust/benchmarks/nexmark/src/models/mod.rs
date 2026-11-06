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

//! Pure reference models for checking Nexmark query outputs.

mod bounded_side_input_join;
mod query0;
mod query1;
mod query2;
mod query3;
mod query5;
mod query7;
mod query8;
mod session_side_input_join;

use beam::Instant;
use beam::coders::IntervalWindow;
use beam::windowing::{BoundedWindow, WindowFn};

pub use self::bounded_side_input_join::bounded_side_input_join;
pub use self::query0::query0;
pub use self::query1::query1;
pub use self::query2::query2;
pub use self::query3::query3;
pub use self::query5::query5;
pub use self::query7::query7;
pub use self::query8::query8;
pub use self::session_side_input_join::session_side_input_join;
use crate::config::{BASE_TIME_MS, NexmarkConfig};
use crate::generator::{Generator, GeneratorConfig};
use crate::model::{Bid, Event};

/// Synthesizes the reference `(Event, Instant)` stream for `config` in wallclock order.
pub fn standard_event_stream(config: &NexmarkConfig) -> Vec<(Event, Instant)> {
    let root = GeneratorConfig::from_nexmark_config(config);
    let num_gens = config.num_event_generators.max(1) as usize;
    if num_gens == 1 {
        return Generator::new(root)
            .map(|ne| (ne.event, Instant::from_millis(ne.event_timestamp)))
            .collect();
    }

    let mut tagged: Vec<_> = root
        .split(num_gens)
        .into_iter()
        .enumerate()
        .flat_map(|(sub_idx, sub)| {
            Generator::with_state(sub, 0, BASE_TIME_MS)
                .map(move |ne| (ne.wallclock_timestamp, sub_idx, ne))
        })
        .collect();
    tagged.sort_by_key(|&(wallclock, sub_idx, _)| (wallclock, sub_idx));
    tagged
        .into_iter()
        .map(|(_, _, ne)| (ne.event, Instant::from_millis(ne.event_timestamp)))
        .collect()
}

pub(crate) fn bid_stream(config: &NexmarkConfig) -> impl Iterator<Item = (Bid, Instant)> {
    standard_event_stream(config)
        .into_iter()
        .filter_map(|(event, ts)| match event {
            Event::Bid(bid) => Some((bid, ts)),
            Event::Person(_) | Event::Auction(_) => None,
        })
}

pub(crate) fn assign_intervals(
    window_fn: &impl WindowFn,
    ts: Instant,
) -> impl Iterator<Item = IntervalWindow> {
    window_fn
        .assign_windows(ts)
        .into_iter()
        .filter_map(|window| match window {
            BoundedWindow::Interval(interval) => Some(interval),
            BoundedWindow::Global(_) => None,
        })
}

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

//! Reference model for [`Query8`](crate::queries::Query8).

use std::collections::BTreeMap;
use std::time::Duration;

use beam::Instant;
use beam::coders::IntervalWindow;
use beam::windowing::FixedWindows;

use super::{assign_intervals, standard_event_stream};
use crate::config::NexmarkConfig;
use crate::model::{Auction, Event, IdNameReserve, Person};

/// Computes expected `(IdNameReserve, Instant)` outputs for Query 8 (`MONITOR_NEW_USERS`).
pub fn query8(config: &NexmarkConfig) -> Vec<(IdNameReserve, Instant)> {
    let size = Duration::from_secs(u64::try_from(config.window_size_sec.max(1)).unwrap_or(1));
    let windows = FixedWindows::of(size);

    let (persons, auctions) = standard_event_stream(config).into_iter().fold(
        (
            BTreeMap::<(IntervalWindow, i64), Vec<Person>>::new(),
            BTreeMap::<(IntervalWindow, i64), Vec<Auction>>::new(),
        ),
        |(mut persons, mut auctions), (event, ts)| {
            match event {
                Event::Person(person) => {
                    if let Some(window) = assign_intervals(&windows, ts).next() {
                        persons.entry((window, person.id)).or_default().push(person);
                    }
                }
                Event::Auction(auction) => {
                    if let Some(window) = assign_intervals(&windows, ts).next() {
                        auctions
                            .entry((window, auction.seller))
                            .or_default()
                            .push(auction);
                    }
                }
                Event::Bid(_) => {}
            }
            (persons, auctions)
        },
    );

    persons
        .into_iter()
        .flat_map(|((window, seller_id), sellers)| {
            let emit_ts = window.max_timestamp();
            let seller_auctions = auctions.get(&(window, seller_id));
            sellers.into_iter().flat_map(move |person| {
                seller_auctions.into_iter().flatten().map(move |auction| {
                    // One seller can open multiple auctions inside the same fixed window.
                    (
                        IdNameReserve {
                            id: person.id,
                            name: person.name.clone(),
                            reserve: auction.reserve,
                        },
                        emit_ts,
                    )
                })
            })
        })
        .collect()
}

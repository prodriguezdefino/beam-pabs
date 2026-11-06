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

//! Reference model for [`Query3`](crate::queries::Query3).

use std::collections::{HashMap, hash_map};

use beam::Instant;

use super::standard_event_stream;
use crate::config::NexmarkConfig;
use crate::model::{Auction, Event, NameCityStateId, Person};
use crate::queries::query3::{is_target_category, is_target_state, project_match};

/// Computes expected `(NameCityStateId, Instant)` outputs for Query 3 (`LOCAL_ITEM_SUGGESTION`).
pub fn query3(config: &NexmarkConfig) -> Vec<(NameCityStateId, Instant)> {
    let (_, _, outputs) = standard_event_stream(config).into_iter().fold(
        (
            HashMap::<i64, Person>::new(),
            HashMap::<i64, Vec<Auction>>::new(),
            Vec::new(),
        ),
        |(mut persons, mut pending_auctions, mut results), (event, ts)| {
            match event {
                Event::Person(person) if is_target_state(&person.state) => {
                    if let hash_map::Entry::Vacant(slot) = persons.entry(person.id) {
                        if let Some(waiting) = pending_auctions.remove(&person.id) {
                            results.extend(waiting.iter().map(|auction| {
                                (project_match(&person, auction), auction.date_time.max(ts))
                            }));
                        }
                        slot.insert(person);
                    }
                }
                Event::Auction(auction) if is_target_category(auction.category) => {
                    match persons.get(&auction.seller) {
                        Some(person) => {
                            results
                                .push((project_match(person, &auction), ts.max(person.date_time)));
                        }
                        None => pending_auctions
                            .entry(auction.seller)
                            .or_default()
                            .push(auction),
                    }
                }
                Event::Person(_) | Event::Auction(_) | Event::Bid(_) => {}
            }
            (persons, pending_auctions, results)
        },
    );
    outputs
}

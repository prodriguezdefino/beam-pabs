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

//! Query 3 (`LOCAL_ITEM_SUGGESTION`): stateful incremental join of sellers and auctions.

use std::time::Duration;

use beam::prelude::*;
use serde::{Deserialize, Serialize};

use super::{just_auctions, just_persons};
use crate::config::NexmarkConfig;
use crate::model::{Auction, Event, NameCityStateId, Person};

/// Joins category-10 [`Auction`] records with sellers in `OR`, `ID`, or `CA` using keyed state.
#[derive(Clone, Copy, Debug)]
pub struct Query3 {
    max_auctions_waiting_time: i32,
}

impl Query3 {
    /// Creates a stateful join expiring per-seller state after `max_auctions_waiting_time` seconds.
    pub fn new(max_auctions_waiting_time: i32) -> Self {
        Self {
            max_auctions_waiting_time: max_auctions_waiting_time.max(0),
        }
    }

    /// Creates a stateful join configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.max_auctions_waiting_time)
    }
}

impl PTransform<PCollection<Event>> for Query3 {
    type Output = PCollection<NameCityStateId>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<NameCityStateId> {
        let auctions = just_auctions(input)
            .filter("InCategory", |auction| is_target_category(auction.category))
            .map("EventByAuctionSeller", |auction| {
                (auction.seller, Event::Auction(auction))
            });
        let persons = just_persons(input)
            .filter("InState", |person| is_target_state(&person.state))
            .map("EventByPersonId", |person| {
                (person.id, Event::Person(person))
            });

        auctions.flatten("MergeStreams", &persons).apply(
            "Join",
            ParDo::new(JoinDoFn {
                max_auctions_waiting_time: self.max_auctions_waiting_time,
            }),
        )
    }
}

pub(crate) const fn is_target_category(category: i64) -> bool {
    category == 10
}

pub(crate) fn is_target_state(state: &str) -> bool {
    matches!(state, "OR" | "ID" | "CA")
}

pub(crate) fn project_match(person: &Person, auction: &Auction) -> NameCityStateId {
    // One seller matches arbitrarily many auctions, so each output owns its projected strings.
    NameCityStateId {
        name: person.name.clone(),
        city: person.city.clone(),
        state: person.state.clone(),
        id: auction.id,
    }
}

#[derive(State)]
struct JoinState {
    person: ValueState<Person>,
    auctions: BagState<Auction>,
}

#[derive(Timers)]
enum JoinTimers {
    #[timer(event_time)]
    StateExpiring,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Register)]
struct JoinDoFn {
    max_auctions_waiting_time: i32,
}

impl StatefulDoFn for JoinDoFn {
    type Key = i64;
    type Value = Event;
    type Out = NameCityStateId;
    type State = JoinState;
    type Timer = JoinTimers;

    fn process_element(
        &mut self,
        _key: &i64,
        value: Event,
        state: &mut JoinState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let existing_person = state.person.read()?;
        let event_time = match value {
            Event::Person(person) => {
                let ts = person.date_time;
                match existing_person {
                    None => {
                        Metrics::counter("Query3", "newPerson").inc();
                        let pending = state.auctions.read()?;
                        for auction in &pending {
                            Metrics::counter("Query3", "oldNewOutput").inc();
                            ctx.output(project_match(&person, auction))
                                .at(auction.date_time.max(ts))
                                .emit()?;
                        }
                        if !pending.is_empty() {
                            state.auctions.clear()?;
                        }
                        state.person.write(person)?;
                    }
                    Some(_) => {
                        Metrics::counter("Query3", "fatal").inc();
                    }
                }
                Some(ts)
            }
            Event::Auction(auction) => {
                let ts = auction.date_time;
                Metrics::counter("Query3", "newAuction").inc();
                match existing_person {
                    Some(person) => {
                        Metrics::counter("Query3", "newOldOutput").inc();
                        ctx.output(project_match(&person, &auction))
                            .at(ts.max(person.date_time))
                            .emit()?;
                    }
                    None => state.auctions.append(auction)?,
                }
                Some(ts)
            }
            Event::Bid(_) => {
                Metrics::counter("Query3", "fatal").inc();
                None
            }
        };

        if let Some(ts) = event_time {
            let wait =
                Duration::from_secs(u64::try_from(self.max_auctions_waiting_time).unwrap_or(0));
            ctx.timer(JoinTimers::StateExpiring).at(ts + wait)?;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer: JoinTimers,
        _key: &i64,
        state: &mut JoinState,
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        match timer {
            JoinTimers::StateExpiring => {
                state.person.clear()?;
                state.auctions.clear()
            }
        }
    }
}

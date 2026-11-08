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

//! Deterministic synthesis of [`Person`] events and person identifiers.

use beam::Instant;

use super::strings::{next_extra, next_long, next_string};
use super::{GeneratorConfig, JavaRandom};
use crate::model::Person;

/// Forward person ID lead so occasional bids reference future people.
const PERSON_ID_LEAD: i64 = 10;

const US_STATES: &[&str] = &["AZ", "CA", "ID", "OR", "WA", "WY"];
const US_CITIES: &[&str] = &[
    "Phoenix",
    "Los Angeles",
    "San Francisco",
    "Boise",
    "Portland",
    "Bend",
    "Redmond",
    "Seattle",
    "Kent",
    "Cheyenne",
];
const FIRST_NAMES: &[&str] = &[
    "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
];
const LAST_NAMES: &[&str] = &[
    "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris",
];

/// Synthesizes the [`Person`] for `event_id` at `timestamp_ms`.
pub(crate) fn next_person(
    event_id: i64,
    random: &mut JavaRandom,
    timestamp_ms: i64,
    config: &GeneratorConfig,
) -> Person {
    let id = last_base0_person_id(event_id) + GeneratorConfig::FIRST_PERSON_ID;
    let name = format!(
        "{} {}",
        choose(FIRST_NAMES, random),
        choose(LAST_NAMES, random)
    );
    let email_address = format!("{}@{}.com", next_string(random, 7), next_string(random, 5));
    let [a, b, c, d] = [(); 4].map(|()| random.next_int_bound(10_000));
    let credit_card = format!("{a:04} {b:04} {c:04} {d:04}");
    let city = choose(US_CITIES, random).to_owned();
    let state = choose(US_STATES, random).to_owned();
    let current_size = 8
        + (name.len() + email_address.len() + credit_card.len() + city.len() + state.len()) as i32;
    let extra = next_extra(
        random,
        current_size,
        config.configuration.avg_person_byte_size,
    );
    Person {
        id,
        name,
        email_address,
        credit_card,
        city,
        state,
        date_time: Instant::from_millis(timestamp_ms),
        extra,
    }
}

/// Samples a zero-based person ID for an auction seller or bid bidder.
pub(crate) fn next_base0_person_id(
    event_id: i64,
    random: &mut JavaRandom,
    config: &GeneratorConfig,
) -> i64 {
    let num_people = last_base0_person_id(event_id) + 1;
    let active_people = num_people.min(i64::from(config.configuration.num_active_people));
    num_people - active_people + next_long(random, active_people + PERSON_ID_LEAD)
}

/// Returns the last zero-based person ID generated at or before `event_id`.
pub(crate) const fn last_base0_person_id(event_id: i64) -> i64 {
    let epoch = event_id / GeneratorConfig::PROPORTION_DENOMINATOR;
    let rem = event_id % GeneratorConfig::PROPORTION_DENOMINATOR;
    let offset = if rem < GeneratorConfig::PERSON_PROPORTION {
        rem
    } else {
        GeneratorConfig::PERSON_PROPORTION - 1
    };
    epoch * GeneratorConfig::PERSON_PROPORTION + offset
}

fn choose<'a>(items: &'a [&'a str], random: &mut JavaRandom) -> &'a str {
    items[random.next_int_bound(items.len() as i32) as usize]
}

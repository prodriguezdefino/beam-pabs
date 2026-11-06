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

use std::fmt::Write as _;

use beam::Instant;
use beam::coders::{Coder, Context};
use nexmark::config::{BASE_TIME_MS, NexmarkConfig, RateShape, RateUnit};
use nexmark::generator::{Generator, GeneratorConfig, JavaRandom};
use nexmark::model::{Auction, Bid, Event, EventCoder, KnownSize, Person};

const RANDOM_TSV: &str = include_str!("fixtures/random_cases.tsv");
const DEFAULT_60_TSV: &str = include_str!("fixtures/default_first_60.tsv");
const SINE_45_TSV: &str = include_str!("fixtures/sine_out_of_order_45.tsv");

struct FixtureEvent<'a> {
    wallclock_offset_ms: i64,
    event_timestamp_ms: i64,
    watermark_ms: i64,
    size_in_bytes: u64,
    outer_hex: &'a str,
    event: Event,
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

fn data_lines(tsv: &str) -> impl Iterator<Item = &str> {
    tsv.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
}

fn parse_event_line(line: &str) -> FixtureEvent<'_> {
    let cols: Vec<&str> = line.split('\t').collect();
    let wallclock_offset_ms = cols[0].parse().expect("wallclock_offset_ms");
    let event_timestamp_ms = cols[1].parse().expect("event_timestamp_ms");
    let watermark_ms = cols[2].parse().expect("watermark_ms");
    let size_in_bytes = cols[3].parse().expect("size_in_bytes");
    let outer_hex = cols[4];
    let event = match cols[5] {
        "P" => Event::Person(Person {
            id: cols[6].parse().expect("person id"),
            name: cols[7].to_owned(),
            email_address: cols[8].to_owned(),
            credit_card: cols[9].to_owned(),
            city: cols[10].to_owned(),
            state: cols[11].to_owned(),
            date_time: Instant::from_millis(cols[12].parse().expect("person ts")),
            extra: cols[13].to_owned(),
        }),
        "A" => Event::Auction(Auction {
            id: cols[6].parse().expect("auction id"),
            item_name: cols[7].to_owned(),
            description: cols[8].to_owned(),
            initial_bid: cols[9].parse().expect("initial_bid"),
            reserve: cols[10].parse().expect("reserve"),
            date_time: Instant::from_millis(cols[11].parse().expect("auction ts")),
            expires: Instant::from_millis(cols[12].parse().expect("expires ts")),
            seller: cols[13].parse().expect("seller"),
            category: cols[14].parse().expect("category"),
            extra: cols[15].to_owned(),
        }),
        "B" => Event::Bid(Bid {
            auction: cols[6].parse().expect("bid auction"),
            bidder: cols[7].parse().expect("bidder"),
            price: cols[8].parse().expect("price"),
            date_time: Instant::from_millis(cols[9].parse().expect("bid ts")),
            extra: cols[10].to_owned(),
        }),
        other => panic!("unknown fixture event tag: {other}"),
    };
    FixtureEvent {
        wallclock_offset_ms,
        event_timestamp_ms,
        watermark_ms,
        size_in_bytes,
        outer_hex,
        event,
    }
}

fn assert_matches_fixture(config: GeneratorConfig, tsv: &str) {
    let wallclock_anchor = BASE_TIME_MS + 123_456;
    let expected: Vec<_> = data_lines(tsv).map(parse_event_line).collect();
    let actual: Vec<_> = Generator::with_state(config, 0, wallclock_anchor)
        .take(expected.len())
        .collect();
    assert_eq!(actual.len(), expected.len());

    let mut wire = Vec::new();
    for (idx, (got, want)) in actual.iter().zip(&expected).enumerate() {
        assert_eq!(
            got.wallclock_timestamp - wallclock_anchor,
            want.wallclock_offset_ms,
            "wallclock offset mismatch at index {idx}"
        );
        assert_eq!(got.event_timestamp, want.event_timestamp_ms, "index {idx}");
        assert_eq!(got.watermark, want.watermark_ms, "index {idx}");
        assert_eq!(got.event, want.event, "index {idx}");
        assert_eq!(
            got.event.estimated_size_bytes(),
            want.size_in_bytes,
            "index {idx}"
        );

        wire.clear();
        EventCoder
            .encode(&got.event, &mut wire, Context::WholeStream)
            .expect("encode event");
        assert_eq!(
            hex_encode(&wire),
            want.outer_hex,
            "wire mismatch at index {idx}"
        );
    }
}

#[test]
fn java_random_matches_reference_vectors() {
    for line in data_lines(RANDOM_TSV) {
        let cols: Vec<&str> = line.split('\t').collect();
        let seed: i64 = cols[0].parse().expect("seed");
        let mut rng = JavaRandom::new(seed);
        assert_eq!(
            rng.next_int(),
            cols[1].parse::<i32>().expect("next_int"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_int_bound(1),
            cols[2].parse::<i32>().expect("bound_1"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_int_bound(16),
            cols[3].parse::<i32>().expect("bound_16"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_int_bound(1 << 30),
            cols[4].parse::<i32>().expect("bound_pow30"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_int_bound(13),
            cols[5].parse::<i32>().expect("bound_13"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_int_bound(10_000),
            cols[6].parse::<i32>().expect("bound_10000"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_int_bound(i32::MAX - 1),
            cols[7].parse::<i32>().expect("bound_max_minus_1"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_long(),
            cols[8].parse::<i64>().expect("next_long"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_double().to_bits(),
            u64::from_str_radix(cols[9], 16).expect("next_double_bits"),
            "seed {seed}"
        );
        assert_eq!(
            rng.next_boolean(),
            cols[10].parse::<bool>().expect("next_bool"),
            "seed {seed}"
        );
    }

    let mut r1 = JavaRandom::new(42);
    let mut r2 = JavaRandom::new(0);
    r2.set_seed(42);
    let mut b1 = [0_u8; 7];
    let mut b2 = [0_u8; 7];
    r1.next_bytes(&mut b1);
    r2.next_bytes(&mut b2);
    assert_eq!(b1, b2);
}

#[test]
fn default_generator_matches_first_60_golden_events() {
    let def = NexmarkConfig::default();
    let cfg = GeneratorConfig::new(def, BASE_TIME_MS, 0, def.num_events, 0);
    assert_matches_fixture(cfg, DEFAULT_60_TSV);
}

#[test]
fn sine_out_of_order_generator_matches_golden_events() {
    let sine = NexmarkConfig {
        rate_shape: RateShape::Sine,
        first_event_rate: 2,
        next_event_rate: 6,
        rate_unit: RateUnit::PerSecond,
        rate_period_sec: 10,
        num_event_generators: 1,
        out_of_order_group_size: 10,
        num_events: 45,
        ..NexmarkConfig::default()
    };
    let cfg = GeneratorConfig::new(sine, BASE_TIME_MS, 0, 45, 0);
    assert_eq!(
        cfg.inter_event_delays_us(),
        &[
            500_000, 500_000, 333_333, 200_000, 166_667, 166_667, 166_667, 200_000, 333_333,
            500_000
        ]
    );
    assert_eq!(cfg.step_length_sec, 1);
    assert_eq!(cfg.epoch_period_ms, 9_497);
    assert_eq!(cfg.events_per_epoch, 37);

    assert_matches_fixture(cfg, SINE_45_TSV);
    assert_eq!(
        Generator::new(GeneratorConfig::new(
            NexmarkConfig::default(),
            BASE_TIME_MS,
            0,
            2,
            0
        ))
        .count(),
        2
    );
}

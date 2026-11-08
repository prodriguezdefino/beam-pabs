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

use nexmark::config::{BASE_TIME_MS, NexmarkConfig};
use nexmark::generator::{Generator, GeneratorConfig, NextEvent};
use nexmark::model::Event;

fn make_config(num_events: i64) -> GeneratorConfig {
    GeneratorConfig::new(NexmarkConfig::default(), BASE_TIME_MS, 0, num_events, 0)
}

fn event_and_ts(ne: NextEvent) -> (Event, i64) {
    (ne.event, ne.event_timestamp)
}

#[test]
fn split_at_event_id_preserves_exact_event_sequence() {
    let n = 55_729_i64;
    let initial_config = make_config(n);
    let expected: Vec<_> = Generator::new(initial_config).map(event_and_ts).collect();
    assert_eq!(expected.len() as i64, n);

    let mut gen0 = Generator::new(initial_config);
    let part0_head: Vec<_> = gen0.by_ref().take(5_000).map(event_and_ts).collect();

    let mut gen1 = Generator::new(gen0.split_at_event_id(9_000));
    let part0_mid: Vec<_> = gen0.by_ref().take(2_000).map(event_and_ts).collect();
    let part1_head: Vec<_> = gen1.by_ref().take(3_000).map(event_and_ts).collect();

    let gen2 = Generator::new(gen1.split_at_event_id(30_000));

    let actual: Vec<_> = part0_head
        .into_iter()
        .chain(part0_mid)
        .chain(gen0.map(event_and_ts))
        .chain(part1_head)
        .chain(gen1.map(event_and_ts))
        .chain(gen2.map(event_and_ts))
        .collect();

    assert_eq!(actual, expected);
}

#[test]
fn parallel_split_preserves_total_event_count_and_handles_small_ranges() {
    let initial_config = make_config(51_237);
    let expected = initial_config.stop_event_id() - initial_config.start_event_id();

    let sub_configs = initial_config.split(20);
    assert_eq!(sub_configs.len(), 20);
    let actual: usize = sub_configs
        .into_iter()
        .map(|cfg| Generator::new(cfg).count())
        .sum();
    assert_eq!(actual as i64, expected);

    let tiny_total: usize = make_config(3)
        .split(10)
        .into_iter()
        .map(|cfg| Generator::new(cfg).count())
        .sum();
    assert_eq!(tiny_total, 3);
}

#[test]
fn checkpoint_resumes_with_identical_remaining_events_and_wallclock() {
    let config = NexmarkConfig {
        num_events: 120,
        num_event_generators: 1,
        out_of_order_group_size: 8,
        ..NexmarkConfig::default()
    };
    let gen_cfg = GeneratorConfig::new(config, BASE_TIME_MS, 0, 120, 0);

    let expected_all: Vec<_> = Generator::with_state(gen_cfg, 0, BASE_TIME_MS + 50_000).collect();

    let mut first_half = Generator::with_state(gen_cfg, 0, BASE_TIME_MS + 50_000);
    let head: Vec<_> = first_half.by_ref().take(47).collect();
    let resumed = first_half.to_checkpoint().to_generator(gen_cfg);
    let actual_all: Vec<_> = head.into_iter().chain(resumed).collect();
    assert_eq!(actual_all, expected_all);

    let sample = expected_all[0].clone();
    let delayed = sample.clone().with_delay(3_000);
    assert_eq!(
        delayed.wallclock_timestamp,
        sample.wallclock_timestamp + 3_000
    );
    assert_eq!(delayed.event_timestamp, sample.event_timestamp);
    assert_eq!(delayed.watermark, sample.watermark);
    assert!(delayed.event.has_annotation("LATE"));
}

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

//! Tests for the accumulator table behind `CombinePerKey`'s partial combining.
//!
//! Eviction starts only above a 64 MiB budget, and pipeline output does not show it. So
//! these tests drive the policy directly through the hidden `combine_internals` handle.

use std::collections::BTreeMap;

use beam::coders::WindowedHeader;
use beam::transforms::Sum;
use beam::transforms::combine_internals::Table;

const WINDOW: &[u8] = b"window";

fn add(table: &mut Table<String, i64>, key: &str, value: i64) {
    table
        .add(
            &Sum,
            WINDOW,
            WindowedHeader::EMPTY,
            key.to_string(),
            value,
            0,
        )
        .expect("add succeeds");
}

fn evict(table: &mut Table<String, i64>) -> Vec<(String, i64)> {
    let mut evicted = Vec::new();
    table
        .evict_coldest(|key, accumulator| {
            evicted.push((key, accumulator.unwrap_or_default()));
            Ok(())
        })
        .expect("eviction succeeds");
    evicted
}

fn drain(table: Table<String, i64>) -> Vec<(String, i64)> {
    let mut drained = Vec::new();
    table
        .drain(|key, accumulator| {
            drained.push((key, accumulator.unwrap_or_default()));
            Ok(())
        })
        .expect("drain succeeds");
    drained
}

#[test]
fn eviction_removes_the_least_recently_used_tenth() {
    let mut table = Table::default();
    (0..100).for_each(|i| add(&mut table, &format!("cold-{i}"), 1));
    // Accessing the first ten keys marks them as most recently used.
    (0..10).for_each(|i| add(&mut table, &format!("cold-{i}"), 1));

    let evicted = evict(&mut table);

    assert_eq!(evicted.len(), 10);
    assert_eq!(table.len(), 90);
    let mut names: Vec<_> = evicted.into_iter().map(|(key, _)| key).collect();
    names.sort();
    let mut expected: Vec<_> = (10..20).map(|i| format!("cold-{i}")).collect();
    expected.sort();
    assert_eq!(names, expected);
}

#[test]
fn hot_keys_survive_repeated_evictions() {
    let mut table = Table::default();
    (0..1_000).for_each(|i| {
        add(&mut table, "hot", 1);
        add(&mut table, &format!("cold-{i}"), 1);
        if i % 100 == 99 {
            assert!(evict(&mut table).iter().all(|(key, _)| key != "hot"));
        }
    });
    let hot = drain(table).into_iter().find(|(key, _)| key == "hot");
    assert_eq!(hot, Some(("hot".to_string(), 1_000)));
}

#[test]
fn evicted_and_drained_partials_add_up_to_the_input() {
    let mut table = Table::default();
    let mut emitted = Vec::new();
    (0..5_000i64).for_each(|i| {
        add(&mut table, &format!("key-{}", i % 37), i);
        if i % 250 == 0 {
            emitted.extend(evict(&mut table));
        }
    });
    emitted.extend(drain(table));

    let totals = emitted
        .into_iter()
        .fold(BTreeMap::new(), |mut totals, (key, partial)| {
            *totals.entry(key).or_insert(0) += partial;
            totals
        });
    let expected = (0..5_000i64).fold(BTreeMap::new(), |mut totals, i| {
        *totals.entry(format!("key-{}", i % 37)).or_insert(0) += i;
        totals
    });
    assert_eq!(totals, expected);
}

#[test]
fn evicting_an_empty_table_emits_nothing() {
    let mut table = Table::<String, i64>::default();
    assert!(evict(&mut table).is_empty());
    assert_eq!(table.estimated_bytes(), 0);
}

#[test]
fn weight_estimate_tracks_key_size() {
    let mut small = Table::default();
    let mut large = Table::default();
    (0..100).for_each(|i| {
        add(&mut small, &format!("k{i}"), 1);
        add(&mut large, &format!("{i}-{}", "x".repeat(200)), 1);
    });
    assert!(small.estimated_bytes() > 0);
    // The 200 extra key bytes appear in the heap estimate. The slot part is the same.
    let extra = large.entry_bytes() - small.entry_bytes();
    assert!(
        (195..=210).contains(&extra),
        "extra bytes per entry: {extra}"
    );
}

#[test]
fn weight_estimate_tracks_accumulator_size() {
    // A Sum accumulator of 0 encodes in 1 byte. A negative accumulator encodes in 10 bytes.
    let entry_bytes = |value: i64| {
        let mut inserted = Table::default();
        (0..16).for_each(|i| add(&mut inserted, &format!("k{i}"), value));
        // One insert followed by size-preserving updates.
        let mut updated = Table::default();
        (0..16).for_each(|_| add(&mut updated, "k", value));
        (inserted.entry_bytes(), updated.entry_bytes())
    };
    let (small_inserted, small_updated) = entry_bytes(0);
    let (large_inserted, large_updated) = entry_bytes(-1);
    assert_eq!(large_inserted - small_inserted, 9, "measured on insert");
    assert_eq!(large_updated - small_updated, 9, "measured on update");
}

#[test]
fn weight_estimate_is_sampled_after_warm_up() {
    let mut table = Table::default();
    let unmeasured = table.entry_bytes();
    assert!(table.is_empty());
    add(&mut table, "k0", 1);
    assert!(!table.is_empty());
    assert!(
        table.entry_bytes() > unmeasured,
        "the first entry is measured"
    );

    // Sixteen warm-up samples, then sample once every 64 operations.
    (1..16).for_each(|i| add(&mut table, &format!("k{i}"), 1));
    let warmed = table.entry_bytes();
    let long_key = |i: usize| format!("{i}-{}", "x".repeat(200));
    (0..63).for_each(|i| add(&mut table, &long_key(i), 1));
    assert_eq!(table.entry_bytes(), warmed, "not sampled between periods");
    add(&mut table, &long_key(63), 1);
    assert!(
        table.entry_bytes() > warmed,
        "the 64th operation is sampled"
    );
}

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

//! Tests for dynamic work rebalancing: dividing an in-flight bundle into the work the SDK
//! keeps and the residual it gives back to the runner.
//!
//! `index` is the element in process; the harness advances it before sending the element
//! downstream. A split must land strictly after it, or the runner reschedules work in progress.

use harness::bundle_processor::{SplitState, compute_split};

/// A bundle that has claimed elements `0..=index`, with no split agreed yet.
fn at(index: i64) -> SplitState {
    SplitState {
        index,
        stop_index: i64::MAX,
    }
}

/// The (last primary, first residual) pair, for terser assertions.
fn split(state: &SplitState, total: i64, fraction: f64) -> Option<(i64, i64)> {
    compute_split(state, total, fraction, &[])
        .map(|point| (point.last_primary, point.first_residual))
}

#[test]
fn a_fresh_bundle_starts_before_the_first_element() {
    let state = SplitState::default();
    assert_eq!(state.read_index(), -1);
    // With nothing in flight the whole element counts as remaining (progress 1.0), so
    // a checkpoint keeps nothing: every element, starting at 0, goes back to the runner.
    assert_eq!(split(&state, 10, 0.0), Some((-1, 0)));
    // And a fresh bundle has no boundary yet, so its first claim succeeds.
    let mut state = state;
    assert!(state.begin_element());
    assert_eq!(state.read_index(), 0);
}

#[test]
fn claiming_advances_the_index_until_the_agreed_boundary() {
    // The final read index is one past the last element, so it doubles as the count the
    // runner compares against the elements it sent.
    for claims in [0, 3] {
        let mut state = SplitState::default();
        for index in 0..claims {
            assert!(state.begin_element());
            assert_eq!(state.read_index(), index, "claim {index}");
        }
        assert_eq!(state.finish(), claims);
    }

    // A split agreed at first_residual = 3 while element 0 is in flight.
    let mut state = SplitState {
        index: 0,
        stop_index: 3,
    };
    assert!(state.begin_element(), "element 1 is primary");
    assert!(state.begin_element(), "element 2 is primary");
    assert_eq!(state.read_index(), 2);
    assert!(!state.begin_element(), "element 3 is the first residual");
    assert!(!state.begin_element(), "refusal is sticky");
    assert_eq!(state.read_index(), 2, "a refused claim must not advance");
    // The final read index then equals first_residual, which runners cross-check.
    assert_eq!(state.finish(), 3);
}

#[test]
fn a_checkpoint_before_the_first_element_claims_nothing() {
    let mut state = SplitState::default();
    let point = compute_split(&state, 10, 0.0, &[]).expect("a checkpoint is possible");
    state.stop_index = point.first_residual;
    assert!(!state.begin_element());
    assert_eq!(state.finish(), 0);
}

#[test]
fn a_split_lands_after_the_element_in_flight() {
    /// (case, state, estimated total, fraction, allowed points, (last primary, first residual)).
    type Case = (
        &'static str,
        SplitState,
        i64,
        f64,
        &'static [i64],
        (i64, i64),
    );
    let granted_at_20 = SplitState {
        index: 5,
        stop_index: 20,
    };
    let cases: [Case; 6] = [
        // The element at index 10 is being processed, so 11 is the earliest boundary.
        ("checkpoint", at(10), 100, 0.0, &[], (10, 11)),
        // 89.5 elements remain (the in-flight one counts as half done): 10 + round(0.5 + 44.75).
        ("half split", at(10), 100, 0.5, &[], (54, 55)),
        // Index 99 is the last known element; the residual is empty unless more data arrives.
        ("end of the known channel", at(99), 100, 0.5, &[], (99, 100)),
        // The estimate (100) is measured against the granted boundary (20).
        (
            "past a granted boundary",
            granted_at_20,
            100,
            0.0,
            &[],
            (5, 6),
        ),
        // 55 is not allowed; 50 is closer than 75 and still ahead of index 10.
        (
            "moves to an allowed point",
            at(10),
            100,
            0.5,
            &[0, 25, 50, 75],
            (49, 50),
        ),
        (
            "an exactly allowed point",
            at(10),
            100,
            0.0,
            &[11, 50],
            (10, 11),
        ),
    ];
    for (case, state, total, fraction, allowed, expected) in cases {
        let got = compute_split(&state, total, fraction, allowed)
            .map(|point| (point.last_primary, point.first_residual));
        assert_eq!(got, Some(expected), "{case}");
    }

    // Whatever the fraction, the runner must not reschedule work already underway.
    for fraction in [0.0, 0.1, 0.25, 0.5, 0.9] {
        let (_, first_residual) = split(&at(10), 100, fraction)
            .unwrap_or_else(|| panic!("expected a split at fraction {fraction}"));
        assert!(
            first_residual > 10,
            "fraction {fraction} produced residual {first_residual}, which reclaims the in-flight element"
        );
    }
}

#[test]
fn no_split_once_the_channel_is_closed() {
    let mut state = SplitState::default();
    state.begin_element();
    state.finish();

    assert_eq!(split(&state, 100, 0.0), None);
}

#[test]
fn an_understated_estimate_falls_back_to_what_we_have_seen() {
    // The runner estimates 5 elements but the bundle is at index 40, so the split ignores the
    // estimate and lands just after index 40.
    assert_eq!(split(&at(40), 5, 0.5), Some((40, 41)));
}

#[test]
fn a_later_split_never_moves_the_boundary_outwards() {
    // A split was granted at 20. A second request cannot move the residual later.
    let state = SplitState {
        index: 5,
        stop_index: 20,
    };

    // The estimate is clamped to the granted boundary (20), so 14.5 elements remain;
    // keeping 90% of that lands at 5 + round(0.5 + 13.05) = 19, inside the boundary.
    assert_eq!(split(&state, 100, 0.9), Some((18, 19)));
    // Keeping everything would land on the boundary itself, which is no split at all.
    assert_eq!(split(&state, 100, 1.0), None);
}

#[test]
fn no_split_when_every_allowed_point_is_behind_us() {
    // Every allowed point is behind the element in flight, so there is no valid split.
    assert_eq!(compute_split(&at(80), 100, 0.0, &[0, 25, 50]), None);
}

/// The earlier point wins only if strictly closer and strictly ahead of the element in flight.
#[test]
fn split_snaps_to_the_nearest_allowed_point() {
    /// (case name, fraction, allowed points, expected (last primary, first residual)).
    type Case = (&'static str, f64, &'static [i64], Option<(i64, i64)>);
    // At index 10 of 100, a half split wants 55 and a checkpoint wants 11.
    let cases: [Case; 5] = [
        ("next is closer", 0.5, &[25, 60], Some((59, 60))),
        ("a tie goes to next", 0.5, &[50, 60], Some((59, 60))),
        ("prev is closer", 0.5, &[50, 75], Some((49, 50))),
        (
            "prev is the element in flight",
            0.0,
            &[10, 20],
            Some((19, 20)),
        ),
        ("past every point", 0.5, &[20, 30], Some((29, 30))),
    ];
    for (name, fraction, allowed, expected) in cases {
        let got = compute_split(&at(10), 100, fraction, allowed)
            .map(|point| (point.last_primary, point.first_residual));
        assert_eq!(got, expected, "{name}: allowed {allowed:?}");
    }
}

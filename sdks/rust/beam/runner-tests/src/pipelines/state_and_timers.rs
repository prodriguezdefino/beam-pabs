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

//! User state, timers, and bundle-scoped behaviour.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use rust_decimal::Decimal;

use crate::dofns::*;

/// Validates Stateful DoFn execution: `ValueState`, `BagState`, and
/// a `#[derive(Timers)]` event-time timer with `on_timer`.
pub fn build_stateful_pardo(p: &TestPipeline) {
    let events = p.apply(
        "ScoreEvents",
        Create::new(vec![
            ("red".to_string(), ("alice".to_string(), 60i64)),
            ("red".to_string(), ("bob".to_string(), 50i64)),
            ("blue".to_string(), ("carol".to_string(), 40i64)),
        ]),
    );

    let milestones = events.apply(
        "StatefulTeamMilestone",
        ParDo::new(StatefulTeamMilestoneDoFn),
    );

    passert::that("AssertMilestones", &milestones).contains_in_any_order(
        [
            "milestone:red:110:alice+bob",
            "timer_flush:blue:40",
            "timer_flush:red:110",
        ]
        .map(String::from),
    );
}

/// Validates `CombiningState` with a `Sum`, a `Mean` (tuple accumulator), a `Max` and a
/// `Decimal` sum per key.
///
/// The adds run in `process_element`, and an event-time timer reads, clears and checks the
/// cells in a later bundle. So the runner stores the accumulators with the bag protocol and
/// the accumulator coders, and gives them back to the harness. `clear` then empties every
/// cell.
pub fn build_combining_state(p: &TestPipeline) {
    let readings = p.apply(
        "Readings",
        Create::new(vec![
            ("a".to_string(), (4i64, Decimal::new(150, 2))),
            ("a".to_string(), (-2, Decimal::new(-25, 2))),
            ("a".to_string(), (10, Decimal::new(1000, 2))),
            ("b".to_string(), (7, Decimal::new(3, 0))),
        ]),
    );
    let reports = readings.apply("CombiningState", ParDo::new(CombiningStateDoFn));
    passert::that("AssertReports", &reports).contains_in_any_order([
        ("a".to_string(), (3, 4.0, 10, Decimal::new(1125, 2), true)),
        ("b".to_string(), (1, 7.0, 7, Decimal::new(3, 0), true)),
    ]);
}

/// Validates that `CombiningState` is scoped to the window: under 10 s fixed windows, one
/// key has a cell in each window, and each window reports only its own readings.
pub fn build_windowed_combining_state(p: &TestPipeline) {
    let readings = p
        .apply(
            "Readings",
            Create::new(vec![
                ("a".to_string(), (4i64, Decimal::new(150, 2), 1_000i64)),
                ("a".to_string(), (6, Decimal::new(250, 2), 3_000)),
                ("a".to_string(), (20, Decimal::new(1, 0), 12_000)),
            ]),
        )
        .par_do_fn(
            "AssignTimestamps",
            |(key, (value, amount, millis)): (String, (i64, Decimal, i64)), ctx| {
                ctx.output((key, (value, amount)))
                    .at(Instant::from_millis(millis))
                    .emit()
            },
        )
        .apply(
            "Fixed",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        );
    let reports = readings.apply("WindowedCombiningState", ParDo::new(CombiningStateDoFn));
    passert::that_windowed("AssertReports", &reports).contains_in_any_order([
        (
            ("a".to_string(), (2, 5.0, 6, Decimal::new(400, 2), true)),
            IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000)),
        ),
        (
            ("a".to_string(), (1, 20.0, 20, Decimal::new(1, 0), true)),
            IntervalWindow::new(Instant::from_millis(10_000), Instant::from_millis(20_000)),
        ),
    ]);
}

/// Validates Stateful DoFn execution with `MapState` and `SetState`.
///
/// A timer callback reads both back, from a bundle that must fetch them from the runner
/// and not from the writing bundle's cache. Prism only: see [`PRISM_ONLY`](crate::PRISM_ONLY).
pub fn build_map_and_set_state(p: &TestPipeline) {
    let events = p.apply(
        "GameEvents",
        Create::new(vec![
            (
                "teamA".to_string(),
                ("alice".to_string(), (10i64, "badge1".to_string())),
            ),
            (
                "teamA".to_string(),
                ("bob".to_string(), (20i64, "badge2".to_string())),
            ),
            (
                "teamB".to_string(),
                ("carol".to_string(), (50i64, "badge3".to_string())),
            ),
            (
                "teamB".to_string(),
                ("carol".to_string(), (50i64, "badge3".to_string())),
            ),
            (
                "teamC".to_string(),
                ("dave".to_string(), (15i64, "badge4".to_string())),
            ),
            (
                "teamC".to_string(),
                ("dave".to_string(), (0i64, "CLEAR".to_string())),
            ),
            (
                "teamC".to_string(),
                ("eve".to_string(), (40i64, "badge5".to_string())),
            ),
        ]),
    );

    let result = events.apply("TrackPlayerInventory", ParDo::new(PlayerInventoryDoFn));

    passert::that("AssertResult", &result).contains_in_any_order(
        [
            "cleared:teamC",
            "entry:teamA:alice:10:false|keys:alice:badges:badge1",
            "entry:teamA:bob:20:false|keys:alice+bob:badges:badge1+badge2",
            "entry:teamB:carol:100:true|keys:carol:badges:badge3",
            "entry:teamB:carol:50:false|keys:carol:badges:badge3",
            "entry:teamC:dave:15:false|keys:dave:badges:badge4",
            "entry:teamC:eve:40:false|keys:eve:badges:badge5",
            "final:teamA|keys:alice+bob:badges:badge1+badge2",
            "final:teamB|keys:carol:badges:badge3",
            // dave and badge4 were cleared before eve arrived, so they must not
            // survive the bundle.
            "final:teamC|keys:eve:badges:badge5",
        ]
        .map(String::from),
    );
}

/// The players of [`build_map_and_set_state_clear`] and [`build_map_and_set_state_remove`].
fn team_scores(p: &TestPipeline) -> PCollection<(String, (String, i64))> {
    p.apply(
        "Scores",
        Create::new(vec![
            ("teamA".to_string(), ("alice".to_string(), 10i64)),
            ("teamA".to_string(), ("bob".to_string(), 20i64)),
            ("teamB".to_string(), ("carol".to_string(), 30i64)),
        ]),
    )
}

/// Validates that clearing a `MapState` or `SetState` reaches the runner.
///
/// A clear in the writing bundle only drops pending writes in the SDK. So
/// `process_element` writes the players, the `mutate` timer clears the committed state over
/// the state channel, and the `verify` timer must then find it empty.
pub fn build_map_and_set_state_clear(p: &TestPipeline) {
    let result = team_scores(p).apply(
        "ClearAcrossBundles",
        ParDo::new(MutateAcrossBundlesDoFn {
            mutation: StateMutation::Clear,
        }),
    );

    passert::that("AssertResult", &result).contains_in_any_order(
        [
            // Empty on both sides: the clear removed every committed entry.
            "after:teamA|keys::players:",
            "after:teamB|keys::players:",
            // Non-empty, which is what makes the clear above a real one.
            "before:teamA|keys:alice+bob",
            "before:teamB|keys:carol",
        ]
        .map(String::from),
    );
}

/// Validates that removing a key from a committed `MapState` or `SetState` reaches the
/// runner, and that removing an absent key changes nothing.
///
/// The `mutate` timer removes `alice` and `ghost` from every team. `alice` is present only in
/// teamA, and `ghost` is never present. The `verify` timer reads each player with `get` and
/// `contains`.
pub fn build_map_and_set_state_remove(p: &TestPipeline) {
    let result = team_scores(p).apply(
        "RemoveAcrossBundles",
        ParDo::new(MutateAcrossBundlesDoFn {
            mutation: StateMutation::Remove(&["alice", "ghost"]),
        }),
    );

    passert::that("AssertResult", &result).contains_in_any_order(
        [
            "before:teamA|keys:alice+bob",
            "before:teamB|keys:carol",
            "after:teamA|scores:bob=20:players:bob",
            "after:teamB|scores:carol=30:players:carol",
        ]
        .map(String::from),
    );
}

/// Validates DoFn bundle lifecycle hooks (`start_bundle` & `finish_bundle`) with
/// transactional batching.
///
/// Bundling is up to the runner, so the check is the sink's invariants: every record is
/// flushed once, no batch exceeds the batch size, only a bundle's last batch is short, and
/// every bundle commits the transaction that `start_bundle` opened.
pub fn build_bundle_lifecycle_batching(p: &TestPipeline) {
    const BATCH_SIZE: usize = 3;
    let records = (1..=7).map(|i| format!("item_{i}")).collect::<Vec<_>>();
    let expected = records.clone();

    let out = p.apply("Create", Create::new(records)).apply(
        "BatchSink",
        ParDo::new(TransactionalBatchSink {
            batch_size: BATCH_SIZE,
            buffer: Vec::new(),
            active_tx: None,
        }),
    );

    passert::that("AssertOut", &out).satisfies(move |lines: &[String]| {
        let batches: Vec<Vec<&str>> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("batch_flush:"))
            .map(|b| b.split('+').collect())
            .collect();
        let commits: Vec<&str> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("tx_commit:"))
            .collect();
        if batches.len() + commits.len() != lines.len() {
            return Err(format!("unexpected output: {lines:?}").into());
        }
        let mut flushed: Vec<&str> = batches.iter().flatten().copied().collect();
        flushed.sort_unstable();
        let mut wanted: Vec<&str> = expected.iter().map(String::as_str).collect();
        wanted.sort_unstable();
        if flushed != wanted {
            return Err(format!("every record must be flushed exactly once: {lines:?}").into());
        }
        if batches.iter().any(|b| b.len() > BATCH_SIZE) {
            return Err(format!("a batch exceeded {BATCH_SIZE} records: {lines:?}").into());
        }
        // Only a bundle's final flush may be short: at most one short batch per commit.
        let short = batches.iter().filter(|b| b.len() < BATCH_SIZE).count();
        if commits.is_empty() || short > commits.len() {
            return Err(format!(
                "{short} short batch(es) for {} bundle(s): {lines:?}",
                commits.len()
            )
            .into());
        }
        if commits.iter().any(|tx| *tx != "101") {
            return Err(format!("every bundle must commit its own transaction: {lines:?}").into());
        }
        Ok(())
    });
}

/// Validates per-window state isolation and timer delivery under `SlidingWindows`.
pub fn build_windowed_stateful_pardo(p: &TestPipeline) {
    let out = p
        .apply(
            "Events",
            Create::new(vec![
                ("k".to_string(), (10i64, 2_000i64)),
                ("k".to_string(), (20i64, 7_000i64)),
            ]),
        )
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .apply(
            "Sliding",
            WindowInto::new(
                SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5)),
            ),
        )
        .apply("WindowedState", ParDo::new(WindowedStatefulTimerDoFn));

    passert::that_windowed("AssertWindowedState", &out).contains_in_any_order([
        (
            ("k:-5000".to_string(), 10),
            IntervalWindow::new(Instant::from_millis(-5_000), Instant::from_millis(5_000)),
        ),
        (
            ("k:0".to_string(), 30),
            IntervalWindow::new(Instant::EPOCH, Instant::from_millis(10_000)),
        ),
        (
            ("k:5000".to_string(), 20),
            IntervalWindow::new(Instant::from_millis(5_000), Instant::from_millis(15_000)),
        ),
    ]);
}

/// Validates ordered list state operations: additions and sorted reads.
///
/// The events can arrive in any order. Each read is sorted by timestamp and has one event
/// more than the read before it, and the last read has every event.
pub fn build_ordered_list_state(p: &TestPipeline) {
    let events = p.apply(
        "DeviceEvents",
        Create::new(vec![
            ("sensorA".to_string(), (30i64, "event30".to_string())),
            ("sensorA".to_string(), (10i64, "event10".to_string())),
            ("sensorA".to_string(), (20i64, "event20".to_string())),
        ]),
    );

    let result = events.apply("ProcessOrderedEvents", ParDo::new(OrderedListLogDoFn));

    passert::that("AssertResult", &result).satisfies(|entries: &[String]| {
        let mut reads: Vec<Option<Vec<i64>>> = entries.iter().map(|e| ordered_read(e)).collect();
        reads.sort_by_key(|read| read.as_ref().map(Vec::len));
        let grows_sorted = reads.len() == 3
            && reads.iter().enumerate().all(|(index, read)| {
                read.as_ref().is_some_and(|read| {
                    read.len() == index + 1 && read.windows(2).all(|pair| pair[0] < pair[1])
                })
            })
            && reads.windows(2).all(|pair| match pair {
                [Some(before), Some(after)] => before.iter().all(|ts| after.contains(ts)),
                _ => false,
            })
            && reads.last() == Some(&Some(vec![10, 20, 30]));
        if grows_sorted {
            Ok(())
        } else {
            Err(format!("unexpected ordered list reads: {entries:?}").into())
        }
    });
}

/// The timestamps of an `entry:sensorA:<ts>=event<ts>,...` read, or `None` if an event is not
/// the one of its timestamp.
fn ordered_read(entry: &str) -> Option<Vec<i64>> {
    entry
        .strip_prefix("entry:sensorA:")?
        .split(',')
        .map(|event| {
            let (ts, value) = event.split_once('=')?;
            let ts: i64 = ts.parse().ok()?;
            (value == format!("event{ts}")).then_some(ts)
        })
        .collect()
}

/// Validates ordered list state clear operations. Prism only: see
/// [`PRISM_ONLY`](crate::PRISM_ONLY).
pub fn build_ordered_list_state_clear(p: &TestPipeline) {
    let events = p.apply(
        "DeviceEventsClear",
        Create::new(vec![
            ("sensorA".to_string(), (30i64, "event30".to_string())),
            ("sensorA".to_string(), (10i64, "event10".to_string())),
            ("sensorA".to_string(), (0i64, "CLEAR".to_string())),
        ]),
    );

    let result = events.apply("ProcessOrderedEventsClear", ParDo::new(OrderedListLogDoFn));

    passert::that("AssertResult", &result).contains_in_any_order(
        [
            "entry:sensorA:30=event30",
            "entry:sensorA:10=event10,30=event30",
            "cleared:sensorA",
        ]
        .map(String::from),
    );
}

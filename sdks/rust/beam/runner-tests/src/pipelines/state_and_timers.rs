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

use crate::dofns::*;

/// Validates Stateful DoFn execution (`ValueState`, `BagState`, and `TimerFamilySpec` with `on_timer`).
pub fn build_stateful_pardo(p: &TestPipeline) {
    let events = p.apply(Create::new(
        "ScoreEvents",
        vec![
            ("red".to_string(), ("alice".to_string(), 60i64)),
            ("red".to_string(), ("bob".to_string(), 50i64)),
            ("blue".to_string(), ("carol".to_string(), 40i64)),
        ],
    ));

    let total_spec = ValueStateSpec::<i64>::new("total_score");
    let scorers_spec = BagStateSpec::<String>::new("scorers");
    let timer_spec = TimerFamilySpec::event_time("flush_timer");

    let milestones = events.apply(
        ParDo::new(
            "StatefulTeamMilestone",
            StatefulTeamMilestoneDoFn {
                total_score: total_spec.clone(),
                scorers: scorers_spec.clone(),
                flush_timer: timer_spec.clone(),
            },
        )
        .with_state_spec(&total_spec)
        .with_state_spec(&scorers_spec)
        .with_timer_family(&timer_spec),
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

/// Validates Stateful DoFn execution with `MapState` and `SetState`.
///
/// A timer callback reads both back, from a bundle that must fetch them from the runner
/// and not from the writing bundle's cache.
pub fn build_map_and_set_state(p: &TestPipeline) {
    let events = p.apply(Create::new(
        "GameEvents",
        vec![
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
        ],
    ));

    let scores_spec = MapStateSpec::<String, i64>::new("player_points");
    let badges_spec = SetStateSpec::<String>::new("unlocked_achievements");
    let flush_spec = TimerFamilySpec::event_time("flush_inventory");

    let result = events.apply(
        ParDo::new(
            "TrackPlayerInventory",
            PlayerInventoryDoFn {
                player_points: scores_spec.clone(),
                unlocked_achievements: badges_spec.clone(),
                flush_timer: flush_spec.clone(),
            },
        )
        .with_state_spec(&scores_spec)
        .with_state_spec(&badges_spec)
        .with_timer_family(&flush_spec),
    );

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

/// Validates that clearing a `MapState` or `SetState` reaches the runner.
///
/// A clear in the writing bundle only drops pending writes in the SDK. So
/// `process_element` writes the players, the `clear` timer clears the committed state over
/// the state channel, and the `verify` timer must then find it empty.
pub fn build_map_and_set_state_clear(p: &TestPipeline) {
    const VERIFY_TIMER: &str = "verify_inventory";

    let events = p.apply(Create::new(
        "Scores",
        vec![
            ("teamA".to_string(), ("alice".to_string(), 10i64)),
            ("teamA".to_string(), ("bob".to_string(), 20i64)),
            ("teamB".to_string(), ("carol".to_string(), 30i64)),
        ],
    ));

    let scores_spec = MapStateSpec::<String, i64>::new("scores");
    let players_spec = SetStateSpec::<String>::new("players");
    let clear_spec = TimerFamilySpec::event_time(CLEAR_TIMER);
    let verify_spec = TimerFamilySpec::event_time(VERIFY_TIMER);

    let result = events.apply(
        ParDo::new(
            "ClearAcrossBundles",
            ClearAcrossBundlesDoFn {
                scores: scores_spec.clone(),
                players: players_spec.clone(),
                clear_timer: clear_spec.clone(),
                verify_timer: verify_spec.clone(),
            },
        )
        .with_state_spec(&scores_spec)
        .with_state_spec(&players_spec)
        .with_timer_family(&clear_spec)
        .with_timer_family(&verify_spec),
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

/// Validates DoFn bundle lifecycle hooks (`start_bundle` & `finish_bundle`) with transactional batching.
///
/// Bundling is up to the runner, so the check is the sink's invariants: every record is
/// flushed once, no batch exceeds the batch size, only a bundle's last batch is short, and
/// every bundle commits the transaction that `start_bundle` opened.
pub fn build_bundle_lifecycle_batching(p: &TestPipeline) {
    const BATCH_SIZE: usize = 3;
    let records = (1..=7).map(|i| format!("item_{i}")).collect::<Vec<_>>();
    let expected = records.clone();

    let out = p.apply(Create::new("Create", records)).apply(ParDo::new(
        "BatchSink",
        TransactionalBatchSink {
            batch_size: BATCH_SIZE,
            buffer: Vec::new(),
            active_tx: None,
        },
    ));

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
    let count_spec = ValueStateSpec::<i64>::new("win_count");
    let timer_spec = TimerFamilySpec::event_time("win_flush");

    let out = p
        .apply(Create::new(
            "Events",
            vec![
                ("k".to_string(), (10i64, 2_000i64)),
                ("k".to_string(), (20i64, 7_000i64)),
            ],
        ))
        .par_do("AssignTimestamps", ValidatesAssignTimestampDoFn)
        .apply(WindowInto::new(
            "Sliding",
            SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5)),
        ))
        .apply(
            ParDo::new(
                "WindowedState",
                WindowedStatefulTimerDoFn {
                    count: count_spec.clone(),
                    flush_timer: timer_spec.clone(),
                },
            )
            .with_state_spec(&count_spec)
            .with_timer_family(&timer_spec),
        );

    passert::that_windowed("AssertWindowedState", &out).contains_in_any_order([
        (
            ("k:-5000".to_string(), 10),
            IntervalWindow::new(-5_000, 5_000),
        ),
        (("k:0".to_string(), 30), IntervalWindow::new(0, 10_000)),
        (
            ("k:5000".to_string(), 20),
            IntervalWindow::new(5_000, 15_000),
        ),
    ]);
}

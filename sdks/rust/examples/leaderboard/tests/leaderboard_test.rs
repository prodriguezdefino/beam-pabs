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

//! Tests for the LeaderBoard example.
//!
//! Shows how to test a streaming pipeline with `TestStream`. Each test scripts
//! the order of elements and watermark advances, then uses `passert` pane
//! filters to verify which results are early, on time, late, or absent.

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};
use gaming::ParseGameEvents;
use leaderboard::{CalculateTeamScores, CalculateUserScores};

/// Team scores use one-minute fixed windows; this is the first of them.
const FIRST_WINDOW: IntervalWindow = IntervalWindow {
    start: Instant::EPOCH,
    end: Instant::from_millis(60_000),
};
const TEAM_WINDOW: Duration = Duration::from_secs(60);
const ALLOWED_LATENESS: Duration = Duration::from_secs(120);
/// An early-firing threshold high enough that no speculative pane fires.
const NO_EARLY_FIRINGS: i32 = 1_000;

/// Event time `secs` seconds after the epoch.
fn secs(secs: u64) -> Instant {
    Instant::EPOCH + Duration::from_secs(secs)
}

/// A CSV game event, stamped with its own event time.
fn event(user: &str, team: &str, score: i64, timestamp: Instant) -> (String, Instant) {
    (
        format!("{user},{team},{score},{}", timestamp.as_millis()),
        timestamp,
    )
}

/// A team and its score.
fn team(team: &str, score: i64) -> (String, i64) {
    (team.to_string(), score)
}

/// Team scores of a scripted stream of CSV events.
fn calculate_team_scores(
    p: &TestPipeline,
    events: TestStream<String>,
    early_count: i32,
) -> PCollection<(String, i64)> {
    p.apply("Events", events)
        .apply("Parse", ParseGameEvents)
        .apply(
            "CalculateTeamScores",
            CalculateTeamScores {
                window: TEAM_WINDOW,
                allowed_lateness: ALLOWED_LATENESS,
                early_count,
            },
        )
}

/// Every event arrives before the watermark passes the end of its window, so
/// each team gets exactly one on-time pane with its full total.
#[tokio::test]
async fn test_team_scores_on_time() {
    let p = TestPipeline::new();
    let events = TestStream::new()
        .add_timestamped_elements([
            event("alice", "red", 10, secs(1)),
            event("carol", "blue", 5, secs(2)),
        ])
        .advance_watermark_to(secs(30))
        .add_timestamped_elements([event("bob", "red", 20, secs(40))])
        .advance_watermark_to(secs(70))
        .advance_watermark_to_infinity();

    let team_scores = calculate_team_scores(&p, events, NO_EARLY_FIRINGS);

    passert::that("AssertTeamScores", &team_scores)
        .in_on_time_pane(FIRST_WINDOW)
        .contains_in_any_order([team("red", 30), team("blue", 5)]);

    p.run().await.expect("on-time team scores must match");
}

/// Reaching the early-firing count emits a speculative pane before the window
/// closes. Panes accumulate, so the on-time pane carries the full total.
#[tokio::test]
async fn test_team_scores_speculative() {
    let p = TestPipeline::new();
    let events = TestStream::new()
        .add_timestamped_elements([
            event("alice", "red", 10, secs(1)),
            event("bob", "red", 20, secs(2)),
        ])
        .advance_watermark_to(secs(20))
        .add_timestamped_elements([event("alice", "red", 15, secs(30))])
        .advance_watermark_to(secs(70))
        .advance_watermark_to_infinity();

    let team_scores = calculate_team_scores(&p, events, 2);

    passert::that("EarlyPane", &team_scores)
        .in_early_panes(FIRST_WINDOW)
        .contains_in_any_order([team("red", 30)]);
    passert::that("OnTimePane", &team_scores)
        .in_on_time_pane(FIRST_WINDOW)
        .contains_in_any_order([team("red", 45)]);

    p.run().await.expect("speculative team scores must match");
}

/// An event that arrives after the window closed, but within the allowed
/// lateness, fires a late pane that also includes the earlier scores.
#[tokio::test]
async fn test_team_scores_observably_late() {
    let p = TestPipeline::new();
    let events = TestStream::new()
        .add_timestamped_elements([event("alice", "red", 10, secs(1))])
        .advance_watermark_to(secs(70))
        .add_timestamped_elements([event("bob", "red", 20, secs(2))])
        .advance_watermark_to_infinity();

    let team_scores = calculate_team_scores(&p, events, NO_EARLY_FIRINGS);

    passert::that("OnTimePane", &team_scores)
        .in_on_time_pane(FIRST_WINDOW)
        .contains_in_any_order([team("red", 10)]);
    passert::that("LatePane", &team_scores)
        .in_late_panes(FIRST_WINDOW)
        .contains_in_any_order([team("red", 30)]);

    p.run().await.expect("late team scores must match");
}

/// An event that arrives after the allowed lateness has expired is dropped.
///
/// Because panes accumulate, the window also emits a final pane when it
/// expires, repeating its total. That total is still 10, which shows the
/// dropped score of 20 never reached it.
#[tokio::test]
async fn test_team_scores_droppably_late() {
    let p = TestPipeline::new();
    let window_expiry = FIRST_WINDOW.end + ALLOWED_LATENESS;
    let events = TestStream::new()
        .add_timestamped_elements([event("alice", "red", 10, secs(1))])
        .advance_watermark_to(window_expiry + Duration::from_secs(20))
        .add_timestamped_elements([event("bob", "red", 20, secs(2))])
        .advance_watermark_to_infinity();

    let team_scores = calculate_team_scores(&p, events, NO_EARLY_FIRINGS);

    passert::that("OnTimePane", &team_scores)
        .in_on_time_pane(FIRST_WINDOW)
        .contains_in_any_order([team("red", 10)]);
    passert::that("FinalPane", &team_scores)
        .in_final_pane(FIRST_WINDOW)
        .contains_in_any_order([team("red", 10)]);

    p.run().await.expect("droppably late data must be ignored");
}

/// User standings live in the global window and fire after every event, so
/// each pane reports that user's running total.
///
/// Once the stream ends, the watermark reaches the end of the global window.
/// Because panes accumulate, each user then gets one final pane repeating
/// their latest total.
#[tokio::test]
async fn test_user_scores_running_totals() {
    let p = TestPipeline::new();
    let events = TestStream::new()
        .add_timestamped_elements([event("alice", "red", 10, secs(1))])
        .advance_watermark_to(secs(1))
        .add_timestamped_elements([event("bob", "red", 20, secs(2))])
        .advance_watermark_to(secs(2))
        .add_timestamped_elements([event("alice", "red", 15, secs(3))])
        .advance_watermark_to_infinity();

    let user_scores = p
        .apply("Events", events)
        .apply("Parse", ParseGameEvents)
        .apply(
            "CalculateUserScores",
            CalculateUserScores { trigger_count: 1 },
        );

    passert::that("AssertUserScores", &user_scores).contains_in_any_order([
        // Running totals, one pane per event.
        ("alice".to_string(), 10),
        ("bob".to_string(), 20),
        ("alice".to_string(), 25),
        // Final panes when the global window closes.
        ("bob".to_string(), 20),
        ("alice".to_string(), 25),
    ]);

    p.run().await.expect("user running totals must match");
}

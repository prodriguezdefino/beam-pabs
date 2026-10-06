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

use beam::prelude::*;
use beam::testing::{TestPipeline, TestStream, passert};
use leaderboard::{GameActionInfo, build_leaderboard_graph, parse_game_action_line};

#[test]
fn test_parse_game_action_line() {
    let line = "user1_red,red,25,1445230923000,2015-10-19 09:02:03";
    let info = parse_game_action_line(line).expect("CSV line must parse");
    assert_eq!(
        info,
        GameActionInfo {
            user: "user1_red".to_string(),
            team: "red".to_string(),
            score: 25,
            timestamp_ms: 1445230923000,
        }
    );
    assert_eq!(parse_game_action_line("malformed,line"), None);
}

/// Team scores use one-minute fixed windows; this is the first of them.
const FIRST_WINDOW: IntervalWindow = IntervalWindow {
    start_millis: 0,
    end_millis: 60_000,
};
const TEAM_WINDOW_SECS: u64 = 60;
const ALLOWED_LATENESS_SECS: u64 = 120;
/// An early-firing threshold high enough that no speculative pane fires.
const NO_EARLY_FIRINGS: i32 = 1_000;

/// A CSV game event, stamped with its own event time.
fn event(user: &str, team: &str, score: i64, timestamp_ms: i64) -> (String, i64) {
    (
        format!("{user},{team},{score},{timestamp_ms}"),
        timestamp_ms,
    )
}

fn team_line(team: &str, score: i64) -> String {
    format!(
        "TEAM|{team}|{score}|[{}..{})",
        FIRST_WINDOW.start_millis, FIRST_WINDOW.end_millis
    )
}

/// Applies the LeaderBoard graph to a scripted stream of CSV events.
fn leaderboard(
    p: &TestPipeline,
    events: TestStream<String>,
    early_count: i32,
    user_trigger_count: i32,
) -> (PCollection<String>, PCollection<String>) {
    build_leaderboard_graph(
        &p.apply(events),
        TEAM_WINDOW_SECS,
        ALLOWED_LATENESS_SECS,
        early_count,
        user_trigger_count,
    )
}

/// Every event arrives before the watermark passes the end of its window, so
/// each team gets exactly one on-time pane with its full total.
#[tokio::test]
async fn test_team_scores_on_time() {
    let p = TestPipeline::new();
    let events = TestStream::new("TestStream")
        .add_timestamped_elements([
            event("alice", "red", 10, 1_000),
            event("carol", "blue", 5, 2_000),
        ])
        .advance_watermark_to(30_000)
        .add_timestamped_elements([event("bob", "red", 20, 40_000)])
        .advance_watermark_to(70_000)
        .advance_watermark_to_infinity();

    let (team_scores, _) = leaderboard(&p, events, NO_EARLY_FIRINGS, NO_EARLY_FIRINGS);

    passert::that("AssertTeamScores", &team_scores)
        .in_on_time_pane(FIRST_WINDOW)
        .contains_in_any_order([team_line("red", 30), team_line("blue", 5)]);

    p.run().await.expect("on-time team scores must match");
}

/// Reaching the early-firing count emits a speculative pane before the window
/// closes. Panes accumulate, so the on-time pane carries the full total.
#[tokio::test]
async fn test_team_scores_speculative() {
    let p = TestPipeline::new();
    let events = TestStream::new("TestStream")
        .add_timestamped_elements([
            event("alice", "red", 10, 1_000),
            event("bob", "red", 20, 2_000),
        ])
        .advance_watermark_to(20_000)
        .add_timestamped_elements([event("alice", "red", 15, 30_000)])
        .advance_watermark_to(70_000)
        .advance_watermark_to_infinity();

    let (team_scores, _) = leaderboard(&p, events, 2, NO_EARLY_FIRINGS);

    passert::that("EarlyPane", &team_scores)
        .in_early_panes(FIRST_WINDOW)
        .contains_in_any_order([team_line("red", 30)]);
    passert::that("OnTimePane", &team_scores)
        .in_on_time_pane(FIRST_WINDOW)
        .contains_in_any_order([team_line("red", 45)]);

    p.run().await.expect("speculative team scores must match");
}

/// An event that arrives after the window closed, but within the allowed
/// lateness, fires a late pane that also includes the earlier scores.
#[tokio::test]
async fn test_team_scores_observably_late() {
    let p = TestPipeline::new();
    let events = TestStream::new("TestStream")
        .add_timestamped_elements([event("alice", "red", 10, 1_000)])
        .advance_watermark_to(70_000)
        .add_timestamped_elements([event("bob", "red", 20, 2_000)])
        .advance_watermark_to_infinity();

    let (team_scores, _) = leaderboard(&p, events, NO_EARLY_FIRINGS, NO_EARLY_FIRINGS);

    passert::that("OnTimePane", &team_scores)
        .in_on_time_pane(FIRST_WINDOW)
        .contains_in_any_order([team_line("red", 10)]);
    passert::that("LatePane", &team_scores)
        .in_late_panes(FIRST_WINDOW)
        .contains_in_any_order([team_line("red", 30)]);

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
    let window_expiry_ms = FIRST_WINDOW.end_millis + (ALLOWED_LATENESS_SECS as i64) * 1_000;
    let events = TestStream::new("TestStream")
        .add_timestamped_elements([event("alice", "red", 10, 1_000)])
        .advance_watermark_to(window_expiry_ms + 20_000)
        .add_timestamped_elements([event("bob", "red", 20, 2_000)])
        .advance_watermark_to_infinity();

    let (team_scores, _) = leaderboard(&p, events, NO_EARLY_FIRINGS, NO_EARLY_FIRINGS);

    passert::that("OnTimePane", &team_scores)
        .in_on_time_pane(FIRST_WINDOW)
        .contains_in_any_order([team_line("red", 10)]);
    passert::that("FinalPane", &team_scores)
        .in_final_pane(FIRST_WINDOW)
        .contains_in_any_order([team_line("red", 10)]);

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
    let events = TestStream::new("TestStream")
        .add_timestamped_elements([event("alice", "red", 10, 1_000)])
        .advance_watermark_to(1_000)
        .add_timestamped_elements([event("bob", "red", 20, 2_000)])
        .advance_watermark_to(2_000)
        .add_timestamped_elements([event("alice", "red", 15, 3_000)])
        .advance_watermark_to_infinity();

    let (_, user_scores) = leaderboard(&p, events, NO_EARLY_FIRINGS, 1);

    passert::that("AssertUserScores", &user_scores).contains_in_any_order(
        [
            // Running totals, one pane per event.
            "USER|alice|10",
            "USER|bob|20",
            "USER|alice|25",
            // Final panes when the global window closes.
            "USER|bob|20",
            "USER|alice|25",
        ]
        .map(String::from),
    );

    p.run().await.expect("user running totals must match");
}

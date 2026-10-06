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

use std::time::Duration;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use windowing::{GameEvent, ParseEvents, TeamStats, UserSessions};

/// The window from `from` to `to` seconds after the epoch.
fn window(from: i64, to: i64) -> IntervalWindow {
    IntervalWindow::new(
        Instant::from_millis(from * 1_000),
        Instant::from_millis(to * 1_000),
    )
}

/// 20 s sliding windows every 10 s, and sessions with a 5 s gap. The bad line is skipped.
#[tokio::test]
async fn scores_per_sliding_window_and_events_per_session() {
    let p = TestPipeline::new();
    let lines = [
        "alice,red,10,1000",
        "alice,red,20,3000",
        "bob,red,5,12000",
        "oops",
    ];

    let events: PCollection<GameEvent> = p
        .apply("Create", Create::new(lines.map(String::from).to_vec()))
        .apply("ParseEvents", ParseEvents);
    let teams = events.apply(
        "TeamStats",
        TeamStats {
            size: Duration::from_secs(20),
            period: Duration::from_secs(10),
        },
    );
    let sessions = events.apply(
        "UserSessions",
        UserSessions {
            gap: Duration::from_secs(5),
        },
    );

    passert::that("AssertTeams", &teams).contains_in_any_order([
        format!(
            "{} team red: count=2 mean=15.0 min=10 max=20",
            window(-10, 10)
        ),
        format!("{} team red: count=3 mean=11.7 min=5 max=20", window(0, 20)),
        format!("{} team red: count=1 mean=5.0 min=5 max=5", window(10, 30)),
    ]);
    passert::that("AssertSessions", &sessions).contains_in_any_order([
        format!("{} user alice: 2 events", window(1, 8)),
        format!("{} user bob: 1 events", window(12, 17)),
    ]);

    p.run().await.expect("pipeline and assertions must pass");
}

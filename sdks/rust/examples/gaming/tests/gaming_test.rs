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

//! Tests for the Mobile Gaming example.

use std::num::NonZeroU32;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use gaming::{
    GameActionInfo, MalformedEvent, ParseGameEvents, TeamMilestoneFn, TeamMilestoneReport,
};

/// Five events of two teams, and one malformed line.
fn lines() -> Vec<String> {
    [
        "user1_RedTeam,RedTeam,30,1447955630000,2015-11-19 09:53:50.000",
        "user2_RedTeam,RedTeam,45,1447955632000,2015-11-19 09:53:52.000",
        "user1_BlueTeam,BlueTeam,60,1447955634000,2015-11-19 09:53:54.000",
        "user3_RedTeam,RedTeam,35,1447955636000,2015-11-19 09:53:56.000",
        "user2_BlueTeam,BlueTeam,50,1447955638000,2015-11-19 09:53:58.000",
        "corrupted,line,not_an_int",
    ]
    .map(String::from)
    .to_vec()
}

#[test]
fn parses_a_game_event() {
    assert_eq!(
        "alice,AmberKookaburra,18,1447955630000,2015-11-19 09:53:50.000".parse(),
        Ok(GameActionInfo {
            user: "alice".to_string(),
            team: "AmberKookaburra".to_string(),
            score: 18,
            timestamp: Instant::from_millis(1447955630000),
        })
    );
    assert_eq!(
        "malformed,line".parse::<GameActionInfo>(),
        Err(MalformedEvent)
    );
}

/// Each event keeps its own event time, and the malformed line is dropped.
#[tokio::test]
async fn parse_stamps_the_event_time() {
    let p = TestPipeline::new();
    let events = p
        .apply("Lines", Create::new(lines()))
        .apply("Parse", ParseGameEvents)
        .par_do_fn(
            "WithTimestamp",
            |(team, (_user, score)): (String, (String, i64)), ctx| {
                let millis = ctx.timestamp().as_millis();
                ctx.emit((team, score, millis))
            },
        );
    passert::that("AssertEvents", &events).contains_in_any_order([
        ("RedTeam".to_string(), 30, 1447955630000),
        ("RedTeam".to_string(), 45, 1447955632000),
        ("BlueTeam".to_string(), 60, 1447955634000),
        ("RedTeam".to_string(), 35, 1447955636000),
        ("BlueTeam".to_string(), 50, 1447955638000),
    ]);
    p.run().await.expect("parsed events must match");
}

/// Both teams pass 100 once, with every user as a contributor.
#[tokio::test]
async fn team_milestones() {
    let p = TestPipeline::new();
    let milestones = p
        .apply("Lines", Create::new(lines()))
        .apply("Parse", ParseGameEvents)
        .apply(
            "TeamMilestones",
            ParDo::new(TeamMilestoneFn {
                threshold: NonZeroU32::new(100).expect("100 is not zero"),
            }),
        );
    passert::that("AssertMilestones", &milestones).contains_in_any_order([
        TeamMilestoneReport {
            team: "BlueTeam".to_string(),
            score: 110,
            contributors: vec!["user1_BlueTeam".to_string(), "user2_BlueTeam".to_string()],
        },
        TeamMilestoneReport {
            team: "RedTeam".to_string(),
            score: 110,
            contributors: ["user1_RedTeam", "user2_RedTeam", "user3_RedTeam"]
                .map(String::from)
                .to_vec(),
        },
    ]);
    p.run().await.expect("team milestones must match");
}

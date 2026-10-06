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

//! Integration tests for the Mobile Gaming example on Prism.

use std::sync::{Arc, Mutex};

use beam::prelude::*;
use gaming::{GameActionInfo, TeamMilestoneReport, build_gaming_graph, parse_game_action_line};

fn sample_gaming_csv_lines() -> Vec<String> {
    vec![
        "user1_RedTeam,RedTeam,30,1447955630000,2015-11-19 09:53:50.000".to_string(),
        "user2_RedTeam,RedTeam,45,1447955632000,2015-11-19 09:53:52.000".to_string(),
        "user1_BlueTeam,BlueTeam,60,1447955634000,2015-11-19 09:53:54.000".to_string(),
        "user3_RedTeam,RedTeam,35,1447955636000,2015-11-19 09:53:56.000".to_string(),
        "user2_BlueTeam,BlueTeam,50,1447955638000,2015-11-19 09:53:58.000".to_string(),
        "corrupted,line,not_an_int".to_string(),
    ]
}

fn expected_gaming_outputs() -> Vec<String> {
    vec![
        "FINAL_SCORE|BlueTeam|110".to_string(),
        "FINAL_SCORE|RedTeam|110".to_string(),
        "MILESTONE|BlueTeam|110|user1_BlueTeam,user2_BlueTeam".to_string(),
        "MILESTONE|RedTeam|110|user1_RedTeam,user2_RedTeam,user3_RedTeam".to_string(),
    ]
}

#[test]
fn test_parse_game_action_line() {
    let valid = "alice,AmberKookaburra,18,1447955630000,2015-11-19 09:53:50.000";
    assert_eq!(
        parse_game_action_line(valid),
        Some(GameActionInfo {
            user: "alice".to_string(),
            team: "AmberKookaburra".to_string(),
            score: 18,
            timestamp_ms: 1447955630000,
        })
    );
    assert_eq!(parse_game_action_line("malformed,line"), None);
}

#[tokio::test]
async fn test_gaming_pipeline_prism_runner() {
    beam::harness::init_logging();
    let p = Pipeline::new();
    let lines = p.apply(Create::new("SampleGamingCsv", sample_gaming_csv_lines()));
    let output = build_gaming_graph(&lines, 100);

    let captured = Arc::new(Mutex::new(Vec::new()));
    let c = Arc::clone(&captured);
    output.inspect("CapturePrism", move |s: &String| {
        c.lock().unwrap().push(s.clone());
    });

    p.run()
        .await
        .expect("Gaming pipeline must succeed on PrismRunner");

    let mut got = captured.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, expected_gaming_outputs());
}

#[test]
fn test_team_milestone_report_row_schema() {
    let report = TeamMilestoneReport {
        team: "RedTeam".to_string(),
        score: 110,
        contributors: vec!["user1".to_string(), "user2".to_string()],
    };
    let empty = TeamMilestoneReport {
        team: String::new(),
        score: 0,
        contributors: Vec::new(),
    };

    // A PCollection has exactly one schema, so the schema must be a function of
    // the type alone — never of the particular value being encoded.
    let shape: Vec<(&str, String)> = TeamMilestoneReport::beam_schema()
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.to_string()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("team", "STRING".to_string()),
            ("score", "INT64".to_string()),
            ("contributors", "ARRAY<STRING>".to_string()),
        ]
    );

    for original in [&report, &empty] {
        let bytes = original.to_row_bytes().expect("Row encoding must succeed");
        let decoded =
            TeamMilestoneReport::from_row_bytes(&bytes).expect("Row decoding must succeed");
        assert_eq!(original, &decoded);
    }
}

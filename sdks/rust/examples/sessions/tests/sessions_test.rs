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

use std::sync::{Arc, Mutex};

use beam::prelude::*;
use sessions::{UserActivityRecord, build_sessions_graph, parse_activity_line};

#[test]
fn test_parse_activity_line() {
    let line4 = "alice,login,100,1600000000000";
    let rec4 = parse_activity_line(line4).expect("4-field CSV must parse");
    assert_eq!(
        rec4,
        UserActivityRecord {
            user: "alice".to_string(),
            action: "login".to_string(),
            timestamp_ms: 1600000000000,
        }
    );

    let line3 = "bob,click,1600000005000";
    let rec3 = parse_activity_line(line3).expect("3-field CSV must parse");
    assert_eq!(
        rec3,
        UserActivityRecord {
            user: "bob".to_string(),
            action: "click".to_string(),
            timestamp_ms: 1600000005000,
        }
    );

    let invalid = "invalid_line";
    assert!(parse_activity_line(invalid).is_none());
}

#[tokio::test]
async fn test_sessions_on_prism_runner() {
    let p = Pipeline::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    let csv_lines = p.apply(Create::new(
        "Create",
        vec![
            "charlie,open,5000".to_string(),
            "charlie,read,7000".to_string(),
            "dana,play,1000".to_string(),
        ],
    ));

    // Gap duration = 5s (5000 ms)
    // charlie: [5000, 10000) and [7000, 12000) -> merged [5000, 12000), duration_ms = 7000, actions = 2
    // dana: [1000, 6000), duration_ms = 5000, actions = 1
    let sessions = build_sessions_graph(&csv_lines, 5);
    sessions.inspect("CaptureSessionsPrism", move |s: &String| {
        captured.lock().unwrap().push(s.clone());
    });

    p.run()
        .await
        .expect("sessions pipeline on prism runner must succeed");

    let mut items = results.lock().unwrap().clone();
    items.sort();

    assert_eq!(items.len(), 2);
    assert!(
        items.contains(&"SESSION|charlie|[5000..12000)|duration_ms=7000|actions=2".to_string()),
        "expected merged charlie session [5000..12000), got: {items:?}"
    );
    assert!(
        items.contains(&"SESSION|dana|[1000..6000)|duration_ms=5000|actions=1".to_string()),
        "expected dana session [1000..6000), got: {items:?}"
    );
}

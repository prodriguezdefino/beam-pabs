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
use state_conformance::{build_conformance_graph, mismatch, render, scenario_for, seed_elements};

#[test]
fn test_render_and_mismatch() {
    assert_eq!(render(vec!["b".to_string(), "a".to_string()]), "a+b");
    assert_eq!(render(Vec::<String>::new()), "");

    let diff = mismatch("test", "teamA", &["bob"], "alice+bob");
    assert!(diff.is_some());
    assert!(
        diff.unwrap()
            .contains("teamA: test is 'alice+bob' but the model says 'bob'")
    );

    let same = mismatch("test", "teamA", &["bob"], "bob");
    assert!(same.is_none());

    assert!(scenario_for("unknown").is_err());
}

#[tokio::test]
async fn test_state_conformance_on_prism_runner() {
    let p = Pipeline::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    let events = p.apply(Create::new("SeedScores", seed_elements()));
    let conformance = build_conformance_graph(&events, false);
    conformance.inspect("CaptureResultsPrism", move |s: &String| {
        captured.lock().unwrap().push(s.clone());
    });

    p.run()
        .await
        .expect("state conformance pipeline on prism runner must succeed");

    let mut items = results.lock().unwrap().clone();
    items.sort();
    assert_eq!(items.len(), 3);
    assert!(
        items
            .iter()
            .all(|s| s.starts_with("DIVERGENCE|") || s.starts_with("MATCH|"))
    );
    assert!(items.iter().any(|s| s.contains("team=clear_all")));
    assert!(items.iter().any(|s| s.contains("team=remove_present")));
    assert!(items.iter().any(|s| s.contains("team=remove_absent")));
}

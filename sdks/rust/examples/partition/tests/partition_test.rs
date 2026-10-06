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

//! Integration tests for the Partition example pipeline.

use std::sync::{Arc, Mutex};

use beam::prelude::*;
use partition::{
    TIER_HONOURS, TIER_PASS, TIER_SUPPORT, build_partition_pipeline, default_students,
    partition_by_tier,
};

#[test]
fn test_partition_by_tier_logic() {
    assert_eq!(partition_by_tier(100), TIER_HONOURS);
    assert_eq!(partition_by_tier(80), TIER_HONOURS);
    assert_eq!(partition_by_tier(79), TIER_PASS);
    assert_eq!(partition_by_tier(50), TIER_PASS);
    assert_eq!(partition_by_tier(49), TIER_SUPPORT);
    assert_eq!(partition_by_tier(0), TIER_SUPPORT);
}

fn expected_output() -> Vec<String> {
    vec![
        "Honours: Alice (95)".to_string(),
        "Honours: Bob (88)".to_string(),
        "Honours: Ellen (82)".to_string(),
        "Honours: Grace (91)".to_string(),
        "Honours: Kelly (85)".to_string(),
        "Pass: Charlie (76)".to_string(),
        "Pass: David (65)".to_string(),
        "Pass: Hannah (58)".to_string(),
        "Pass: Jack (70)".to_string(),
        "Support: Frank (45)".to_string(),
        "Support: Isaac (33)".to_string(),
        "Support: Liam (40)".to_string(),
    ]
}

#[tokio::test]
async fn test_partition_on_prism_runner() {
    let pipeline = Pipeline::new();
    let output = build_partition_pipeline(&pipeline, default_students());

    let captured = Arc::new(Mutex::new(Vec::new()));
    let c = Arc::clone(&captured);

    output.inspect("CapturePrismResults", move |elem: &String| {
        c.lock().unwrap().push(elem.clone());
    });

    let result = pipeline
        .run()
        .await
        .expect("Partition pipeline should execute successfully on PrismRunner");

    assert_eq!(result.state, "DONE");

    let mut got = captured.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, expected_output());
}

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

//! Integration tests for the Join example pipeline.

use std::sync::{Arc, Mutex};

use beam::prelude::*;
use join::{build_join_pipeline, default_orders, default_users};

fn expected_results() -> Vec<String> {
    vec![
        "COGBK: user user_1 -> names=[\"Alice\"], orders=[\"Laptop\", \"Mouse\"]".to_string(),
        "COGBK: user user_2 -> names=[\"Bob\"], orders=[\"Keyboard\"]".to_string(),
        "COGBK: user user_3 -> names=[\"Charlie\"], orders=[\"Monitor\"]".to_string(),
        "COGBK: user user_4 -> names=[\"Diana\"], orders=[]".to_string(),
        "COGBK: user user_5 -> names=[], orders=[\"Headphones\"]".to_string(),
        "INNER: user user_1 (Alice) ordered Laptop".to_string(),
        "INNER: user user_1 (Alice) ordered Mouse".to_string(),
        "INNER: user user_2 (Bob) ordered Keyboard".to_string(),
        "INNER: user user_3 (Charlie) ordered Monitor".to_string(),
        "LEFT: user user_1 (Alice) ordered Laptop".to_string(),
        "LEFT: user user_1 (Alice) ordered Mouse".to_string(),
        "LEFT: user user_2 (Bob) ordered Keyboard".to_string(),
        "LEFT: user user_3 (Charlie) ordered Monitor".to_string(),
        "LEFT: user user_4 (Diana) placed no orders".to_string(),
    ]
}

#[tokio::test]
async fn test_join_on_prism_runner() {
    let pipeline = Pipeline::new();
    let output = build_join_pipeline(&pipeline, default_users(), default_orders());

    let captured = Arc::new(Mutex::new(Vec::new()));
    let c = Arc::clone(&captured);

    output.inspect("CapturePrismResults", move |elem: &String| {
        c.lock().unwrap().push(elem.clone());
    });

    let result = pipeline
        .run()
        .await
        .expect("Join pipeline should execute successfully on PrismRunner");

    assert_eq!(result.state, "DONE");

    let mut got = captured.lock().unwrap().clone();
    got.sort();
    let mut expected = expected_results();
    expected.sort();
    assert_eq!(got, expected);
}

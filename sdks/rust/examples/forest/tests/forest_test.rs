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

//! Integration tests for the Forest example pipeline.

use std::sync::{Arc, Mutex};

use beam::prelude::*;
use forest::build_forest;

#[tokio::test]
async fn test_forest_on_prism_runner() {
    let pipeline = Pipeline::new();
    let roots = build_forest(&pipeline, 2, 2);

    let tree0 = Arc::new(Mutex::new(Vec::new()));
    let tree1 = Arc::new(Mutex::new(Vec::new()));
    let t0 = Arc::clone(&tree0);
    let t1 = Arc::clone(&tree1);

    roots[0].inspect("CaptureTree0Prism", move |&val: &i64| {
        t0.lock().unwrap().push(val);
    });
    roots[1].inspect("CaptureTree1Prism", move |&val: &i64| {
        t1.lock().unwrap().push(val);
    });

    let result = pipeline
        .run()
        .await
        .expect("Forest pipeline should execute successfully on PrismRunner");

    assert_eq!(result.state, "DONE");

    let mut got0 = tree0.lock().unwrap().clone();
    got0.sort();
    assert_eq!(got0, vec![1, 2, 3, 4, 5, 6, 7]);

    let mut got1 = tree1.lock().unwrap().clone();
    got1.sort();
    assert_eq!(got1, vec![8, 9, 10, 11, 12, 13, 14]);
}

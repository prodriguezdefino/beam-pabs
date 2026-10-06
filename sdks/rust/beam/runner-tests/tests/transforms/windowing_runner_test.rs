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

//! Windowing checks that count calls in the test process, so they need a loopback runner.
//! The windowing checks that run on every runner are registry builders.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::prelude::*;
use prism::PrismRunner;
use tests::dofns::AssignTimestampDoFn;

/// Extracts the active element's timestamp and sorted values downstream of GroupByKey.
#[derive(Clone, Default)]
struct ExtractTimestampAndValues;

impl DoFn for ExtractTimestampAndValues {
    type In = (String, BeamIterable<i64>);
    type Out = (String, (Instant, Vec<i64>));

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let (k, v) = element;
        let mut v = v.into_vec()?;
        v.sort();
        ctx.emit((k, (ctx.timestamp(), v)))
    }
}

#[tokio::test]
async fn test_sliding_windows_oblivious_transform_runs_once_and_preserves_windows() {
    let p = Pipeline::new();
    let invocations = Arc::new(AtomicUsize::new(0));
    let inv = Arc::clone(&invocations);
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    // Timestamp 7_000ms falls into two sliding windows ([0, 10_000) and [5_000, 15_000)),
    // but a window-oblivious Map runs once on the compressed multi-window element while
    // preserving membership in both windows for the downstream GroupByKey.
    p.apply(
        "Create",
        Create::new(vec![("k".to_string(), (5i64, 7_000i64))]),
    )
    .par_do("AssignTimestamps", AssignTimestampDoFn)
    .apply(
        "SlidingWindows",
        WindowInto::new(SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5))),
    )
    .map("ObliviousDouble", move |(k, v): (String, i64)| {
        inv.fetch_add(1, Ordering::SeqCst);
        (k, v * 2)
    })
    .group_by_key("Group")
    .par_do("ExtractTs", ExtractTimestampAndValues)
    .inspect("Capture", move |entry: &(String, (Instant, Vec<i64>))| {
        captured.lock().unwrap().push(entry.clone());
    });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("sliding windows oblivious transform pipeline must succeed");

    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    let mut got = results.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("k".to_string(), (Instant::from_millis(9_999), vec![10])),
            ("k".to_string(), (Instant::from_millis(14_999), vec![10])),
        ]
    );
}

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

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

//! Combine transforms crossed with every window type.
//!
//! Combining aggregates *within a window*. The pre-combine step buffers accumulators
//! across a bundle, so it has to keep one accumulator per key per window assignment and
//! put each one back into the windows it came from. Getting that wrong does not fail the
//! job: the accumulators land in the wrong window, or in no window at all, and the runner
//! reports success having produced nothing. Each test here pins a different way for that
//! to go wrong.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::prelude::*;
use beam::transforms::Sum;
use prism::PrismRunner;

type Captured<T> = Arc<Mutex<Vec<T>>>;

/// Assigns explicit event timestamps so window assignment is deterministic.
#[derive(Clone, Default)]
struct AssignTimestampDoFn;

impl DoFn for AssignTimestampDoFn {
    type In = (String, (i64, i64));
    type Out = (String, i64);

    fn process_element(
        &mut self,
        (key, (value, timestamp)): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        ctx.output((key, value)).at(timestamp).emit()
    }
}

fn collector<T>() -> (Captured<T>, Captured<T>) {
    let results: Captured<T> = Arc::new(Mutex::new(Vec::new()));
    let handle = Arc::clone(&results);
    (results, handle)
}

fn sorted<T: Clone + Ord>(results: &Captured<T>) -> Vec<T> {
    let mut got = results.lock().unwrap().clone();
    got.sort();
    got
}

/// Four values for one key: two in each of two adjacent 10s fixed windows.
fn two_windows_of_input() -> Vec<(String, (i64, i64))> {
    vec![
        ("k".to_string(), (1, 1_000)),
        ("k".to_string(), (2, 5_000)),
        ("k".to_string(), (3, 11_000)),
        ("k".to_string(), (4, 15_000)),
    ]
}

#[tokio::test]
async fn test_combine_per_key_respects_window_fns() {
    let p = Pipeline::new();
    let (fixed_res, fixed_cap) = collector();
    let (sliding_res, sliding_cap) = collector();
    let (sessions_res, sessions_cap) = collector();
    let (count_res, count_cap) = collector();

    let source = p
        .apply(Create::new(
            "Create",
            vec![
                ("a".to_string(), (1i64, 1_000i64)),
                ("a".to_string(), (2i64, 4_000i64)),
                ("b".to_string(), (3i64, 4_000i64)),
                ("a".to_string(), (4i64, 12_000i64)),
                ("b".to_string(), (5i64, 16_000i64)),
            ],
        ))
        .par_do("AssignTimestamps", AssignTimestampDoFn);

    source
        .clone()
        .apply(WindowInto::new(
            "WindowFixed",
            FixedWindows::of(Duration::from_secs(10)),
        ))
        .combine_per_key("SumFixed", Sum)
        .inspect("CaptureFixed", move |kv: &(String, i64)| {
            fixed_cap.lock().unwrap().push(kv.clone());
        });

    source
        .clone()
        .apply(WindowInto::new(
            "WindowSliding",
            SlidingWindows::of(Duration::from_secs(20)).every(Duration::from_secs(10)),
        ))
        .combine_per_key("SumSliding", Sum)
        .inspect("CaptureSliding", move |kv: &(String, i64)| {
            sliding_cap.lock().unwrap().push(kv.clone());
        });

    source
        .clone()
        .apply(WindowInto::new(
            "WindowSessions",
            Sessions::with_gap_duration(Duration::from_secs(5)),
        ))
        .combine_per_key("SumSessions", Sum)
        .inspect("CaptureSessions", move |kv: &(String, i64)| {
            sessions_cap.lock().unwrap().push(kv.clone());
        });

    source
        .map("DropVal", |(k, _): (String, i64)| k)
        .apply(WindowInto::new(
            "WindowCountFixed",
            FixedWindows::of(Duration::from_secs(10)),
        ))
        .count_per_element("CountFixed")
        .inspect("CaptureCount", move |kv: &(String, i64)| {
            count_cap.lock().unwrap().push(kv.clone());
        });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("windowed combine per key pipeline must succeed");

    assert_eq!(
        sorted(&fixed_res),
        vec![
            ("a".to_string(), 3),
            ("a".to_string(), 4),
            ("b".to_string(), 3),
            ("b".to_string(), 5)
        ]
    );
    assert_eq!(
        sorted(&sliding_res),
        vec![
            ("a".to_string(), 3),
            ("a".to_string(), 4),
            ("a".to_string(), 7),
            ("b".to_string(), 3),
            ("b".to_string(), 5),
            ("b".to_string(), 8)
        ]
    );
    assert_eq!(
        sorted(&sessions_res),
        vec![
            ("a".to_string(), 3),
            ("a".to_string(), 4),
            ("b".to_string(), 3),
            ("b".to_string(), 5)
        ]
    );
    assert_eq!(
        sorted(&count_res),
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 1),
            ("b".to_string(), 1)
        ]
    );
}

#[tokio::test]
async fn test_global_combines_are_per_window() {
    let p = Pipeline::new();
    let (sum_results, sum_captured) = collector();
    let (count_results, count_captured) = collector();

    let windowed = p
        .apply(Create::new("Create", two_windows_of_input()))
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .map("DropKey", |(_k, v): (String, i64)| v)
        .apply(WindowInto::new(
            "WindowInto",
            FixedWindows::of(Duration::from_secs(10)),
        ));

    windowed
        .clone()
        .combine_globally_without_defaults("SumAll", Sum)
        .inspect("CaptureSum", move |v: &i64| {
            sum_captured.lock().unwrap().push(*v);
        });

    windowed
        .apply(CountGlobally::new("CountAll").without_defaults())
        .inspect("CaptureCount", move |v: &i64| {
            count_captured.lock().unwrap().push(*v);
        });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("windowed global combines must succeed");

    assert_eq!(sorted(&sum_results), vec![3, 7]);
    assert_eq!(sorted(&count_results), vec![2, 2]);
}

#[test]
#[should_panic(expected = "only means something in the global window")]
fn test_combine_globally_with_defaults_rejects_non_global_windows() {
    let p = Pipeline::new();
    p.apply(Create::new("Create", vec![1i64, 2, 3]))
        .apply(WindowInto::new(
            "WindowInto",
            FixedWindows::of(Duration::from_secs(10)),
        ))
        .combine_globally("SumAll", Sum);
}

#[tokio::test]
async fn test_combine_globally_empty_input_without_defaults() {
    let p = Pipeline::new();
    let (results, captured) = collector();

    let empty: Vec<i64> = vec![];
    p.apply(Create::new("Create", empty))
        .combine_globally_without_defaults("SumAll", Sum)
        .inspect("Capture", move |v: &i64| {
            captured.lock().unwrap().push(*v);
        });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("empty global combine without defaults must succeed");

    assert_eq!(*results.lock().unwrap(), Vec::<i64>::new());
}

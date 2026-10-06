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

//! Integration tests for Windowing, Triggers, and Watermarks across runners.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::coders::WindowedHeader;
use beam::prelude::*;
use beam::windowing::OutputTime;
use prism::PrismRunner;

type WindowedItem = (WindowedHeader, (String, i64));

/// Assigns explicit event timestamps to elements with `ctx.output(v).at(ts)`.
#[derive(Clone, Default)]
struct AssignTimestampDoFn;

impl DoFn for AssignTimestampDoFn {
    type In = (String, (i64, i64)); // (key, (value, timestamp_millis))
    type Out = (String, i64);

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let (k, (v, ts)) = element;
        ctx.output((k, v)).at(ts).emit()
    }
}

/// Extracts the active element's timestamp and sorted values downstream of GroupByKey.
#[derive(Clone, Default)]
struct ExtractTimestampAndValues;

impl DoFn for ExtractTimestampAndValues {
    type In = (String, BeamIterable<i64>);
    type Out = (String, (i64, Vec<i64>));

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
async fn test_sessions_merging_group_by_key() {
    let p = Pipeline::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    // Session gap = 5 seconds (5_000 ms).
    // ("s", 10) at 1_000 -> window [1_000, 6_000)
    // ("s", 20) at 4_000 -> window [4_000, 9_000) overlaps [1_000, 6_000) -> merges into [1_000, 9_000)
    // ("s", 30) at 20_000 -> window [20_000, 25_000), gap is 11s > 5s -> separate session
    p.apply(Create::new(
        "Create",
        vec![
            ("s".to_string(), (10i64, 1_000i64)),
            ("s".to_string(), (20i64, 4_000i64)),
            ("s".to_string(), (30i64, 20_000i64)),
        ],
    ))
    .par_do("AssignTimestamps", AssignTimestampDoFn)
    .apply(WindowInto::new(
        "WindowInto",
        Sessions::with_gap_duration(Duration::from_secs(5)),
    ))
    .group_by_key("GroupSessions")
    .map("SortValues", |(k, values): (String, BeamIterable<i64>)| {
        let mut values = values.into_vec().unwrap();
        values.sort();
        (k, values)
    })
    .inspect("Capture", move |kv: &(String, Vec<i64>)| {
        captured.lock().unwrap().push(kv.clone());
    });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("session merging pipeline must succeed");

    let mut got = results.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![("s".to_string(), vec![10, 20]), ("s".to_string(), vec![30]),]
    );
}

#[tokio::test]
async fn test_output_timestamp_rules() {
    for (mode, expected_ts) in [
        (OutputTime::EndOfWindow, 9_999i64),
        (OutputTime::EarliestInPane, 1_000i64),
        (OutputTime::LatestInPane, 5_000i64),
    ] {
        let p = Pipeline::new();
        let results = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&results);

        p.apply(Create::new(
            "Create",
            vec![
                ("k".to_string(), (1i64, 1_000i64)),
                ("k".to_string(), (2i64, 5_000i64)),
            ],
        ))
        .par_do("AssignTimestamps", AssignTimestampDoFn)
        .apply(
            WindowInto::new("WindowInto", FixedWindows::of(Duration::from_secs(10)))
                .with_output_time(mode),
        )
        .group_by_key("Group")
        .par_do("ExtractTs", ExtractTimestampAndValues)
        .inspect("Capture", move |entry: &(String, (i64, Vec<i64>))| {
            captured.lock().unwrap().push(entry.clone());
        });

        p.run_with_runner(&PrismRunner::new())
            .await
            .expect("output timestamp pipeline must succeed");

        let got = results.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "k");
        assert_eq!(
            (got[0].1).0,
            expected_ts,
            "Mode {mode:?} should produce timestamp {expected_ts}"
        );
        assert_eq!((got[0].1).1, vec![1, 2]);
    }
}

/// Buffers elements with their headers and flushes with `ctx.output(v).windowed(&header)`.
#[derive(Clone, Default)]
struct BufferAndEmitWindowedInFinishBundleDoFn {
    buffer: Arc<Mutex<Vec<WindowedItem>>>,
}

impl DoFn for BufferAndEmitWindowedInFinishBundleDoFn {
    type In = (String, i64);
    type Out = (String, i64);

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        self.buffer
            .lock()
            .unwrap()
            .push((ctx.header().clone(), element));
        Ok(())
    }

    fn finish_bundle(&mut self, ctx: &mut ProcessContext<'_, Self::Out>) -> Result {
        let mut buffer = self.buffer.lock().unwrap();
        for (header, item) in buffer.drain(..) {
            ctx.output(item).windowed(&header).emit()?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_finish_bundle_with_windowed_output_succeeds_on_interval_window() {
    let p = Pipeline::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    // Two elements in window [0, 10s), two in window [10s, 20s)
    p.apply(Create::new(
        "Create",
        vec![
            ("k".to_string(), (1i64, 1_000i64)),
            ("k".to_string(), (2i64, 5_000i64)),
            ("k".to_string(), (3i64, 11_000i64)),
            ("k".to_string(), (4i64, 15_000i64)),
        ],
    ))
    .par_do("AssignTimestamps", AssignTimestampDoFn)
    .apply(WindowInto::new(
        "WindowInto",
        FixedWindows::of(Duration::from_secs(10)),
    ))
    .par_do(
        "BufferAndFlushWindowed",
        BufferAndEmitWindowedInFinishBundleDoFn::default(),
    )
    .group_by_key("Group")
    .map("Sum", |(k, vs): (String, BeamIterable<i64>)| {
        (k, vs.into_iter().sum::<i64>())
    })
    .inspect("Capture", move |kv: &(String, i64)| {
        captured.lock().unwrap().push(kv.clone());
    });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("finish_bundle with windowed output must succeed");

    let mut got = results.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![("k".to_string(), 3), ("k".to_string(), 7)]);
}

#[tokio::test]
async fn test_sliding_windows_pardo_observes_each_window() {
    let p = Pipeline::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    // 10s sliding windows every 5s: timestamp 7_000ms falls into [0, 10_000) and [5_000, 15_000).
    p.apply(Create::new(
        "Create",
        vec![("k".to_string(), (1i64, 7_000i64))],
    ))
    .par_do("AssignTimestamps", AssignTimestampDoFn)
    .apply(WindowInto::new(
        "SlidingWindows",
        SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5)),
    ))
    .par_do_fn("ReadWindowStart", |(k, v): (String, i64), ctx| {
        let win = ctx.interval_window().expect("interval window");
        ctx.emit((k, (win.start_millis, v)))
    })
    .inspect("Capture", move |entry: &(String, (i64, i64))| {
        captured.lock().unwrap().push(entry.clone());
    });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("sliding windows ParDo pipeline must succeed");

    let mut got = results.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![("k".to_string(), (0, 1)), ("k".to_string(), (5_000, 1)),]
    );
}

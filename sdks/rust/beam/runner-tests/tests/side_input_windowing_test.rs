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

//! Side inputs read from a windowed main input.
//!
//! A side input is materialised by the runner under the window its *view* maps to, not
//! under the window of the element reading it. Views here map to the global window, so a
//! read from inside a fixed window still has to ask for the global window. Asking for the
//! main input's own window instead is quiet: iterable and multimap reads come back empty
//! and only the singleton read complains.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::prelude::*;
use prism::PrismRunner;

/// Assigns explicit event timestamps so window assignment is deterministic.
#[derive(Clone, Default)]
struct AssignTimestampDoFn;

impl DoFn for AssignTimestampDoFn {
    type In = (i64, i64);
    type Out = i64;

    fn process_element(
        &mut self,
        (value, timestamp): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        ctx.output(value).at(timestamp).emit()
    }
}

/// Two values, one in each of two adjacent 10s fixed windows.
fn windowed_main(p: &Pipeline) -> PCollection<i64> {
    p.apply(Create::new(
        "Main",
        vec![(1i64, 1_000i64), (3i64, 11_000i64)],
    ))
    .par_do("AssignTimestamps", AssignTimestampDoFn)
    .apply(WindowInto::new(
        "WindowInto",
        FixedWindows::of(Duration::from_secs(10)),
    ))
}

fn sorted<T: Clone + Ord>(results: &Arc<Mutex<Vec<T>>>) -> Vec<T> {
    let mut got = results.lock().unwrap().clone();
    got.sort();
    got
}

#[tokio::test]
async fn test_global_side_inputs_read_from_fixed_windows() {
    let p = Pipeline::new();
    let iter_results = Arc::new(Mutex::new(Vec::new()));
    let iter_captured = Arc::clone(&iter_results);
    let single_results = Arc::new(Mutex::new(Vec::new()));
    let single_captured = Arc::clone(&single_results);
    let map_results = Arc::new(Mutex::new(Vec::new()));
    let map_captured = Arc::clone(&map_results);

    let main = windowed_main(&p);

    let iter_side = p.apply(Create::new("IterSide", vec![100i64, 200i64]));
    main.clone()
        .with_side_iter("AddSide", &iter_side, |v: i64, side_values| {
            v + side_values.iter().sum::<i64>()
        })
        .inspect("CaptureIter", move |v: &i64| {
            iter_captured.lock().unwrap().push(*v);
        });

    let single_side = p.apply(Create::new("SingleSide", vec![10i64]));
    main.clone()
        .with_side_singleton("MultiplySide", &single_side, |v: i64, factor| v * factor)
        .inspect("CaptureSingle", move |v: &i64| {
            single_captured.lock().unwrap().push(*v);
        });

    let map_side = p.apply(Create::new(
        "MapSide",
        vec![(1i64, "one".to_string()), (3i64, "three".to_string())],
    ));
    main.with_side_map("LookupSide", &map_side, |v: i64, lookup| {
        Ok(lookup(&v)?.first().cloned().unwrap_or_default())
    })
    .inspect("CaptureMap", move |s: &String| {
        map_captured.lock().unwrap().push(s.clone());
    });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("global side inputs under windowing must succeed");

    assert_eq!(sorted(&iter_results), vec![301, 303]);
    assert_eq!(sorted(&single_results), vec![10, 30]);
    assert_eq!(
        sorted(&map_results),
        vec!["one".to_string(), "three".to_string()]
    );
}

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

//! `Flatten` under windowing.
//!
//! Flatten does not touch elements, so its output has to stay windowed the way its
//! inputs were. Putting the output on the pipeline's default strategy instead tells the
//! runner to read interval-windowed elements as global-windowed ones, which decodes into
//! nonsense rather than failing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::prelude::*;
use beam::transforms::Flatten;
use prism::PrismRunner;

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

fn timestamped(
    p: &Pipeline,
    name: &'static str,
    rows: Vec<(String, (i64, i64))>,
) -> PCollection<(String, i64)> {
    p.apply(Create::new(name, rows))
        .par_do(name, AssignTimestampDoFn)
}

#[tokio::test]
async fn test_flatten_preserves_input_windowing() {
    let p = Pipeline::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    let window = || WindowInto::new("WindowInto", FixedWindows::of(Duration::from_secs(10)));

    let left = timestamped(
        &p,
        "Left",
        vec![
            ("k".to_string(), (1, 1_000)),
            ("k".to_string(), (3, 11_000)),
        ],
    )
    .apply(window());

    let right = timestamped(
        &p,
        "Right",
        vec![
            ("k".to_string(), (2, 5_000)),
            ("k".to_string(), (4, 15_000)),
        ],
    )
    .apply(window());

    Flatten::pcollections("FlattenBoth", &[&left, &right])
        .group_by_key("Group")
        .map("Sum", |(k, vs): (String, BeamIterable<i64>)| {
            (k, vs.into_iter().sum::<i64>())
        })
        .inspect("Capture", move |kv: &(String, i64)| {
            captured.lock().unwrap().push(kv.clone());
        });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("flatten of windowed inputs must succeed");

    // 1 and 2 land in [0s,10s); 3 and 4 in [10s,20s). Losing the strategy yields a single
    // ("k", 10), and reading interval windows as global ones yields garbage such as ("", 0).
    let mut got = results.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![("k".to_string(), 3), ("k".to_string(), 7)]);
}

/// Beam requires every Flatten input to agree on its window function. Silently picking
/// one is how the elements of the others end up decoded against the wrong window coder.
#[test]
#[should_panic(expected = "share one window function")]
fn test_flatten_rejects_mismatched_windowing() {
    let p = Pipeline::new();

    let fixed = timestamped(&p, "Fixed", vec![("k".to_string(), (1, 1_000))]).apply(
        WindowInto::new("WindowInto", FixedWindows::of(Duration::from_secs(10))),
    );
    let global = timestamped(&p, "Global", vec![("k".to_string(), (2, 5_000))]);

    Flatten::pcollections("FlattenMismatched", &[&fixed, &global]);
}

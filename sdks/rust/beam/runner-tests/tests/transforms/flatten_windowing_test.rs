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
//! nonsense rather than failing. The registry builder `flatten_preserves_windowing` runs
//! the pipeline; this file checks the construction error.

use std::time::Duration;

use beam::prelude::*;
use beam::transforms::Flatten;
use beam_runner_tests::dofns::AssignTimestampDoFn;

fn timestamped(
    p: &Pipeline,
    name: &'static str,
    rows: Vec<(String, (i64, i64))>,
) -> PCollection<(String, i64)> {
    p.apply(format!("Create{name}"), Create::new(rows))
        .par_do(format!("Timestamp{name}"), AssignTimestampDoFn)
}

/// Beam requires every Flatten input to agree on its window function. Silently picking
/// one is how the elements of the others end up decoded against the wrong window coder.
#[test]
fn test_flatten_rejects_mismatched_windowing() {
    let p = Pipeline::new();

    let fixed = timestamped(&p, "Fixed", vec![("k".to_string(), (1, 1_000))]).apply(
        "WindowInto",
        WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
    );
    let global = timestamped(&p, "Global", vec![("k".to_string(), (2, 5_000))]);

    Flatten::pcollections("FlattenMismatched", &[&fixed, &global]);
    let err = p.validate().unwrap_err();
    assert!(
        err.to_string().contains("share one window function"),
        "{err}"
    );
}

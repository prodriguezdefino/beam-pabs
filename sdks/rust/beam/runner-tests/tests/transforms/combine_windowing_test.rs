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

//! Combine transforms crossed with every window type.
//!
//! Combining aggregates *within a window*. The pre-combine step buffers accumulators
//! across a bundle, so it has to keep one accumulator per key per window assignment and
//! put each one back into the windows it came from. Getting that wrong does not fail the
//! job: the accumulators land in the wrong window, or in no window at all, and the runner
//! reports success having produced nothing. Each test here pins a different way for that
//! to go wrong. The checks that run a pipeline are registry builders, so they run on every
//! runner.

use std::time::Duration;

use beam::prelude::*;

#[test]
fn test_combine_globally_with_defaults_rejects_non_global_windows() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![1i64, 2, 3]))
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .combine_globally("SumAll", Sum::new());
    let err = p.validate().unwrap_err();
    assert!(err.to_string().contains("needs the global window"), "{err}");
}

#[tokio::test]
async fn test_combine_fanout_with_sliding_and_session_windows() {
    use beam::testing::{TestPipeline, passert};
    use beam_runner_prism::PrismRunner;
    use beam_runner_tests::dofns::AssignTimestampDoFn;

    let p = TestPipeline::new();

    // Two impulses flattened so elements arrive in separate bundles and hit different shards;
    // Sessions(10s) must still merge [1s, 11s) and [5s, 15s) across shards in PostCombine.
    let s1 = p
        .apply(
            "SessionPart1",
            Create::new(vec![("k".to_string(), (10i64, 1_000i64))]),
        )
        .par_do("Ts1", AssignTimestampDoFn);
    let s2 = p
        .apply(
            "SessionPart2",
            Create::new(vec![("k".to_string(), (25i64, 5_000i64))]),
        )
        .par_do("Ts2", AssignTimestampDoFn);
    let sessions = Flatten::pcollections("MergeParts", &[&s1, &s2])
        .apply(
            "Sessions",
            WindowInto::new(Sessions::with_gap_duration(Duration::from_secs(10))),
        )
        .apply(
            "SumSessions",
            CombinePerKey::new(Sum::new()).with_hot_key_fanout(4),
        );
    passert::that("MergedSession", &sessions).contains_in_any_order([("k".to_string(), 35i64)]);

    let sliding = p
        .apply(
            "SlidingIn",
            Create::new(vec![
                ("x".to_string(), (3i64, 3_000i64)),
                ("x".to_string(), (7, 7_000)),
            ]),
        )
        .par_do("SlidingTs", AssignTimestampDoFn)
        .map("Values", |(_, v): (String, i64)| v)
        .apply(
            "SlidingWin",
            WindowInto::new(
                SlidingWindows::of(Duration::from_secs(10)).every(Duration::from_secs(5)),
            ),
        )
        .apply(
            "MaxSliding",
            CombineGlobally::new(Max::new())
                .without_defaults()
                .with_fanout(4),
        );
    // 3s falls in [-5, 5) and [0, 10); 7s falls in [0, 10) and [5, 15).
    passert::that("SlidingMaxes", &sliding).contains_in_any_order([3i64, 7, 7]);

    p.run_with(&PrismRunner::new())
        .await
        .expect("windowed fanout pipeline must succeed");
}

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

//! Every window and pane selector can fail.

use beam::prelude::*;
use testing::{TestPipeline, passert};

use crate::{FIRST, assert_fails, kv, one_two_three, pipeline, windowed_counts};

type SelectorCase = (&'static str, &'static str, fn(&TestPipeline));

#[tokio::test]
async fn every_selector_fails_on_the_wrong_pane() {
    let cases: &[SelectorCase] = &[
        (
            "OnlyFirst in window [0, 10000)",
            "expected 3 element(s), but got 2",
            |p| {
                passert::that("OnlyFirst", &windowed_counts(p))
                    .in_window(FIRST)
                    .has_count(3);
            },
        ),
        (
            "Nowhere in window [20000, 30000)",
            "expected at least one element, but got none",
            |p| {
                passert::that("Nowhere", &windowed_counts(p))
                    .in_window(IntervalWindow::new(20_000, 30_000))
                    .not_empty();
            },
        ),
        (
            "OnTime in window [0, 10000) (on-time pane)",
            "expected no elements",
            |p| {
                passert::that("OnTime", &windowed_counts(p))
                    .in_on_time_pane(FIRST)
                    .empty();
            },
        ),
        (
            "Final in window [0, 10000) (final pane)",
            "expected no elements",
            |p| {
                passert::that("Final", &windowed_counts(p))
                    .in_final_pane(FIRST)
                    .empty();
            },
        ),
        (
            "Early in window [0, 10000) (early panes)",
            "expected at least one element, but got none",
            |p| {
                passert::that("Early", &windowed_counts(p))
                    .in_early_panes(FIRST)
                    .not_empty();
            },
        ),
        (
            "Late in window [0, 10000) (late panes)",
            "expected at least one element, but got none",
            |p| {
                passert::that("Late", &windowed_counts(p))
                    .in_late_panes(FIRST)
                    .not_empty();
            },
        ),
        (
            "SingleFirst in window [0, 10000) (on-time pane)",
            "expected exactly one element, but got 2",
            |p| {
                passert::that_singleton("SingleFirst", &windowed_counts(p))
                    .in_on_time_pane(FIRST)
                    .is_equal_to(kv("a", 2));
            },
        ),
        (
            "Global",
            "cannot select a window: the element is in the global window",
            |p| {
                passert::that("Global", &one_two_three(p))
                    .in_window(FIRST)
                    .empty();
            },
        ),
    ];

    for &(name, expected, setup) in cases {
        let p = pipeline();
        setup(&p);
        assert_fails(p, name, expected).await;
    }
}

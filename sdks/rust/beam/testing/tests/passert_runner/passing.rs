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

//! Passing assertions.

use beam::prelude::*;
use testing::{TestPipeline, passert};

use crate::{FIRST, SECOND, kv, nothing, one_two_three, pipeline, windowed_counts};

#[tokio::test]
async fn passing_assertions_run_on_prism_via_the_default_runner() {
    let p = TestPipeline::new();
    let doubled = p
        .apply(Create::new("Create", vec![1i64, 2, 3]))
        .apply(Map::new("Double", |x: i64| x * 2));
    passert::that("PAssert", &doubled).contains_in_any_order([2, 4, 6]);

    p.run().await.expect("assertions should pass");
}

#[tokio::test]
async fn every_check_passes_on_matching_input() {
    let p = pipeline();
    let values = one_two_three(&p);
    passert::that("PAssert", &values)
        .contains_in_any_order([3, 1, 2])
        .contains([2])
        .has_count(3)
        .not_empty()
        .all("positive", |x| *x > 0)
        .satisfies(|xs| match xs.iter().sum::<i64>() {
            6 => Ok(()),
            other => Err(format!("sum {other}").into()),
        });
    passert::that("PAssert", &nothing(&p)).empty().has_count(0);
    passert::that_singleton("PAssert", &p.apply(Create::new("One", vec![7i64])))
        .is_equal_to(7)
        .satisfies(|x| if *x == 7 { Ok(()) } else { Err("not 7".into()) });

    assert_eq!(p.assertion_count(), 10);
    let result = p.run().await.expect("assertions should pass");
    let metrics = result.metrics().expect("Prism reports metrics");
    assert_eq!(passert::assertion_counts(metrics), (10, 0));
    for name in passert::assertion_names(p.pipeline()) {
        assert_eq!(
            metrics.counter(
                passert::PASSERT_NAMESPACE,
                &passert::success_counter_name(&name)
            ),
            Some(1),
            "assertion {name}"
        );
    }
}

#[tokio::test]
async fn every_pane_selector_passes_on_matching_input() {
    let p = pipeline();
    let counts = windowed_counts(&p);
    passert::that("Window", &counts)
        .in_window(FIRST)
        .contains_in_any_order([kv("a", 2), kv("b", 1)]);
    passert::that("OnTime", &counts)
        .in_on_time_pane(FIRST)
        .contains_in_any_order([kv("a", 2), kv("b", 1)]);
    passert::that("Final", &counts)
        .in_final_pane(SECOND)
        .contains_in_any_order([kv("a", 1)]);
    passert::that("Early", &counts)
        .in_early_panes(FIRST)
        .empty();
    passert::that("Late", &counts).in_late_panes(FIRST).empty();
    passert::that_singleton("SingletonWindow", &counts)
        .in_window(SECOND)
        .is_equal_to(kv("a", 1));

    p.run().await.expect("assertions should pass");
}

#[tokio::test]
async fn grouped_assertions_ignore_value_order() {
    let p = pipeline();
    let grouped = p
        .apply(Create::new(
            "Create",
            vec![kv("a", 1), kv("b", 5), kv("a", 2), kv("a", 1)],
        ))
        .apply(GroupByKey::new("Group"));
    passert::that_grouped("PAssert", &grouped)
        .contains_in_any_order([("b".to_string(), vec![5]), ("a".to_string(), vec![2, 1, 1])]);

    p.run().await.expect("assertions should pass");
}

#[tokio::test]
async fn windowed_assertions_pair_elements_with_their_windows() {
    let p = pipeline();
    passert::that_windowed("PAssert", &windowed_counts(&p)).contains_in_any_order([
        (kv("a", 2), FIRST),
        (kv("b", 1), FIRST),
        (kv("a", 1), SECOND),
    ]);

    p.run().await.expect("assertions should pass");
}

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

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{BeamRow, FieldType, FieldValue};
use beam::transforms::ToRows;
use testing::{TestPipeline, passert};

use crate::{FIRST, SECOND, kv, nothing, one_two_three, pipeline, windowed_counts};

#[tokio::test]
async fn passing_assertions_run_on_prism_via_the_default_runner() {
    let p = TestPipeline::new();
    let doubled = p
        .apply("Create", Create::new(vec![1i64, 2, 3]))
        .apply("Double", Map::new(|x: i64| x * 2));
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
    passert::that("PAssertNothing", &nothing(&p))
        .empty()
        .has_count(0);
    passert::that_singleton("PAssertSingleton", &p.apply("One", Create::new(vec![7i64])))
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
        .apply(
            "Create",
            Create::new(vec![kv("a", 1), kv("b", 5), kv("a", 2), kv("a", 1)]),
        )
        .apply("Group", GroupByKey::new());
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

/// The assertion keeps the row coder of its input, so the rows keep their schema.
#[tokio::test]
async fn assertions_on_rows_keep_the_row_schema() {
    let schema = Arc::new(
        Schema::builder()
            .field("word", FieldType::string())
            .field("count", FieldType::int64())
            .build(),
    );
    let row = |word: &str, count: i64| {
        Row::new(
            Arc::clone(&schema),
            vec![
                Some(FieldValue::String(word.to_string())),
                Some(FieldValue::Int64(count)),
            ],
        )
        .expect("the values match the schema")
    };
    let p = pipeline();
    let rows = p.apply("Rows", Create::new(vec![row("a", 1), row("b", 2)]));
    passert::that("PAssertRows", &rows).contains_in_any_order([row("b", 2), row("a", 1)]);

    p.run().await.expect("assertions should pass");
}

#[derive(BeamRow, Clone, Debug, PartialEq)]
struct WordCount {
    word: String,
    count: i64,
}

/// `ToRows` gives the rows the schema of the derived type.
#[tokio::test]
async fn derived_values_become_rows_with_their_schema() {
    let counts = [
        WordCount {
            word: "a".to_string(),
            count: 1,
        },
        WordCount {
            word: "b".to_string(),
            count: 2,
        },
    ];
    let p = pipeline();
    let rows = p
        .apply("Counts", Create::new(counts.to_vec()))
        .apply("ToRows", ToRows::new());
    assert_eq!(rows.row_schema().as_ref(), Some(WordCount::beam_schema()));
    let expected = counts.map(|count| count.to_row().expect("a derived value converts"));
    passert::that("PAssertDerivedRows", &rows).contains_in_any_order(expected);

    p.run().await.expect("assertions should pass");
}

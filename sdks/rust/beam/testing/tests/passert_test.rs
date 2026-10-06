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
    reason = "test helpers; a failure is a test failure"
)]

//! Tests for graph construction, result verification, misuse detection and window
//! decoding in `passert`. None of these tests needs a runner.
//!
//! `tests/passert_runner` executes the assertions on Prism, and shows that each check
//! can fail.

use beam::coders::{Coder, Context, IntervalWindow, IntervalWindowCoder, PaneInfo, WindowedHeader};
use beam::metrics::{MetricResults, MetricsContainer};
use beam::options::PipelineOptions;
use beam::pipeline::URN_GROUP_BY_KEY;
use beam::prelude::*;
use beam::runners::PipelineResult;
use model::pipeline as proto;
use testing::{TestPipeline, passert};

fn components(p: &Pipeline) -> proto::Components {
    p.to_proto().components.unwrap()
}

fn subtransform_names(components: &proto::Components, composite: &str) -> Vec<String> {
    let mut names = components.transforms[composite].subtransforms.clone();
    names.sort();
    names
}

fn result_with_counters(counters: &[(&str, i64)]) -> PipelineResult {
    let container = MetricsContainer::new();
    for (i, (name, value)) in counters.iter().enumerate() {
        container.inc_counter(&format!("t{i}"), passert::PASSERT_NAMESPACE, name, *value);
    }
    PipelineResult::new("job", "DONE").with_metrics(MetricResults::from_container(&container))
}

#[test]
fn assertion_is_a_composite_consuming_its_input() {
    let p = Pipeline::new();
    let values = p.apply(Create::new("Create", vec![1i64, 2, 3]));
    passert::that("PAssert", &values).contains_in_any_order([1, 2, 3]);

    let components = components(&p);
    let composite = &components.transforms["PAssert"];
    assert!(composite.spec.is_none(), "PAssert should be a composite");
    assert_eq!(
        composite.inputs.values().collect::<Vec<_>>(),
        vec![values.id()]
    );
    assert!(composite.outputs.is_empty());
    assert!(
        composite
            .annotations
            .contains_key(passert::PASSERT_ANNOTATION)
    );
    assert_eq!(
        subtransform_names(&components, "PAssert"),
        vec![
            "PAssert/Check",
            "PAssert/Flatten",
            "PAssert/GroupGlobally",
            "PAssert/Key",
            "PAssert/RewindowGlobally",
            "PAssert/Sentinel",
        ]
    );
    assert_eq!(
        components.transforms["PAssert/GroupGlobally"]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        URN_GROUP_BY_KEY
    );
    p.validate().unwrap();
}

#[test]
fn each_check_is_a_separate_uniquely_named_assertion() {
    let p = Pipeline::new();
    let values = p.apply(Create::new("Create", vec![1i64]));
    passert::that("PAssert", &values).has_count(1).not_empty();
    passert::that("PAssert", &values).empty();
    passert::that_singleton(
        "OnlyA",
        &p.apply(Create::new("CreateA", vec!["a".to_string()])),
    )
    .is_equal_to("a".to_string());

    let components = components(&p);
    for name in ["PAssert", "PAssert_2", "PAssert_3"] {
        assert!(
            components.transforms.contains_key(&format!("{name}/Check")),
            "missing check for {name}"
        );
    }
    assert!(components.transforms.contains_key("OnlyA"));
    assert!(components.transforms.contains_key("OnlyA/Check"));
    p.validate().unwrap();
}

#[test]
fn window_selection_adds_a_filter_step() {
    let p = Pipeline::new();
    let values = p.apply(Create::new("Create", vec![1i64]));
    passert::that("Whole", &values).has_count(1);
    passert::that("Windowed", &values)
        .in_on_time_pane(IntervalWindow::new(0, 10))
        .has_count(1);

    let components = components(&p);
    assert!(!components.transforms.contains_key("Whole/SelectWindow"));
    assert!(components.transforms.contains_key("Windowed/SelectWindow"));
    assert!(
        subtransform_names(&components, "Windowed").contains(&"Windowed/SelectWindow".to_string())
    );
    p.validate().unwrap();
}

#[test]
fn verify_success_count_reports_each_outcome() {
    let ok = result_with_counters(&[(passert::SUCCESS_COUNTER, 2), (passert::SUCCESS_COUNTER, 1)]);
    passert::verify_success_count(&ok, 3).unwrap();

    let unrun = result_with_counters(&[(passert::SUCCESS_COUNTER, 2)]);
    let err = passert::verify_success_count(&unrun, 3).unwrap_err();
    assert!(err.contains("expected 3") && err.contains("2 did"), "{err}");

    let failed =
        result_with_counters(&[(passert::SUCCESS_COUNTER, 3), (passert::FAILURE_COUNTER, 1)]);
    let err = passert::verify_success_count(&failed, 3).unwrap_err();
    assert!(err.contains("1 PAssert assertion(s) failed"), "{err}");

    let no_metrics = PipelineResult::new("job", "DONE");
    let err = passert::verify_success_count(&no_metrics, 0).unwrap_err();
    assert_eq!(err, "the runner reported no metrics for this pipeline");
}

#[test]
fn assertion_names_lists_every_assertion_under_its_unique_name() {
    let p = TestPipeline::with_options(PipelineOptions::default()).without_run_enforcement();
    assert_eq!(p.assertion_count(), 0);

    let values = p.apply(Create::new("Create", vec![1i64, 2]));
    passert::that("PAssert", &values).has_count(2).not_empty();
    passert::that("Custom", &values).empty();
    passert::that_singleton("Custom", &p.apply(Create::new("Zero", vec![0i64]))).is_equal_to(0);

    assert_eq!(
        passert::assertion_names(p.pipeline()),
        vec!["Custom", "Custom_2", "PAssert", "PAssert_2"]
    );
    assert_eq!(p.assertion_count(), 4);
    assert_eq!(passert::count_assertions(p.pipeline()), 4);
}

#[test]
fn unchecked_assertions_panic() {
    type UncheckedCase = (&'static str, fn(&Pipeline));
    let cases: [UncheckedCase; 2] = [
        ("that", |p: &Pipeline| {
            let values = p.apply(Create::new("Create", vec![1i64]));
            let _unchecked = passert::that("Forgotten", &values);
        }),
        ("that_singleton", |p: &Pipeline| {
            let values = p.apply(Create::new("Create", vec![1i64]));
            let _unchecked = passert::that_singleton("PAssert", &values);
        }),
    ];
    for (name, setup) in cases {
        let Err(payload) = std::panic::catch_unwind(|| {
            let p = Pipeline::new();
            setup(&p);
        }) else {
            panic!("dropping an unchecked {name} assertion must panic");
        };
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(
            message.contains("was dropped without a check"),
            "{name}: {message:?}"
        );
    }
}

#[test]
fn a_check_through_any_clone_satisfies_the_guard() {
    let p = Pipeline::new();
    let values = p.apply(Create::new("Create", vec![1i64]));
    let builder = passert::that("PAssert", &values);
    let _checked = builder.clone().has_count(1);
    drop(builder);
    assert_eq!(passert::count_assertions(&p), 1);
}

fn encoded(window: IntervalWindow) -> Vec<u8> {
    let mut bytes = Vec::new();
    IntervalWindowCoder
        .encode(&window, &mut bytes, Context::Nested)
        .unwrap();
    bytes
}

#[test]
fn interval_windows_decodes_every_window() {
    let a = IntervalWindow::new(0, 10);
    let b = IntervalWindow::new(5, 15);
    let header = WindowedHeader::new(7, &[encoded(a), encoded(b)], PaneInfo::NO_FIRING);
    assert_eq!(passert::interval_windows(&header).unwrap(), vec![a, b]);
}

#[test]
fn interval_windows_rejects_malformed_headers() {
    let mut trailing = encoded(IntervalWindow::new(0, 10));
    trailing.push(0xff);

    let cases: &[(&str, WindowedHeader, &str)] = &[
        (
            "missing header",
            WindowedHeader::EMPTY.clone(),
            "the element carries no window information",
        ),
        (
            "global window",
            WindowedHeader::global(0, PaneInfo::NO_FIRING),
            "the element is in the global window",
        ),
        (
            "undecodable window",
            WindowedHeader::new(0, &[vec![1, 2, 3]], PaneInfo::NO_FIRING),
            "window 1 of 1 is not a decodable interval window",
        ),
        (
            "trailing bytes",
            WindowedHeader::new(0, &[trailing], PaneInfo::NO_FIRING),
            "1 byte(s) left over after decoding 1 interval window(s)",
        ),
        (
            "truncated header",
            WindowedHeader::from_wire(&[0; 10], 10),
            "the window header is truncated (10 byte(s))",
        ),
    ];

    for (name, header, expected_prefix) in cases {
        let err = passert::interval_windows(header).unwrap_err();
        assert!(
            err.contains(expected_prefix),
            "{name}: expected error containing {expected_prefix:?}, got {err:?}"
        );
    }
}

#[test]
fn grouped_and_windowed_assertions_are_counted() {
    let p = Pipeline::new();
    let kvs = p.apply(Create::new("Create", vec![("a".to_string(), 1i64)]));
    passert::that_grouped("PAssert", &kvs.apply(GroupByKey::new("Group")))
        .contains_in_any_order([("a".to_string(), vec![1])]);
    passert::that_windowed("PAssert", &kvs).contains_in_any_order([]);

    assert_eq!(passert::count_assertions(&p), 2);
    assert!(
        components(&p)
            .transforms
            .contains_key("PAssertReifyWindows")
    );
    p.validate().unwrap();
}

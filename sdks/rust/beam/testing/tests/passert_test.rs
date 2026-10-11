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
use beam::options::PipelineOptions;
use beam::pipeline::URN_GROUP_BY_KEY;
use beam::prelude::*;
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

#[test]
fn assertion_is_a_composite_consuming_its_input() {
    let p = Pipeline::new();
    let values = p.apply("Create", Create::new(vec![1i64, 2, 3]));
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
    let values = p.apply("Create", Create::new(vec![1i64]));
    passert::that("PAssert", &values).has_count(1).not_empty();
    passert::that("PAssertEmpty", &values).empty();
    passert::that_singleton(
        "OnlyA",
        &p.apply("CreateA", Create::new(vec!["a".to_string()])),
    )
    .is_equal_to("a".to_string());

    let components = components(&p);
    // Check `n` of one builder is named `{name}_{n}`.
    for name in ["PAssert", "PAssert_2", "PAssertEmpty"] {
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
fn two_assertions_with_one_name_fail_validation() {
    let p = Pipeline::new();
    let values = p.apply("Create", Create::new(vec![1i64]));
    passert::that("PAssert", &values).has_count(1);
    passert::that("PAssert", &values).not_empty();

    assert_eq!(
        p.validate().unwrap_err(),
        beam::pipeline::PipelineError::DuplicateTransformName {
            name: "PAssert".to_string()
        }
    );
}

#[test]
fn window_selection_adds_a_filter_step() {
    let p = Pipeline::new();
    let values = p.apply("Create", Create::new(vec![1i64]));
    passert::that("Whole", &values).has_count(1);
    passert::that("Windowed", &values)
        .in_on_time_pane(IntervalWindow::new(
            Instant::from_millis(0),
            Instant::from_millis(10),
        ))
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
fn assertion_names_lists_every_assertion_under_its_unique_name() {
    let p = TestPipeline::with_options(PipelineOptions::default()).without_run_enforcement();
    assert_eq!(p.assertion_count(), 0);

    let values = p.apply("Create", Create::new(vec![1i64, 2]));
    passert::that("PAssert", &values).has_count(2).not_empty();
    passert::that("Custom", &values).empty();
    passert::that_singleton("Zero", &p.apply("CreateZero", Create::new(vec![0i64]))).is_equal_to(0);

    assert_eq!(
        passert::assertion_names(p.pipeline()),
        vec!["Custom", "PAssert", "PAssert_2", "Zero"]
    );
    assert_eq!(p.assertion_count(), 4);
    assert_eq!(passert::count_assertions(p.pipeline()), 4);
}

#[test]
fn unchecked_assertions_panic() {
    type UncheckedCase = (&'static str, fn(&Pipeline));
    let cases: [UncheckedCase; 2] = [
        ("that", |p: &Pipeline| {
            let values = p.apply("Create", Create::new(vec![1i64]));
            let _unchecked = passert::that("Forgotten", &values);
        }),
        ("that_singleton", |p: &Pipeline| {
            let values = p.apply("Create", Create::new(vec![1i64]));
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
    let values = p.apply("Create", Create::new(vec![1i64]));
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
    let a = IntervalWindow::new(Instant::from_millis(0), Instant::from_millis(10));
    let b = IntervalWindow::new(Instant::from_millis(5), Instant::from_millis(15));
    let header = WindowedHeader::new(7, &[encoded(a), encoded(b)], PaneInfo::NO_FIRING);
    assert_eq!(passert::interval_windows(&header).unwrap(), vec![a, b]);
}

#[test]
fn interval_windows_rejects_malformed_headers() {
    let mut trailing = encoded(IntervalWindow::new(
        Instant::from_millis(0),
        Instant::from_millis(10),
    ));
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
    let kvs = p.apply("Create", Create::new(vec![("a".to_string(), 1i64)]));
    passert::that_grouped("PAssert", &kvs.apply("Group", GroupByKey::new()))
        .contains_in_any_order([("a".to_string(), vec![1])]);
    passert::that_windowed("PAssertWindowed", &kvs).contains_in_any_order([]);

    assert_eq!(passert::count_assertions(&p), 2);
    assert!(
        components(&p)
            .transforms
            .contains_key("PAssertWindowed/ReifyWindows")
    );
    p.validate().unwrap();
}

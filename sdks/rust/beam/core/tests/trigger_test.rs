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

//! Runner API serialization and continuation tests for all variants of the [`Trigger`] DSL.
//!
//! `windowing_test.rs` covers the triggers that pipelines usually build. This file covers
//! the other variants and the degenerate protos that a runner can send back. No other
//! test exercises them.

use std::time::Duration;

use beam::windowing::{TimestampTransform, Trigger};
use model::pipeline as proto;

/// Asserts that `trigger` survives a `From` / `TryFrom` round trip unchanged.
fn assert_roundtrips(trigger: Trigger) {
    let encoded = proto::Trigger::from(&trigger);
    let decoded = Trigger::try_from(&encoded).expect("a self-encoded trigger must decode");
    assert_eq!(decoded, trigger, "round trip changed the trigger");
}

/// Wraps an inner proto trigger in the `Trigger` message runners exchange.
fn wrap(inner: proto::trigger::Trigger) -> proto::Trigger {
    proto::Trigger {
        trigger: Some(inner),
    }
}

#[test]
fn every_trigger_variant_roundtrips() {
    let leaves = [
        Trigger::Default,
        Trigger::after_end_of_window().into(),
        Trigger::after_processing_time(Duration::from_millis(1_500)),
        Trigger::AfterSynchronizedProcessingTime,
        Trigger::after_count(7),
        Trigger::always(),
        Trigger::never(),
    ];

    for leaf in &leaves {
        assert_roundtrips(leaf.clone());
    }

    // after_each, after_all and after_any nest every leaf, so each recursive arm runs.
    assert_roundtrips(Trigger::repeatedly(Trigger::after_count(3)));
    assert_roundtrips(Trigger::after_each(leaves.to_vec()));
    assert_roundtrips(Trigger::after_all(leaves.to_vec()));
    assert_roundtrips(Trigger::after_any(leaves.to_vec()));
    assert_roundtrips(Trigger::or_finally(
        Trigger::repeatedly(Trigger::after_count(2)),
        Trigger::after_end_of_window(),
    ));
    assert_roundtrips(
        Trigger::after_end_of_window()
            .with_early_firings(Trigger::after_processing_time(Duration::from_secs(30)))
            .with_late_firings(Trigger::repeatedly(Trigger::after_count(1)))
            .into(),
    );
}

fn delay_transform(millis: i64) -> proto::TimestampTransform {
    proto::TimestampTransform {
        timestamp_transform: Some(proto::timestamp_transform::TimestampTransform::Delay(
            proto::timestamp_transform::Delay {
                delay_millis: millis,
            },
        )),
    }
}

#[test]
fn every_trigger_variant_encodes_to_the_runner_api_arm() {
    use proto::trigger::Trigger as Inner;

    let count = |n| {
        wrap(Inner::ElementCount(proto::trigger::ElementCount {
            element_count: n,
        }))
    };
    let leaves = [
        (
            Trigger::Default,
            wrap(Inner::Default(proto::trigger::Default {})),
        ),
        (
            Trigger::after_end_of_window().into(),
            wrap(Inner::AfterEndOfWindow(Box::new(
                proto::trigger::AfterEndOfWindow {
                    early_firings: None,
                    late_firings: None,
                },
            ))),
        ),
        (
            Trigger::after_processing_time(Duration::from_millis(1_500)),
            wrap(Inner::AfterProcessingTime(
                proto::trigger::AfterProcessingTime {
                    timestamp_transforms: vec![delay_transform(1_500)],
                },
            )),
        ),
        (
            Trigger::AfterSynchronizedProcessingTime,
            wrap(Inner::AfterSynchronizedProcessingTime(
                proto::trigger::AfterSynchronizedProcessingTime {},
            )),
        ),
        (Trigger::after_count(7), count(7)),
        (
            Trigger::always(),
            wrap(Inner::Always(proto::trigger::Always {})),
        ),
        (
            Trigger::never(),
            wrap(Inner::Never(proto::trigger::Never {})),
        ),
    ];
    for (trigger, expected) in &leaves {
        assert_eq!(&proto::Trigger::from(trigger), expected, "{trigger:?}");
    }

    let (triggers, protos): (Vec<_>, Vec<_>) = leaves.into_iter().unzip();
    let composites = [
        (
            Trigger::repeatedly(Trigger::after_count(3)),
            wrap(Inner::Repeat(Box::new(proto::trigger::Repeat {
                subtrigger: Some(Box::new(count(3))),
            }))),
        ),
        (
            Trigger::after_each(triggers.clone()),
            wrap(Inner::AfterEach(proto::trigger::AfterEach {
                subtriggers: protos.clone(),
            })),
        ),
        (
            Trigger::after_all(triggers.clone()),
            wrap(Inner::AfterAll(proto::trigger::AfterAll {
                subtriggers: protos.clone(),
            })),
        ),
        (
            Trigger::after_any(triggers),
            wrap(Inner::AfterAny(proto::trigger::AfterAny {
                subtriggers: protos,
            })),
        ),
        (
            Trigger::or_finally(Trigger::after_count(2), Trigger::never()),
            wrap(Inner::OrFinally(Box::new(proto::trigger::OrFinally {
                main: Some(Box::new(count(2))),
                finally: Some(Box::new(wrap(Inner::Never(proto::trigger::Never {})))),
            }))),
        ),
        (
            Trigger::after_end_of_window()
                .with_early_firings(Trigger::after_processing_time(Duration::from_secs(30)))
                .with_late_firings(Trigger::after_count(1))
                .into(),
            wrap(Inner::AfterEndOfWindow(Box::new(
                proto::trigger::AfterEndOfWindow {
                    early_firings: Some(Box::new(wrap(Inner::AfterProcessingTime(
                        proto::trigger::AfterProcessingTime {
                            timestamp_transforms: vec![delay_transform(30_000)],
                        },
                    )))),
                    late_firings: Some(Box::new(count(1))),
                },
            ))),
        ),
    ];
    for (trigger, expected) in composites {
        assert_eq!(proto::Trigger::from(&trigger), expected, "{trigger:?}");
        // The decoder maps the same proto back to the same trigger.
        assert_eq!(Trigger::try_from(&expected).unwrap(), trigger);
    }
}

#[test]
fn early_and_late_firings_leave_the_other_side_unset() {
    // `with_*_firings` exists only on `AfterWatermark`; converting it into a `Trigger`
    // yields `AfterEndOfWindow` with just the configured side.
    let early =
        Trigger::from(Trigger::after_end_of_window().with_early_firings(Trigger::after_count(5)));
    assert!(matches!(
        early,
        Trigger::AfterEndOfWindow {
            early: Some(ref e),
            late: None,
            ..
        } if **e == Trigger::after_count(5)
    ));
    let late =
        Trigger::from(Trigger::after_end_of_window().with_late_firings(Trigger::after_count(9)));
    assert!(matches!(
        late,
        Trigger::AfterEndOfWindow {
            early: None,
            late: Some(ref l),
            ..
        } if **l == Trigger::after_count(9)
    ));
}

#[test]
fn existing_firings_are_replaced_not_merged() {
    let trigger: Trigger = Trigger::after_end_of_window()
        .with_early_firings(Trigger::after_count(1))
        .with_early_firings(Trigger::after_count(2))
        .into();

    assert!(matches!(
        trigger,
        Trigger::AfterEndOfWindow {
            early: Some(ref e),
            late: None,
            ..
        } if **e == Trigger::after_count(2)
    ));
}

#[test]
fn an_absent_trigger_decodes_as_default() {
    // Runners routinely leave the field unset to mean "use the default trigger".
    let decoded =
        Trigger::try_from(&proto::Trigger { trigger: None }).expect("an unset trigger must decode");
    assert_eq!(decoded, Trigger::Default);
}

#[test]
fn processing_time_without_transforms_decodes_with_none() {
    let encoded = wrap(proto::trigger::Trigger::AfterProcessingTime(
        proto::trigger::AfterProcessingTime {
            timestamp_transforms: Vec::new(),
        },
    ));

    assert_eq!(
        Trigger::try_from(&encoded).expect("must decode"),
        Trigger::after_processing_time_with(Vec::new())
    );
}

#[test]
fn processing_time_keeps_every_transform_in_order() {
    let encoded = wrap(proto::trigger::Trigger::AfterProcessingTime(
        proto::trigger::AfterProcessingTime {
            timestamp_transforms: vec![
                proto::TimestampTransform {
                    timestamp_transform: Some(
                        proto::timestamp_transform::TimestampTransform::AlignTo(
                            proto::timestamp_transform::AlignTo {
                                period: 1_000,
                                offset: 0,
                            },
                        ),
                    ),
                },
                proto::TimestampTransform {
                    timestamp_transform: Some(
                        proto::timestamp_transform::TimestampTransform::Delay(
                            proto::timestamp_transform::Delay { delay_millis: 250 },
                        ),
                    ),
                },
            ],
        },
    ));

    let expected = Trigger::after_processing_time_with(vec![
        TimestampTransform::AlignTo {
            period: Duration::from_secs(1),
            offset: Duration::ZERO,
        },
        TimestampTransform::Delay(Duration::from_millis(250)),
    ]);
    assert_eq!(Trigger::try_from(&encoded).expect("must decode"), expected);
    assert_eq!(proto::Trigger::from(&expected), encoded);
}

#[test]
fn a_negative_processing_time_delay_is_clamped_to_zero() {
    // `Duration` cannot be negative; clamping keeps a malformed proto from panicking.
    let encoded = wrap(proto::trigger::Trigger::AfterProcessingTime(
        proto::trigger::AfterProcessingTime {
            timestamp_transforms: vec![proto::TimestampTransform {
                timestamp_transform: Some(proto::timestamp_transform::TimestampTransform::Delay(
                    proto::timestamp_transform::Delay {
                        delay_millis: -5_000,
                    },
                )),
            }],
        },
    ));

    assert_eq!(
        Trigger::try_from(&encoded).expect("must decode"),
        Trigger::after_processing_time(Duration::ZERO)
    );
}

#[test]
fn missing_subtriggers_decode_as_default() {
    let repeat = wrap(proto::trigger::Trigger::Repeat(Box::new(
        proto::trigger::Repeat { subtrigger: None },
    )));
    assert_eq!(
        Trigger::try_from(&repeat).expect("must decode"),
        Trigger::repeatedly(Trigger::Default)
    );

    let or_finally = wrap(proto::trigger::Trigger::OrFinally(Box::new(
        proto::trigger::OrFinally {
            main: None,
            finally: None,
        },
    )));
    assert_eq!(
        Trigger::try_from(&or_finally).expect("must decode"),
        Trigger::or_finally(Trigger::Default, Trigger::Default)
    );
}

#[test]
fn empty_subtrigger_lists_decode_as_empty() {
    let cases = [
        (
            wrap(proto::trigger::Trigger::AfterEach(
                proto::trigger::AfterEach {
                    subtriggers: Vec::new(),
                },
            )),
            Trigger::after_each(Vec::new()),
        ),
        (
            wrap(proto::trigger::Trigger::AfterAll(
                proto::trigger::AfterAll {
                    subtriggers: Vec::new(),
                },
            )),
            Trigger::after_all(Vec::new()),
        ),
        (
            wrap(proto::trigger::Trigger::AfterAny(
                proto::trigger::AfterAny {
                    subtriggers: Vec::new(),
                },
            )),
            Trigger::after_any(Vec::new()),
        ),
    ];

    for (encoded, expected) in cases {
        assert_eq!(Trigger::try_from(&encoded).expect("must decode"), expected);
    }
}

#[test]
fn after_end_of_window_firings_are_optional_independently() {
    let early_only: Trigger = Trigger::after_end_of_window()
        .with_early_firings(Trigger::after_count(4))
        .into();
    let late_only: Trigger = Trigger::after_end_of_window()
        .with_late_firings(Trigger::after_count(4))
        .into();

    let early_proto = proto::Trigger::from(&early_only);
    let late_proto = proto::Trigger::from(&late_only);

    let Some(proto::trigger::Trigger::AfterEndOfWindow(early_inner)) = &early_proto.trigger else {
        panic!("expected an AfterEndOfWindow trigger, got {early_proto:?}");
    };
    assert!(early_inner.early_firings.is_some());
    assert!(early_inner.late_firings.is_none());

    let Some(proto::trigger::Trigger::AfterEndOfWindow(late_inner)) = &late_proto.trigger else {
        panic!("expected an AfterEndOfWindow trigger, got {late_proto:?}");
    };
    assert!(late_inner.early_firings.is_none());
    assert!(late_inner.late_firings.is_some());
}

#[test]
fn element_count_preserves_the_exact_value() {
    for count in [1, 2, 10, 1_000, i32::MAX] {
        let decoded = Trigger::try_from(&proto::Trigger::from(&Trigger::after_count(count)))
            .expect("element count must decode");
        assert_eq!(decoded, Trigger::after_count(count));
    }
}
#[test]
fn element_count_zero_or_negative_fails_conversion() {
    for count in [i32::MIN, -1, 0] {
        let proto = proto::Trigger {
            trigger: Some(proto::trigger::Trigger::ElementCount(
                proto::trigger::ElementCount {
                    element_count: count,
                },
            )),
        };
        assert_eq!(
            Trigger::try_from(&proto),
            Err(beam::windowing::WindowingError::InvalidTriggerCount(count))
        );
    }
}

#[test]
fn continuation_fires_once_for_each_upstream_pane() {
    let delay = Duration::from_secs(5);
    let cases = [
        (Trigger::Default, Trigger::Default),
        (Trigger::Always, Trigger::Always),
        (Trigger::Never, Trigger::Never),
        (Trigger::after_count(100), Trigger::after_count(1)),
        (
            Trigger::after_processing_time(delay),
            Trigger::AfterSynchronizedProcessingTime,
        ),
        (
            Trigger::AfterSynchronizedProcessingTime,
            Trigger::AfterSynchronizedProcessingTime,
        ),
        (
            Trigger::repeatedly(Trigger::after_count(10)),
            Trigger::repeatedly(Trigger::after_count(1)),
        ),
        (
            Trigger::after_end_of_window()
                .with_early_firings(Trigger::after_processing_time(delay))
                .with_late_firings(Trigger::after_count(3))
                .into(),
            Trigger::after_end_of_window()
                .with_early_firings(Trigger::AfterSynchronizedProcessingTime)
                .with_late_firings(Trigger::after_count(1))
                .into(),
        ),
        (
            Trigger::after_each(vec![Trigger::after_count(2), Trigger::after_count(5)]),
            Trigger::repeatedly(Trigger::after_any(vec![
                Trigger::after_count(1),
                Trigger::after_count(1),
            ])),
        ),
        (
            Trigger::after_all(vec![Trigger::after_count(2), Trigger::Default]),
            Trigger::after_all(vec![Trigger::after_count(1), Trigger::Default]),
        ),
        (
            Trigger::after_any(vec![Trigger::after_processing_time(delay)]),
            Trigger::after_any(vec![Trigger::AfterSynchronizedProcessingTime]),
        ),
        (
            Trigger::or_finally(Trigger::after_count(4), Trigger::after_count(9)),
            Trigger::or_finally(Trigger::after_count(1), Trigger::after_count(1)),
        ),
    ];
    for (trigger, expected) in cases {
        assert_eq!(
            trigger.continuation(),
            expected,
            "continuation of {trigger:?}"
        );
    }
}


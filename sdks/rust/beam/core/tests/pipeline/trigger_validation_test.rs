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

//! `Pipeline::validate` rejects a `GroupByKey` input whose root trigger can finish before
//! the window expires, and accepts the trigger trees that never close the window early.

use std::time::Duration;

use beam_core::pipeline::PipelineError;
use beam_core::prelude::*;

const LATENESS: Duration = Duration::from_secs(60);

fn every_minute() -> Trigger {
    Trigger::after_processing_time(Duration::from_secs(60))
}

/// Builds `Create -> WindowInto(trigger, lateness) -> GroupByKey` and validates it.
fn validate_grouped(trigger: Trigger, lateness: Duration) -> Result<(), PipelineError> {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![("k".to_string(), 1_i64)]))
        .apply(
            "Window",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
                .with_trigger(trigger)
                .with_allowed_lateness(lateness),
        )
        .apply("Group", GroupByKey::new());
    p.validate()
}

fn assert_rejected(trigger: &Trigger, lateness: Duration) {
    match validate_grouped(trigger.clone(), lateness) {
        Err(PipelineError::UnsafeTrigger {
            transform_id,
            trigger: shown,
        }) => {
            assert_eq!(transform_id, "Group");
            assert_eq!(shown, format!("{trigger:?}"));
        }
        other => {
            panic!("{trigger:?} with lateness {lateness:?}: expected UnsafeTrigger, got {other:?}")
        }
    }
}

fn assert_accepted(trigger: &Trigger, lateness: Duration) {
    if let Err(err) = validate_grouped(trigger.clone(), lateness) {
        panic!("{trigger:?} with lateness {lateness:?}: expected Ok, got {err:?}");
    }
}

#[test]
fn finishing_root_triggers_are_rejected() {
    let rejected = [
        Trigger::after_count(2),
        every_minute(),
        Trigger::AfterSynchronizedProcessingTime,
        Trigger::after_all(vec![Trigger::repeatedly(Trigger::after_count(1))]),
        Trigger::after_any(vec![Trigger::repeatedly(Trigger::after_count(1))]),
        Trigger::after_each(vec![Trigger::after_count(1), every_minute()]),
        Trigger::or_finally(
            Trigger::repeatedly(Trigger::after_count(1)),
            Trigger::after_count(5),
        ),
        Trigger::or_finally(Trigger::after_count(1), Trigger::never()),
    ];
    for trigger in rejected {
        assert_rejected(&trigger, Duration::ZERO);
        assert_rejected(&trigger, LATENESS);
    }
}

#[test]
fn after_end_of_window_without_late_firings_needs_zero_lateness() {
    let on_time = Trigger::from(Trigger::after_end_of_window());
    let early =
        Trigger::from(Trigger::after_end_of_window().with_early_firings(Trigger::after_count(1)));
    for trigger in [on_time, early] {
        assert_accepted(&trigger, Duration::ZERO);
        assert_rejected(&trigger, LATENESS);
    }
}

#[test]
fn nested_after_end_of_window_is_rejected_with_zero_lateness() {
    let trigger = Trigger::after_each(vec![Trigger::after_end_of_window().into()]);
    assert_rejected(&trigger, Duration::ZERO);
}

#[test]
fn non_finishing_root_triggers_are_accepted() {
    let accepted = [
        Trigger::Default,
        Trigger::always(),
        Trigger::never(),
        Trigger::repeatedly(Trigger::after_count(2)),
        Trigger::repeatedly(Trigger::after_each(vec![
            Trigger::after_count(1),
            every_minute(),
        ])),
        Trigger::after_end_of_window()
            .with_late_firings(Trigger::after_count(1))
            .into(),
        Trigger::after_end_of_window()
            .with_early_firings(every_minute())
            .with_late_firings(Trigger::after_any(vec![
                Trigger::after_count(3),
                every_minute(),
            ]))
            .into(),
        Trigger::after_each(vec![
            Trigger::after_count(1),
            Trigger::repeatedly(Trigger::after_count(2)),
        ]),
        Trigger::or_finally(
            Trigger::repeatedly(Trigger::after_count(1)),
            Trigger::never(),
        ),
    ];
    for trigger in accepted {
        assert_accepted(&trigger, Duration::ZERO);
        assert_accepted(&trigger, LATENESS);
    }
}

#[test]
fn combine_per_key_input_is_checked() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![("k".to_string(), 1_i64)]))
        .apply(
            "Window",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
                .with_trigger(Trigger::after_count(2)),
        )
        .apply("Sum", CombinePerKey::new(Sum::new()));
    match p.validate() {
        Err(PipelineError::UnsafeTrigger { transform_id, .. }) => {
            assert_eq!(transform_id, "Sum/GroupByKey");
        }
        other => panic!("expected UnsafeTrigger, got {other:?}"),
    }
}

#[test]
fn finishing_trigger_without_grouping_is_accepted() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![1_i64]))
        .apply(
            "Window",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10)))
                .with_trigger(Trigger::after_count(2)),
        )
        .apply("Double", Map::new(|n: i64| n * 2));
    p.validate()
        .expect("a trigger has no effect without an aggregation");
}

#[test]
fn may_finish_follows_the_model() {
    let finishing = [
        Trigger::after_count(1),
        every_minute(),
        Trigger::AfterSynchronizedProcessingTime,
        Trigger::after_end_of_window().into(),
        Trigger::after_all(vec![Trigger::always()]),
        Trigger::after_any(vec![Trigger::always()]),
        Trigger::after_each(vec![Trigger::after_count(1), Trigger::after_count(2)]),
        Trigger::or_finally(Trigger::always(), Trigger::after_count(1)),
        Trigger::or_finally(Trigger::after_count(1), Trigger::always()),
    ];
    let never_finishing = [
        Trigger::Default,
        Trigger::always(),
        Trigger::never(),
        Trigger::repeatedly(Trigger::after_count(1)),
        Trigger::after_end_of_window()
            .with_late_firings(Trigger::after_count(1))
            .into(),
        Trigger::after_each(vec![Trigger::after_count(1), Trigger::always()]),
        Trigger::or_finally(Trigger::always(), Trigger::never()),
    ];
    for trigger in finishing {
        assert!(trigger.may_finish(), "{trigger:?} must finish");
    }
    for trigger in never_finishing {
        assert!(!trigger.may_finish(), "{trigger:?} must not finish");
    }
}

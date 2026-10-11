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

//! `Trigger::after_end_of_window()` with early and late firings, converted to the proto.

use std::time::Duration;

use beam::windowing::Trigger;
use model::pipeline::trigger::Trigger as Inner;

#[test]
fn after_watermark_with_early_and_late_firings_converts_both_parts() {
    let trigger: Trigger = Trigger::after_end_of_window()
        .with_early_firings(Trigger::after_processing_time(Duration::from_secs(10)))
        .with_late_firings(Trigger::after_count(1))
        .into();

    let proto = model::pipeline::Trigger::from(&trigger);
    let Some(Inner::AfterEndOfWindow(after)) = &proto.trigger else {
        panic!("expected AfterEndOfWindow, got {proto:?}");
    };
    let early = after.early_firings.as_ref().expect("early firings");
    assert_eq!(
        **early,
        model::pipeline::Trigger::from(&Trigger::after_processing_time(Duration::from_secs(10)))
    );
    let late = after.late_firings.as_ref().expect("late firings");
    assert!(
        matches!(&late.trigger, Some(Inner::ElementCount(c)) if c.element_count == 1),
        "{late:?}"
    );

    assert_eq!(Trigger::try_from(&proto).expect("try_from"), trigger);
}

#[test]
fn after_watermark_with_only_early_firings_leaves_late_unset() {
    let trigger: Trigger = Trigger::after_end_of_window()
        .with_early_firings(Trigger::after_count(3))
        .into();
    let proto = model::pipeline::Trigger::from(&trigger);
    let Some(Inner::AfterEndOfWindow(after)) = &proto.trigger else {
        panic!("expected AfterEndOfWindow, got {proto:?}");
    };
    assert_eq!(
        after.early_firings.as_deref(),
        Some(&model::pipeline::Trigger::from(&Trigger::after_count(3)))
    );
    assert_eq!(after.late_firings, None);
}

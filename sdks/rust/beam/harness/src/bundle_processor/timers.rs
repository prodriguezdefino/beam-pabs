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

//! The timers that a bundle set or cleared, checked against the timers the runner delivers.
//!
//! The runner does not see a change until the bundle sends it, so it can deliver a timer that
//! the bundle already changed. The bundle skips that timer. Before a delivered timer, the
//! bundle fires the timers it set for the same key and window that are due no later.

use std::collections::HashMap;

use beam::Instant;
use beam::coders::{TimerAction, TimerRecord};

/// The last change of one timer of a key: its family, tag and windows.
struct Change {
    /// The index of the family in the plan.
    family: usize,
    tag: String,
    windows: Vec<Vec<u8>>,
    action: TimerAction,
    /// When the bundle last fired the timer inline. It fires inline again only at a later time,
    /// so a callback that sets its own timer at the same time does not loop.
    fired_at: Option<Instant>,
}

/// The timers that a bundle changed, by encoded key. The last change of a timer wins.
#[derive(Default)]
pub(super) struct BundleTimers {
    by_key: HashMap<Vec<u8>, Vec<Change>>,
}

impl BundleTimers {
    /// Records `record` as the last change of its timer in family `family`.
    pub(super) fn record(&mut self, family: usize, record: TimerRecord) {
        let TimerRecord {
            user_key,
            dynamic_tag,
            windows,
            action,
        } = record;
        let changes = self.by_key.entry(user_key).or_default();
        match changes
            .iter_mut()
            .find(|c| c.family == family && c.tag == dynamic_tag && c.windows == windows)
        {
            Some(change) => change.action = action,
            None => changes.push(Change {
                family,
                tag: dynamic_tag,
                windows,
                action,
                fired_at: None,
            }),
        }
    }

    /// True if the bundle changed the timer of `fired` to another firing or cleared it.
    pub(super) fn is_stale(&self, family: usize, fired: &TimerRecord) -> bool {
        self.by_key
            .get(fired.user_key.as_slice())
            .and_then(|changes| {
                changes.iter().find(|c| {
                    c.family == family && c.tag == fired.dynamic_tag && c.windows == fired.windows
                })
            })
            .is_some_and(|change| !same_firing(change.action, fired.action))
    }

    /// Takes the earliest timer that the bundle set for the key and windows of `fired`, in a
    /// family that `eligible` accepts, due at or before `fired`. Records it as cleared.
    pub(super) fn take_due(
        &mut self,
        fired: &TimerRecord,
        eligible: impl Fn(usize) -> bool,
    ) -> Option<(usize, TimerRecord)> {
        let TimerAction::Set {
            fire_timestamp: limit,
            ..
        } = fired.action
        else {
            return None;
        };
        let change = self
            .by_key
            .get_mut(fired.user_key.as_slice())?
            .iter_mut()
            .filter(|c| eligible(c.family) && c.windows == fired.windows)
            .filter_map(|c| match c.action {
                TimerAction::Set {
                    fire_timestamp,
                    hold_timestamp,
                    ..
                } if fire_timestamp <= limit && c.fired_at.is_none_or(|at| fire_timestamp > at) => {
                    Some(((fire_timestamp, hold_timestamp), c))
                }
                TimerAction::Set { .. } | TimerAction::Clear => None,
            })
            .min_by_key(|(order, _)| *order)
            .map(|(_, change)| change)?;
        let action = std::mem::replace(&mut change.action, TimerAction::Clear);
        if let TimerAction::Set { fire_timestamp, .. } = action {
            change.fired_at = Some(fire_timestamp);
        }
        let due = TimerRecord {
            user_key: fired.user_key.clone(),
            dynamic_tag: change.tag.clone(),
            windows: change.windows.clone(),
            action,
        };
        Some((change.family, due))
    }
}

/// True if both actions clear the timer, or set it to the same firing and hold.
fn same_firing(a: TimerAction, b: TimerAction) -> bool {
    match (a, b) {
        (
            TimerAction::Set {
                fire_timestamp: fire_a,
                hold_timestamp: hold_a,
                ..
            },
            TimerAction::Set {
                fire_timestamp: fire_b,
                hold_timestamp: hold_b,
                ..
            },
        ) => fire_a == fire_b && hold_a == hold_b,
        (TimerAction::Clear, TimerAction::Clear) => true,
        (TimerAction::Set { .. }, TimerAction::Clear)
        | (TimerAction::Clear, TimerAction::Set { .. }) => false,
    }
}

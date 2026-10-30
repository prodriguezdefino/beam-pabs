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

//! [`KeyRangeTracker`]: claimed row keys, scan heartbeats and tablet-aware splits of a
//! [`KeyRangeRestriction`].

use std::sync::{Arc, Mutex, PoisonError};

use beam::transforms::sdf::{RestrictionError, RestrictionProgress, RestrictionTracker};
use tokio::sync::Notify;

use super::{KeyRangeRestriction, SplitTarget, TabletBoundary, choose_split};
use crate::bigtable::key;

#[derive(Debug)]
struct State {
    restriction: KeyRangeRestriction,
    last_claimed: Option<Vec<u8>>,
    stopped: bool,
}

/// Tracks claimed row keys and scanned filter heartbeats within a [`KeyRangeRestriction`].
pub struct KeyRangeTracker {
    state: Mutex<State>,
    split_notify: Arc<Notify>,
}

impl KeyRangeTracker {
    /// Creates a tracker for `restriction`.
    pub fn new(restriction: KeyRangeRestriction) -> Self {
        Self {
            state: Mutex::new(State {
                restriction,
                last_claimed: None,
                stopped: false,
            }),
            split_notify: Arc::new(Notify::new()),
        }
    }

    /// Returns a handle that wakes when [`try_split`](Self::try_split) narrows the restriction.
    pub fn split_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.split_notify)
    }

    /// Returns the last claimed or scanned key, if any.
    pub fn last_claimed_key(&self) -> Option<Vec<u8>> {
        self.lock().last_claimed.clone()
    }

    /// Marks the restriction as completely scanned when the server stream reaches the end of
    /// the range without a split stopping it.
    pub fn mark_done(&self) {
        let mut s = self.lock();
        if !s.stopped {
            s.stopped = true;
        }
    }

    /// Claims `position` without allocating a new key buffer when `last_claimed` is already
    /// initialized.
    pub fn try_claim_slice(&self, position: &[u8]) -> bool {
        let mut s = self.lock();
        if s.stopped {
            return false;
        }
        if position < s.restriction.start_key.as_slice() {
            return false;
        }
        if let Some(prev) = &s.last_claimed
            && position < prev.as_slice()
        {
            return false;
        }
        if !s.restriction.end_key.is_empty() && position >= s.restriction.end_key.as_slice() {
            s.stopped = true;
            return false;
        }
        match &mut s.last_claimed {
            Some(buf) => {
                buf.clear();
                buf.extend_from_slice(position);
            }
            None => {
                s.last_claimed = Some(position.to_vec());
            }
        }
        true
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl RestrictionTracker for KeyRangeTracker {
    type Position = Vec<u8>;
    type Restriction = KeyRangeRestriction;

    fn try_claim(&self, position: &Vec<u8>) -> bool {
        self.try_claim_slice(position)
    }

    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Option<(KeyRangeRestriction, KeyRangeRestriction)> {
        if !(0.0..1.0).contains(&fraction_of_remainder) {
            return None;
        }
        let mut s = self.lock();
        if s.stopped {
            return None;
        }
        let unprocessed_start = match &s.last_claimed {
            Some(last) => key::next_key(last),
            None => {
                if fraction_of_remainder <= 0.0 {
                    // Cannot checkpoint at 0.0 before claiming or scanning at least one key,
                    // because primary would be empty.
                    return None;
                }
                s.restriction.start_key.clone()
            }
        };
        if !key::is_valid_range(&unprocessed_start, &s.restriction.end_key) {
            return None;
        }

        // Filter interior tablets to those still ahead of `unprocessed_start`, and rebase
        // their byte offsets relative to `unprocessed_start`.
        let claimed_offset = s
            .restriction
            .tablets
            .iter()
            .rfind(|t| t.end_key.as_slice() <= unprocessed_start.as_slice())
            .map_or(0, |t| t.offset_bytes);
        let remaining_bytes = (s.restriction.estimated_bytes - claimed_offset).max(2);
        let remaining_tablets: Vec<TabletBoundary> = s
            .restriction
            .tablets
            .iter()
            .filter(|t| {
                t.end_key.as_slice() > unprocessed_start.as_slice()
                    && (s.restriction.end_key.is_empty()
                        || t.end_key.as_slice() < s.restriction.end_key.as_slice())
            })
            .map(|t| TabletBoundary {
                end_key: t.end_key.clone(),
                offset_bytes: (t.offset_bytes - claimed_offset).max(1),
            })
            .collect();

        let target = choose_split(
            &unprocessed_start,
            &s.restriction.end_key,
            remaining_bytes,
            &remaining_tablets,
            fraction_of_remainder,
        )?;

        let (split_key, primary_rem_bytes, primary_tablets, residual_tablets) = match target {
            SplitTarget::Checkpoint(key) => {
                s.stopped = true;
                (key, 0, Vec::new(), remaining_tablets)
            }
            SplitTarget::Tablet {
                index,
                key,
                primary_bytes,
            } => {
                let primary_tabs = remaining_tablets[..index].to_vec();
                let residual_tabs = remaining_tablets[index + 1..]
                    .iter()
                    .map(|t| TabletBoundary {
                        end_key: t.end_key.clone(),
                        offset_bytes: (t.offset_bytes - primary_bytes).max(1),
                    })
                    .collect();
                (key, primary_bytes, primary_tabs, residual_tabs)
            }
            SplitTarget::Interpolated { key, primary_bytes } => {
                (key, primary_bytes, Vec::new(), Vec::new())
            }
        };

        let old_end = s.restriction.end_key.clone();
        let primary_estimated = (claimed_offset + primary_rem_bytes).max(1);
        let residual_estimated = (remaining_bytes - primary_rem_bytes).max(1);

        s.restriction.end_key = split_key.clone();
        s.restriction.estimated_bytes = primary_estimated;
        s.restriction.tablets = primary_tablets;

        let primary = s.restriction.clone();
        let residual = KeyRangeRestriction {
            start_key: split_key,
            end_key: old_end,
            estimated_bytes: residual_estimated,
            tablets: residual_tablets,
        };
        drop(s);
        self.split_notify.notify_waiters();
        Some((primary, residual))
    }

    fn current_restriction(&self) -> KeyRangeRestriction {
        self.lock().restriction.clone()
    }

    fn current_progress(&self) -> RestrictionProgress {
        let s = self.lock();
        let total = (s.restriction.estimated_bytes.max(1)) as f64;
        if s.stopped {
            return RestrictionProgress::new(total, 0.0);
        }
        let Some(last) = &s.last_claimed else {
            return RestrictionProgress::new(0.0, total);
        };
        let fraction =
            key::estimate_fraction(&s.restriction.start_key, &s.restriction.end_key, last);
        let completed = total * fraction;
        RestrictionProgress::new(completed, (total - completed).max(0.0))
    }

    fn check_done(&self) -> Result<(), RestrictionError> {
        let s = self.lock();
        if s.stopped || s.last_claimed.is_some() {
            Ok(())
        } else {
            Err(RestrictionError::IncompleteWork(format!(
                "Key range [{:?}, {:?}) finished before scanning or claiming any key",
                s.restriction.start_key, s.restriction.end_key
            )))
        }
    }
}

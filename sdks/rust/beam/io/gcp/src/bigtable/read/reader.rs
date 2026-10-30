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

//! Splittable [`ReadRangesFn`] `DoFn` that reads key ranges through the background
//! `ReadRows` stream task.

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;

use beam::transforms::ProcessContext;
use beam::transforms::sdf::{ProcessContinuation, RestrictionTracker, SplittableDoFn};

pub use super::output::{ReadOutput, read_error_schema};

use super::assembler::ScanEvent;
use super::options::ReadOptions;
use super::restriction::{KeyRangeRestriction, KeyRangeTracker};
use super::stream::{StreamBatch, spawn_range_reader};
use crate::bigtable::client::BigtableClient;
use crate::bigtable::key::{compare_end, is_valid_range, next_key};
use crate::runtime::recv_blocking;

/// Splittable `DoFn` that reads [`KeyRangeRestriction`]s from Cloud Bigtable.
pub struct ReadRangesFn<O> {
    pub options: ReadOptions,
    pub encoded_rows: bool,
    pub client: Option<Arc<BigtableClient>>,
    _marker: PhantomData<fn() -> O>,
}

impl<O> ReadRangesFn<O> {
    pub fn new(
        options: ReadOptions,
        encoded_rows: bool,
        client: Option<Arc<BigtableClient>>,
    ) -> Self {
        Self {
            options,
            encoded_rows,
            client,
            _marker: PhantomData,
        }
    }
}

impl<O> Clone for ReadRangesFn<O> {
    fn clone(&self) -> Self {
        Self::new(self.options.clone(), self.encoded_rows, self.client.clone())
    }
}

impl<O: ReadOutput> ReadRangesFn<O> {
    /// Claims and emits the events of one batch. Returns a continuation when the read must
    /// stop or checkpoint.
    fn process_events(
        &self,
        events: Vec<ScanEvent>,
        tracker: &KeyRangeTracker,
        silent_scan_start: &mut Instant,
        ctx: &mut ProcessContext<'_, O>,
    ) -> beam::Result<Option<ProcessContinuation>> {
        // The assembler puts the rows of a batch before its scan heartbeat.
        if let Some(ScanEvent::Row(_)) = events.first() {
            *silent_scan_start = Instant::now();
        }
        for event in events {
            match event {
                ScanEvent::Row(raw_row) => {
                    if !tracker.try_claim_slice(&raw_row.key) {
                        return Ok(Some(ProcessContinuation::Stop));
                    }
                    O::emit_raw_row(
                        raw_row,
                        self.options.shape,
                        self.encoded_rows,
                        &self.options.target,
                        ctx,
                    )?;
                }
                ScanEvent::Scanned(scanned_key) => {
                    if !tracker.try_claim_slice(&scanned_key) {
                        return Ok(Some(ProcessContinuation::Stop));
                    }
                    if silent_scan_start.elapsed() >= self.options.max_silent_scan_duration
                        && is_valid_range(
                            &next_key(&scanned_key),
                            &tracker.current_restriction().end_key,
                        )
                    {
                        return Ok(Some(ProcessContinuation::Resume));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Emits a permanent stream error and selects the continuation that reads the rest of the
    /// range. See [`ReadOutput::emit_failure`].
    fn on_stream_error(
        &self,
        tracker: &KeyRangeTracker,
        last_key: &[u8],
        message: String,
        ctx: &mut ProcessContext<'_, O>,
    ) -> beam::Result<ProcessContinuation> {
        let KeyRangeRestriction {
            start_key, end_key, ..
        } = tracker.current_restriction();
        let target = &self.options.target;
        match tracker.last_claimed_key() {
            Some(last) if is_valid_range(&next_key(&last), &end_key) => {
                O::emit_failure(target, last_key, message, ctx)?;
                Ok(ProcessContinuation::Resume)
            }
            Some(_) => {
                O::emit_failure(target, last_key, message, ctx)?;
                tracker.mark_done();
                Ok(ProcessContinuation::Stop)
            }
            None => {
                let message = format!(
                    "{message}. The key range [{}, {}) is not read",
                    start_key.escape_ascii(),
                    end_key.escape_ascii()
                );
                O::emit_failure(target, last_key, message, ctx)?;
                tracker.mark_done();
                Ok(ProcessContinuation::Stop)
            }
        }
    }
}

impl<O: ReadOutput> SplittableDoFn for ReadRangesFn<O> {
    type In = KeyRangeRestriction;
    type Out = O;
    type Restriction = KeyRangeRestriction;
    type Tracker = KeyRangeTracker;

    fn initial_restriction(&self, element: &Self::In) -> beam::Result<Self::Restriction> {
        Ok(element.clone())
    }

    fn split_restriction(
        &self,
        _element: &Self::In,
        restriction: &Self::Restriction,
    ) -> Vec<Self::Restriction> {
        if self.options.sub_split_tablets {
            restriction.expand_tablets()
        } else {
            vec![restriction.clone()]
        }
    }

    fn restriction_size(&self, _element: &Self::In, restriction: &Self::Restriction) -> f64 {
        (restriction.estimated_bytes.max(1)) as f64
    }

    fn create_tracker(&self, restriction: &Self::Restriction) -> Self::Tracker {
        KeyRangeTracker::new(restriction.clone())
    }

    fn process_element(
        &mut self,
        _element: Self::In,
        tracker: &Self::Tracker,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result<ProcessContinuation> {
        let current = tracker.current_restriction();
        if !is_valid_range(&current.start_key, &current.end_key) {
            tracker.mark_done();
            return Ok(ProcessContinuation::Stop);
        }

        let (mut rx, _abort_guard) = spawn_range_reader(
            self.client.clone(),
            self.options.clone(),
            current.start_key,
            current.end_key,
            tracker.split_notifier(),
        )?;

        let mut silent_scan_start = Instant::now();

        loop {
            let batch = match recv_blocking(&mut rx) {
                Some(Some(batch)) => batch,
                Some(None) => {
                    tracker.mark_done();
                    return Ok(ProcessContinuation::Stop);
                }
                None => {
                    return Err(beam::Error::msg("The Bigtable reader task panicked"));
                }
            };

            match batch {
                StreamBatch::SplitNotified => {
                    if let Some(last) = tracker.last_claimed_key() {
                        let end_key = tracker.current_restriction().end_key;
                        if compare_end(&last, &end_key) != Ordering::Less {
                            tracker.mark_done();
                            return Ok(ProcessContinuation::Stop);
                        }
                    }
                }
                StreamBatch::Events(events) => {
                    if let Some(continuation) =
                        self.process_events(events, tracker, &mut silent_scan_start, ctx)?
                    {
                        return Ok(continuation);
                    }
                }
                StreamBatch::Error { last_key, message } => {
                    return self.on_stream_error(tracker, &last_key, message, ctx);
                }
            }
        }
    }
}

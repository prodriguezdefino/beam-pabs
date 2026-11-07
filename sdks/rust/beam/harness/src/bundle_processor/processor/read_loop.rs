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

//! The loop that pushes the inbound data and timers of a bundle through the chain.

use std::borrow::Cow;
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use super::{BundleProcessor, emit_timers};
use crate::bundle_processor::BundleError;
use crate::bundle_processor::cache::BundlePlan;
use crate::bundle_processor::chain::{ChainCtx, Instances};
use crate::bundle_processor::decoding::{PortFraming, StreamCarry};
use crate::bundle_processor::timers::BundleTimers;
use crate::bundle_processor::tracker::ActiveBundleTracker;
use crate::data::DataError;
use beam::coders::WindowedHeader;
use beam_model::fn_execution::elements::Timers as ElementTimers;

/// Interval between warnings while a bundle waits for its first element.
const INBOUND_WAIT_WARNING: Duration = Duration::from_secs(15);
/// How long a bundle that has read data may receive nothing before it logs a warning.
const INBOUND_IDLE_WARNING: Duration = Duration::from_secs(60);
/// Waits until the buffered output must go out, or forever when no time limit applies.
async fn flush_deadline(after: Option<Duration>) {
    match after {
        Some(after) => tokio::time::sleep(after).await,
        None => std::future::pending().await,
    }
}
/// A bundle's inbound queues.
pub(super) struct Inputs<'a> {
    pub(super) data: crate::data::InboundReceiver<Vec<u8>>,
    /// `None` when the plan has no timer families.
    pub(super) timers: Option<crate::data::InboundReceiver<ElementTimers>>,
    pub(super) plan: &'a BundlePlan,
    pub(super) instruction_id: &'a str,
}
/// The next message of the timer queue. Without a queue it never completes.
async fn next_timers(
    timers: &mut Option<crate::data::InboundReceiver<ElementTimers>>,
) -> Option<Option<ElementTimers>> {
    match timers {
        Some(timers) => timers.recv().await,
        None => std::future::pending().await,
    }
}
/// How far a bundle has read its inbound data, and the timers it changed.
pub(in crate::bundle_processor) struct ReadProgress {
    /// The trailing partial element of the last chunk. The next chunk completes it.
    pub(super) carry: StreamCarry,
    /// The header of the last element or fired timer.
    pub(in crate::bundle_processor) last_header: WindowedHeader,
    /// The timers that the bundle set or cleared.
    pub(in crate::bundle_processor) timers: BundleTimers,
    pub(super) elements: i64,
    /// The final read index. It is set when the input ends.
    pub(super) final_index: i64,
    pub(super) started: Instant,
    /// True when the runner has taken back the tail of this bundle.
    pub(super) split_reached: bool,
    /// When the last data or timer chunk arrived.
    pub(super) last_input: Instant,
    /// Idle warnings logged since the last chunk.
    pub(super) idle_warnings: u32,
}
impl ReadProgress {
    pub(super) fn new() -> Self {
        let now = Instant::now();
        Self {
            carry: StreamCarry::default(),
            last_header: WindowedHeader::default(),
            timers: BundleTimers::default(),
            elements: 0,
            final_index: 0,
            started: now,
            split_reached: false,
            last_input: now,
            idle_warnings: 0,
        }
    }

    pub(super) fn input_arrived(&mut self) {
        self.last_input = Instant::now();
        self.idle_warnings = 0;
    }
}

impl BundleProcessor {
    /// Pushes the bundle data and timers through the chain until both streams end or the
    /// runner splits off the rest of the bundle.
    pub(super) async fn read_inputs(
        &self,
        ctx: &mut ChainCtx<'_>,
        instances: &mut Instances,
        tracker: &ActiveBundleTracker,
        source_coder_id: &str,
        mut inputs: Inputs<'_>,
        read: &mut ReadProgress,
    ) -> Result<(), BundleError> {
        let instruction_id = inputs.instruction_id;
        let mut data_closed = false;
        let mut timers_closed = inputs.timers.is_none();
        // Each pass restarts the timer, so it fires after a full interval without a pass.
        let wait_warning = tokio::time::sleep(INBOUND_WAIT_WARNING);
        tokio::pin!(wait_warning);
        while !(data_closed && timers_closed) && !read.split_reached {
            let flush_timeout = ctx.outbound.time_until_flush();

            tokio::select! {
                // Input first: a busy bundle flushes by size, not by time.
                biased;
                msg = inputs.data.recv(), if !data_closed => match msg {
                    Some(Some(bytes)) => {
                        read.input_arrived();
                        tracker.set_consuming(true);
                        let framing = inputs.plan.source_framing();
                        let pushed = match self
                            .push_chunk(ctx, instances, tracker, framing, read, bytes)
                            .await
                        {
                            Ok(()) => emit_timers(inputs.plan, ctx, &mut read.timers),
                            failed @ Err(_) => failed,
                        };
                        tracker.set_consuming(false);
                        pushed?;
                        if read.split_reached {
                            debug!(
                                "Stopping instruction '{}' at split boundary (elements processed: {})",
                                instruction_id, read.elements
                            );
                        }
                    }
                    Some(None) => {
                        // Bytes left at the end of the stream are a partial element. The
                        // framing and the runner disagree, so do not drop them silently.
                        let carry = read.carry.bytes();
                        if !carry.is_empty() {
                            return Err(BundleError::Coder(format!(
                                "Inbound data stream closed for instruction '{}' (descriptor: '{}', \
                                 source_coder_id: '{}', elements read: {}) with {} trailing unconsumed \
                                 bytes: {:02x?}",
                                instruction_id,
                                ctx.descriptor.id,
                                source_coder_id,
                                read.elements,
                                carry.len(),
                                &carry[..carry.len().min(64)]
                            )));
                        }
                        debug!(
                            "Inbound data stream closed for instruction '{}' (total read: {})",
                            instruction_id, read.elements
                        );
                        data_closed = true;
                    }
                    // The stream ended before `is_last`. Fail so that the runner retries.
                    None => return Err(DataError::StreamEnded(instruction_id.to_string()).into()),
                },
                msg = next_timers(&mut inputs.timers), if !timers_closed => match msg {
                    Some(Some(element_timers)) => {
                        read.input_arrived();
                        tracker.set_consuming(true);
                        let pushed = match self.push_timer_element(
                            ctx,
                            instances,
                            inputs.plan,
                            &element_timers,
                            read,
                        ) {
                            Ok(()) => ctx.outbound.drain().await.map_err(BundleError::from),
                            failed @ Err(_) => failed,
                        };
                        tracker.set_consuming(false);
                        pushed?;
                    }
                    Some(None) => {
                        debug!("Inbound timer stream closed for instruction '{instruction_id}'");
                        timers_closed = true;
                    }
                    None => return Err(DataError::StreamEnded(instruction_id.to_string()).into()),
                },
                _ = flush_deadline(flush_timeout), if flush_timeout.is_some() => {
                    ctx.outbound.flush().map_err(BundleError::from)?;
                }
                _ = &mut wait_warning => {
                    if read.elements == 0 {
                        warn!(
                            "Still waiting for inbound data on instruction '{}' (descriptor: '{}', elapsed: {}s, buffered_raw_bytes: {})",
                            instruction_id,
                            ctx.descriptor.id,
                            read.started.elapsed().as_secs(),
                            read.carry.bytes().len()
                        );
                    } else if read.last_input.elapsed() >= INBOUND_IDLE_WARNING * (read.idle_warnings + 1) {
                        read.idle_warnings += 1;
                        let inbound = self.data_manager.describe_inbound(instruction_id);
                        warn!(
                            "Instruction '{}' has received no inbound data for {}s (elements read: {}, \
                             data closed: {}, timers closed: {}); {}; {}",
                            instruction_id,
                            read.last_input.elapsed().as_secs(),
                            read.elements,
                            data_closed,
                            timers_closed,
                            tracker.describe_runner_requests(),
                            inbound
                        );
                    }
                }
            }
            wait_warning
                .as_mut()
                .reset(tokio::time::Instant::now() + INBOUND_WAIT_WARNING);
        }
        // Close the queues so that the stream skips this bundle while it finishes.
        inputs.data.close();
        if let Some(timers) = inputs.timers.as_mut() {
            timers.close();
        }
        Ok(())
    }

    /// Decodes one data chunk and pushes each element through the chain before the next, with
    /// no buffer between operators. The read index advances per element for real progress.
    async fn push_chunk(
        &self,
        ctx: &mut ChainCtx<'_>,
        instances: &mut Instances,
        tracker: &ActiveBundleTracker,
        framing: &PortFraming,
        read: &mut ReadProgress,
        bytes: Vec<u8>,
    ) -> Result<(), BundleError> {
        let mut elements = framing.open_chunk(&mut read.carry, Cow::Owned(bytes));
        while let Some(element) = framing.next_element(&mut elements)? {
            // Claim the element before any work on it. The claim fails when the runner has
            // taken back the tail of this bundle; this and all later elements belong to it.
            if !tracker.begin_element() {
                read.split_reached = true;
                break;
            }
            self.push_source_element(
                ctx,
                instances,
                &element.header,
                element.payload(elements.chunk()),
            )?;
            read.last_header = element.header;
            // Drain a full outbound queue here, between elements, to apply back-pressure.
            if ctx.outbound.has_pending() {
                ctx.outbound.drain().await?;
            }
            read.elements += 1;
        }
        framing.close_chunk(&mut read.carry, elements, !read.split_reached)
    }
}

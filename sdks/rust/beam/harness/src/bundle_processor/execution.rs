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

//! Bundle operator lifecycle and timer dispatch.

use std::sync::Arc;

use beam::internals::{MetricsContainer, MetricsScope};
use model::pipeline::PTransform;
use thiserror::Error;

use beam::coders::{TimerAction, TimerRecord, WindowedHeader};

use super::BundleProcessor;
use super::cache::BundlePlan;
use super::chain::{ChainCtx, Instances, OperatorGraph, finish_chain, push_source};
use super::plan::TimerFamily;
use super::processor::{ReadProgress, emit_timers};
use super::sampler::{ExecutionSampler, Phase};
use super::timers::BundleTimers;
use crate::data::DataError;

/// Describes errors that occur during bundle execution.
#[derive(Error, Debug)]
pub enum BundleError {
    /// Indicates a missing DATA_SOURCE transform in the descriptor.
    #[error("Missing DATA_SOURCE transform in ProcessBundleDescriptor '{0}'")]
    MissingSource(String),
    /// Indicates a missing transform handler in the registry.
    #[error("Missing transform handler: {0}")]
    MissingHandler(String),
    /// Indicates payload decode failure.
    #[error("Failed to decode payload: {0}")]
    DecodePayload(#[from] prost::DecodeError),
    /// Indicates a failure on the data stream channel.
    #[error("Data streaming error: {0}")]
    Data(#[from] DataError),
    /// Indicates an element coder failure.
    #[error("Coder error: {0}")]
    Coder(String),
    /// Indicates a transform handler failure.
    #[error("Handler error: {0}")]
    Handler(String),
    /// Indicates a timer encoding failure.
    #[error("Timer encoding error: {0}")]
    TimerCoding(#[source] std::io::Error),
    /// Indicates an unsupported data sink coder.
    #[error("Unsupported data sink coder: {0}")]
    UnsupportedSinkCoder(String),
    /// Indicates that transform setup failed.
    #[error("Setup failed: {0}")]
    Setup(String),
    /// Indicates an invalid bundle operator graph.
    #[error("Invalid bundle graph: {0}")]
    InvalidGraph(String),
}

/// The IDs of all output PCollections of a source transform.
pub(super) fn source_output_pcolls(source_id: &str, source_t: &PTransform) -> Vec<String> {
    if source_t.outputs.is_empty() {
        vec![source_id.to_string()]
    } else {
        source_t.outputs.values().cloned().collect()
    }
}

impl BundleProcessor {
    /// Calls `start_bundle` on the handler instances of the processor, in topological order.
    ///
    /// # Errors
    ///
    /// Returns [`BundleError::Handler`] when a handler fails.
    pub(super) fn start_bundle_handlers(
        &self,
        instruction_id: &str,
        graph: &OperatorGraph,
        instances: &mut Instances,
        metrics_container: &Arc<MetricsContainer>,
        sampler: &ExecutionSampler,
        data_sampler: Option<&super::sampling::DataSampler>,
    ) -> Result<(), BundleError> {
        sampler.set_phase(Phase::Start);
        let result = instances
            .iter_mut(&graph.operator_ids)
            .try_for_each(|(t_id, handler)| {
                let _metrics_scope =
                    MetricsScope::enter(Arc::clone(metrics_container), t_id.into());
                let previous = graph.operator_index(t_id).map(|idx| sampler.enter(idx));
                let res = handler.start_bundle().map_err(BundleError::Handler);
                if let (Err(BundleError::Coder(err) | BundleError::Handler(err)), Some(sampler)) =
                    (&res, data_sampler)
                    && let Some(pcoll_id) = graph.default_output_pcoll(t_id)
                {
                    sampler.record_exception(pcoll_id, instruction_id, t_id, err, None);
                }
                if let Some(prev) = previous {
                    sampler.exit(prev);
                }
                res
            });
        sampler.set_phase(Phase::Process);
        result
    }

    /// Pushes one decoded source element through the entire operator chain. It reaches the
    /// sinks before this call returns, so a return is a checkpoint: all output up to and
    /// including this element is emitted.
    pub(super) fn push_source_element(
        &self,
        ctx: &mut ChainCtx<'_>,
        instances: &mut Instances,
        header: &WindowedHeader,
        payload: &[u8],
    ) -> Result<(), BundleError> {
        let _metrics_scope = ctx.enter_metrics_scope();
        push_source(ctx, instances, header, payload)
    }

    /// Calls `finish_bundle` on all handlers and flushes the last elements to the sinks.
    pub(super) fn finish_bundle_handlers(
        &self,
        chain_ctx: &mut ChainCtx<'_>,
        instances: &mut Instances,
        last_header: &WindowedHeader,
    ) -> Result<(), BundleError> {
        let _metrics_scope = chain_ctx.enter_metrics_scope();
        chain_ctx.sampler.set_phase(Phase::Finish);
        finish_chain(chain_ctx, instances, last_header)
    }

    /// Delivers inbound timers to the operator of their target transform.
    ///
    /// Before each timer, fires the timers that the bundle set for the same key, window and
    /// time domain that are due no later. Skips a timer that the bundle changed.
    pub(super) fn push_timer_element(
        &self,
        ctx: &mut ChainCtx<'_>,
        instances: &mut Instances,
        plan: &BundlePlan,
        timers: &model::fn_execution::elements::Timers,
        read: &mut ReadProgress,
    ) -> Result<(), BundleError> {
        let ReadProgress {
            last_header,
            timers: bundle_timers,
            ..
        } = read;
        let model::fn_execution::elements::Timers {
            transform_id,
            timer_family_id,
            timers: timer_bytes,
            ..
        } = timers;
        if timer_bytes.is_empty() {
            return Ok(());
        }
        let _metrics_scope = ctx.enter_metrics_scope();
        let operator = ctx
            .graph
            .operator_index(transform_id)
            .ok_or_else(|| BundleError::MissingHandler(transform_id.clone()))?;
        let family = plan
            .timer_family_index(timers)
            .and_then(|index| plan.timer_family(index).map(|family| (index, family)));

        let (key_coder_id, window_coder_id) = family
            .and_then(|(_, family)| ctx.descriptor.coders.get(&family.coder_id))
            .and_then(|c| {
                let k = c.component_coder_ids.first()?.as_str();
                let w = c.component_coder_ids.get(1)?.as_str();
                Some((k, w))
            })
            .unwrap_or(("", ""));

        let key_plan = beam::coders::SkipPlan::new(key_coder_id, &ctx.descriptor.coders);
        let window_plan = beam::coders::SkipPlan::new(window_coder_id, &ctx.descriptor.coders);
        let fired = Fired {
            plan,
            operator,
            transform_id,
        };
        let mut cursor = timer_bytes.as_slice();
        while !cursor.is_empty() {
            let record =
                beam::coders::TimerCoder::decode_with(&mut cursor, &key_plan, &window_plan)
                    .map_err(BundleError::TimerCoding)?;
            if let Some((index, family)) = family {
                fire_due_timers(
                    ctx,
                    instances,
                    bundle_timers,
                    &fired,
                    family,
                    &record,
                    last_header,
                )?;
                if bundle_timers.is_stale(index, &record) {
                    continue;
                }
            }
            fire_timer(
                ctx,
                instances,
                &fired,
                timer_family_id,
                &record,
                last_header,
            )?;
            emit_timers(plan, ctx, bundle_timers)?;
        }
        Ok(())
    }
}

/// The plan of the bundle, and the transform and operator that a timer fires in.
struct Fired<'a> {
    plan: &'a BundlePlan,
    operator: usize,
    transform_id: &'a str,
}

/// Fires the timers that the bundle set for the key and windows of `record`, in the transform
/// and time domain of `family`, due at or before `record`. Sends a clear for each first, so the
/// runner does not fire it again.
fn fire_due_timers(
    ctx: &mut ChainCtx<'_>,
    instances: &mut Instances,
    bundle_timers: &mut BundleTimers,
    fired: &Fired<'_>,
    family: &TimerFamily,
    record: &TimerRecord,
    last_header: &mut WindowedHeader,
) -> Result<(), BundleError> {
    let plan = fired.plan;
    let eligible = |index: usize| {
        plan.timer_family(index)
            .is_some_and(|f| f.transform_id == family.transform_id && f.domain == family.domain)
    };
    while let Some((index, mut due)) = bundle_timers.take_due(record, eligible) {
        let Some(due_family) = plan.timer_family(index) else {
            continue;
        };
        let action = std::mem::replace(&mut due.action, TimerAction::Clear);
        ctx.outbound
            .write_timer(&due_family.transform_id, &due_family.family_id, |buf| {
                beam::coders::TimerCoder::encode(&due, buf)
            })?;
        due.action = action;
        fire_timer(
            ctx,
            instances,
            fired,
            &due_family.family_id,
            &due,
            last_header,
        )?;
        emit_timers(plan, ctx, bundle_timers)?;
    }
    Ok(())
}

/// Calls `on_timer` for `record` at the hold of the timer. A cleared timer does nothing.
fn fire_timer(
    ctx: &mut ChainCtx<'_>,
    instances: &mut Instances,
    fired: &Fired<'_>,
    family_id: &str,
    record: &TimerRecord,
    last_header: &mut WindowedHeader,
) -> Result<(), BundleError> {
    let TimerAction::Set {
        hold_timestamp,
        pane,
        ..
    } = record.action
    else {
        return Ok(());
    };
    *last_header = WindowedHeader::new(hold_timestamp.as_millis(), &record.windows, pane);
    let res = super::chain::invoke_operator(
        ctx,
        instances.all(),
        fired.operator,
        last_header,
        Some(record.user_key.as_slice()),
        None,
        |handler, h_ctx| handler.on_timer(family_id, record, h_ctx),
    );
    if let (Err(BundleError::Coder(err) | BundleError::Handler(err)), Some(sampler)) =
        (&res, ctx.data_sampler)
        && let Some(pcoll_id) = ctx.graph.default_output_pcoll(fired.transform_id)
    {
        sampler.record_exception(pcoll_id, ctx.instruction_id, fired.transform_id, err, None);
    }
    res
}

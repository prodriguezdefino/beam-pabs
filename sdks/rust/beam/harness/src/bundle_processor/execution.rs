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

use super::BundleProcessor;
use super::chain::{ChainCtx, Instances, OperatorGraph, finish_chain, push_source};
use super::sampler::{ExecutionSampler, Phase};
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
                let _span = tracing::debug_span!("transform", transform_id = %t_id).entered();
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
        header: &beam::coders::WindowedHeader,
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
        last_header: &beam::coders::WindowedHeader,
    ) -> Result<(), BundleError> {
        let _metrics_scope = chain_ctx.enter_metrics_scope();
        chain_ctx.sampler.set_phase(Phase::Finish);
        finish_chain(chain_ctx, instances, last_header)
    }

    /// Delivers inbound timers to the operator of their target transform.
    pub(super) fn push_timer_element(
        &self,
        ctx: &mut ChainCtx<'_>,
        instances: &mut Instances,
        timers: &model::fn_execution::elements::Timers,
        timer_coder_id: Option<&str>,
        last_header: &mut beam::coders::WindowedHeader,
    ) -> Result<(), BundleError> {
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

        let (key_coder_id, window_coder_id) = timer_coder_id
            .and_then(|cid| ctx.descriptor.coders.get(cid))
            .and_then(|c| {
                let k = c.component_coder_ids.first()?.as_str();
                let w = c.component_coder_ids.get(1)?.as_str();
                Some((k, w))
            })
            .unwrap_or(("", ""));

        let key_plan = beam::coders::SkipPlan::new(key_coder_id, &ctx.descriptor.coders);
        let window_plan = beam::coders::SkipPlan::new(window_coder_id, &ctx.descriptor.coders);
        let mut cursor = std::io::Cursor::new(timer_bytes.as_slice());
        while (cursor.position() as usize) < timer_bytes.len() {
            let record =
                beam::coders::TimerCoder::decode_with(&mut cursor, &key_plan, &window_plan)
                    .map_err(BundleError::TimerCoding)?;

            let beam::coders::TimerAction::Set {
                fire_timestamp,
                pane,
                ..
            } = record.action
            else {
                continue;
            };

            *last_header = beam::coders::WindowedHeader::new(
                fire_timestamp.as_millis(),
                &record.windows,
                pane,
            );

            let res = super::chain::invoke_operator(
                ctx,
                instances.all(),
                operator,
                last_header,
                Some(record.user_key.as_slice()),
                None,
                |handler, h_ctx| handler.on_timer(timer_family_id, &record, h_ctx),
            );
            if let (Err(BundleError::Coder(err) | BundleError::Handler(err)), Some(sampler)) =
                (&res, ctx.data_sampler)
                && let Some(pcoll_id) = ctx.graph.default_output_pcoll(transform_id)
            {
                sampler.record_exception(pcoll_id, ctx.instruction_id, transform_id, err, None);
            }
            res?;
        }
        Ok(())
    }
}

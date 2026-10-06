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

//! Bundle lifecycle, progress, splits and metrics collection.

use std::collections::HashMap;
use std::sync::Arc;

use beam::internals::DynamicSplitHandler;
use model::fn_execution::process_bundle_split_request::DesiredSplit;
use model::fn_execution::process_bundle_split_response::ChannelSplit;
use model::fn_execution::{
    ProcessBundleProgressResponse, ProcessBundleSplitRequest, ProcessBundleSplitResponse,
};
use model::pipeline::{MonitoringInfo, PTransform};
use thiserror::Error;
use tracing::{debug, warn};

use super::BundleProcessor;
use super::chain::{ChainCtx, Instances, OperatorGraph, finish_chain, push_source};
use super::split::{SplitState, compute_split_with_progress};
use crate::data::DataError;

#[derive(Error, Debug)]
pub enum BundleError {
    #[error("Missing DATA_SOURCE transform in ProcessBundleDescriptor '{0}'")]
    MissingSource(String),
    #[error("Missing transform handler: {0}")]
    MissingHandler(String),
    #[error("Failed to decode RemoteGrpcPort payload: {0}")]
    DecodePayload(#[from] prost::DecodeError),
    #[error("Data streaming error: {0}")]
    Data(#[from] DataError),
    #[error("Coder error: {0}")]
    Coder(String),
    #[error("Timer encoding error: {0}")]
    TimerCoding(#[source] std::io::Error),
    #[error("Unsupported data sink coder: {0}")]
    UnsupportedSinkCoder(String),
    #[error("Setup failed: {0}")]
    Setup(String),
    #[error("Invalid bundle graph: {0}")]
    InvalidGraph(String),
}

/// Progress and split state of a running bundle.
#[derive(Clone)]
pub(super) struct ActiveBundleTracker {
    source_id: String,
    split: Arc<std::sync::Mutex<SplitState>>,
    dynamic_split_registrar: Arc<beam::internals::DynamicSplitRegistrar>,
    pcollection_counts: Arc<std::sync::Mutex<HashMap<String, i64>>>,
    pcollection_byte_sizes:
        Arc<std::sync::Mutex<HashMap<String, beam::metrics::DistributionValue>>>,
    transform_msecs: Arc<std::sync::Mutex<HashMap<String, i64>>>,
    metrics_container: Arc<beam::metrics::MetricsContainer>,
}

/// Returns the PCollections a source transform feeds into.
///
/// A source with no declared outputs uses its own id. Runners use that key for a bundle
/// whose source is also its only operator.
pub(super) fn source_output_pcolls(source_id: &str, source_t: &PTransform) -> Vec<String> {
    if source_t.outputs.is_empty() {
        vec![source_id.to_string()]
    } else {
        source_t.outputs.values().cloned().collect()
    }
}

impl ActiveBundleTracker {
    pub(super) fn new(
        source_id: impl Into<String>,
        metrics_container: Arc<beam::metrics::MetricsContainer>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            split: Arc::new(std::sync::Mutex::new(SplitState::default())),
            dynamic_split_registrar: Arc::new(beam::internals::DynamicSplitRegistrar::new()),
            pcollection_counts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            pcollection_byte_sizes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            transform_msecs: Arc::new(std::sync::Mutex::new(HashMap::new())),
            metrics_container,
        }
    }

    pub(super) fn dynamic_split_registrar(&self) -> &Arc<beam::internals::DynamicSplitRegistrar> {
        &self.dynamic_split_registrar
    }

    pub(super) fn begin_element(&self) -> bool {
        self.split
            .lock()
            .is_ok_and(|mut state| state.begin_element())
    }

    pub(super) fn finish(&self, fallback_elements: i64) -> i64 {
        self.split
            .lock()
            .map(|mut state| state.finish())
            .unwrap_or(fallback_elements)
    }

    pub(super) fn sync_from(&self, chain_ctx: &ChainCtx<'_>) {
        if let Ok(mut counts) = self.pcollection_counts.lock() {
            *counts = chain_ctx.stats.counts_by_id(chain_ctx.graph);
        }
        if let Ok(mut sizes) = self.pcollection_byte_sizes.lock() {
            *sizes = chain_ctx.stats.sizes_by_id(chain_ctx.graph);
        }
        if let Ok(mut msecs) = self.transform_msecs.lock() {
            *msecs = chain_ctx
                .sampler
                .msecs(&chain_ctx.graph.operator_ids)
                .map(|(t_id, msecs)| (t_id.to_string(), msecs))
                .collect();
        }
    }
}

impl BundleProcessor {
    /// Returns live progress metrics for a running bundle, or `None` if it is not running.
    pub fn get_bundle_progress(
        &self,
        instruction_id: &str,
    ) -> Option<ProcessBundleProgressResponse> {
        let tracker = self.active_bundle(instruction_id)?;

        // In the Fn API, a progress read_index is the 0-based index of the current
        // element, or -1 before the first element. The executor advances it per element,
        // so the runner can estimate the remaining work.
        let read_index = tracker.split.lock().ok()?.read_index();
        let pcol_counts = tracker.pcollection_counts.lock().ok()?.clone();
        let pcol_bytes = tracker.pcollection_byte_sizes.lock().ok()?.clone();
        let transform_msecs = tracker.transform_msecs.lock().ok()?.clone();
        let mut user_metrics = tracker.metrics_container.to_monitoring_infos();
        // The work left in the current SDF element. Without it, a runner sees one element in
        // progress and cannot tell how much of the bundle is left.
        if let Some(sdf) = tracker.dynamic_split_registrar.current_handler() {
            let work = sdf.current_progress();
            user_metrics.push(beam::metrics::work_completed(
                sdf.transform_id(),
                work.work_completed,
            ));
            user_metrics.push(beam::metrics::work_remaining(
                sdf.transform_id(),
                work.work_remaining,
            ));
        }

        let (monitoring_infos, monitoring_data) = self.collect_bundle_metrics(
            &tracker.source_id,
            read_index,
            pcol_counts,
            pcol_bytes,
            transform_msecs,
            user_metrics,
        );

        Some(ProcessBundleProgressResponse {
            monitoring_infos,
            monitoring_data,
            consuming_received_data: Some(false),
        })
    }

    /// Answers a runner request to give back part of a running bundle.
    ///
    /// Records the split point so that the executor stops before the residual. Returns the
    /// boundary so that the runner can schedule the remainder elsewhere.
    ///
    /// A direct SDF split has the highest priority. It applies when the request names the
    /// active SDF transform. Next is an in-flight SDF split, which splits the input channel
    /// inside the current element. A channel split between element boundaries is last.
    ///
    /// An empty response means that no split is available.
    pub fn try_split(&self, request: &ProcessBundleSplitRequest) -> ProcessBundleSplitResponse {
        let Some(tracker) = self.active_bundle(&request.instruction_id) else {
            debug!(
                "Split requested for unknown or finished instruction '{}'",
                request.instruction_id
            );
            return ProcessBundleSplitResponse::default();
        };

        self.try_direct_sdf_split(&tracker, request)
            .or_else(|| self.try_channel_or_inflight_split(&tracker, request))
            .unwrap_or_default()
    }

    /// Tries a direct dynamic split when the request names the active SDF transform.
    fn try_direct_sdf_split(
        &self,
        tracker: &ActiveBundleTracker,
        request: &ProcessBundleSplitRequest,
    ) -> Option<ProcessBundleSplitResponse> {
        let active_sdf = tracker.dynamic_split_registrar.current_handler()?;
        let desired = request.desired_splits.get(active_sdf.transform_id())?;
        let split = active_sdf.try_split(desired.fraction_of_remainder)?;

        debug!(
            "Direct SDF split on instruction '{}' at transform '{}'",
            request.instruction_id,
            active_sdf.transform_id()
        );

        Some(ProcessBundleSplitResponse {
            primary_roots: vec![split.primary],
            residual_roots: vec![split.residual],
            channel_splits: vec![],
        })
    }

    /// Tries a split of the root input channel. Tries an in-flight SDF split first, then a
    /// split at an element boundary.
    fn try_channel_or_inflight_split(
        &self,
        tracker: &ActiveBundleTracker,
        request: &ProcessBundleSplitRequest,
    ) -> Option<ProcessBundleSplitResponse> {
        let Some(desired) = request.desired_splits.get(&tracker.source_id) else {
            debug!(
                "Split request for instruction '{}' names no known transform (have '{}', got {:?})",
                request.instruction_id,
                tracker.source_id,
                request.desired_splits.keys().collect::<Vec<_>>()
            );
            return None;
        };

        let mut state = tracker.split.lock().ok()?;
        let active_sdf = tracker.dynamic_split_registrar.current_handler();
        let current_element_progress = active_sdf
            .as_ref()
            .map(|sdf| sdf.current_progress().fraction_completed())
            .unwrap_or(if state.index >= 0 { 0.5 } else { 1.0 });

        self.try_inflight_sdf_split(
            tracker,
            request,
            &mut state,
            active_sdf.as_deref(),
            desired,
            current_element_progress,
        )
        .or_else(|| {
            self.try_channel_split(
                tracker,
                request,
                &mut state,
                desired,
                current_element_progress,
            )
        })
    }

    /// Splits the active restriction of the current SDF element.
    fn try_inflight_sdf_split(
        &self,
        tracker: &ActiveBundleTracker,
        request: &ProcessBundleSplitRequest,
        state: &mut SplitState,
        active_sdf: Option<&dyn DynamicSplitHandler>,
        desired: &DesiredSplit,
        current_element_progress: f64,
    ) -> Option<ProcessBundleSplitResponse> {
        if current_element_progress >= 1.0 {
            return None;
        }

        let total = desired
            .estimated_input_elements
            .clamp(state.index + 1, state.stop_index);
        let remainder = total as f64 - state.index as f64 - current_element_progress;
        let keep = remainder * desired.fraction_of_remainder;
        let keep_of_element_remainder = keep / (1.0 - current_element_progress);

        let can_split_at_current = (desired.allowed_split_points.is_empty()
            || desired.allowed_split_points.contains(&state.index))
            && (desired.allowed_split_points.is_empty()
                || desired.allowed_split_points.contains(&(state.index + 1)));

        let sdf = active_sdf.filter(|_| keep_of_element_remainder < 1.0 && can_split_at_current)?;
        let split = sdf.try_split(keep_of_element_remainder)?;

        state.stop_index = state.index + 1;
        debug!(
            "Split instruction '{}' at SDF transform '{}' within element {} (primary: [0, {}], residual: [{}, {}))",
            request.instruction_id,
            sdf.transform_id(),
            state.index,
            state.index - 1,
            state.stop_index,
            total
        );

        Some(ProcessBundleSplitResponse {
            primary_roots: vec![split.primary],
            residual_roots: vec![split.residual],
            channel_splits: vec![ChannelSplit {
                transform_id: tracker.source_id.clone(),
                last_primary_element: state.index - 1,
                first_residual_element: state.stop_index,
            }],
        })
    }

    /// Splits the channel at an element boundary.
    fn try_channel_split(
        &self,
        tracker: &ActiveBundleTracker,
        request: &ProcessBundleSplitRequest,
        state: &mut SplitState,
        desired: &DesiredSplit,
        current_element_progress: f64,
    ) -> Option<ProcessBundleSplitResponse> {
        let point = compute_split_with_progress(
            state,
            desired.estimated_input_elements,
            desired.fraction_of_remainder,
            &desired.allowed_split_points,
            current_element_progress,
        )?;

        state.stop_index = point.first_residual;
        debug!(
            "Split instruction '{}': keeping elements [0, {}], returning [{}, {}) to the runner",
            request.instruction_id,
            point.last_primary,
            point.first_residual,
            desired.estimated_input_elements
        );

        Some(ProcessBundleSplitResponse {
            primary_roots: vec![],
            residual_roots: vec![],
            channel_splits: vec![ChannelSplit {
                transform_id: tracker.source_id.clone(),
                last_primary_element: point.last_primary,
                first_residual_element: point.first_residual,
            }],
        })
    }

    /// Collects monitoring infos and their short-id payloads for the bundle metrics.
    ///
    /// The metrics are the data channel read index, element counts, byte sizes, execution
    /// time and user metrics.
    pub(super) fn collect_bundle_metrics(
        &self,
        source_id: &str,
        total_elements_read: i64,
        pcollection_counts: HashMap<String, i64>,
        pcollection_byte_sizes: HashMap<String, beam::metrics::DistributionValue>,
        transform_msecs: HashMap<String, i64>,
        user_metrics: Vec<MonitoringInfo>,
    ) -> (Vec<MonitoringInfo>, HashMap<String, Vec<u8>>) {
        let system_infos = std::iter::once(beam::metrics::data_channel_read_index(
            source_id,
            total_elements_read,
        ))
        .chain(
            pcollection_counts
                .into_iter()
                .map(|(pcol_id, count)| beam::metrics::element_count(&pcol_id, count)),
        )
        .chain(pcollection_byte_sizes.into_iter().map(|(pcol_id, dist)| {
            beam::metrics::sampled_byte_size(&pcol_id, dist.count, dist.sum, dist.min, dist.max)
        }))
        .chain(
            transform_msecs
                .into_iter()
                .map(|(t_id, msecs)| beam::metrics::process_bundle_msecs(&t_id, msecs)),
        );

        let short_id_cache = self.short_id_cache();
        system_infos
            .filter_map(|res| {
                res.map_err(|err| warn!("Failed to encode monitoring info metric: {err:?}"))
                    .ok()
            })
            .chain(user_metrics)
            .map(|info| {
                let short_id = short_id_cache.get_or_create_short_id(&info);
                (short_id, info)
            })
            .fold(
                (Vec::new(), HashMap::new()),
                |(mut infos, mut data), (short_id, info)| {
                    data.insert(short_id, info.payload.clone());
                    infos.push(info);
                    (infos, data)
                },
            )
    }

    /// Calls `start_bundle` on the handler instances of the processor, in topological order.
    ///
    /// # Errors
    ///
    /// Returns [`BundleError::Coder`] when a handler fails.
    pub(super) fn start_bundle_handlers(
        &self,
        graph: &OperatorGraph,
        instances: &mut Instances,
        metrics_container: &Arc<beam::metrics::MetricsContainer>,
    ) -> Result<(), BundleError> {
        instances
            .iter_mut(&graph.operator_ids)
            .try_for_each(|(t_id, handler)| {
                let _span = tracing::debug_span!("transform", transform_id = %t_id).entered();
                let _metrics_scope =
                    beam::metrics::MetricsScope::enter(Arc::clone(metrics_container), t_id.into());
                handler.start_bundle().map_err(BundleError::Coder)
            })
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

        let mut cursor = std::io::Cursor::new(timer_bytes.as_slice());
        while (cursor.position() as usize) < timer_bytes.len() {
            let record = beam::coders::TimerCoder::decode(
                &mut cursor,
                key_coder_id,
                window_coder_id,
                &ctx.descriptor.coders,
            )
            .map_err(BundleError::TimerCoding)?;

            if record.clear {
                continue;
            }

            *last_header = beam::coders::WindowedHeader::new(
                record.fire_timestamp,
                &record.windows,
                record.pane,
            );

            super::chain::invoke_operator(
                ctx,
                instances.all(),
                operator,
                last_header,
                Some(record.user_key.as_slice()),
                None,
                |handler, h_ctx| handler.on_timer(timer_family_id, &record, h_ctx),
            )?;
        }
        Ok(())
    }
}

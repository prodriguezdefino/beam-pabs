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
use super::sampler::{ExecutionSampler, Phase, TransformExecutionTimes};
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

/// Execution phase of a running bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BundlePhase {
    ReadingInput = 0,
    Finishing = 1,
}

impl std::fmt::Display for BundlePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadingInput => write!(f, "reading input"),
            Self::Finishing => write!(f, "finishing"),
        }
    }
}

/// Status report row for one active bundle.
#[derive(Debug, Clone)]
pub struct ActiveBundleReport {
    pub instruction_id: String,
    pub descriptor_id: String,
    pub current_transform: Option<String>,
    pub lull: std::time::Duration,
    pub elements_read: i64,
    pub phase: BundlePhase,
}

/// Progress and split state of a running bundle.
#[derive(Clone)]
pub(super) struct ActiveBundleTracker {
    descriptor_id: String,
    source_id: String,
    split: Arc<std::sync::Mutex<SplitState>>,
    dynamic_split_registrar: Arc<beam::internals::DynamicSplitRegistrar>,
    pcollection_counts: Arc<std::sync::Mutex<HashMap<String, i64>>>,
    pcollection_byte_sizes:
        Arc<std::sync::Mutex<HashMap<String, beam::metrics::DistributionValue>>>,
    transform_times: Arc<std::sync::Mutex<HashMap<String, TransformExecutionTimes>>>,
    metrics_container: Arc<beam::metrics::MetricsContainer>,
    /// True while the bundle processes received data, false while it waits for more.
    consuming: Arc<std::sync::atomic::AtomicBool>,
    phase: Arc<std::sync::atomic::AtomicU8>,
    sampler: Option<ExecutionSampler>,
    runner_requests: Arc<std::sync::Mutex<RunnerRequests>>,
}

/// The runner's latest progress and split requests for one bundle, for stall reports.
#[derive(Default)]
struct RunnerRequests {
    progress_requests: u64,
    last_progress: Option<std::time::Instant>,
    split_requests: u64,
    last_split: Option<(std::time::Instant, String)>,
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
        descriptor_id: impl Into<String>,
        source_id: impl Into<String>,
        metrics_container: Arc<beam::metrics::MetricsContainer>,
        sampler: Option<ExecutionSampler>,
    ) -> Self {
        Self {
            descriptor_id: descriptor_id.into(),
            source_id: source_id.into(),
            split: Arc::new(std::sync::Mutex::new(SplitState::default())),
            dynamic_split_registrar: Arc::new(beam::internals::DynamicSplitRegistrar::new()),
            pcollection_counts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            pcollection_byte_sizes: Arc::new(std::sync::Mutex::new(HashMap::new())),
            transform_times: Arc::new(std::sync::Mutex::new(HashMap::new())),
            metrics_container,
            consuming: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            phase: Arc::new(std::sync::atomic::AtomicU8::new(
                BundlePhase::ReadingInput as u8,
            )),
            sampler,
            runner_requests: Arc::new(std::sync::Mutex::new(RunnerRequests::default())),
        }
    }

    fn record_progress_request(&self) {
        if let Ok(mut requests) = self.runner_requests.lock() {
            requests.progress_requests += 1;
            requests.last_progress = Some(std::time::Instant::now());
        }
    }

    fn record_split_request(&self, summary: String) {
        if let Ok(mut requests) = self.runner_requests.lock() {
            requests.split_requests += 1;
            requests.last_split = Some((std::time::Instant::now(), summary));
        }
    }

    /// Describes the runner's requests for this bundle and its read state, for stall reports.
    pub(super) fn describe_runner_requests(&self) -> String {
        let (index, stop) = self
            .split
            .lock()
            .map(|s| (s.index, s.stop_index))
            .unwrap_or((-1, -1));
        let consuming = self.consuming.load(std::sync::atomic::Ordering::Relaxed);
        self.runner_requests.lock().map_or_else(
            |_| "runner requests: unavailable".to_string(),
            |r| {
                let age = |at: Option<std::time::Instant>| {
                    at.map_or_else(
                        || "never".to_string(),
                        |t| format!("{}s ago", t.elapsed().as_secs()),
                    )
                };
                let split = r.last_split.as_ref().map_or_else(
                    || "never".to_string(),
                    |(at, summary)| format!("{}s ago: {summary}", at.elapsed().as_secs()),
                );
                format!(
                    "read index: {index}, stop index: {stop}, consuming: {consuming}; \
                     progress requests: {} (last {}); split requests: {} (last {split})",
                    r.progress_requests,
                    age(r.last_progress),
                    r.split_requests
                )
            },
        )
    }

    /// Sets the current execution phase of the bundle.
    pub(super) fn set_phase(&self, phase: BundlePhase) {
        self.phase
            .store(phase as u8, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns the current execution phase of the bundle.
    pub(super) fn phase(&self) -> BundlePhase {
        match self.phase.load(std::sync::atomic::Ordering::Relaxed) {
            1 => BundlePhase::Finishing,
            _ => BundlePhase::ReadingInput,
        }
    }

    /// Returns the number of elements read by this bundle so far.
    pub(super) fn elements_read(&self) -> i64 {
        self.split
            .lock()
            .map(|s| if s.index >= 0 { s.index + 1 } else { 0 })
            .unwrap_or(0)
    }

    /// Records whether the bundle processes received data (`true`) or waits for more (`false`).
    pub(super) fn set_consuming(&self, consuming: bool) {
        self.consuming
            .store(consuming, std::sync::atomic::Ordering::Relaxed);
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
        if let Ok(mut times) = self.transform_times.lock() {
            *times = chain_ctx
                .sampler
                .execution_times(&chain_ctx.graph.operator_ids)
                .map(|(t_id, t_times)| (t_id.to_string(), t_times))
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
        tracker.record_progress_request();

        // In the Fn API, a progress read_index is the 0-based index of the current
        // element, or -1 before the first element. The executor advances it per element,
        // so the runner can estimate the remaining work.
        let read_index = tracker.split.lock().ok()?.read_index();
        let pcol_counts = tracker.pcollection_counts.lock().ok()?.clone();
        let pcol_bytes = tracker.pcollection_byte_sizes.lock().ok()?.clone();
        let transform_times = tracker.transform_times.lock().ok()?.clone();
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
            transform_times,
            user_metrics,
        );

        Some(ProcessBundleProgressResponse {
            monitoring_infos,
            monitoring_data,
            consuming_received_data: Some(
                tracker.consuming.load(std::sync::atomic::Ordering::Relaxed),
            ),
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
        let desired: Vec<String> = request
            .desired_splits
            .iter()
            .map(|(transform_id, d)| {
                format!(
                    "{transform_id}: fraction {:.3}, estimated {}, allowed {}",
                    d.fraction_of_remainder,
                    d.estimated_input_elements,
                    d.allowed_split_points.len()
                )
            })
            .collect();
        let Some(tracker) = self.active_bundle(&request.instruction_id) else {
            debug!(
                "Split request for unknown or finished instruction '{}' ({}): no split",
                request.instruction_id,
                desired.join("; ")
            );
            return ProcessBundleSplitResponse::default();
        };

        let response = self
            .try_direct_sdf_split(&tracker, request)
            .or_else(|| self.try_channel_or_inflight_split(&tracker, request))
            .unwrap_or_default();
        let channel_splits: Vec<String> = response
            .channel_splits
            .iter()
            .map(|c| {
                format!(
                    "{}: last primary {}, first residual {}",
                    c.transform_id, c.last_primary_element, c.first_residual_element
                )
            })
            .collect();
        let summary = format!(
            "({}) at element {}: {} primary roots, {} residual roots, channel splits [{}]",
            desired.join("; "),
            tracker.split.lock().map(|s| s.index).unwrap_or(-1),
            response.primary_roots.len(),
            response.residual_roots.len(),
            channel_splits.join("; ")
        );
        debug!(
            "Split request for instruction '{}' {summary}",
            request.instruction_id
        );
        tracker.record_split_request(summary);
        response
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
            residual_roots: split.residuals,
            channel_splits: vec![],
        })
    }

    /// A copy of the active bundles for the status report, so formatting holds no lock.
    pub fn active_bundle_reports(&self) -> Vec<ActiveBundleReport> {
        let Ok(active) = self.active_bundles.read() else {
            return Vec::new();
        };
        let mut reports: Vec<ActiveBundleReport> = active
            .iter()
            .map(|(inst_id, tracker)| {
                let running = tracker.sampler.as_ref().and_then(|s| s.running());
                let (current_transform, lull) = match running {
                    Some((transform, lull)) => (Some(transform.to_string()), lull),
                    None => (None, std::time::Duration::ZERO),
                };
                ActiveBundleReport {
                    instruction_id: inst_id.clone(),
                    descriptor_id: tracker.descriptor_id.clone(),
                    current_transform,
                    lull,
                    elements_read: tracker.elements_read(),
                    phase: tracker.phase(),
                }
            })
            .collect();
        reports.sort_by_key(|a| std::cmp::Reverse(a.lull));
        reports
    }

    /// Returns open named data stream IDs from the data manager.
    pub fn open_named_streams(&self) -> Vec<String> {
        self.data_manager.open_named_streams()
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
            residual_roots: split.residuals,
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
        transform_times: HashMap<String, TransformExecutionTimes>,
        user_metrics: Vec<MonitoringInfo>,
    ) -> (Vec<MonitoringInfo>, HashMap<String, Vec<u8>>) {
        let execution_infos = transform_times
            .into_iter()
            .flat_map(|(t_id, times)| {
                [
                    (times.start_msecs > 0).then(|| {
                        beam::metrics::system::start_bundle_msecs(&t_id, times.start_msecs)
                    }),
                    (times.process_msecs > 0).then(|| {
                        beam::metrics::system::process_bundle_msecs(&t_id, times.process_msecs)
                    }),
                    (times.finish_msecs > 0).then(|| {
                        beam::metrics::system::finish_bundle_msecs(&t_id, times.finish_msecs)
                    }),
                    (times.total_msecs > 0)
                        .then(|| beam::metrics::system::total_msecs(&t_id, times.total_msecs)),
                ]
            })
            .flatten()
            .collect::<Vec<_>>();

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
        .chain(execution_infos);

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
        instruction_id: &str,
        graph: &OperatorGraph,
        instances: &mut Instances,
        metrics_container: &Arc<beam::metrics::MetricsContainer>,
        sampler: &ExecutionSampler,
        data_sampler: Option<&super::sampling::DataSampler>,
    ) -> Result<(), BundleError> {
        sampler.set_phase(Phase::Start);
        let result = instances
            .iter_mut(&graph.operator_ids)
            .try_for_each(|(t_id, handler)| {
                let _span = tracing::debug_span!("transform", transform_id = %t_id).entered();
                let _metrics_scope =
                    beam::metrics::MetricsScope::enter(Arc::clone(metrics_container), t_id.into());
                let previous = graph.operator_index(t_id).map(|idx| sampler.enter(idx));
                let res = handler.start_bundle().map_err(BundleError::Coder);
                if let (Err(BundleError::Coder(err)), Some(sampler)) = (&res, data_sampler) {
                    let pcoll_id = graph.default_output_pcoll(t_id).unwrap_or(t_id);
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

            let res = super::chain::invoke_operator(
                ctx,
                instances.all(),
                operator,
                last_header,
                Some(record.user_key.as_slice()),
                None,
                |handler, h_ctx| handler.on_timer(timer_family_id, &record, h_ctx),
            );
            if let (Err(BundleError::Coder(err)), Some(sampler)) = (&res, ctx.data_sampler) {
                let pcoll_id = ctx
                    .graph
                    .default_output_pcoll(transform_id)
                    .unwrap_or(transform_id);
                sampler.record_exception(pcoll_id, ctx.instruction_id, transform_id, err, None);
            }
            res?;
        }
        Ok(())
    }
}

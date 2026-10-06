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

//! Active bundle tracking, progress reporting and metrics collection.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use beam::internals::{DynamicSplitRegistrar, DynamicSplitResult, MetricsContainer};
use model::fn_execution::process_bundle_split_response::ChannelSplit;
use model::fn_execution::{
    ProcessBundleProgressResponse, ProcessBundleSplitRequest, ProcessBundleSplitResponse,
};
use model::pipeline::MonitoringInfo;
use tracing::warn;

use super::BundleProcessor;
use super::chain::ChainCtx;
use super::roots::RootEncoder;
use super::sampler::{ExecutionSampler, TransformExecutionTimes};
use super::split::SplitState;

/// Execution phase of an active bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BundlePhase {
    /// Bundle processor reads inputs from the data channel.
    ReadingInput = 0,
    /// Bundle processor finishes active transforms and flushes outputs.
    Finishing = 1,
}

impl From<BundlePhase> for u8 {
    fn from(phase: BundlePhase) -> Self {
        phase as Self
    }
}

impl TryFrom<u8> for BundlePhase {
    type Error = u8;

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0 => Ok(Self::ReadingInput),
            1 => Ok(Self::Finishing),
            other => Err(other),
        }
    }
}

impl fmt::Display for BundlePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadingInput => write!(f, "reading input"),
            Self::Finishing => write!(f, "finishing"),
        }
    }
}

/// Status report row for one active bundle.
#[derive(Debug, Clone)]
pub struct ActiveBundleReport {
    /// The instruction ID of the bundle.
    pub instruction_id: String,
    /// The process bundle descriptor ID.
    pub descriptor_id: String,
    /// The transform ID active when sampled.
    pub current_transform: Option<String>,
    /// How long the bundle spent in the current state.
    pub lull: Duration,
    /// Number of elements read so far.
    pub elements_read: i64,
    /// The current execution phase of the bundle.
    pub phase: BundlePhase,
}

/// The progress metrics of a bundle at a point in time.
#[derive(Default, Clone)]
pub(super) struct ProgressSnapshot {
    pub(super) pcollection_counts: HashMap<String, i64>,
    pub(super) pcollection_byte_sizes: HashMap<String, beam::metrics::DistributionValue>,
    pub(super) transform_times: HashMap<String, TransformExecutionTimes>,
}

/// Progress and split state of an active bundle.
#[derive(Clone)]
pub(super) struct ActiveBundleTracker {
    pub(super) descriptor_id: String,
    pub(super) source_id: String,
    /// Rewrites split applications into the wire encoding of the source port.
    /// Writes split roots in the names and the encoding of the runner.
    roots: Arc<RootEncoder>,
    /// Why a granted split could not be sent. The SDF already narrowed its restriction then,
    /// so the bundle must fail and the runner retries it.
    split_failure: Arc<OnceLock<String>>,
    pub(super) split: Arc<Mutex<SplitState>>,
    pub(super) dynamic_split_registrar: Arc<DynamicSplitRegistrar>,
    progress_snapshot: Arc<Mutex<ProgressSnapshot>>,
    metrics_container: Arc<MetricsContainer>,
    /// True while the bundle processes received data, false while it waits for more.
    consuming: Arc<AtomicBool>,
    phase: Arc<AtomicU8>,
    pub(super) current_index: Arc<AtomicI64>,
    sampler: Option<ExecutionSampler>,
    runner_requests: Arc<Mutex<RunnerRequests>>,
}

/// The runner's latest progress and split requests for one bundle, for stall reports.
#[derive(Default)]
struct RunnerRequests {
    progress_requests: u64,
    last_progress: Option<Instant>,
    split_requests: u64,
    last_split: Option<SplitRecord>,
}

/// A split request of the runner and the answer of the bundle.
///
/// The record keeps typed values. It makes text only when a stall report or an enabled debug
/// line formats it, so other readers (for example a status page) can use the same values.
pub(super) struct SplitRecord {
    at: Instant,
    requested: Vec<RequestedSplit>,
    /// The index of the element that the bundle read when it answered.
    index: i64,
    primary_roots: usize,
    residual_roots: usize,
    channel_splits: Vec<ChannelBoundary>,
}

impl SplitRecord {
    pub(super) fn new(
        request: &ProcessBundleSplitRequest,
        index: i64,
        response: &ProcessBundleSplitResponse,
    ) -> Self {
        Self {
            at: Instant::now(),
            requested: RequestedSplit::all(request),
            index,
            primary_roots: response.primary_roots.len(),
            residual_roots: response.residual_roots.len(),
            channel_splits: response
                .channel_splits
                .iter()
                .map(ChannelBoundary::from)
                .collect(),
        }
    }
}

impl fmt::Display for SplitRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({}) at element {}: {} primary roots, {} residual roots, channel splits [{}]",
            Joined(&self.requested),
            self.index,
            self.primary_roots,
            self.residual_roots,
            Joined(&self.channel_splits)
        )
    }
}

/// One split that the runner asked for, at one transform.
pub(super) struct RequestedSplit {
    transform_id: String,
    fraction_of_remainder: f64,
    estimated_input_elements: i64,
    allowed_split_points: usize,
}

impl RequestedSplit {
    pub(super) fn all(request: &ProcessBundleSplitRequest) -> Vec<Self> {
        request
            .desired_splits
            .iter()
            .map(|(transform_id, desired)| Self {
                transform_id: transform_id.clone(),
                fraction_of_remainder: desired.fraction_of_remainder,
                estimated_input_elements: desired.estimated_input_elements,
                allowed_split_points: desired.allowed_split_points.len(),
            })
            .collect()
    }
}

impl fmt::Display for RequestedSplit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: fraction {:.3}, estimated {}, allowed {}",
            self.transform_id,
            self.fraction_of_remainder,
            self.estimated_input_elements,
            self.allowed_split_points
        )
    }
}

/// Where the bundle divided its input channel in the answer to a split request.
struct ChannelBoundary {
    transform_id: String,
    last_primary_element: i64,
    first_residual_element: i64,
}

impl From<&ChannelSplit> for ChannelBoundary {
    fn from(split: &ChannelSplit) -> Self {
        Self {
            transform_id: split.transform_id.clone(),
            last_primary_element: split.last_primary_element,
            first_residual_element: split.first_residual_element,
        }
    }
}

impl fmt::Display for ChannelBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: last primary {}, first residual {}",
            self.transform_id, self.last_primary_element, self.first_residual_element
        )
    }
}

/// Formats a list of values, with `; ` between them.
pub(super) struct Joined<'a, T>(pub(super) &'a [T]);

impl<T: fmt::Display> fmt::Display for Joined<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().enumerate().try_for_each(|(i, item)| match i {
            0 => write!(f, "{item}"),
            _ => write!(f, "; {item}"),
        })
    }
}

impl ActiveBundleTracker {
    pub(super) fn new(
        descriptor_id: impl Into<String>,
        source_id: impl Into<String>,
        roots: Arc<RootEncoder>,
        metrics_container: Arc<MetricsContainer>,
        sampler: Option<ExecutionSampler>,
    ) -> Self {
        Self {
            descriptor_id: descriptor_id.into(),
            source_id: source_id.into(),
            roots,
            split_failure: Arc::new(OnceLock::new()),
            split: Arc::new(Mutex::new(SplitState::default())),
            dynamic_split_registrar: Arc::new(DynamicSplitRegistrar::new()),
            progress_snapshot: Arc::new(Mutex::new(ProgressSnapshot::default())),
            metrics_container,
            consuming: Arc::new(AtomicBool::new(false)),
            phase: Arc::new(AtomicU8::new(u8::from(BundlePhase::ReadingInput))),
            current_index: Arc::new(AtomicI64::new(-1)),
            sampler,
            runner_requests: Arc::new(Mutex::new(RunnerRequests::default())),
        }
    }

    /// The roots of an SDF split for the runner. On failure, records the reason for
    /// [`split_failure`](Self::split_failure) and returns `None`, so the runner gets no split.
    pub(super) fn split_roots(
        &self,
        split: DynamicSplitResult,
    ) -> Option<ProcessBundleSplitResponse> {
        self.roots
            .split(split)
            .inspect_err(|e| {
                warn!("Refusing an SDF split that cannot be encoded for the runner: {e}");
                let _first = self.split_failure.set(e.to_string());
            })
            .ok()
    }

    /// Why a granted split could not be sent, if one could not.
    pub(super) fn split_failure(&self) -> Option<&str> {
        self.split_failure.get().map(String::as_str)
    }

    fn record_progress_request(&self) {
        if let Ok(mut requests) = self.runner_requests.lock() {
            requests.progress_requests += 1;
            requests.last_progress = Some(Instant::now());
        }
    }

    pub(super) fn record_split_request(&self, record: SplitRecord) {
        if let Ok(mut requests) = self.runner_requests.lock() {
            requests.split_requests += 1;
            requests.last_split = Some(record);
        }
    }

    /// Describes the runner's requests for this bundle and its read state, for stall reports.
    pub(super) fn describe_runner_requests(&self) -> String {
        let index = self.current_index.load(Ordering::Relaxed);
        let stop = self.split.lock().map(|s| s.stop_index).unwrap_or(i64::MAX);
        let consuming = self.consuming.load(Ordering::Relaxed);
        self.runner_requests.lock().map_or_else(
            |_| "runner requests: unavailable".to_string(),
            |r| {
                let age = |at: Option<Instant>| {
                    at.map_or_else(
                        || "never".to_string(),
                        |t| format!("{}s ago", t.elapsed().as_secs()),
                    )
                };
                let split = r.last_split.as_ref().map_or_else(
                    || "never".to_string(),
                    |record| format!("{}s ago: {record}", record.at.elapsed().as_secs()),
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

    pub(super) fn channel_split_response(
        &self,
        last_primary_element: i64,
        first_residual_element: i64,
        mut response: ProcessBundleSplitResponse,
    ) -> ProcessBundleSplitResponse {
        response.channel_splits.push(ChannelSplit {
            transform_id: self.source_id.clone(),
            last_primary_element,
            first_residual_element,
        });
        response
    }

    /// Sets the current execution phase of the bundle.
    pub(super) fn set_phase(&self, phase: BundlePhase) {
        self.phase.store(u8::from(phase), Ordering::Relaxed);
    }

    /// Returns the current execution phase of the bundle.
    pub(super) fn phase(&self) -> BundlePhase {
        BundlePhase::try_from(self.phase.load(Ordering::Relaxed))
            .unwrap_or(BundlePhase::ReadingInput)
    }

    /// Returns the number of elements read by this bundle so far.
    pub(super) fn elements_read(&self) -> i64 {
        let index = self.current_index.load(Ordering::Relaxed);
        if index >= 0 { index + 1 } else { 0 }
    }

    /// Records whether the bundle processes received data (`true`) or waits for more (`false`).
    pub(super) fn set_consuming(&self, consuming: bool) {
        self.consuming.store(consuming, Ordering::Relaxed);
    }

    pub(super) fn dynamic_split_registrar(&self) -> &Arc<DynamicSplitRegistrar> {
        &self.dynamic_split_registrar
    }

    pub(super) fn begin_element(&self) -> bool {
        if let Ok(mut state) = self.split.lock() {
            let ok = state.begin_element();
            self.current_index.store(state.index, Ordering::Relaxed);
            ok
        } else {
            true
        }
    }

    pub(super) fn finish(&self, fallback_elements: i64) -> i64 {
        if let Ok(mut state) = self.split.lock() {
            state.finish()
        } else {
            fallback_elements
        }
    }

    pub(super) fn sync_from(&self, chain_ctx: &ChainCtx<'_>) {
        if let Ok(mut snapshot) = self.progress_snapshot.lock() {
            snapshot.pcollection_counts = chain_ctx.stats.counts_by_id(chain_ctx.graph);
            snapshot.pcollection_byte_sizes = chain_ctx.stats.sizes_by_id(chain_ctx.graph);
            snapshot.transform_times = chain_ctx
                .sampler
                .execution_times(&chain_ctx.graph.operator_ids)
                .map(|(t_id, t_times)| (t_id.to_string(), t_times))
                .collect();
        }
    }
}

impl BundleProcessor {
    /// Returns live progress metrics for an active bundle, or `None` if it is not active.
    pub fn get_bundle_progress(
        &self,
        instruction_id: &str,
    ) -> Option<ProcessBundleProgressResponse> {
        let tracker = self.active_bundle(instruction_id)?;
        tracker.record_progress_request();

        // In the Fn API, a progress read_index is the 0-based index of the current
        // element, or -1 before the first element. The executor advances it per element,
        // so the runner can estimate the remaining work.
        let read_index = tracker.current_index.load(Ordering::Relaxed);
        let snapshot = tracker.progress_snapshot.lock().ok()?.clone();
        let mut user_metrics = tracker.metrics_container.to_monitoring_infos();
        // The work left in the current SDF element. Without it, a runner sees one element in
        // progress and cannot tell how much of the bundle is left.
        if let Some(sdf) = tracker.dynamic_split_registrar.current_handler() {
            let work = sdf.current_progress();
            user_metrics.push(beam::internals::work_completed(
                sdf.transform_id(),
                work.work_completed,
            ));
            user_metrics.push(beam::internals::work_remaining(
                sdf.transform_id(),
                work.work_remaining,
            ));
        }

        let (monitoring_infos, monitoring_data) = self.collect_bundle_metrics(
            &tracker.source_id,
            read_index,
            snapshot.pcollection_counts,
            snapshot.pcollection_byte_sizes,
            snapshot.transform_times,
            user_metrics,
        );

        Some(ProcessBundleProgressResponse {
            monitoring_infos,
            monitoring_data,
            consuming_received_data: Some(tracker.consuming.load(Ordering::Relaxed)),
        })
    }

    /// Returns status reports for all active bundles, ordered by longest lull first.
    pub fn active_bundle_reports(&self) -> Vec<ActiveBundleReport> {
        let trackers: Vec<(String, ActiveBundleTracker)> =
            if let Ok(active) = self.active_bundles.read() {
                active
                    .iter()
                    .map(|(inst_id, tracker)| (inst_id.clone(), tracker.clone()))
                    .collect()
            } else {
                Vec::new()
            };

        let mut reports: Vec<ActiveBundleReport> = trackers
            .into_iter()
            .map(|(inst_id, tracker)| {
                let running = tracker.sampler.as_ref().and_then(|s| s.running());
                let (current_transform, lull) = match running {
                    Some((transform, lull)) => (Some(transform.to_string()), lull),
                    None => (None, Duration::ZERO),
                };
                ActiveBundleReport {
                    instruction_id: inst_id,
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
                    (times.start_msecs > 0)
                        .then(|| beam::internals::start_bundle_msecs(&t_id, times.start_msecs)),
                    (times.process_msecs > 0)
                        .then(|| beam::internals::process_bundle_msecs(&t_id, times.process_msecs)),
                    (times.finish_msecs > 0)
                        .then(|| beam::internals::finish_bundle_msecs(&t_id, times.finish_msecs)),
                    (times.total_msecs > 0)
                        .then(|| beam::internals::total_msecs(&t_id, times.total_msecs)),
                ]
            })
            .flatten()
            .collect::<Vec<_>>();

        let system_infos = std::iter::once(beam::internals::data_channel_read_index(
            source_id,
            total_elements_read,
        ))
        .chain(
            pcollection_counts
                .into_iter()
                .map(|(pcol_id, count)| beam::internals::element_count(&pcol_id, count)),
        )
        .chain(
            pcollection_byte_sizes
                .into_iter()
                .map(|(pcol_id, dist)| beam::internals::sampled_byte_size(&pcol_id, &dist)),
        )
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
                    // The proto response carries both monitoring_infos and monitoring_data.
                    data.insert(short_id, info.payload.clone());
                    infos.push(info);
                    (infos, data)
                },
            )
    }
}

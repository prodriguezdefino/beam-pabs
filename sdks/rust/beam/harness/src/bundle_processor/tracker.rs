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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use beam::coders::VarIntCoder;
use beam::internals::{DynamicSplitRegistrar, DynamicSplitResult, MetricsContainer};
use beam::metrics::DistributionValue;
use beam_model::fn_execution::process_bundle_split_response::ChannelSplit;
use beam_model::fn_execution::{
    ProcessBundleProgressResponse, ProcessBundleSplitRequest, ProcessBundleSplitResponse,
};
use beam_model::pipeline::MonitoringInfo;
use tracing::warn;

use super::BundleProcessor;
use super::cache::{MetricValue, PlanMetric, PlanMetrics};
use super::chain::PCollectionStatsReader;
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

/// Marks `claimed` when splits or finish require the mutex path.
const SLOW_PATH: u64 = 1 << 63;

/// Progress and split state of an active bundle, behind one shared `Arc`.
#[derive(Clone)]
pub(super) struct ActiveBundleTracker(Arc<ActiveBundleTrackerInner>);

struct ActiveBundleTrackerInner {
    plan_metrics: Arc<PlanMetrics>,
    /// Writes split roots in the names and the encoding of the runner.
    roots: Arc<RootEncoder>,
    /// Why a granted split could not be sent. The SDF already narrowed its restriction then,
    /// so the bundle must fail and the runner retries it.
    split_failure: OnceLock<String>,
    split: Mutex<SplitState>,
    dynamic_split_registrar: Arc<DynamicSplitRegistrar>,
    /// The PCollection stats that the bundle thread writes, read for progress.
    stats: PCollectionStatsReader,
    metrics_container: Arc<MetricsContainer>,
    /// True while the bundle processes received data, false while it waits for more.
    consuming: AtomicBool,
    phase: AtomicU8,
    /// Encodes `(elements claimed) | SLOW_PATH` for lock-free claims on the hot path.
    claimed: AtomicU64,
    sampler: Option<ExecutionSampler>,
    runner_requests: Mutex<RunnerRequests>,
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
        plan_metrics: Arc<PlanMetrics>,
        roots: Arc<RootEncoder>,
        metrics_container: Arc<MetricsContainer>,
        stats: PCollectionStatsReader,
        sampler: Option<ExecutionSampler>,
    ) -> Self {
        Self(Arc::new(ActiveBundleTrackerInner {
            plan_metrics,
            roots,
            split_failure: OnceLock::new(),
            split: Mutex::new(SplitState::default()),
            dynamic_split_registrar: Arc::new(DynamicSplitRegistrar::new()),
            stats,
            metrics_container,
            consuming: AtomicBool::new(false),
            phase: AtomicU8::new(u8::from(BundlePhase::ReadingInput)),
            claimed: AtomicU64::new(0),
            sampler,
            runner_requests: Mutex::new(RunnerRequests::default()),
        }))
    }

    pub(super) fn descriptor_id(&self) -> &str {
        &self.0.plan_metrics.descriptor_id
    }

    pub(super) fn source_id(&self) -> &str {
        &self.0.plan_metrics.source_id
    }

    /// The roots of an SDF split for the runner. On failure, records the reason for
    /// [`split_failure`](Self::split_failure) and returns `None`, so the runner gets no split.
    pub(super) fn split_roots(
        &self,
        split: DynamicSplitResult,
    ) -> Option<ProcessBundleSplitResponse> {
        self.0
            .roots
            .split(split)
            .inspect_err(|e| {
                warn!("Refusing an SDF split that cannot be encoded for the runner: {e}");
                let _first = self.0.split_failure.set(e.to_string());
            })
            .ok()
    }

    /// Why a granted split could not be sent, if one could not.
    pub(super) fn split_failure(&self) -> Option<&str> {
        self.0.split_failure.get().map(String::as_str)
    }

    fn record_progress_request(&self) {
        if let Ok(mut requests) = self.0.runner_requests.lock() {
            requests.progress_requests += 1;
            requests.last_progress = Some(Instant::now());
        }
    }

    pub(super) fn record_split_request(&self, record: SplitRecord) {
        if let Ok(mut requests) = self.0.runner_requests.lock() {
            requests.split_requests += 1;
            requests.last_split = Some(record);
        }
    }

    /// Describes the runner's requests for this bundle and its read state, for stall reports.
    pub(super) fn describe_runner_requests(&self) -> String {
        let index = self.current_index();
        let stop = self
            .0
            .split
            .lock()
            .map(|s| s.stop_index)
            .unwrap_or(i64::MAX);
        let consuming = self.0.consuming.load(Ordering::Relaxed);
        self.0.runner_requests.lock().map_or_else(
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
            transform_id: self.source_id().to_string(),
            last_primary_element,
            first_residual_element,
        });
        response
    }

    /// Sets the current execution phase of the bundle.
    pub(super) fn set_phase(&self, phase: BundlePhase) {
        self.0.phase.store(u8::from(phase), Ordering::Relaxed);
    }

    /// Returns the current execution phase of the bundle.
    pub(super) fn phase(&self) -> BundlePhase {
        BundlePhase::try_from(self.0.phase.load(Ordering::Relaxed))
            .unwrap_or(BundlePhase::ReadingInput)
    }

    /// Returns the 0-based index of the current element, or -1 before the first element.
    pub(super) fn current_index(&self) -> i64 {
        (self.0.claimed.load(Ordering::Relaxed) & !SLOW_PATH) as i64 - 1
    }

    /// Returns the number of elements read by this bundle so far.
    pub(super) fn elements_read(&self) -> i64 {
        (self.0.claimed.load(Ordering::Relaxed) & !SLOW_PATH) as i64
    }

    /// Records whether the bundle processes received data (`true`) or waits for more (`false`).
    pub(super) fn set_consuming(&self, consuming: bool) {
        self.0.consuming.store(consuming, Ordering::Relaxed);
    }

    pub(super) fn dynamic_split_registrar(&self) -> &Arc<DynamicSplitRegistrar> {
        &self.0.dynamic_split_registrar
    }

    pub(super) fn begin_element(&self) -> bool {
        let cur = self.0.claimed.load(Ordering::Relaxed);
        if cur & SLOW_PATH == 0
            && self
                .0
                .claimed
                .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            return true;
        }
        self.begin_element_slow()
    }

    #[cold]
    fn begin_element_slow(&self) -> bool {
        let Ok(mut state) = self.0.split.lock() else {
            return true;
        };
        let raw = self.0.claimed.load(Ordering::Acquire);
        if raw & SLOW_PATH == 0 {
            state.index = (raw & !SLOW_PATH) as i64 - 1;
        }
        if !state.begin_element() {
            return false;
        }
        self.0.claimed.store(
            ((state.index + 1) as u64) | (raw & SLOW_PATH),
            Ordering::Release,
        );
        true
    }

    /// Runs `f` on the split state synchronized with [`Self::begin_element`].
    ///
    /// Sets [`SLOW_PATH`] before reading `claimed` so fast-path claims cannot race with `f`.
    /// Clears [`SLOW_PATH`] on exit only when it was clear on entry and `stop_index` is still
    /// `i64::MAX`, so a granted split or finish never allows an ABA fast-path claim.
    pub(super) fn with_split_state<R>(&self, f: impl FnOnce(&mut SplitState) -> R) -> Option<R> {
        let mut state = self.0.split.lock().ok()?;
        let prev = self.0.claimed.fetch_or(SLOW_PATH, Ordering::AcqRel);
        let had_slow = prev & SLOW_PATH != 0;
        if !had_slow {
            state.index = (prev & !SLOW_PATH) as i64 - 1;
        }
        let out = f(&mut state);
        if !had_slow && state.stop_index == i64::MAX {
            self.0.claimed.fetch_and(!SLOW_PATH, Ordering::Release);
        }
        Some(out)
    }

    pub(super) fn finish(&self, fallback_elements: i64) -> i64 {
        self.with_split_state(SplitState::finish)
            .unwrap_or(fallback_elements)
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
        let read_index = tracker.current_index();
        let counts = tracker.0.stats.counts();
        let sizes = tracker.0.stats.sizes();
        let times: Vec<TransformExecutionTimes> = tracker
            .0
            .sampler
            .as_ref()
            .map(|sampler| sampler.slot_times().collect())
            .unwrap_or_default();
        let mut user_metrics = tracker.0.metrics_container.to_monitoring_infos();
        // The work left in the current SDF element. Without it, a runner sees one element in
        // progress and cannot tell how much of the bundle is left.
        if let Some(sdf) = tracker.0.dynamic_split_registrar.current_handler() {
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

        let (monitoring_infos, monitoring_data) = self
            .collect_plan_metrics(
                &tracker.0.plan_metrics,
                read_index,
                &counts,
                &sizes,
                times,
                user_metrics,
            )
            .into_fields();

        Some(ProcessBundleProgressResponse {
            monitoring_infos,
            monitoring_data,
            consuming_received_data: Some(tracker.0.consuming.load(Ordering::Relaxed)),
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
                let running = tracker.0.sampler.as_ref().and_then(|s| s.running());
                let (current_transform, lull) = match running {
                    Some((transform, lull)) => (Some(transform.to_string()), lull),
                    None => (None, Duration::ZERO),
                };
                ActiveBundleReport {
                    instruction_id: inst_id,
                    descriptor_id: tracker.descriptor_id().to_string(),
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

    /// Collects the bundle metrics in one pass in the form that the runner reads.
    ///
    /// The metrics are the data channel read index, element counts, byte sizes, execution
    /// time and user metrics.
    pub(super) fn collect_plan_metrics(
        &self,
        plan: &PlanMetrics,
        read_index: i64,
        counts: &[i64],
        sizes: &[Option<DistributionValue>],
        times: impl IntoIterator<Item = TransformExecutionTimes>,
        user_metrics: Vec<MonitoringInfo>,
    ) -> BundleMetrics {
        let capacity = 1 + counts.len() * 2 + user_metrics.len();
        let int64 = MetricValue::Int64;
        let mut metrics = Vec::with_capacity(capacity);
        metrics.push((PlanMetric::ReadIndex, int64(read_index)));
        metrics.extend(
            counts
                .iter()
                .enumerate()
                .filter(|&(_, &count)| count > 0)
                .map(|(idx, &count)| (PlanMetric::ElementCount(idx), int64(count))),
        );
        metrics.extend(sizes.iter().enumerate().filter_map(|(idx, size)| {
            size.as_ref().map(|dist| {
                (
                    PlanMetric::SampledByteSize(idx),
                    MetricValue::Distribution(dist),
                )
            })
        }));
        for (idx, t) in times.into_iter().enumerate() {
            if t.total_msecs <= 0 {
                continue;
            }
            let phases = [
                (PlanMetric::StartMsecs(idx), t.start_msecs),
                (PlanMetric::ProcessMsecs(idx), t.process_msecs),
                (PlanMetric::FinishMsecs(idx), t.finish_msecs),
            ];
            metrics.extend(
                phases
                    .into_iter()
                    .filter(|&(_, msecs)| msecs > 0)
                    .map(|(metric, msecs)| (metric, int64(msecs))),
            );
            metrics.push((PlanMetric::TotalMsecs(idx), int64(t.total_msecs)));
        }
        if self.accepts_short_ids() {
            let short_id_cache = self.short_id_cache();
            let mut data: HashMap<String, Vec<u8>> = metrics
                .into_iter()
                .filter_map(|(metric, value)| {
                    let payload = match value {
                        MetricValue::Int64(v) => encode_varint_payload(v),
                        MetricValue::Distribution(dist) => dist.encode_payload(),
                    };
                    Some((plan.short_id(metric, short_id_cache)?, payload))
                })
                .collect();
            for info in user_metrics {
                data.insert(short_id_cache.get_or_create_short_id(&info), info.payload);
            }
            BundleMetrics::ShortIds(data)
        } else {
            let infos = metrics
                .into_iter()
                .filter_map(|(metric, value)| plan.info(metric, value))
                .chain(user_metrics)
                .collect();
            BundleMetrics::Infos(infos)
        }
    }
}

fn encode_varint_payload(value: i64) -> Vec<u8> {
    let mut payload = Vec::new();
    VarIntCoder::encode_varint_to_vec(value, &mut payload);
    payload
}

/// The metrics of a bundle in the form that the runner reads.
pub(super) enum BundleMetrics {
    /// Complete monitoring infos, for a runner without short ids.
    Infos(Vec<MonitoringInfo>),
    /// Payloads by short id. The runner asks for the infos of the ids that it does not know.
    ShortIds(HashMap<String, Vec<u8>>),
}

impl BundleMetrics {
    /// The `monitoring_infos` and `monitoring_data` fields of a response.
    pub(super) fn into_fields(self) -> (Vec<MonitoringInfo>, HashMap<String, Vec<u8>>) {
        match self {
            Self::Infos(infos) => (infos, HashMap::new()),
            Self::ShortIds(data) => (Vec::new(), data),
        }
    }
}

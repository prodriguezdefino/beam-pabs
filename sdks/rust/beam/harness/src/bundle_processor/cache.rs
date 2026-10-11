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

//! Bundle processors, reused across the bundles of a descriptor.
//!
//! A bundle processor is a private set of handler instances for one
//! `ProcessBundleDescriptor`, cloned from the registered prototypes and set up once, plus
//! their routing. It runs one bundle at a time, so concurrent bundles each take their own.
//! After a bundle succeeds, the processor goes back to an idle pool. After a failure it can
//! hold half-finished state, so it is torn down, as is one idle longer than the idle timeout.

use std::collections::HashMap;
use std::iter;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use beam::internals::{
    ShortIdCache, TransformFn, data_channel_read_index, element_count, finish_bundle_msecs,
    process_bundle_msecs, sampled_byte_size, start_bundle_msecs, total_msecs,
};
use beam::metrics::DistributionValue;
use beam_model::fn_execution::{ProcessBundleDescriptor, elements::Timers as ElementTimers};
use beam_model::pipeline::MonitoringInfo;
use tracing::{debug, warn};

use super::BundleError;
use super::chain::{Instances, OperatorGraph, Source};
use super::decoding::PortFraming;
use super::execution::source_output_pcolls;
use super::plan::{
    TimerFamily, collect_sinks, collect_timer_families, decode_port, has_urn, reject_v2_coders,
    topological_order,
};
use super::processor::{URN_DATA_SINK, URN_DATA_SOURCE};
use super::roots::RootEncoder;
use super::timers::BundleTimers;

use crate::state::SideInputSpecs;
use crate::user_state::UserStateSpecs;

/// How long an idle processor of a batch pipeline stays in the pool before teardown.
pub const BATCH_PROCESSOR_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long an idle processor of a streaming pipeline stays in the pool before teardown.
pub const STREAMING_PROCESSOR_IDLE_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Lazily registered short IDs for the system metrics of one [`BundlePlan`].
pub(super) struct PlanShortIds {
    read_index: OnceLock<String>,
    element_count: Box<[OnceLock<String>]>,
    sampled_byte_size: Box<[OnceLock<String>]>,
    start_msecs: Box<[OnceLock<String>]>,
    process_msecs: Box<[OnceLock<String>]>,
    finish_msecs: Box<[OnceLock<String>]>,
    total_msecs: Box<[OnceLock<String>]>,
}

/// Static metric metadata and cached short IDs built once per [`PreparedProcessor`].
pub(super) struct PlanMetrics {
    pub(super) descriptor_id: Arc<str>,
    pub(super) source_id: Arc<str>,
    pub(super) pcollection_ids: Box<[String]>,
    pub(super) operator_ids: Arc<[String]>,
    short_ids: PlanShortIds,
}

fn once_locks(len: usize) -> Box<[OnceLock<String>]> {
    (0..len).map(|_| OnceLock::new()).collect()
}

/// A system metric of a bundle plan, with the index of its PCollection or operator.
#[derive(Clone, Copy, Debug)]
pub(super) enum PlanMetric {
    ReadIndex,
    ElementCount(usize),
    SampledByteSize(usize),
    StartMsecs(usize),
    ProcessMsecs(usize),
    FinishMsecs(usize),
    TotalMsecs(usize),
}

/// The value of a [`PlanMetric`].
#[derive(Clone, Copy, Debug)]
pub(super) enum MetricValue<'a> {
    Int64(i64),
    Distribution(&'a DistributionValue),
}

/// The distribution of the template that registers a sampled byte size short id.
const EMPTY_DISTRIBUTION: DistributionValue = DistributionValue {
    count: 0,
    sum: 0,
    min: 0,
    max: 0,
};

impl PlanMetrics {
    pub(super) fn new(descriptor_id: &str, source_id: &str, graph: &OperatorGraph) -> Self {
        let pcollection_ids: Box<[String]> = graph.pcollection_ids().map(str::to_string).collect();
        let pcoll_len = pcollection_ids.len();
        let op_len = graph.operator_ids.len();
        Self {
            descriptor_id: Arc::from(descriptor_id),
            source_id: Arc::from(source_id),
            pcollection_ids,
            operator_ids: Arc::clone(&graph.operator_ids),
            short_ids: PlanShortIds {
                read_index: OnceLock::new(),
                element_count: once_locks(pcoll_len),
                sampled_byte_size: once_locks(pcoll_len),
                start_msecs: once_locks(op_len),
                process_msecs: once_locks(op_len),
                finish_msecs: once_locks(op_len),
                total_msecs: once_locks(op_len),
            },
        }
    }

    /// The PCollection or operator that `metric` labels.
    fn label(&self, metric: PlanMetric) -> Option<&str> {
        match metric {
            PlanMetric::ReadIndex => Some(&self.source_id),
            PlanMetric::ElementCount(idx) | PlanMetric::SampledByteSize(idx) => {
                self.pcollection_ids.get(idx).map(String::as_str)
            }
            PlanMetric::StartMsecs(idx)
            | PlanMetric::ProcessMsecs(idx)
            | PlanMetric::FinishMsecs(idx)
            | PlanMetric::TotalMsecs(idx) => self.operator_ids.get(idx).map(String::as_str),
        }
    }

    /// Builds the `MonitoringInfo` of `metric` with `value`. Returns `None` for an index
    /// outside the plan or a value of the wrong kind. Encoding into a `Vec` cannot fail.
    pub(super) fn info(
        &self,
        metric: PlanMetric,
        value: MetricValue<'_>,
    ) -> Option<MonitoringInfo> {
        let label = self.label(metric)?;
        let info = match (metric, value) {
            (PlanMetric::ReadIndex, MetricValue::Int64(v)) => data_channel_read_index(label, v),
            (PlanMetric::ElementCount(_), MetricValue::Int64(v)) => element_count(label, v),
            (PlanMetric::StartMsecs(_), MetricValue::Int64(v)) => start_bundle_msecs(label, v),
            (PlanMetric::ProcessMsecs(_), MetricValue::Int64(v)) => process_bundle_msecs(label, v),
            (PlanMetric::FinishMsecs(_), MetricValue::Int64(v)) => finish_bundle_msecs(label, v),
            (PlanMetric::TotalMsecs(_), MetricValue::Int64(v)) => total_msecs(label, v),
            (PlanMetric::SampledByteSize(_), MetricValue::Distribution(d)) => {
                sampled_byte_size(label, d)
            }
            (PlanMetric::SampledByteSize(_), MetricValue::Int64(_))
            | (
                PlanMetric::ReadIndex
                | PlanMetric::ElementCount(_)
                | PlanMetric::StartMsecs(_)
                | PlanMetric::ProcessMsecs(_)
                | PlanMetric::FinishMsecs(_)
                | PlanMetric::TotalMsecs(_),
                MetricValue::Distribution(_),
            ) => return None,
        };
        info.ok()
    }

    /// Returns the short id of `metric`, and registers it in `cache` on the first call.
    pub(super) fn short_id(&self, metric: PlanMetric, cache: &ShortIdCache) -> Option<String> {
        let ids = &self.short_ids;
        let slot = match metric {
            PlanMetric::ReadIndex => Some(&ids.read_index),
            PlanMetric::ElementCount(idx) => ids.element_count.get(idx),
            PlanMetric::SampledByteSize(idx) => ids.sampled_byte_size.get(idx),
            PlanMetric::StartMsecs(idx) => ids.start_msecs.get(idx),
            PlanMetric::ProcessMsecs(idx) => ids.process_msecs.get(idx),
            PlanMetric::FinishMsecs(idx) => ids.finish_msecs.get(idx),
            PlanMetric::TotalMsecs(idx) => ids.total_msecs.get(idx),
        }?;
        if let Some(id) = slot.get() {
            return Some(id.clone());
        }
        let value = match metric {
            PlanMetric::SampledByteSize(_) => MetricValue::Distribution(&EMPTY_DISTRIBUTION),
            PlanMetric::ReadIndex
            | PlanMetric::ElementCount(_)
            | PlanMetric::StartMsecs(_)
            | PlanMetric::ProcessMsecs(_)
            | PlanMetric::FinishMsecs(_)
            | PlanMetric::TotalMsecs(_) => MetricValue::Int64(0),
        };
        let template = self.info(metric, value)?;
        Some(
            slot.get_or_init(|| cache.get_or_create_short_id(&template))
                .clone(),
        )
    }
}

/// Immutable topology and port metadata resolved once per `ProcessBundleDescriptor`.
pub(super) struct BundlePlan {
    /// Static metric IDs and lazily populated short IDs shared with active bundle trackers.
    metrics: Arc<PlanMetrics>,
    source_coder_id: String,
    /// The framing of the source port. Split requests share it from another task.
    source_framing: Arc<PortFraming>,
    /// Writes the roots of the splittable transforms. Split requests share it from another
    /// task.
    roots: Arc<RootEncoder>,
    sink_ids: Vec<String>,
    /// The timer families of the descriptor. The bundle refers to a family by its index.
    timer_families: Vec<TimerFamily>,
    /// The data service of the source and sink ports, if they name one.
    data_endpoint: Option<String>,
    /// The side inputs of the descriptor, if a transform reads any.
    side_inputs: Option<Arc<SideInputSpecs>>,
    /// The user state cells of the descriptor, if it declares any.
    user_state: Option<Arc<UserStateSpecs>>,
    /// The routing between the operators of this processor.
    graph: OperatorGraph,
}

impl BundlePlan {
    pub(super) fn source_id(&self) -> &str {
        &self.metrics.source_id
    }

    pub(super) fn metrics(&self) -> &Arc<PlanMetrics> {
        &self.metrics
    }

    pub(super) fn data_endpoint(&self) -> Option<&str> {
        self.data_endpoint.as_deref()
    }

    pub(super) fn side_inputs(&self) -> Option<&Arc<SideInputSpecs>> {
        self.side_inputs.as_ref()
    }

    pub(super) fn user_state(&self) -> Option<&Arc<UserStateSpecs>> {
        self.user_state.as_ref()
    }

    pub(super) fn source_coder_id(&self) -> &str {
        &self.source_coder_id
    }

    pub(super) fn source_framing(&self) -> &Arc<PortFraming> {
        &self.source_framing
    }

    pub(super) fn roots(&self) -> &Arc<RootEncoder> {
        &self.roots
    }

    pub(super) fn sink_ids(&self) -> impl Iterator<Item = &str> {
        self.sink_ids.iter().map(String::as_str)
    }

    pub(super) fn sink_count(&self) -> usize {
        self.sink_ids.len()
    }

    pub(super) fn timer_families(&self) -> impl Iterator<Item = (&str, &str)> {
        self.timer_families
            .iter()
            .map(|f| (f.transform_id.as_str(), f.family_id.as_str()))
    }

    pub(super) fn graph(&self) -> &OperatorGraph {
        &self.graph
    }

    pub(super) fn has_timer_families(&self) -> bool {
        !self.timer_families.is_empty()
    }

    /// The index of the family of `timers`, if its transform declares it.
    pub(super) fn timer_family_index(&self, timers: &ElementTimers) -> Option<usize> {
        self.timer_families.iter().position(|f| {
            f.transform_id == timers.transform_id && f.family_id == timers.timer_family_id
        })
    }

    /// The timer family at `index`, from [`timer_family_index`](Self::timer_family_index).
    pub(super) fn timer_family(&self, index: usize) -> Option<&TimerFamily> {
        self.timer_families.get(index)
    }

    /// Writes the timers set since the last call to `outbound`, and records them in `timers`.
    /// Drops a timer of a family that its transform does not declare.
    pub(super) fn encode_timers_to_outbound(
        &self,
        timer_collector: &beam::internals::TimerCollector,
        outbound: &mut crate::data::Outbound,
        timers: &mut BundleTimers,
    ) -> Result<(), BundleError> {
        timer_collector
            .drain()
            .into_iter()
            .filter_map(|change| {
                self.timer_families
                    .iter()
                    .position(|f| {
                        f.transform_id == change.transform_id.as_ref()
                            && f.family_id == change.family.as_ref()
                    })
                    .map(|index| (index, change))
            })
            .try_for_each(|(index, change)| {
                let beam::internals::TimerChange { record, .. } = change;
                // `outbound` has the timer families of this plan in the same order.
                outbound
                    .write_timer(index, |buf| beam::coders::TimerCoder::encode(&record, buf))?;
                timers.record(index, record);
                Ok::<(), crate::data::DataError>(())
            })
            .map_err(BundleError::from)
    }
}

/// A compiled bundle plan paired with its stateful handler instances, reused across bundles.
pub(super) struct PreparedProcessor {
    plan: BundlePlan,
    instances: Instances,
}

impl PreparedProcessor {
    /// Plans the descriptor and sets up a fresh instance of every handler it runs.
    fn build(
        descriptor: &ProcessBundleDescriptor,
        prototypes: &HashMap<String, TransformFn>,
    ) -> Result<Self, BundleError> {
        let (source_id, source_t) = only_source(descriptor)?;
        let sinks = collect_sinks(descriptor)?;
        let ordered_transform_ids = topological_order(descriptor, source_id, source_t);
        let source_port = decode_port(source_t)?;
        let timer_families = collect_timer_families(descriptor)?;
        reject_v2_coders(
            iter::once(source_port.coder_id.as_str())
                .chain(sinks.iter().map(|(_, port)| port.coder_id.as_str()))
                .chain(timer_families.iter().map(|family| family.coder_id.as_str())),
            &descriptor.coders,
        )?;
        // The plan outlives the borrow of the descriptor, so it keeps its own copy of the
        // coders. This runs once for each processor, not for each bundle.
        let coders = Arc::new(descriptor.coders.clone());
        let source_framing = Arc::new(PortFraming::resolve(
            &source_port.coder_id,
            Arc::clone(&coders),
        )?);
        let roots = Arc::new(RootEncoder::new(descriptor, Arc::clone(&source_framing))?);
        let source_pcolls = source_output_pcolls(source_id, source_t);
        let source = Source {
            pcollections: &source_pcolls,
            coder_id: &source_port.coder_id,
        };
        let (graph, mut instances) = OperatorGraph::build(
            descriptor,
            source,
            &ordered_transform_ids,
            &sinks,
            prototypes,
        )?;
        instances.setup(&graph.operator_ids)?;

        debug!(
            "Created bundle processor for descriptor '{}' (source: '{}', transforms: {:?}, sinks: {:?})",
            descriptor.id,
            source_id,
            ordered_transform_ids,
            sinks.iter().map(|(id, _)| id).collect::<Vec<_>>()
        );
        let sink_ids = sinks.into_iter().map(|(id, _)| id).collect();
        let data_endpoint = descriptor
            .transforms
            .values()
            .filter(|t| has_urn(t, URN_DATA_SOURCE) || has_urn(t, URN_DATA_SINK))
            .filter_map(|t| decode_port(t).ok()?.api_service_descriptor)
            .map(|service| service.url)
            .find(|url| !url.is_empty());
        let side_inputs =
            SideInputSpecs::from_descriptor(descriptor, Arc::clone(&coders)).map(Arc::new);
        let user_state = UserStateSpecs::from_descriptor(descriptor, &coders).map(Arc::new);
        let metrics = Arc::new(PlanMetrics::new(&descriptor.id, source_id, &graph));
        Ok(Self {
            plan: BundlePlan {
                metrics,
                source_coder_id: source_port.coder_id,
                source_framing,
                roots,
                sink_ids,
                timer_families,
                data_endpoint,
                side_inputs,
                user_state,
                graph,
            },
            instances,
        })
    }

    pub(super) fn parts(&mut self) -> (&BundlePlan, &mut Instances) {
        (&self.plan, &mut self.instances)
    }

    /// Consumes the processor and runs teardown on each handler instance.
    fn teardown(mut self) {
        self.instances.teardown(&self.plan.graph.operator_ids);
    }
}

/// The one DATA_SOURCE transform of `descriptor`. Fails when it has none or more than one,
/// so the data of a second source is never dropped.
fn only_source(
    descriptor: &ProcessBundleDescriptor,
) -> Result<(&String, &beam_model::pipeline::PTransform), BundleError> {
    let mut sources = descriptor
        .transforms
        .iter()
        .filter(|(_, t)| has_urn(t, URN_DATA_SOURCE));
    match (sources.next(), sources.next()) {
        (Some(source), None) => Ok(source),
        (None, _) => Err(BundleError::MissingSource(descriptor.id.clone())),
        (Some(first), Some(second)) => {
            let mut ids: Vec<String> = [first, second]
                .into_iter()
                .chain(sources)
                .map(|(id, _)| id.clone())
                .collect();
            ids.sort_unstable();
            Err(BundleError::MultipleSources {
                descriptor: descriptor.id.clone(),
                sources: ids,
            })
        }
    }
}

/// An idle processor and the time it went back to the pool.
type Idle = (Instant, PreparedProcessor);

/// The worker's bundle processors, pooled per descriptor id.
pub(super) struct ProcessorCache {
    /// The handlers registered with the pipeline. The cache clones them and never runs them.
    prototypes: Arc<HashMap<String, TransformFn>>,
    idle: Mutex<HashMap<String, Vec<Idle>>>,
    idle_timeout: Duration,
}

impl ProcessorCache {
    pub(super) fn new(prototypes: Arc<HashMap<String, TransformFn>>) -> Self {
        Self {
            prototypes,
            idle: Mutex::new(HashMap::new()),
            idle_timeout: BATCH_PROCESSOR_IDLE_TIMEOUT,
        }
    }

    /// Sets how long an idle processor stays in the pool before teardown.
    pub(super) fn with_idle_timeout(self, idle_timeout: Duration) -> Self {
        Self {
            idle_timeout,
            ..self
        }
    }

    pub(super) fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// Tears down the processors idle longer than the idle timeout. Returns how many.
    pub(super) fn sweep(&self) -> usize {
        let expired = self.with_idle(|idle| take_expired(idle, Instant::now(), self.idle_timeout));
        let count = expired.len();
        // Tear down outside the lock, because teardown runs user code.
        expired.into_iter().for_each(PreparedProcessor::teardown);
        count
    }

    /// The number of idle processors of each descriptor, in descriptor order.
    pub(super) fn idle_counts(&self) -> Vec<(String, usize)> {
        let mut counts: Vec<(String, usize)> = self.with_idle(|idle| {
            idle.iter()
                .map(|(descriptor, processors)| (descriptor.clone(), processors.len()))
                .collect()
        });
        counts.sort_unstable();
        counts
    }

    /// Takes an idle processor for `descriptor`, or builds a new one. Dropping the lease
    /// tears down the processor; call [`ProcessorLease::recycle`] to keep it.
    pub(super) fn acquire(
        &self,
        descriptor: &ProcessBundleDescriptor,
    ) -> Result<ProcessorLease<'_>, BundleError> {
        let reused = self.with_idle(|idle| {
            idle.get_mut(&descriptor.id)
                .and_then(Vec::pop)
                .map(|(_, processor)| processor)
        });
        let processor = match reused {
            Some(processor) => processor,
            None => PreparedProcessor::build(descriptor, &self.prototypes)?,
        };
        Ok(ProcessorLease {
            cache: self,
            descriptor_id: descriptor.id.clone(),
            processor: Some(processor),
        })
    }

    /// Tears down every idle processor. Call this when the worker shuts down.
    pub(super) fn shutdown(&self) {
        self.with_idle(std::mem::take)
            .into_values()
            .flatten()
            .for_each(|(_, processor)| processor.teardown());
    }

    fn recycle(&self, descriptor_id: String, processor: PreparedProcessor) {
        let now = Instant::now();
        let expired = self.with_idle(|idle| {
            idle.entry(descriptor_id)
                .or_default()
                .push((now, processor));
            take_expired(idle, now, self.idle_timeout)
        });
        // Tear down outside the lock, because teardown runs user code.
        expired.into_iter().for_each(PreparedProcessor::teardown);
    }

    /// Runs `f` on the idle pool and holds the lock only for `f`. Do not block in `f`.
    fn with_idle<T>(&self, f: impl FnOnce(&mut HashMap<String, Vec<Idle>>) -> T) -> T {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut idle)
    }
}

/// Removes the processors that have been idle longer than `timeout`.
fn take_expired(
    idle: &mut HashMap<String, Vec<Idle>>,
    now: Instant,
    timeout: Duration,
) -> Vec<PreparedProcessor> {
    let is_expired = |returned: &Instant| now.duration_since(*returned) > timeout;
    let expired = idle
        .values_mut()
        .flat_map(|pool| {
            let (stale, fresh) = std::mem::take(pool)
                .into_iter()
                .partition::<Vec<_>, _>(|(returned, _)| is_expired(returned));
            *pool = fresh;
            stale
        })
        .map(|(_, processor)| processor)
        .collect();
    idle.retain(|_, pool| !pool.is_empty());
    expired
}

/// Exclusive use of one bundle processor for the duration of a bundle.
pub(super) struct ProcessorLease<'a> {
    cache: &'a ProcessorCache,
    descriptor_id: String,
    /// `Some` until the lease is recycled or dropped.
    processor: Option<PreparedProcessor>,
}

impl ProcessorLease<'_> {
    /// Returns the processor to the pool for the next bundle of its descriptor. Call this
    /// only after the bundle succeeds; a dropped lease tears down the processor.
    pub(super) fn recycle(mut self) {
        if let Some(processor) = self.processor.take() {
            self.cache
                .recycle(std::mem::take(&mut self.descriptor_id), processor);
        }
    }
}

impl Deref for ProcessorLease<'_> {
    type Target = PreparedProcessor;

    #[expect(
        clippy::expect_used,
        reason = "processor is Some until recycle or drop takes it"
    )]
    fn deref(&self) -> &PreparedProcessor {
        self.processor
            .as_ref()
            .expect("a lease holds its processor until recycled or dropped")
    }
}

impl DerefMut for ProcessorLease<'_> {
    #[expect(
        clippy::expect_used,
        reason = "processor is Some until recycle or drop takes it"
    )]
    fn deref_mut(&mut self) -> &mut PreparedProcessor {
        self.processor
            .as_mut()
            .expect("a lease holds its processor until recycled or dropped")
    }
}

impl Drop for ProcessorLease<'_> {
    fn drop(&mut self) {
        if let Some(processor) = self.processor.take() {
            warn!(
                "Discarding bundle processor for descriptor '{}' after a failed bundle",
                self.descriptor_id
            );
            processor.teardown();
        }
    }
}

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

//! Runs the bundles of a worker: plans, inputs, outputs and the response.

mod finalize;
mod read_loop;
mod state_access;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use tracing::{Instrument, debug};

use super::BundleError;
use super::cache::{BundlePlan, ProcessorCache, ProcessorLease};
use super::chain::{ChainCtx, Instances, PCollectionStats};
use super::timers::BundleTimers;
use super::tracker::{ActiveBundleTracker, BundlePhase};
use crate::data::DataManager;
use crate::provisioning::RunnerCapabilities;
use beam::internals::{MetricsContainer, ShortIdCache, TransformFn};
use beam_model::fn_execution::{Elements, ProcessBundleDescriptor, ProcessBundleResponse};
use finalize::PendingFinalizations;
use read_loop::Inputs;
use state_access::StateAccess;

pub(super) use read_loop::ReadProgress;

pub use finalize::spawn_finalizer_sweeper;

/// Writes the timers that the last element or timer set, and records them in `timers`.
pub(super) fn emit_timers(
    plan: &BundlePlan,
    ctx: &mut ChainCtx<'_>,
    timers: &mut BundleTimers,
) -> Result<(), BundleError> {
    match ctx.timer_collector {
        Some(collector) => plan.encode_timers_to_outbound(collector, ctx.outbound, timers),
        None => Ok(()),
    }
}

pub const URN_DATA_SOURCE: &str = "beam:runner:source:v1";
pub const URN_DATA_SINK: &str = "beam:runner:sink:v1";

pub struct BundleProcessor {
    pub(super) data_manager: DataManager,
    /// Bundle processors built from the registered handlers, reused across bundles.
    processors: ProcessorCache,
    short_id_cache: ShortIdCache,
    pub(super) active_bundles: Arc<std::sync::RwLock<HashMap<String, ActiveBundleTracker>>>,
    pending_finalizations: Arc<Mutex<PendingFinalizations>>,
    /// The number of finalization callbacks that expired before they could run.
    expired_finalizations: AtomicU64,
    /// The state streams that the bundles share.
    state_clients: crate::state::StateClients,
    /// The protocols that the runner declares.
    capabilities: RunnerCapabilities,
    /// Runner state that bundles with the same cache token share.
    state_cache: Arc<crate::state::StateCache>,
    data_sampler: Option<Arc<super::sampling::DataSampler>>,
}

impl BundleProcessor {
    pub fn new(data_manager: DataManager) -> Self {
        Self::with_handlers(data_manager, HashMap::new())
    }
    pub fn with_handlers(
        data_manager: DataManager,
        transform_handlers: HashMap<String, TransformFn>,
    ) -> Self {
        Self {
            data_manager,
            processors: ProcessorCache::new(Arc::new(transform_handlers)),
            short_id_cache: ShortIdCache::new(),
            active_bundles: Arc::new(std::sync::RwLock::new(HashMap::new())),
            pending_finalizations: Arc::new(Mutex::new(HashMap::new())),
            expired_finalizations: AtomicU64::new(0),
            state_clients: crate::state::StateClients::new(String::new()),
            capabilities: RunnerCapabilities::default(),
            state_cache: Arc::default(),
            data_sampler: None,
        }
    }
    pub(super) fn active_bundle(&self, instruction_id: &str) -> Option<ActiveBundleTracker> {
        self.active_bundles
            .read()
            .ok()?
            .get(instruction_id)
            .cloned()
    }
    /// Sets the worker id that the state channel sends as gRPC metadata, so the runner can
    /// match the stream with the worker that registered on the control channel.
    /// the worker attaches it to the other channels at connection time.
    pub fn with_worker_id(mut self, worker_id: String) -> Self {
        self.state_clients = crate::state::StateClients::new(worker_id);
        self
    }
    /// Sets the protocols that the runner declares. Without them, the bundles use none of
    /// the optional protocols.
    pub fn with_runner_capabilities(mut self, capabilities: RunnerCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }
    /// Whether the runner reads bundle metrics by short id.
    pub(super) fn accepts_short_ids(&self) -> bool {
        self.capabilities
            .contains(beam::metrics::PROTOCOL_MONITORING_INFO_SHORT_IDS)
    }
    /// Replaces the state cache, which has the default maximum size otherwise.
    pub fn with_state_cache(mut self, state_cache: Arc<crate::state::StateCache>) -> Self {
        self.state_cache = state_cache;
        self
    }
    /// Samples elements and exceptions into `sampler` when data sampling is on.
    pub fn with_data_sampler(mut self, sampler: Option<Arc<super::sampling::DataSampler>>) -> Self {
        self.data_sampler = sampler;
        self
    }
    /// The data sampler, when data sampling is on.
    pub fn data_sampler(&self) -> Option<&Arc<super::sampling::DataSampler>> {
        self.data_sampler.as_ref()
    }
    /// Tears down the idle bundle processors and runs teardown on each handler instance.
    pub fn shutdown(&self) {
        self.processors.shutdown();
    }
    /// Returns the short ID cache that registers bundle metrics.
    pub fn short_id_cache(&self) -> &ShortIdCache {
        &self.short_id_cache
    }
    /// Runs one bundle as its `ProcessBundleDescriptor` describes.
    pub async fn process_bundle(
        &self,
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        data_stream_id: &str,
        cache_tokens: &crate::state::CacheTokens,
        embedded_elements: Option<Elements>,
    ) -> Result<ProcessBundleResponse, BundleError> {
        let span = tracing::debug_span!("bundle", instruction_id = %instruction_id);
        self.process_bundle_inner(
            instruction_id,
            descriptor,
            data_stream_id,
            cache_tokens,
            embedded_elements,
        )
        .instrument(span)
        .await
    }
    async fn process_bundle_inner(
        &self,
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        data_stream_id: &str,
        cache_tokens: &crate::state::CacheTokens,
        embedded_elements: Option<Elements>,
    ) -> Result<ProcessBundleResponse, BundleError> {
        let mut processor = self.open_processor(descriptor, data_stream_id).await?;
        let (plan, instances) = processor.parts();
        debug!(
            "Processing bundle instruction '{}' (descriptor: '{}', source: '{}', {} transforms, {} sinks)",
            instruction_id,
            descriptor.id,
            plan.source_id(),
            plan.graph().operator_ids.len(),
            plan.sink_count()
        );

        let inbound = self
            .data_manager
            .register_bundle_inbound(instruction_id, plan.has_timer_families());
        if let Some(elements) = embedded_elements {
            let dm = self.data_manager.clone();
            tokio::spawn(async move {
                dm.handle_inbound_elements(elements).await;
            });
        }
        let inputs = Inputs {
            data: inbound.data,
            timers: inbound.timers,
            plan,
            instruction_id,
        };
        let request = BundleRequest {
            instruction_id,
            descriptor,
            data_stream_id,
            cache_tokens,
        };
        let mut parts = BundleParts::new(&request, plan);
        let executed = self.execute(&request, instances, &mut parts, inputs).await;
        // The inbound queues go also when the bundle fails.
        drop(inbound.registration);
        let embedded_elements_out = executed?;

        let response = self.bundle_response(&request, plan, parts, embedded_elements_out)?;
        // The bundle succeeded, so its processor is clean and can run the next bundle.
        processor.recycle();
        Ok(response)
    }
    /// Takes a processor for `descriptor` and opens its data stream. The processor holds
    /// set-up handler instances and the plan, which bundles of the same descriptor reuse.
    async fn open_processor(
        &self,
        descriptor: &ProcessBundleDescriptor,
        data_stream_id: &str,
    ) -> Result<ProcessorLease<'_>, BundleError> {
        let mut processor = self.processors.acquire(descriptor)?;
        let opened = self
            .data_manager
            .ensure_stream_endpoint(data_stream_id, processor.parts().0.data_endpoint())
            .await;
        match opened {
            Ok(()) => Ok(processor),
            Err(e) => {
                // No element reached the handlers, so the processor is clean.
                processor.recycle();
                Err(e.into())
            }
        }
    }
    /// Starts the handlers, reads the inputs, finishes the handlers and sends or embeds the
    /// output. Returns the output to embed in the response, if any.
    async fn execute(
        &self,
        request: &BundleRequest<'_>,
        instances: &mut Instances,
        parts: &mut BundleParts,
        inputs: Inputs<'_>,
    ) -> Result<Option<Elements>, BundleError> {
        let &BundleRequest {
            instruction_id,
            descriptor,
            data_stream_id,
            ..
        } = request;
        let plan = inputs.plan;
        let graph = plan.graph();
        let data_sampler = self.data_sampler.as_deref();
        // Take the PCollection samplers once, so the element path indexes a slice.
        let pcollection_samplers: Vec<_> = data_sampler
            .filter(|sampler| sampler.config().mode == super::sampling::SamplingMode::All)
            .map(|sampler| {
                graph
                    .pcollection_ids()
                    .map(|id| sampler.sampler_for_pcollection(id))
                    .collect()
            })
            .unwrap_or_default();
        self.start_bundle_handlers(
            instruction_id,
            graph,
            instances,
            &parts.metrics_container,
            &parts.sampler,
            data_sampler,
        )?;

        // Split and progress requests see the bundle only when the handlers have started. A
        // split before that point would end the bundle before it reads an element.
        let _active = self.activate(instruction_id, &parts.tracker);

        let state = StateAccess::new(request, plan, &self.state_clients, &self.state_cache);
        let mut outbound = self.data_manager.outbound(
            data_stream_id,
            instruction_id,
            plan.sink_ids(),
            plan.timer_families(),
        );
        let mut chain_ctx = ChainCtx {
            descriptor,
            graph,
            outbound: &mut outbound,
            side_input_reader: state.side_inputs.as_ref(),
            state_backends: &state.scoped,
            timer_collector: parts.timer_collector.as_ref(),
            residual_collector: Some(&parts.residual_collector),
            metrics_container: Some(&parts.metrics_container),
            transform_metrics: Some(&parts.transform_metrics),
            bundle_finalizer: Some(&parts.bundle_finalizer),
            dynamic_split_registrar: Some(parts.tracker.dynamic_split_registrar()),
            state_stream_reader: state.stream_reader.as_ref(),
            stats: &mut parts.pcollection_stats,
            sampler: &parts.sampler,
            data_sampler,
            pcollection_samplers: &pcollection_samplers,
            instruction_id,
            scratch: Vec::new(),
        };
        self.read_inputs(
            &mut chain_ctx,
            instances,
            &parts.tracker,
            plan.source_coder_id(),
            inputs,
            &mut parts.read,
        )
        .await?;
        // The input has ended. Close the channel, so a later split request gets no split.
        parts.read.final_index = parts.tracker.finish(parts.read.elements);
        if let Some(reason) = parts.tracker.split_failure() {
            return Err(BundleError::Coder(format!(
                "An SDF split was granted but could not be sent to the runner: {reason}"
            )));
        }

        parts.tracker.set_phase(BundlePhase::Finishing);
        self.finish_bundle_handlers(&mut chain_ctx, instances, &parts.read.last_header)?;
        if let Some(user_state) = &state.user_state {
            user_state.commit().map_err(BundleError::Handler)?;
        }

        emit_timers(plan, &mut chain_ctx, &mut parts.read.timers)?;
        let can_embed = self
            .capabilities
            .contains(beam::pipeline::PROTOCOL_CONTROL_RESPONSE_ELEMENTS_EMBEDDING);
        // A bundle without outbound endpoints leaves `elements` unset: Dataflow rejects a
        // present but empty message as incomplete.
        let has_outbound = plan.sink_count() > 0 || plan.has_timer_families();
        // The proto allows embedded output only when no output went on the data stream.
        if can_embed && has_outbound && !outbound.has_flushed() {
            Ok(Some(outbound.take_elements_for_embedding()))
        } else {
            outbound.finish().await.map_err(BundleError::from)?;
            Ok(None)
        }
    }
    /// Builds the response of a bundle that succeeded: metrics, residuals and finalization.
    fn bundle_response(
        &self,
        request: &BundleRequest<'_>,
        plan: &BundlePlan,
        parts: BundleParts,
        elements: Option<Elements>,
    ) -> Result<ProcessBundleResponse, BundleError> {
        let BundleParts {
            residual_collector,
            bundle_finalizer,
            pcollection_stats,
            sampler,
            metrics_container,
            read,
            ..
        } = parts;
        // The final read index counts the processed elements, and the runner compares it
        // with the granted split. Progress reports the index of the current element.
        let final_read_index = read.final_index;
        let stats = pcollection_stats.reader();
        let (monitoring_infos, monitoring_data) = self
            .collect_plan_metrics(
                plan.metrics(),
                final_read_index,
                &stats.counts(),
                &stats.sizes(),
                sampler.slot_times(),
                metrics_container.to_monitoring_infos(),
            )
            .into_fields();
        debug!(
            "Successfully completed bundle instruction '{}' (elements read: {}, metrics: {}, data: {})",
            request.instruction_id,
            read.elements,
            monitoring_infos.len(),
            monitoring_data.len()
        );

        let residual_roots = residual_collector
            .drain()
            .into_iter()
            .map(|r| plan.roots().residual(r))
            .collect::<Result<Vec<_>, _>>()?;
        let requires_finalization = bundle_finalizer.has_callbacks();
        if requires_finalization {
            self.store_finalization(request.instruction_id, bundle_finalizer.drain());
        }
        Ok(ProcessBundleResponse {
            residual_roots,
            monitoring_infos,
            requires_finalization,
            monitoring_data,
            elements,
        })
    }
    /// Makes the bundle visible to split and progress requests until the guard drops.
    fn activate<'a>(
        &'a self,
        instruction_id: &'a str,
        tracker: &ActiveBundleTracker,
    ) -> ActiveBundle<'a> {
        if let Ok(mut active) = self.active_bundles.write() {
            active.insert(instruction_id.to_string(), tracker.clone());
        }
        ActiveBundle {
            bundles: &self.active_bundles,
            instruction_id,
        }
    }
}

/// What a `ProcessBundle` request names.
struct BundleRequest<'a> {
    instruction_id: &'a str,
    descriptor: &'a ProcessBundleDescriptor,
    data_stream_id: &'a str,
    cache_tokens: &'a crate::state::CacheTokens,
}

/// The objects that one bundle fills while it runs and that its response reads.
struct BundleParts {
    timer_collector: Option<Arc<beam::internals::TimerCollector>>,
    residual_collector: Arc<beam::internals::ResidualCollector>,
    bundle_finalizer: Arc<beam::internals::BundleFinalizerCollector>,
    pcollection_stats: PCollectionStats,
    sampler: super::sampler::ExecutionSampler,
    metrics_container: Arc<MetricsContainer>,
    transform_metrics: Arc<beam::internals::TransformMetrics>,
    tracker: ActiveBundleTracker,
    read: ReadProgress,
}

impl BundleParts {
    fn new(request: &BundleRequest<'_>, plan: &BundlePlan) -> Self {
        let graph = plan.graph();
        // The sampler comes after the graph, so that every operator has a sampling slot.
        let sampler = super::sampler::ExecutionSampler::new(
            request.instruction_id,
            Arc::clone(&graph.operator_ids),
        );
        let metrics_container = Arc::new(MetricsContainer::new());
        let transform_metrics = Arc::new(beam::internals::TransformMetrics::new(
            Arc::clone(&metrics_container),
            Arc::clone(&graph.transform_ids),
        ));
        let pcollection_stats = PCollectionStats::new(graph);
        let tracker = ActiveBundleTracker::new(
            Arc::clone(plan.metrics()),
            Arc::clone(plan.roots()),
            Arc::clone(&metrics_container),
            pcollection_stats.reader(),
            Some(sampler.clone()),
        );
        Self {
            timer_collector: plan
                .has_timer_families()
                .then(|| Arc::new(beam::internals::TimerCollector::new())),
            residual_collector: Arc::new(beam::internals::ResidualCollector::new()),
            bundle_finalizer: Arc::new(beam::internals::BundleFinalizerCollector::new()),
            pcollection_stats,
            sampler,
            metrics_container,
            transform_metrics,
            tracker,
            read: ReadProgress::new(),
        }
    }
}

/// Removes a bundle from the active bundles when dropped, also when the bundle fails or
/// panics, so later split and progress requests do not see it.
struct ActiveBundle<'a> {
    bundles: &'a std::sync::RwLock<HashMap<String, ActiveBundleTracker>>,
    instruction_id: &'a str,
}

impl Drop for ActiveBundle<'_> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.bundles.write() {
            active.remove(self.instruction_id);
        }
    }
}

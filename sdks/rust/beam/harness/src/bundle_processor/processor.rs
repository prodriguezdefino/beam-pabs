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

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval_at};
use tracing::{Instrument, debug, warn};

use crate::data::{DataError, DataManager};
use beam::coders::WindowedHeader;
use beam::internals::{MetricsContainer, PendingFinalizer, ShortIdCache, TransformFn};
use model::fn_execution::{
    Elements, ProcessBundleDescriptor, ProcessBundleResponse, elements::Timers as ElementTimers,
};

use super::BundleError;
use super::cache::{BundlePlan, ProcessorCache};
use super::chain::{ChainCtx, Instances, OperatorGraph, PCollectionStats};
use super::decoding::PortFraming;
use super::tracker::{ActiveBundleTracker, BundlePhase};

/// Maximum age of the progress that the runner reads while a bundle runs.
pub const PROGRESS_SYNC_INTERVAL: Duration = Duration::from_millis(100);

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

/// Writes the timers that the last element or timer set.
fn emit_timers(plan: &BundlePlan, ctx: &mut ChainCtx<'_>) -> Result<(), BundleError> {
    match ctx.timer_collector {
        Some(collector) => plan.encode_timers_to_outbound(collector, ctx.outbound),
        None => Ok(()),
    }
}

pub const URN_DATA_SOURCE: &str = "beam:runner:source:v1";
pub const URN_DATA_SINK: &str = "beam:runner:sink:v1";

/// The pending finalization callbacks of each bundle, by instruction id.
type PendingFinalizations = HashMap<String, Vec<PendingFinalizer>>;

/// Starts a task that drops the expired finalization callbacks of `bundles` at intervals of
/// `every`. The first sweep is one interval after the start. The task stops at the first tick
/// after the processor is dropped.
#[doc(hidden)]
pub fn spawn_finalizer_sweeper(bundles: Weak<BundleProcessor>, every: Duration) -> JoinHandle<()> {
    let mut ticker = interval_at(tokio::time::Instant::now() + every, every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::spawn(async move {
        loop {
            ticker.tick().await;
            let Some(processor) = bundles.upgrade() else {
                return;
            };
            processor.sweep_expired_finalizations();
        }
    })
}

/// Removes the callbacks in `map` that expired at `now`, and the bundles with no callback left.
fn take_expired(map: &mut PendingFinalizations, now: Instant) -> Vec<PendingFinalizer> {
    let expired = map
        .values_mut()
        .flat_map(|pending| pending.extract_if(.., |finalizer| finalizer.is_expired(now)))
        .collect();
    map.retain(|_, pending| !pending.is_empty());
    expired
}

pub struct BundleProcessor {
    pub(super) data_manager: DataManager,
    /// Bundle processors built from the registered handlers, reused across bundles.
    processors: ProcessorCache,
    short_id_cache: ShortIdCache,
    pub(super) active_bundles: Arc<std::sync::RwLock<HashMap<String, ActiveBundleTracker>>>,
    pending_finalizations: Arc<Mutex<PendingFinalizations>>,
    /// The number of finalization callbacks that expired before they could run.
    expired_finalizations: AtomicU64,
    worker_id: String,
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
            worker_id: String::new(),
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

    /// Stores the finalization callbacks of a completed bundle. A runner does not finalize every
    /// bundle, for example when the commit of a bundle fails. The finalizer sweeper drops such
    /// callbacks when they expire.
    pub fn store_finalization(&self, instruction_id: &str, callbacks: Vec<PendingFinalizer>) {
        if let Ok(mut map) = self.pending_finalizations.lock() {
            map.insert(instruction_id.to_string(), callbacks);
        }
    }

    /// Drops the expired callbacks of all pending bundles and returns their number. The task of
    /// [`spawn_finalizer_sweeper`] calls this function at each tick.
    #[doc(hidden)]
    pub fn sweep_expired_finalizations(&self) -> usize {
        // The callbacks drop after the guard, so the state that they own drops outside the lock.
        let expired = self
            .pending_finalizations
            .lock()
            .map(|mut map| take_expired(&mut map, Instant::now()))
            .unwrap_or_default();
        self.record_dropped(
            expired.len(),
            "bundles that the runner did not finalize before the deadline",
        );
        expired.len()
    }

    /// Counts the finalization callbacks that expired before they could run. The count includes
    /// the callbacks that the runner did not finalize and the callbacks that it finalized late.
    #[doc(hidden)]
    pub fn expired_finalization_count(&self) -> u64 {
        self.expired_finalizations.load(Ordering::Relaxed)
    }

    /// Counts and logs `dropped` expired callbacks of `scope`.
    fn record_dropped(&self, dropped: usize, scope: impl fmt::Display) {
        if dropped > 0 {
            self.expired_finalizations
                .fetch_add(dropped as u64, Ordering::Relaxed);
            warn!(
                "Dropped {dropped} expired bundle finalization callback(s) of {scope}. These \
                 callbacks do not run."
            );
        }
    }

    /// Runs the callbacks of the bundle `instruction_id` that have not expired. On failure,
    /// returns the joined messages of the failed callbacks or a poisoned-lock message.
    pub fn finalize_bundle(&self, instruction_id: &str) -> Result<(), String> {
        let callbacks = self
            .pending_finalizations
            .lock()
            .map_err(|e| format!("Lock poisoned: {e}"))?
            .remove(instruction_id);

        let Some(callbacks) = callbacks else {
            debug!(
                "No pending finalizations for instruction '{}' (may already be finalized)",
                instruction_id
            );
            return Ok(());
        };

        let now = Instant::now();
        let (expired, live): (Vec<_>, Vec<_>) = callbacks
            .into_iter()
            .partition(|finalizer| finalizer.is_expired(now));
        self.record_dropped(
            expired.len(),
            format_args!("instruction '{instruction_id}'"),
        );
        let count = live.len();
        let errors: Vec<String> = live
            .into_iter()
            .filter_map(|finalizer| (finalizer.callback)().err().map(|e| e.to_string()))
            .collect();

        if errors.is_empty() {
            debug!(
                "Successfully executed {} bundle finalization callback(s) for instruction '{}'",
                count, instruction_id
            );
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// Sets the worker id that the state channel sends as gRPC metadata, so the runner can
    /// match the stream with the worker that registered on the control channel.
    /// the worker attaches it to the other channels at connection time.
    pub fn with_worker_id(mut self, worker_id: String) -> Self {
        self.worker_id = worker_id;
        self
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
        let endpoint = descriptor
            .transforms
            .values()
            .find(|t| {
                super::plan::has_urn(t, URN_DATA_SOURCE) || super::plan::has_urn(t, URN_DATA_SINK)
            })
            .and_then(|t| super::plan::decode_port(t).ok())
            .and_then(|port| port.api_service_descriptor)
            .map(|desc| desc.url)
            .filter(|url| !url.is_empty());

        self.data_manager
            .ensure_stream_endpoint(data_stream_id, endpoint.as_deref())
            .await?;

        // The processor holds set-up handler instances and the plan (source, sinks,
        // topological order, routing). Bundles of the same descriptor reuse both.
        let mut processor = self.processors.acquire(descriptor)?;
        let (plan, instances) = processor.parts();
        let graph = plan.graph();

        debug!(
            "Processing bundle instruction '{}' (descriptor: '{}', source: '{}', {} transforms, {} sinks)",
            instruction_id,
            descriptor.id,
            plan.source_id(),
            graph.operator_ids.len(),
            plan.sink_count()
        );

        let data_inbound = self.data_manager.register_inbound(instruction_id).await;
        let timers_inbound = self
            .data_manager
            .register_inbound_timers(instruction_id)
            .await;

        if let Some(elements) = embedded_elements {
            let dm = self.data_manager.clone();
            tokio::spawn(async move {
                dm.handle_inbound_elements(elements).await;
            });
        }

        let inputs = Inputs {
            data: data_inbound,
            timers: timers_inbound,
            plan,
            instruction_id,
        };
        let timer_collector = Arc::new(beam::internals::TimerCollector::new());
        let residual_collector = Arc::new(beam::internals::ResidualCollector::new());
        let bundle_finalizer = Arc::new(beam::internals::BundleFinalizerCollector::new());

        let mut pcollection_stats = PCollectionStats::new(graph);
        // Create the sampler after the graph so that every operator has a sampling slot.
        let sampler = super::sampler::ExecutionSampler::new(instruction_id, &graph.operator_ids);
        let mut read = ReadProgress::new();

        let metrics_container = Arc::new(MetricsContainer::new());
        let transform_metrics = Arc::new(beam::internals::TransformMetrics::new(
            Arc::clone(&metrics_container),
            Arc::clone(&graph.transform_ids),
        ));

        let tracker = ActiveBundleTracker::new(
            &descriptor.id,
            plan.source_id(),
            Arc::clone(plan.roots()),
            metrics_container.clone(),
            Some(sampler.clone()),
        );

        let exec_result: Result<Option<Elements>, BundleError> = async {
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
                &metrics_container,
                &sampler,
                data_sampler,
            )?;

            // Register for split and progress requests only when the handlers have started. A
            // split before that point would end the bundle before it reads an element.
            if let Ok(mut active) = self.active_bundles.write() {
                active.insert(instruction_id.to_string(), tracker.clone());
            }

            let state = StateAccess::new(
                instruction_id,
                descriptor,
                &self.worker_id,
                graph,
                &self.state_cache,
                cache_tokens,
            );
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
                timer_collector: Some(&timer_collector),
                residual_collector: Some(&residual_collector),
                metrics_container: Some(&metrics_container),
                transform_metrics: Some(&transform_metrics),
                bundle_finalizer: Some(&bundle_finalizer),
                dynamic_split_registrar: Some(tracker.dynamic_split_registrar()),
                state_stream_reader: state.stream_reader.as_ref(),
                stats: &mut pcollection_stats,
                sampler: &sampler,
                data_sampler,
                pcollection_samplers: &pcollection_samplers,
                instruction_id,
            };
            // Progress reports the start phase while the bundle waits for its first input.
            tracker.sync_from(&chain_ctx);

            self.read_inputs(
                &mut chain_ctx,
                instances,
                &tracker,
                plan.source_coder_id(),
                inputs,
                &mut read,
            )
            .await?;
            if let Some(reason) = tracker.split_failure() {
                return Err(BundleError::Coder(format!(
                    "An SDF split was granted but could not be sent to the runner: {reason}"
                )));
            }

            tracker.set_phase(BundlePhase::Finishing);
            self.finish_bundle_handlers(&mut chain_ctx, instances, &read.last_header)?;
            if let Some(user_state) = &state.user_state {
                user_state.commit().map_err(BundleError::Handler)?;
            }
            tracker.sync_from(&chain_ctx);

            emit_timers(plan, &mut chain_ctx)?;
            let can_embed = crate::provisioning::runner_capabilities()
                .contains(beam::pipeline::PROTOCOL_CONTROL_RESPONSE_ELEMENTS_EMBEDDING);
            // A bundle without outbound endpoints leaves `elements` unset: Dataflow rejects a
            // present but empty message as incomplete.
            let has_outbound = plan.sink_count() > 0 || plan.has_timer_families();
            // The proto allows embedded output only when no output went on the data stream.
            if can_embed && has_outbound && !outbound.has_flushed() {
                let embedded = outbound.take_elements_for_embedding();
                Ok(Some(embedded))
            } else {
                outbound.finish().await.map_err(BundleError::from)?;
                Ok(None)
            }
        }
        .await;

        // Clean up the inbound registrations and the tracker also when the bundle fails.
        self.unregister_bundle(instruction_id).await;

        let embedded_elements_out = exec_result?;

        // The final read index is the count of processed elements. Progress reports
        // contain the index of the current element. Do not make the two values the
        // same: the runner compares the final value with the split it granted.
        let final_read_index = tracker.finish(read.elements);

        let user_metrics = metrics_container.to_monitoring_infos();
        let transform_msecs = sampler
            .execution_times(&graph.operator_ids)
            .map(|(t_id, times)| (t_id.to_string(), times))
            .collect();

        let (monitoring_infos, monitoring_data) = self.collect_bundle_metrics(
            plan.source_id(),
            final_read_index,
            pcollection_stats.counts_by_id(graph),
            pcollection_stats.sizes_by_id(graph),
            transform_msecs,
            user_metrics,
        );

        debug!(
            "Successfully completed bundle instruction '{}' (elements read: {}, metrics: {}, data: {})",
            instruction_id,
            read.elements,
            monitoring_infos.len(),
            monitoring_data.len()
        );

        let residual_roots: Vec<_> = residual_collector
            .drain()
            .into_iter()
            .map(|r| plan.roots().residual(r))
            .collect::<Result<Vec<_>, _>>()?;

        // The bundle succeeded, so its processor is clean and can run the next bundle.
        processor.recycle();

        let requires_finalization = bundle_finalizer.has_callbacks();
        if requires_finalization {
            self.store_finalization(instruction_id, bundle_finalizer.drain());
        }

        Ok(ProcessBundleResponse {
            residual_roots,
            monitoring_infos,
            requires_finalization,
            monitoring_data,
            elements: embedded_elements_out,
        })
    }

    /// Pushes the bundle data and timers through the chain until both streams end or the
    /// runner splits off the rest of the bundle.
    async fn read_inputs(
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
        let mut timers_closed = !inputs.plan.has_timer_families();
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
                            Ok(()) => emit_timers(inputs.plan, ctx),
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
                        if !read.carry.is_empty() {
                            return Err(BundleError::Coder(format!(
                                "Inbound data stream closed for instruction '{}' (descriptor: '{}', \
                                 source_coder_id: '{}', elements read: {}) with {} trailing unconsumed \
                                 bytes: {:02x?}",
                                instruction_id,
                                ctx.descriptor.id,
                                source_coder_id,
                                read.elements,
                                read.carry.len(),
                                &read.carry[..read.carry.len().min(64)]
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
                msg = inputs.timers.recv(), if !timers_closed => match msg {
                    Some(Some(timers)) => {
                        read.input_arrived();
                        tracker.set_consuming(true);
                        let coder_id = inputs.plan.timer_coder_id(&timers);
                        let pushed = match self
                            .push_timer_element(ctx, instances, &timers, coder_id, &mut read.last_header)
                            .and_then(|()| emit_timers(inputs.plan, ctx))
                        {
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
                _ = tokio::time::sleep(INBOUND_WAIT_WARNING) => {
                    if read.elements == 0 {
                        warn!(
                            "Still waiting for inbound data on instruction '{}' (descriptor: '{}', elapsed: {}s, buffered_raw_bytes: {})",
                            instruction_id,
                            ctx.descriptor.id,
                            read.started.elapsed().as_secs(),
                            read.carry.len()
                        );
                    } else if read.last_input.elapsed() >= INBOUND_IDLE_WARNING * (read.idle_warnings + 1) {
                        read.idle_warnings += 1;
                        let inbound = self.data_manager.describe_inbound(instruction_id).await;
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
        }
        // Close the queues so that the stream skips this bundle while it finishes.
        inputs.data.close();
        inputs.timers.close();
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
        // Decode from the received chunk; carry over only a trailing partial element.
        let chunk = if read.carry.is_empty() {
            bytes
        } else {
            read.carry.extend_from_slice(&bytes);
            std::mem::take(&mut read.carry)
        };
        let (elements, consumed) = framing.decode_ready_elements(&chunk)?;

        for element in elements {
            // Claim the element before any work on it. The claim fails when the runner has
            // taken back the tail of this bundle; this and all later elements belong to it.
            if !tracker.begin_element() {
                read.split_reached = true;
                break;
            }
            self.push_source_element(ctx, instances, &element.header, element.payload(&chunk))?;
            read.last_header = element.header;
            // Drain a full outbound queue here, between elements, to apply back-pressure.
            if ctx.outbound.has_pending() {
                ctx.outbound.drain().await?;
            }
            read.elements += 1;
            // Publishing progress copies every counter map, so throttle it. The runner
            // polls for progress much less often than this interval.
            if read.last_sync.elapsed() >= PROGRESS_SYNC_INTERVAL {
                tracker.sync_from(ctx);
                read.last_sync = Instant::now();
            }
        }
        read.carry = chunk;
        read.carry.drain(..consumed);
        Ok(())
    }
}

/// A bundle's inbound queues.
struct Inputs<'a> {
    data: mpsc::Receiver<Option<Vec<u8>>>,
    timers: mpsc::Receiver<Option<ElementTimers>>,
    plan: &'a BundlePlan,
    instruction_id: &'a str,
}

/// How far a bundle has read its inbound data.
struct ReadProgress {
    /// The trailing partial element of the last chunk. The next chunk completes it.
    carry: Vec<u8>,
    last_header: WindowedHeader,
    elements: i64,
    started: Instant,
    last_sync: Instant,
    /// True when the runner has taken back the tail of this bundle.
    split_reached: bool,
    /// When the last data or timer chunk arrived.
    last_input: Instant,
    /// Idle warnings logged since the last chunk.
    idle_warnings: u32,
}

impl ReadProgress {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            carry: Vec::new(),
            last_header: WindowedHeader::default(),
            elements: 0,
            started: now,
            last_sync: now,
            split_reached: false,
            last_input: now,
            idle_warnings: 0,
        }
    }

    fn input_arrived(&mut self) {
        self.last_input = Instant::now();
        self.idle_warnings = 0;
    }
}

/// A bundle's access to runner state: side inputs, user state and state-backed iterables.
struct StateAccess {
    stream_reader: Option<Arc<dyn beam::coders::StateStreamReader>>,
    side_inputs: Option<Arc<dyn beam::internals::SideInputReader>>,
    user_state: Option<crate::user_state::BundleUserState>,
    /// User state scoped once per operator (not per call), in graph operator order.
    scoped: Vec<Arc<dyn beam::internals::UserStateBackend>>,
}

impl StateAccess {
    fn new(
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        worker_id: &str,
        graph: &OperatorGraph,
        cache: &Arc<crate::state::StateCache>,
        cache_tokens: &crate::state::CacheTokens,
    ) -> Self {
        let channel =
            crate::state::StateChannel::from_descriptor(instruction_id, descriptor, worker_id);
        let stream_reader = channel
            .as_ref()
            .map(|ch| Arc::new(ch.clone()) as Arc<dyn beam::coders::StateStreamReader>);
        let coders = Arc::new(descriptor.coders.clone());
        let side_inputs = crate::state::FnApiSideInputReader::from_channel_with_coders(
            descriptor,
            channel.clone(),
            Arc::clone(&coders),
        )
        .map(|reader| reader.with_shared_cache(Arc::clone(cache), &cache_tokens.side_input_tokens))
        .map(|r| Arc::new(r) as Arc<dyn beam::internals::SideInputReader>);
        let user_state_cache =
            cache_tokens
                .user_state_token
                .clone()
                .map(|token| crate::user_state::UserStateCache {
                    cache: Arc::clone(cache),
                    token,
                });
        let user_state = channel.and_then(|ch| {
            crate::user_state::BundleUserState::from_channel_with_cache(
                descriptor,
                ch,
                Arc::clone(&coders),
                user_state_cache,
            )
        });
        let scoped = user_state
            .as_ref()
            .map(|state| {
                graph
                    .operator_ids
                    .iter()
                    .map(|t_id| state.scoped(t_id))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            stream_reader,
            side_inputs,
            user_state,
            scoped,
        }
    }
}

impl BundleProcessor {
    /// Drops the inbound queues of `instruction_id` and its split and progress state. A bundle
    /// that ends, also with an error or a panic, must call it, so later requests do not see it.
    pub(crate) async fn unregister_bundle(&self, instruction_id: &str) {
        self.data_manager.unregister_inbound(instruction_id).await;
        self.data_manager
            .unregister_inbound_timers(instruction_id)
            .await;
        if let Ok(mut active) = self.active_bundles.write() {
            active.remove(instruction_id);
        }
    }
}

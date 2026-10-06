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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{Instrument, debug, warn};

use crate::data::{DataError, DataManager};
use beam::coders::WindowedHeader;
use model::fn_execution::{
    DelayedBundleApplication, ProcessBundleDescriptor, ProcessBundleResponse,
    elements::Timers as ElementTimers,
};

use super::BundleError;
use super::cache::{BundlePlan, ProcessorCache};
use super::chain::{ChainCtx, Instances, OperatorGraph, PCollectionStats};
use super::decoding::decode_ready_elements;
use super::execution::ActiveBundleTracker;
use beam::internals::TransformFn;

/// Maximum age of the progress that the runner reads while a bundle runs.
const PROGRESS_SYNC_INTERVAL: Duration = Duration::from_millis(100);

/// Interval between warnings while a bundle waits for its first element.
const INBOUND_WAIT_WARNING: Duration = Duration::from_secs(15);

pub const URN_DATA_SOURCE: &str = "beam:runner:source:v1";
pub const URN_DATA_SINK: &str = "beam:runner:sink:v1";

pub struct BundleProcessor {
    data_manager: DataManager,
    /// Bundle processors built from the registered handlers, reused across bundles.
    processors: ProcessorCache,
    short_id_cache: beam::metrics::ShortIdCache,
    active_bundles: Arc<std::sync::RwLock<HashMap<String, ActiveBundleTracker>>>,
    pending_finalizations:
        Arc<std::sync::Mutex<HashMap<String, Vec<beam::internals::FinalizationCallback>>>>,
    worker_id: String,
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
            short_id_cache: beam::metrics::ShortIdCache::new(),
            active_bundles: Arc::new(std::sync::RwLock::new(HashMap::new())),
            pending_finalizations: Arc::new(std::sync::Mutex::new(HashMap::new())),
            worker_id: String::new(),
        }
    }

    pub(super) fn active_bundle(&self, instruction_id: &str) -> Option<ActiveBundleTracker> {
        self.active_bundles
            .read()
            .ok()?
            .get(instruction_id)
            .cloned()
    }

    /// Stores registered bundle finalization callbacks for a completed bundle.
    pub fn store_finalization(
        &self,
        instruction_id: &str,
        callbacks: Vec<beam::internals::FinalizationCallback>,
    ) {
        if let Ok(mut map) = self.pending_finalizations.lock() {
            map.insert(instruction_id.to_string(), callbacks);
        }
    }

    /// Runs the finalization callbacks registered for the bundle `instruction_id`. On
    /// failure, returns the joined messages of the failed callbacks or a poisoned-lock message.
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

        let count = callbacks.len();
        let errors: Vec<String> = callbacks
            .into_iter()
            .filter_map(|cb| cb().err().map(|e| e.to_string()))
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

    /// Tears down the idle bundle processors and runs teardown on each handler instance.
    pub fn shutdown(&self) {
        self.processors.shutdown();
    }

    /// Returns the short ID cache that registers bundle metrics.
    pub fn short_id_cache(&self) -> &beam::metrics::ShortIdCache {
        &self.short_id_cache
    }

    /// Runs one bundle as its `ProcessBundleDescriptor` describes.
    pub async fn process_bundle(
        &self,
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        data_stream_id: &str,
    ) -> Result<ProcessBundleResponse, BundleError> {
        let span = tracing::debug_span!("bundle", instruction_id = %instruction_id);
        self.process_bundle_inner(instruction_id, descriptor, data_stream_id)
            .instrument(span)
            .await
    }

    async fn process_bundle_inner(
        &self,
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        data_stream_id: &str,
    ) -> Result<ProcessBundleResponse, BundleError> {
        self.data_manager.ensure_stream(data_stream_id).await?;

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

        let inputs = Inputs {
            data: self.data_manager.register_inbound(instruction_id).await,
            timers: self
                .data_manager
                .register_inbound_timers(instruction_id)
                .await,
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

        let metrics_container = Arc::new(beam::metrics::MetricsContainer::new());

        let tracker = ActiveBundleTracker::new(plan.source_id(), metrics_container.clone());
        if let Ok(mut active) = self.active_bundles.write() {
            active.insert(instruction_id.to_string(), tracker.clone());
        }

        let exec_result: Result<(), BundleError> = async {
            self.start_bundle_handlers(graph, instances, &metrics_container)?;

            let state = StateAccess::new(instruction_id, descriptor, &self.worker_id, graph);
            let mut outbound =
                self.data_manager
                    .outbound(data_stream_id, instruction_id, plan.sink_ids());
            let mut chain_ctx = ChainCtx {
                descriptor,
                graph,
                outbound: &mut outbound,
                side_input_reader: state.side_inputs.as_ref(),
                state_readers: &state.scoped,
                timer_collector: Some(&timer_collector),
                residual_collector: Some(&residual_collector),
                metrics_container: Some(&metrics_container),
                bundle_finalizer: Some(&bundle_finalizer),
                dynamic_split_registrar: Some(tracker.dynamic_split_registrar()),
                state_stream_reader: state.stream_reader.as_ref(),
                stats: &mut pcollection_stats,
                sampler: &sampler,
            };

            self.read_inputs(
                &mut chain_ctx,
                instances,
                &tracker,
                plan.source_coder_id(),
                inputs,
                &mut read,
            )
            .await?;

            self.finish_bundle_handlers(&mut chain_ctx, instances, &read.last_header)?;
            if let Some(user_state) = &state.user_state {
                user_state.commit().map_err(BundleError::Coder)?;
            }
            tracker.sync_from(&chain_ctx);

            let timers = plan.encode_end_of_bundle_timers(
                instruction_id,
                timer_collector.drain_family_records(),
            )?;
            outbound.finish(timers).await.map_err(BundleError::from)
        }
        .await;

        // Clean up the inbound registrations and the tracker also when the bundle fails.
        self.data_manager.unregister_inbound(instruction_id).await;
        self.data_manager
            .unregister_inbound_timers(instruction_id)
            .await;
        if let Ok(mut active) = self.active_bundles.write() {
            active.remove(instruction_id);
        }

        exec_result?;

        // The final read index is the count of processed elements. Progress reports
        // contain the index of the current element. Do not make the two values the
        // same: the runner compares the final value with the split it granted.
        let final_read_index = tracker.finish(read.elements);

        let user_metrics = metrics_container.to_monitoring_infos();
        let transform_msecs = sampler
            .msecs(&graph.operator_ids)
            .map(|(t_id, msecs)| (t_id.to_string(), msecs))
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

        let residual_roots = residual_collector
            .drain()
            .into_iter()
            .map(delayed_application)
            .collect();

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
            elements: None,
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
            tokio::select! {
                msg = inputs.data.recv(), if !data_closed => match msg {
                    Some(Some(bytes)) => {
                        self.push_chunk(ctx, instances, tracker, source_coder_id, read, bytes)
                            .await?;
                        if read.split_reached {
                            debug!(
                                "Stopping instruction '{}' at split boundary (elements processed: {})",
                                instruction_id, read.elements
                            );
                        }
                    }
                    Some(None) => {
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
                        let coder_id = inputs.plan.timer_coder_id(&timers);
                        self.push_timer_element(ctx, instances, &timers, coder_id, &mut read.last_header)?;
                        ctx.outbound.drain().await?;
                    }
                    Some(None) => {
                        debug!("Inbound timer stream closed for instruction '{instruction_id}'");
                        timers_closed = true;
                    }
                    None => return Err(DataError::StreamEnded(instruction_id.to_string()).into()),
                },
                _ = tokio::time::sleep(INBOUND_WAIT_WARNING) => {
                    if read.elements == 0 {
                        warn!(
                            "Still waiting for inbound data on instruction '{}' (descriptor: '{}', elapsed: {}s, buffered_raw_bytes: {})",
                            instruction_id,
                            ctx.descriptor.id,
                            read.started.elapsed().as_secs(),
                            read.carry.len()
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
        source_coder_id: &str,
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
        let (elements, consumed) =
            decode_ready_elements(&chunk, source_coder_id, &ctx.descriptor.coders)?;

        for element in &elements {
            // Claim the element before any work on it. The claim fails when the runner has
            // taken back the tail of this bundle; this and all later elements belong to it.
            if !tracker.begin_element() {
                read.split_reached = true;
                break;
            }
            read.last_header = element.header.clone();
            self.push_source_element(ctx, instances, &element.header, element.payload(&chunk))?;
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
        }
    }
}

/// A bundle's access to runner state: side inputs, user state and state-backed iterables.
struct StateAccess {
    stream_reader: Option<Arc<dyn beam::coders::StateStreamReader>>,
    side_inputs: Option<Arc<dyn beam::internals::SideInputReader>>,
    user_state: Option<crate::user_state::BundleUserState>,
    /// User state scoped once per operator (not per call), in graph operator order.
    scoped: Vec<Arc<dyn beam::internals::UserStateReader>>,
}

impl StateAccess {
    fn new(
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        worker_id: &str,
        graph: &OperatorGraph,
    ) -> Self {
        let channel =
            crate::state::StateChannel::from_descriptor(instruction_id, descriptor, worker_id);
        let stream_reader = channel
            .as_ref()
            .map(|ch| Arc::new(ch.clone()) as Arc<dyn beam::coders::StateStreamReader>);
        let side_inputs =
            crate::state::FnApiSideInputReader::from_channel(descriptor, channel.clone())
                .map(|r| Arc::new(r) as Arc<dyn beam::internals::SideInputReader>);
        let user_state =
            channel.and_then(|ch| crate::user_state::BundleUserState::from_channel(descriptor, ch));
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

/// Converts a residual of the bundle into the form that the runner expects.
fn delayed_application(r: beam::internals::ResidualApplication) -> DelayedBundleApplication {
    let output_watermarks = r
        .output_watermarks
        .into_iter()
        .map(|(out_tag, wm_millis)| (out_tag, beam::windowing::watermark_to_proto(wm_millis)))
        .collect();
    let is_bounded = if r.is_bounded {
        model::pipeline::is_bounded::Enum::Bounded
    } else {
        model::pipeline::is_bounded::Enum::Unbounded
    } as i32;
    DelayedBundleApplication {
        application: Some(model::fn_execution::BundleApplication {
            transform_id: r.transform_id,
            input_id: r.input_id,
            element: r.element,
            output_watermarks,
            is_bounded,
        }),
        requested_time_delay: r.delay.map(beam::windowing::duration_to_proto),
    }
}

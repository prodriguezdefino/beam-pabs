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
//! hold half-finished state, so it is torn down, as is one idle longer than [`IDLE_TIMEOUT`].

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use model::fn_execution::{ProcessBundleDescriptor, elements::Timers as ElementTimers};
use tracing::{debug, warn};

use super::BundleError;
use super::chain::{Instances, OperatorGraph, Source};
use super::execution::source_output_pcolls;
use super::plan::{collect_sinks, collect_timer_families, decode_port, has_urn, topological_order};
use super::processor::URN_DATA_SOURCE;
use beam::internals::TransformFn;

/// Maximum time that an idle processor stays in the pool before teardown.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Immutable topology and port metadata resolved once per `ProcessBundleDescriptor`.
pub(super) struct BundlePlan {
    source_id: String,
    source_coder_id: String,
    sink_ids: Vec<String>,
    /// Maps a transform id to the timer families it declares, as `(family id, coder id)`.
    timer_families: HashMap<String, Vec<(String, String)>>,
    /// The routing between the operators of this processor.
    graph: OperatorGraph,
}

impl BundlePlan {
    pub(super) fn source_id(&self) -> &str {
        &self.source_id
    }

    pub(super) fn source_coder_id(&self) -> &str {
        &self.source_coder_id
    }

    pub(super) fn sink_ids(&self) -> impl Iterator<Item = &str> {
        self.sink_ids.iter().map(String::as_str)
    }

    pub(super) fn sink_count(&self) -> usize {
        self.sink_ids.len()
    }

    pub(super) fn graph(&self) -> &OperatorGraph {
        &self.graph
    }

    pub(super) fn has_timer_families(&self) -> bool {
        !self.timer_families.is_empty()
    }

    /// Returns the coder of the timer family of `timers`, if the transform declares it.
    pub(super) fn timer_coder_id(&self, timers: &ElementTimers) -> Option<&str> {
        self.timer_families
            .get(&timers.transform_id)?
            .iter()
            .find(|(family_id, _)| *family_id == timers.timer_family_id)
            .map(|(_, coder_id)| coder_id.as_str())
    }

    /// Returns the fired timers of each timer family, if any, then its end-of-stream marker.
    pub(super) fn encode_end_of_bundle_timers(
        &self,
        instruction_id: &str,
        mut fired: HashMap<String, Vec<beam::coders::TimerRecord>>,
    ) -> Result<Vec<ElementTimers>, BundleError> {
        self.timer_families
            .iter()
            .flat_map(|(t_id, families)| families.iter().map(move |(fam_id, _)| (t_id, fam_id)))
            .map(|(t_id, fam_id)| {
                let mut encoded = Vec::new();
                fired
                    .remove(fam_id)
                    .unwrap_or_default()
                    .iter()
                    .try_for_each(|rec| beam::coders::TimerCoder::encode(rec, &mut encoded))
                    .map_err(BundleError::TimerCoding)?;
                let chunk = |timers: Vec<u8>, is_last: bool| ElementTimers {
                    instruction_id: instruction_id.to_string(),
                    transform_id: t_id.clone(),
                    timer_family_id: fam_id.clone(),
                    timers,
                    is_last,
                };
                let fired = (!encoded.is_empty()).then(|| chunk(encoded, false));
                Ok(fired.into_iter().chain([chunk(Vec::new(), true)]))
            })
            .collect::<Result<Vec<_>, BundleError>>()
            .map(|chunks| chunks.into_iter().flatten().collect())
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
        let (source_id, source_t) = descriptor
            .transforms
            .iter()
            .find(|(_, t)| has_urn(t, URN_DATA_SOURCE))
            .ok_or_else(|| BundleError::MissingSource(descriptor.id.clone()))?;
        let sinks = collect_sinks(descriptor)?;
        let ordered_transform_ids = topological_order(descriptor, source_id, source_t);
        let source_port = decode_port(source_t)?;
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
            "Created bundle processor for descriptor '{}' ({} transforms)",
            descriptor.id,
            ordered_transform_ids.len()
        );
        let sink_ids = sinks.into_iter().map(|(id, _)| id).collect();
        Ok(Self {
            plan: BundlePlan {
                source_id: source_id.clone(),
                source_coder_id: source_port.coder_id,
                sink_ids,
                timer_families: collect_timer_families(descriptor),
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

/// An idle processor and the time it went back to the pool.
type Idle = (Instant, PreparedProcessor);

/// The worker's bundle processors, pooled per descriptor id.
pub(super) struct ProcessorCache {
    /// The handlers registered with the pipeline. The cache clones them and never runs them.
    prototypes: Arc<HashMap<String, TransformFn>>,
    idle: Mutex<HashMap<String, Vec<Idle>>>,
}

impl ProcessorCache {
    pub(super) fn new(prototypes: Arc<HashMap<String, TransformFn>>) -> Self {
        Self {
            prototypes,
            idle: Mutex::new(HashMap::new()),
        }
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
            take_expired(idle, now)
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

/// Removes the processors that have been idle longer than [`IDLE_TIMEOUT`].
fn take_expired(idle: &mut HashMap<String, Vec<Idle>>, now: Instant) -> Vec<PreparedProcessor> {
    let is_expired = |returned: &Instant| now.duration_since(*returned) > IDLE_TIMEOUT;
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

    fn deref(&self) -> &PreparedProcessor {
        self.processor
            .as_ref()
            .expect("a lease holds its processor until recycled or dropped")
    }
}

impl DerefMut for ProcessorLease<'_> {
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

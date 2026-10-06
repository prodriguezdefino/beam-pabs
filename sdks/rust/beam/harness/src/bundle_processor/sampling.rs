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

//! Data sampling for PCollections and bundle exceptions.
//!
//! Pipeline experiments turn sampling on:
//! - `enable_data_sampling`: samples the elements of all PCollections and the failed elements.
//! - `enable_always_on_exception_sampling`: samples failed elements only.
//! - `disable_always_on_exception_sampling`: turns off the sampling of failed elements.
//!
//! The runner reads and clears the samples with `SampleDataRequest`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use model::fn_execution::{
    SampleDataRequest, SampleDataResponse, SampledElement, sample_data_response::ElementList,
    sampled_element::Exception as SampledException,
};

/// The default number of samples that one PCollection keeps.
pub const DEFAULT_MAX_SAMPLES: usize = 10;

/// After the first [`DEFAULT_MAX_SAMPLES`] elements, a PCollection samples one element in this
/// many.
pub const DEFAULT_SAMPLE_EVERY_N: u64 = 1000;

/// The experiment that samples all PCollections and the failed elements.
pub const ENABLE_DATA_SAMPLING_EXPERIMENT: &str = "enable_data_sampling";

/// The experiment that samples the failed elements only.
pub const ENABLE_ALWAYS_ON_EXCEPTION_SAMPLING_EXPERIMENT: &str =
    "enable_always_on_exception_sampling";

/// The experiment that turns off the sampling of failed elements.
pub const DISABLE_ALWAYS_ON_EXCEPTION_SAMPLING_EXPERIMENT: &str =
    "disable_always_on_exception_sampling";

/// The failed bundles that the sampler remembers, to keep one sample per failure.
const REMEMBERED_FAILED_BUNDLES: usize = 100;

/// The sampling settings that all PCollections share.
#[derive(Clone, Copy, Debug)]
pub struct SamplingConfig {
    pub max_samples: usize,
    pub sample_every_n: u64,
    /// Sample failed elements only.
    pub only_exceptions: bool,
}

/// The samples of one PCollection.
#[derive(Default)]
struct Samples {
    elements: VecDeque<SampledElement>,
    exceptions: VecDeque<SampledElement>,
}

/// Keeps the last samples of one PCollection.
pub struct PCollectionSampler {
    /// The elements seen so far. It selects the elements to sample without a lock.
    count: AtomicU64,
    samples: Mutex<Samples>,
    config: SamplingConfig,
}

impl PCollectionSampler {
    pub fn new(config: SamplingConfig) -> Self {
        Self {
            count: AtomicU64::new(0),
            samples: Mutex::default(),
            config,
        }
    }

    /// Samples `element` if the schedule selects it. Other elements cost one atomic add.
    #[inline]
    pub fn sample(&self, element: &[u8]) {
        if self.config.only_exceptions {
            return;
        }
        let index = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        if index > self.config.max_samples as u64
            && !index.is_multiple_of(self.config.sample_every_n)
        {
            return;
        }
        let sample = SampledElement {
            element: element.to_vec(),
            sample_timestamp: current_timestamp(),
            exception: None,
        };
        push_bounded(&mut self.lock().elements, sample, self.config.max_samples);
    }

    fn record_exception(&self, exception: SampledException, element: Option<&[u8]>) {
        let sample = SampledElement {
            element: element.map(<[u8]>::to_vec).unwrap_or_default(),
            sample_timestamp: current_timestamp(),
            exception: Some(exception),
        };
        push_bounded(&mut self.lock().exceptions, sample, self.config.max_samples);
    }

    /// Takes all samples, failed elements first.
    pub fn drain_samples(&self) -> Vec<SampledElement> {
        let mut samples = self.lock();
        let Samples {
            elements,
            exceptions,
        } = &mut *samples;
        exceptions.drain(..).chain(elements.drain(..)).collect()
    }

    /// Locks the samples. A panic in another thread cannot leave them in an invalid state.
    fn lock(&self) -> MutexGuard<'_, Samples> {
        self.samples.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Appends `item`, and removes the oldest item when `queue` holds more than `max`.
fn push_bounded<T>(queue: &mut VecDeque<T>, item: T, max: usize) {
    if queue.len() >= max {
        queue.pop_front();
    }
    queue.push_back(item);
}

/// The samplers of all PCollections of the worker.
pub struct DataSampler {
    samplers: Mutex<HashMap<String, Arc<PCollectionSampler>>>,
    /// Bundles that have a failure sample already.
    failed_bundles: Mutex<VecDeque<String>>,
    config: SamplingConfig,
}

impl DataSampler {
    /// The sampler that the pipeline experiments ask for, or `None` when they ask for none.
    pub fn from_experiments(experiments: &[String]) -> Option<Self> {
        let has = |name: &str| experiments.iter().any(|e| e == name);
        let exceptions = has(ENABLE_ALWAYS_ON_EXCEPTION_SAMPLING_EXPERIMENT)
            && !has(DISABLE_ALWAYS_ON_EXCEPTION_SAMPLING_EXPERIMENT);
        let elements = has(ENABLE_DATA_SAMPLING_EXPERIMENT);
        (elements || exceptions)
            .then(|| Self::new(DEFAULT_MAX_SAMPLES, DEFAULT_SAMPLE_EVERY_N, !elements))
    }

    pub fn new(max_samples: usize, sample_every_n: u64, only_exceptions: bool) -> Self {
        Self {
            samplers: Mutex::default(),
            failed_bundles: Mutex::default(),
            config: SamplingConfig {
                max_samples,
                sample_every_n,
                only_exceptions,
            },
        }
    }

    pub fn config(&self) -> SamplingConfig {
        self.config
    }

    /// The sampler of `pcollection_id`. Bundles call it once per PCollection.
    pub fn sampler_for_pcollection(&self, pcollection_id: &str) -> Arc<PCollectionSampler> {
        let mut samplers = self.samplers.lock().unwrap_or_else(PoisonError::into_inner);
        match samplers.get(pcollection_id) {
            Some(sampler) => Arc::clone(sampler),
            None => Arc::clone(
                samplers
                    .entry(pcollection_id.to_string())
                    .or_insert_with(|| Arc::new(PCollectionSampler::new(self.config))),
            ),
        }
    }

    /// Records a failure. Only the first call of a bundle records: the failing transform.
    pub fn record_exception(
        &self,
        pcollection_id: &str,
        instruction_id: &str,
        transform_id: &str,
        error: &str,
        element: Option<&[u8]>,
    ) {
        {
            let mut failed = self
                .failed_bundles
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if failed.iter().any(|id| id == instruction_id) {
                return;
            }
            push_bounded(
                &mut failed,
                instruction_id.to_string(),
                REMEMBERED_FAILED_BUNDLES,
            );
        }
        let exception = SampledException {
            instruction_id: instruction_id.to_string(),
            transform_id: transform_id.to_string(),
            error: error.to_string(),
        };
        self.sampler_for_pcollection(pcollection_id)
            .record_exception(exception, element);
    }

    /// Takes the samples of the PCollections in `request`, or of all when it names none.
    pub fn handle_sample_data(&self, request: &SampleDataRequest) -> SampleDataResponse {
        let wanted: HashSet<&str> = request.pcollection_ids.iter().map(String::as_str).collect();
        let samplers = self.samplers.lock().unwrap_or_else(PoisonError::into_inner);
        let element_samples = samplers
            .iter()
            .filter(|(id, _)| wanted.is_empty() || wanted.contains(id.as_str()))
            .map(|(id, sampler)| {
                (
                    id.clone(),
                    ElementList {
                        elements: sampler.drain_samples(),
                    },
                )
            })
            .collect();
        SampleDataResponse { element_samples }
    }
}

fn current_timestamp() -> Option<prost_types::Timestamp> {
    let since_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?;
    Some(prost_types::Timestamp {
        seconds: i64::try_from(since_epoch.as_secs()).ok()?,
        nanos: i32::try_from(since_epoch.subsec_nanos()).ok()?,
    })
}

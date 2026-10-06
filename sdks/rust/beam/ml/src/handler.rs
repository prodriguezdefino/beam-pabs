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

//! Model handler abstractions for model lifecycle, loading, batching, and inference.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::transforms::BatchConverter;

/// Micro-batching bounds for a [`ModelHandler`]: minimum size, maximum size, flush time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BatchBounds {
    /// Minimum elements required before dispatching a batch.
    pub min_batch_size: usize,
    /// Maximum elements allowed in a single batch.
    pub max_batch_size: usize,
    /// Maximum duration that an element can stay buffered in memory before a flush.
    pub max_batch_duration: Option<Duration>,
}

impl Default for BatchBounds {
    fn default() -> Self {
        Self {
            min_batch_size: 1,
            max_batch_size: 64,
            max_batch_duration: Some(Duration::from_millis(50)),
        }
    }
}

impl BatchBounds {
    /// Panics if `min_batch_size` is 0 or `max_batch_size < min_batch_size`.
    pub fn new(min_batch_size: usize, max_batch_size: usize) -> Self {
        assert!(min_batch_size >= 1, "min_batch_size must be >= 1");
        assert!(
            max_batch_size >= min_batch_size,
            "max_batch_size must be >= min_batch_size"
        );
        Self {
            min_batch_size,
            max_batch_size,
            max_batch_duration: None,
        }
    }

    /// Sets the maximum batch duration.
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.max_batch_duration = Some(duration);
        self
    }

    /// Sets the maximum batch duration in seconds.
    pub fn with_duration_secs(mut self, secs: f64) -> Self {
        self.max_batch_duration = Some(Duration::from_secs_f64(secs));
        self
    }
}

/// Optional inference parameters, for example generation parameters or thresholds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InferenceArgs {
    parameters: HashMap<String, String>,
}

impl InferenceArgs {
    pub fn new() -> Self {
        Self {
            parameters: HashMap::new(),
        }
    }

    /// Inserts a parameter key-value pair.
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(key.into(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.parameters.get(key).map(|s| s.as_str())
    }

    /// Returns all parameters.
    pub fn all(&self) -> &HashMap<String, String> {
        &self.parameters
    }
}

/// Extension point defining how to load models, batch elements, and run inference.
pub trait ModelHandler<In: DefaultCoder, Out: DefaultCoder>: Clone + Send + Sync + 'static {
    /// Loaded model object held in worker memory during bundle processing.
    type Model: Send + Sync + 'static;
    /// Batch representation consumed by inference (`Vec<In>`, Arrow `RecordBatch`, etc.).
    type Batch: DefaultCoder;
    /// Converter mapping individual elements into the batch representation.
    type Converter: BatchConverter<In, Self::Batch>;

    /// Loads the model artifact. Called once per bundle processor, in `setup`.
    fn load_model(&self) -> beam::Result<Self::Model>;

    /// Runs inference on a batch. Must return one output per input, in input order.
    fn run_inference(
        &self,
        batch: &Self::Batch,
        model: &Self::Model,
        inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<Out>>;

    /// Returns the batch converter that gathers individual elements into `Self::Batch`.
    fn get_batch_converter(&self) -> Self::Converter;

    /// Returns the sizing and latency boundaries governing micro-batch formation.
    fn get_batch_bounds(&self) -> BatchBounds {
        BatchBounds::default()
    }

    /// Optional identifier for metrics tracking and model version logging.
    fn model_id(&self) -> Option<String> {
        None
    }

    /// Updates the model artifact path, for example from a side input during model
    /// refresh. The default ignores the path.
    fn update_model_path(&mut self, _model_path: &str) -> beam::Result {
        Ok(())
    }
}

/// Wraps a [`ModelHandler`] to run inference on `(K, In)` pairs and keep `K` with each
/// result.
#[derive(Clone)]
pub struct KeyedModelHandler<K, In, Out, H> {
    inner: H,
    _marker: std::marker::PhantomData<(K, In, Out)>,
}

impl<K, In, Out, H> KeyedModelHandler<K, In, Out, H>
where
    K: DefaultCoder,
    In: DefaultCoder,
    Out: DefaultCoder,
    H: ModelHandler<In, Out>,
{
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the underlying inner model handler.
    pub fn inner(&self) -> &H {
        &self.inner
    }
}

impl<K, In, Out, H> fmt::Debug for KeyedModelHandler<K, In, Out, H>
where
    H: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedModelHandler")
            .field("inner", &self.inner)
            .finish()
    }
}

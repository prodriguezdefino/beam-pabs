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

//! Candle pure-Rust inference support with hardware acceleration.
//!
//! Provides [`CandleModelHandler`] for any model behind a [`CandleAdapter`], plus device
//! selection and [`CandleConfig`]. Artifacts load through the Beam filesystem registry
//! ([`crate::artifact`]). The ready-to-use BERT embedding handler is in [`bert`].

use std::sync::Arc;
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::options::PipelineOptionGroup;
use beam::transforms::VecBatchConverter;
use candle_core::{DType, Device};
use candle_nn::VarBuilder;
use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::artifact::read_artifact;
use crate::handler::{BatchBounds, InferenceArgs, ModelHandler};

pub mod bert;

pub use bert::{
    BertEmbeddingAdapter, BertEmbeddingModelHandler, LoadedBertEmbeddingModel, TextDocument,
    VectorEmbedding, configure_tokenizer, masked_mean_pool_l2,
};

/// Hardware device target for Candle execution.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum CandleDevice {
    /// Pure CPU execution using SIMD vector instructions.
    #[default]
    Cpu,
    /// Apple Silicon GPU acceleration through Metal Performance Shaders.
    Metal {
        /// Device index (defaults to 0).
        device_id: usize,
    },
    /// NVIDIA GPU acceleration through CUDA runtime.
    Cuda {
        /// GPU ordinal index (defaults to 0).
        device_id: usize,
    },
}

impl CandleDevice {
    /// Initializes the physical [`Device`].
    ///
    /// If the accelerator cannot be initialized (including when candle was built without the
    /// matching `candle-cuda` / `candle-metal` feature), this returns CPU with a warning when
    /// `allow_cpu_fallback` is set, and an error otherwise.
    pub fn to_device(&self, allow_cpu_fallback: bool) -> beam::Result<Device> {
        let accelerator = match *self {
            Self::Cpu => return Ok(Device::Cpu),
            Self::Metal { device_id } => Device::new_metal(device_id),
            Self::Cuda { device_id } => Device::new_cuda(device_id),
        };
        accelerator.or_else(|err| {
            if allow_cpu_fallback {
                tracing::warn!(
                    "{self:?} unavailable: {err}. allow_cpu_fallback is set, using CPU."
                );
                Ok(Device::Cpu)
            } else {
                Err(format!("{self:?} initialization failed: {err}").into())
            }
        })
    }
}

/// Candle device as selected on the command line (`--device`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum CandleDeviceKind {
    /// CPU with SIMD kernels.
    #[default]
    Cpu,
    /// NVIDIA GPU (requires the `candle-cuda` feature).
    Cuda,
    /// Apple Silicon GPU (requires the `candle-metal` feature).
    Metal,
}

/// Standard Candle device options, reusable by any pipeline.
///
/// Flatten it into a pipeline's arguments to get `--device`, `--device_id` and
/// `--allow_cpu_fallback`, then apply it with [`CandleConfig::with_device_options`]:
///
/// ```ignore
/// #[derive(clap::Args, serde::Serialize, serde::Deserialize, Clone, Debug)]
/// pub struct MyArgs {
///     #[command(flatten)]
///     #[serde(flatten)]
///     pub accelerator: CandleDeviceOptions,
/// }
///
/// let config = CandleConfig::new(weights, config, tokenizer)
///     .with_device_options(&args.accelerator);
/// ```
///
/// It is also a standalone [`PipelineOptionGroup`], readable with
/// `options.view_as::<CandleDeviceOptions>()`.
#[derive(Args, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct CandleDeviceOptions {
    /// Device used for inference.
    #[arg(long, value_enum, default_value_t = CandleDeviceKind::Cpu)]
    pub device: CandleDeviceKind,

    /// GPU device ordinal index.
    #[arg(long, default_value_t = 0)]
    pub device_id: usize,

    /// Whether to run on CPU if the requested accelerator fails to initialize.
    #[arg(long, num_args = 0..=1, default_missing_value = "true", default_value_t = false)]
    pub allow_cpu_fallback: bool,
}

impl CandleDeviceOptions {
    /// The Candle device selected by `--device` and `--device_id`.
    pub fn candle_device(&self) -> CandleDevice {
        let device_id = self.device_id;
        match self.device {
            CandleDeviceKind::Cpu => CandleDevice::Cpu,
            CandleDeviceKind::Cuda => CandleDevice::Cuda { device_id },
            CandleDeviceKind::Metal => CandleDevice::Metal { device_id },
        }
    }
}

impl PipelineOptionGroup for CandleDeviceOptions {}

/// Configuration specifying artifact locations and runtime settings for Candle models.
#[derive(Clone, Debug)]
pub struct CandleConfig {
    /// Path or URI of the safetensors model weights (`model.safetensors`).
    pub weights_path: String,
    /// Path or URI of the model architecture JSON (`config.json`).
    pub config_path: String,
    /// Path or URI of the Hugging Face tokenizer JSON (`tokenizer.json`).
    pub tokenizer_path: String,
    /// Target hardware execution device.
    pub device: CandleDevice,
    /// Whether to fall back to CPU if requested accelerator fails to initialize.
    pub allow_cpu_fallback: bool,
    /// Tensor data type for model weights and forward computation (defaults to `DType::F32`).
    pub dtype: DType,
    /// Maximum sequence length (tokens, including special tokens) used for truncation.
    pub max_seq_len: usize,
    /// Number of texts per forward pass within a Beam batch (`SentenceTransformer.encode`'s
    /// `batch_size`). Texts are sorted by length before chunking, like sentence-transformers.
    pub inference_batch_size: usize,
    /// Micro-batch bounds.
    pub batch_bounds: BatchBounds,
}

impl CandleConfig {
    /// Creates a new `CandleConfig` pointing to model artifacts (local paths or URIs).
    pub fn new(
        weights_path: impl Into<String>,
        config_path: impl Into<String>,
        tokenizer_path: impl Into<String>,
    ) -> Self {
        Self {
            weights_path: weights_path.into(),
            config_path: config_path.into(),
            tokenizer_path: tokenizer_path.into(),
            device: CandleDevice::Cpu,
            allow_cpu_fallback: false,
            dtype: DType::F32,
            max_seq_len: 256,
            inference_batch_size: 32,
            batch_bounds: BatchBounds {
                min_batch_size: 1,
                max_batch_size: 32,
                max_batch_duration: Some(Duration::from_millis(50)),
            },
        }
    }

    /// Creates a new `CandleConfig` pointing only to the weights artifact.
    pub fn from_weights(weights_path: impl Into<String>) -> Self {
        Self::new(weights_path, "", "")
    }

    /// Sets the tensor data type for weights and forward computation.
    pub fn with_dtype(mut self, dtype: DType) -> Self {
        self.dtype = dtype;
        self
    }

    /// Sets the architecture configuration file path or URI.
    pub fn with_config_path(mut self, path: impl Into<String>) -> Self {
        self.config_path = path.into();
        self
    }

    /// Sets the tokenizer configuration file path or URI.
    pub fn with_tokenizer_path(mut self, path: impl Into<String>) -> Self {
        self.tokenizer_path = path.into();
        self
    }

    /// Sets the target hardware device.
    pub fn with_device(mut self, device: CandleDevice) -> Self {
        self.device = device;
        self
    }

    /// Enables CPU fallback if the accelerator is unavailable.
    pub fn with_cpu_fallback(mut self, allow: bool) -> Self {
        self.allow_cpu_fallback = allow;
        self
    }

    /// Applies command-line [`CandleDeviceOptions`]: device and CPU fallback.
    pub fn with_device_options(self, options: &CandleDeviceOptions) -> Self {
        self.with_device(options.candle_device())
            .with_cpu_fallback(options.allow_cpu_fallback)
    }

    /// Sets the maximum token sequence length.
    pub fn with_max_seq_len(mut self, max_seq_len: usize) -> Self {
        self.max_seq_len = max_seq_len;
        self
    }

    /// Sets the number of texts per forward pass.
    pub fn with_inference_batch_size(mut self, inference_batch_size: usize) -> Self {
        self.inference_batch_size = inference_batch_size;
        self
    }

    /// Sets the micro-batch bounds.
    pub fn with_batch_bounds(mut self, bounds: BatchBounds) -> Self {
        self.batch_bounds = bounds;
        self
    }
}

/// Trait defining model loading and inference execution for a Candle model.
pub trait CandleAdapter<In, Out>: Send + Sync + 'static {
    /// The loaded model structure (e.g., neural network struct, tokenizer, or wrapper).
    type Model: Send + Sync + 'static;

    /// Loads the model, tokenizer, and/or architecture config onto `device` using `vb`.
    fn load_model(
        &self,
        device: &Device,
        vb: VarBuilder<'_>,
        config: &CandleConfig,
    ) -> beam::Result<Self::Model>;

    /// Executes inference on a batch of elements using the loaded model.
    fn run_inference(
        &self,
        model: &Self::Model,
        batch: &[In],
        inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<Out>>;
}

/// A [`ModelHandler`] implementation for universal Candle models.
pub struct CandleModelHandler<In, Out, A> {
    config: CandleConfig,
    adapter: Arc<A>,
    model_id: Option<String>,
    _phantom: std::marker::PhantomData<fn(In) -> Out>,
}

impl<In, Out, A> Clone for CandleModelHandler<In, Out, A> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            adapter: Arc::clone(&self.adapter),
            model_id: self.model_id.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<In, Out, A> std::fmt::Debug for CandleModelHandler<In, Out, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CandleModelHandler")
            .field("config", &self.config)
            .field("model_id", &self.model_id)
            .finish()
    }
}

impl<In, Out, A> CandleModelHandler<In, Out, A>
where
    In: DefaultCoder + Clone,
    Out: DefaultCoder + Clone,
    A: CandleAdapter<In, Out>,
{
    /// Creates a new `CandleModelHandler` with the specified configuration and adapter.
    pub fn new(config: CandleConfig, adapter: A) -> Self {
        Self {
            config,
            adapter: Arc::new(adapter),
            model_id: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Sets the model identifier.
    pub fn with_model_id(mut self, id: impl Into<String>) -> Self {
        self.model_id = Some(id.into());
        self
    }

    /// Returns a reference to the Candle configuration.
    pub fn config(&self) -> &CandleConfig {
        &self.config
    }

    /// Returns a reference to the adapter.
    pub fn adapter(&self) -> &A {
        &self.adapter
    }
}

impl<In, Out, A> ModelHandler<In, Out> for CandleModelHandler<In, Out, A>
where
    In: DefaultCoder + Clone,
    Out: DefaultCoder + Clone,
    A: CandleAdapter<In, Out>,
{
    type Model = Arc<A::Model>;
    type Batch = Vec<In>;
    type Converter = VecBatchConverter<In>;

    fn load_model(&self) -> beam::Result<Self::Model> {
        let config = &self.config;
        let device = config.device.to_device(config.allow_cpu_fallback)?;
        let weights = read_artifact(&config.weights_path)?;
        tracing::info!(
            "Loading Candle weights {} ({} bytes) on {device:?}",
            config.weights_path,
            weights.len()
        );
        let vb = VarBuilder::from_buffered_safetensors(weights, config.dtype, &device)?;
        let model = self.adapter.load_model(&device, vb, config)?;
        Ok(Arc::new(model))
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        model: &Self::Model,
        inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<Out>> {
        self.adapter
            .run_inference(model.as_ref(), batch, inference_args)
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }

    fn get_batch_bounds(&self) -> BatchBounds {
        self.config.batch_bounds
    }

    fn model_id(&self) -> Option<String> {
        self.model_id.clone()
    }
}

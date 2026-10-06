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

//! Model inference using ONNX Runtime.
//!
//! Provides [`OnnxModelHandler`], hardware execution provider selection (CPU, CUDA,
//! TensorRT, CoreML), dynamic library loading (`libonnxruntime.so` /
//! `libonnxruntime.dylib`), and thread-safe session concurrency.
//!
//! Models load through the Beam filesystem registry (see [`crate::artifact`]), so any
//! registered scheme (local, `gs://`, ...) works. Accelerator execution providers are
//! registered with `error_on_failure`. If the requested provider cannot be registered,
//! the handler fails. If [`OnnxConfig::allow_cpu_fallback`] is set, the same model runs
//! on the CPU provider instead and a warning is logged.

use std::fmt::{self, Debug};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::options::PipelineOptionGroup;
use beam::transforms::VecBatchConverter;
use clap::{Args, ValueEnum};
pub use ort;
use ort::ep::ExecutionProviderDispatch;
use ort::session::builder::GraphOptimizationLevel;
pub use ort::session::{Session, SessionInputs, SessionOutputs};
use serde::{Deserialize, Serialize};

use crate::artifact::read_artifact;
use crate::handler::{BatchBounds, InferenceArgs, ModelHandler};

/// Hardware execution provider for ONNX Runtime.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum OnnxExecutionProvider {
    /// Pure CPU execution provider (default).
    #[default]
    Cpu,
    /// NVIDIA CUDA execution provider.
    Cuda {
        /// GPU device ordinal index.
        device_id: usize,
    },
    /// NVIDIA TensorRT execution provider.
    TensorRt {
        /// GPU device ordinal index.
        device_id: usize,
    },
    /// Apple Silicon CoreML execution provider.
    CoreMl,
}

impl fmt::Display for OnnxExecutionProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cpu => f.write_str("CPU"),
            Self::Cuda { device_id } => write!(f, "CUDA(device {device_id})"),
            Self::TensorRt { device_id } => write!(f, "TensorRT(device {device_id})"),
            Self::CoreMl => f.write_str("CoreML"),
        }
    }
}

impl OnnxExecutionProvider {
    /// The `ort` registration for this provider, or `None` for the built-in CPU provider.
    ///
    /// Registrations are marked `error_on_failure` so an unavailable accelerator surfaces as
    /// an error instead of ONNX Runtime silently falling back to CPU.
    fn dispatch(&self) -> Result<Option<ExecutionProviderDispatch>, OnnxError> {
        match *self {
            Self::Cpu => Ok(None),
            Self::Cuda { device_id } => cuda_dispatch(gpu_ordinal(device_id)?).map(Some),
            Self::TensorRt { device_id } => tensorrt_dispatch(gpu_ordinal(device_id)?).map(Some),
            Self::CoreMl => coreml_dispatch().map(Some),
        }
        .map(|dispatch| dispatch.map(ExecutionProviderDispatch::error_on_failure))
    }

    /// Test hook: whether `dispatch` registers an accelerator, without touching ONNX Runtime.
    #[doc(hidden)]
    pub fn registers_accelerator(&self) -> Result<bool, OnnxError> {
        self.dispatch().map(|dispatch| dispatch.is_some())
    }
}

fn gpu_ordinal(device_id: usize) -> Result<i32, OnnxError> {
    i32::try_from(device_id).map_err(|_| OnnxError::InvalidDeviceId(device_id))
}

/// ONNX Runtime execution provider as selected on the command line (`--device`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum OnnxDeviceKind {
    /// ONNX Runtime CPU execution provider.
    #[default]
    Cpu,
    /// NVIDIA CUDA execution provider (requires the `onnx-cuda` feature).
    Cuda,
    /// NVIDIA TensorRT execution provider (requires the `onnx-tensorrt` feature).
    Tensorrt,
    /// Apple CoreML execution provider (requires the `onnx-coreml` feature).
    Coreml,
}

/// Standard ONNX Runtime device options, reusable by any pipeline.
///
/// Flatten it into a pipeline's arguments to get `--device`, `--device_id`,
/// `--allow_cpu_fallback` and `--dylib_path`, then apply it with
/// [`OnnxConfig::with_device_options`]:
///
/// ```ignore
/// #[derive(clap::Args, serde::Serialize, serde::Deserialize, Clone, Debug)]
/// pub struct MyArgs {
///     #[arg(long)]
///     pub model_path: String,
///     #[command(flatten)]
///     #[serde(flatten)]
///     pub accelerator: OnnxDeviceOptions,
/// }
///
/// let config = OnnxConfig::new(&args.model_path).with_device_options(&args.accelerator);
/// ```
///
/// It is also a standalone [`PipelineOptionGroup`], readable with
/// `options.view_as::<OnnxDeviceOptions>()`.
#[derive(Args, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct OnnxDeviceOptions {
    /// ONNX Runtime execution provider.
    #[arg(long, value_enum, default_value_t = OnnxDeviceKind::Cpu)]
    pub device: OnnxDeviceKind,

    /// GPU device ordinal index (CUDA and TensorRT).
    #[arg(long, default_value_t = 0)]
    pub device_id: usize,

    /// Whether to run on CPU if the requested accelerator fails to initialize.
    #[arg(long, num_args = 0..=1, default_missing_value = "true", default_value_t = false)]
    pub allow_cpu_fallback: bool,

    /// Optional explicit path to `libonnxruntime.so` / `libonnxruntime.dylib`.
    #[arg(long)]
    pub dylib_path: Option<PathBuf>,
}

impl OnnxDeviceOptions {
    /// The execution provider selected by `--device` and `--device_id`.
    pub fn execution_provider(&self) -> OnnxExecutionProvider {
        let device_id = self.device_id;
        match self.device {
            OnnxDeviceKind::Cpu => OnnxExecutionProvider::Cpu,
            OnnxDeviceKind::Cuda => OnnxExecutionProvider::Cuda { device_id },
            OnnxDeviceKind::Tensorrt => OnnxExecutionProvider::TensorRt { device_id },
            OnnxDeviceKind::Coreml => OnnxExecutionProvider::CoreMl,
        }
    }
}

impl PipelineOptionGroup for OnnxDeviceOptions {}

#[cfg(feature = "onnx-cuda")]
fn cuda_dispatch(device_id: i32) -> Result<ExecutionProviderDispatch, OnnxError> {
    Ok(ort::ep::CUDA::default().with_device_id(device_id).build())
}

#[cfg(not(feature = "onnx-cuda"))]
fn cuda_dispatch(_device_id: i32) -> Result<ExecutionProviderDispatch, OnnxError> {
    Err(OnnxError::ProviderNotCompiled {
        provider: "CUDA",
        feature: "onnx-cuda",
    })
}

#[cfg(feature = "onnx-tensorrt")]
fn tensorrt_dispatch(device_id: i32) -> Result<ExecutionProviderDispatch, OnnxError> {
    Ok(ort::ep::TensorRT::default()
        .with_device_id(device_id)
        .build())
}

#[cfg(not(feature = "onnx-tensorrt"))]
fn tensorrt_dispatch(_device_id: i32) -> Result<ExecutionProviderDispatch, OnnxError> {
    Err(OnnxError::ProviderNotCompiled {
        provider: "TensorRT",
        feature: "onnx-tensorrt",
    })
}

#[cfg(feature = "onnx-coreml")]
fn coreml_dispatch() -> Result<ExecutionProviderDispatch, OnnxError> {
    Ok(ort::ep::CoreML::default().build())
}

#[cfg(not(feature = "onnx-coreml"))]
fn coreml_dispatch() -> Result<ExecutionProviderDispatch, OnnxError> {
    Err(OnnxError::ProviderNotCompiled {
        provider: "CoreML",
        feature: "onnx-coreml",
    })
}

/// Errors raised while loading an ONNX model.
#[derive(Debug, thiserror::Error)]
pub enum OnnxError {
    /// `libonnxruntime` could not be loaded from the configured path.
    #[error("failed to load ONNX Runtime from {path:?}: {message}")]
    Dylib { path: PathBuf, message: String },
    /// The model file could not be read.
    #[error("failed to read ONNX model: {0}")]
    Artifact(#[from] io::Error),
    /// The requested execution provider was not compiled into this binary.
    #[error("{provider} execution provider requested but the '{feature}' feature is not compiled")]
    ProviderNotCompiled {
        provider: &'static str,
        feature: &'static str,
    },
    /// The GPU ordinal does not fit ONNX Runtime's device id type.
    #[error("GPU device id {0} is out of range")]
    InvalidDeviceId(usize),
    /// ONNX Runtime rejected the session configuration, provider registration or model.
    #[error("failed to create ONNX Runtime session on {provider}: {message}")]
    Session {
        provider: OnnxExecutionProvider,
        message: String,
    },
}

/// Configuration settings for ONNX model execution.
#[derive(Clone, Debug)]
pub struct OnnxConfig {
    /// Path or URI of the ONNX model file (`.onnx`), e.g. `/models/m.onnx` or `gs://bucket/m.onnx`.
    pub model_path: String,
    /// Optional explicit path to `libonnxruntime.so` / `libonnxruntime.dylib` for dynamic loading.
    pub dylib_path: Option<PathBuf>,
    /// Hardware execution provider.
    pub execution_provider: OnnxExecutionProvider,
    /// Intra-op thread count (parallelism within individual operators).
    pub intra_threads: usize,
    /// Inter-op thread count (parallelism across operators).
    pub inter_threads: usize,
    /// Optimization level.
    pub optimization_level: GraphOptimizationLevel,
    /// Whether to run the model on the CPU provider if the requested accelerator fails to initialize.
    pub allow_cpu_fallback: bool,
    /// Micro-batch sizing boundaries.
    pub batch_bounds: BatchBounds,
}

impl OnnxConfig {
    /// Creates a new `OnnxConfig` for an ONNX model file.
    pub fn new(model_path: impl Into<String>) -> Self {
        Self {
            model_path: model_path.into(),
            dylib_path: None,
            execution_provider: OnnxExecutionProvider::Cpu,
            intra_threads: 0, // Auto-detect
            inter_threads: 0,
            optimization_level: GraphOptimizationLevel::Level2,
            allow_cpu_fallback: false,
            batch_bounds: BatchBounds {
                min_batch_size: 1,
                max_batch_size: 32,
                max_batch_duration: Some(Duration::from_millis(50)),
            },
        }
    }

    /// Sets an explicit dynamic library path for `libonnxruntime`.
    pub fn with_dylib_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.dylib_path = Some(path.into());
        self
    }

    /// Sets the hardware execution provider.
    pub fn with_execution_provider(mut self, ep: OnnxExecutionProvider) -> Self {
        self.execution_provider = ep;
        self
    }

    /// Enables or disables CPU fallback when the hardware accelerator fails.
    pub fn with_cpu_fallback(mut self, allow: bool) -> Self {
        self.allow_cpu_fallback = allow;
        self
    }

    /// Applies command-line [`OnnxDeviceOptions`]: execution provider, CPU fallback and,
    /// when given, the `libonnxruntime` path.
    pub fn with_device_options(self, options: &OnnxDeviceOptions) -> Self {
        let config = self
            .with_execution_provider(options.execution_provider())
            .with_cpu_fallback(options.allow_cpu_fallback);
        match &options.dylib_path {
            Some(path) => config.with_dylib_path(path),
            None => config,
        }
    }

    /// Sets the intra-op parallelism thread count.
    pub fn with_intra_threads(mut self, threads: usize) -> Self {
        self.intra_threads = threads;
        self
    }

    /// Sets the micro-batch bounds.
    pub fn with_batch_bounds(mut self, bounds: BatchBounds) -> Self {
        self.batch_bounds = bounds;
        self
    }

    /// Builds a session for `model` on `provider` with this config's session options.
    fn build_session(
        &self,
        provider: &OnnxExecutionProvider,
        model: &[u8],
    ) -> Result<Session, OnnxError> {
        let builder = Session::builder().map_err(session_error(provider))?;
        let builder = match provider.dispatch()? {
            Some(ep) => builder
                .with_execution_providers([ep])
                .map_err(session_error(provider))?,
            None => builder,
        };
        let builder = match self.intra_threads {
            0 => builder,
            n => builder
                .with_intra_threads(n)
                .map_err(session_error(provider))?,
        };
        let builder = match self.inter_threads {
            0 => builder,
            n => builder
                .with_inter_threads(n)
                .map_err(session_error(provider))?,
        };
        let mut builder = builder
            .with_optimization_level(self.optimization_level)
            .map_err(session_error(provider))?;
        builder
            .commit_from_memory(model)
            .map_err(session_error(provider))
    }
}

fn session_error<R>(provider: &OnnxExecutionProvider) -> impl Fn(ort::Error<R>) -> OnnxError + '_ {
    move |e| OnnxError::Session {
        provider: provider.clone(),
        message: e.to_string(),
    }
}

/// Trait defining tensor preparation and output parsing for an ONNX model.
pub trait OnnxAdapter<In, Out>: Send + Sync + 'static {
    /// Formats an input batch into ONNX session inputs.
    fn prepare_inputs<'a>(&self, batch: &'a [In]) -> beam::Result<SessionInputs<'a, 'a>>;

    /// Parses the raw ONNX session outputs into output predictions.
    fn parse_outputs(&self, batch: &[In], outputs: SessionOutputs<'_>) -> beam::Result<Vec<Out>>;
}

/// A [`ModelHandler`] implementation for universal ONNX models.
pub struct OnnxModelHandler<In, Out, A> {
    config: OnnxConfig,
    adapter: Arc<A>,
    model_id: Option<String>,
    _phantom: std::marker::PhantomData<fn(In) -> Out>,
}

impl<In, Out, A> Clone for OnnxModelHandler<In, Out, A> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            adapter: Arc::clone(&self.adapter),
            model_id: self.model_id.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<In, Out, A> OnnxModelHandler<In, Out, A>
where
    In: DefaultCoder + Clone,
    Out: DefaultCoder + Clone,
    A: OnnxAdapter<In, Out>,
{
    /// Creates a new `OnnxModelHandler` with the specified configuration and adapter.
    pub fn new(config: OnnxConfig, adapter: A) -> Self {
        Self {
            config,
            adapter: Arc::new(adapter),
            model_id: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Sets an explicit model identifier.
    pub fn with_model_id(mut self, id: impl Into<String>) -> Self {
        self.model_id = Some(id.into());
        self
    }
}

impl<In, Out, A> ModelHandler<In, Out> for OnnxModelHandler<In, Out, A>
where
    In: DefaultCoder + Clone,
    Out: DefaultCoder + Clone,
    A: OnnxAdapter<In, Out>,
{
    type Model = Arc<Mutex<Session>>;
    type Batch = Vec<In>;
    type Converter = VecBatchConverter<In>;

    fn load_model(&self) -> beam::Result<Self::Model> {
        let config = &self.config;
        if let Some(dylib) = &config.dylib_path {
            ort::init_from(dylib)
                .map_err(|e| OnnxError::Dylib {
                    path: dylib.clone(),
                    message: e.to_string(),
                })?
                .commit();
        }

        let model = read_artifact(&config.model_path).map_err(OnnxError::Artifact)?;
        let requested = &config.execution_provider;
        let (session, provider) = match config.build_session(requested, &model) {
            Ok(session) => (session, requested.clone()),
            Err(err) if config.allow_cpu_fallback && *requested != OnnxExecutionProvider::Cpu => {
                tracing::warn!("{err}; allow_cpu_fallback is set, running the model on CPU");
                let cpu = OnnxExecutionProvider::Cpu;
                (config.build_session(&cpu, &model)?, cpu)
            }
            Err(err) => return Err(err.into()),
        };
        tracing::info!(
            "Loaded ONNX model {} ({} bytes) on {provider}",
            config.model_path,
            model.len()
        );
        Ok(Arc::new(Mutex::new(session)))
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        model: &Self::Model,
        _inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<Out>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let mut session = model
            .lock()
            .map_err(|e| format!("ONNX session lock poisoned: {e}"))?;
        let inputs = self.adapter.prepare_inputs(batch)?;
        let outputs = session.run(inputs)?;
        self.adapter.parse_outputs(batch, outputs)
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

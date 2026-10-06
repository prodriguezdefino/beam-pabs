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

//! Machine learning inference, hardware acceleration, and ModelHandler framework for Apache Beam Rust.
//!
//! Provides [`RunInference`], [`ModelHandler`], [`WorkerModelCache`], and support for micro-batching,
//! Dead-Letter Queues (DLQ), and model lifecycle management.

pub mod cache;
pub mod handler;
pub mod prediction;
pub mod run_inference;

#[cfg(feature = "remote")]
pub mod remote;

#[cfg(any(feature = "candle", feature = "onnx"))]
pub mod artifact;

#[cfg(feature = "candle")]
pub mod candle;

#[cfg(feature = "onnx")]
pub mod onnx;

pub use cache::WorkerModelCache;
pub use handler::{BatchBounds, InferenceArgs, KeyedModelHandler, ModelHandler};
pub use prediction::{PredictionResult, PredictionResultCoder};
pub use run_inference::{RunInference, RunInferenceMulti};

#[cfg(feature = "dynamic-refresh")]
pub use cache::SwappableModel;

#[cfg(feature = "remote")]
pub use remote::{
    GeminiAdapter, LLMResponse, PromptRequest, RemoteAuth, RemoteConfig, RemoteEndpointAdapter,
    RemoteInferenceError, RemoteModelHandler, ResolvedAuth,
};

#[cfg(feature = "candle")]
pub use candle::{
    BertEmbeddingAdapter, BertEmbeddingModelHandler, CandleAdapter, CandleConfig, CandleDevice,
    CandleDeviceKind, CandleDeviceOptions, CandleModelHandler, LoadedBertEmbeddingModel,
    TextDocument, VectorEmbedding,
};

#[cfg(feature = "onnx")]
pub use onnx::{OnnxAdapter, OnnxConfig, OnnxError, OnnxExecutionProvider, OnnxModelHandler};

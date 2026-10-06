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

//! Remote LLM Inference pipeline logic demonstrating high-throughput asynchronous
//! micro-batching with Google Cloud Vertex AI / Gemini and Dead-Letter Queue (DLQ) routing.
//!
//! Sends one prompt per input line and writes output in `Input: ..., Output: ...` format.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

use beam::ml::remote::{
    GeminiAdapter, GeminiEndpoint, GenerationConfig, LLMResponse, PromptRequest, RemoteAuth,
    RemoteConfig, RemoteModelHandler, ThinkingConfig,
};
use beam::ml::{BatchBounds, PredictionResult, RunInference};
use beam::options::Secret;
use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

pub use beam::ml::remote;

pub const DEFAULT_OUTPUT: &str = "/tmp/gemini_predictions.txt";
pub const DEFAULT_DLQ_OUTPUT: &str = "/tmp/gemini_dlq.txt";

/// Command line arguments for the Remote LLM Inference example pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "remote_llm_inference",
    about = "Apache Beam Rust Remote LLM Inference with Gemini / Vertex AI Example"
)]
pub struct RemoteLlmArgs {
    /// Text file with one prompt per line (local path or `gs://...`; required). The benchmark
    /// set of 1,000 prompts is built by `scripts/make_prompts.py` from the public Shakespeare
    /// corpus.
    #[arg(long)]
    pub input: String,

    /// Path to write successful LLM predictions (local path or `gs://...`).
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    pub output: String,

    /// Path to write failed requests in Dead-Letter Queue (local path or `gs://...`).
    #[arg(long, default_value = DEFAULT_DLQ_OUTPUT)]
    pub dlq_output: String,

    /// Google Cloud project billed for Vertex AI requests. Without it, and without
    /// `--endpoint_url`, requests go to the Gemini Developer API.
    #[arg(long)]
    pub cloud_project: Option<String>,

    /// Vertex AI location serving the model, used with `--cloud_project`.
    #[arg(long, default_value = "us-central1")]
    pub cloud_region: String,

    /// A full `generateContent` URL, overriding `--cloud_project` and `--cloud_region`.
    #[arg(long, conflicts_with = "cloud_project")]
    pub endpoint_url: Option<String>,

    /// Where the API key is held: `env:<VAR>`, `file:<PATH>` or
    /// `gcp:projects/<p>/secrets/<s>/versions/<v>`.
    #[arg(long, conflicts_with = "bearer_token")]
    pub api_key: Option<Secret>,

    /// Where an OAuth 2.0 access token is held, in the same forms as `--api_key`.
    /// Without either, workers authenticate as their Google Cloud service account.
    #[arg(long)]
    pub bearer_token: Option<Secret>,

    /// Gemini model identifier.
    #[arg(long, default_value = "gemini-2.5-flash")]
    pub model_name: String,

    /// Sampling temperature; the model default when unset.
    #[arg(long)]
    pub temperature: Option<f32>,

    /// Maximum generated tokens, thinking included; the model default when unset.
    #[arg(long)]
    pub max_output_tokens: Option<u32>,

    /// Maximum thinking tokens (`0` turns thinking off); the model default when unset.
    #[arg(long)]
    pub thinking_budget: Option<u32>,

    /// Maximum prompts per batch. The requests of a batch are sent concurrently; `1`
    /// sends one request per bundle thread at a time.
    #[arg(long, default_value = "1")]
    pub batch_size: usize,

    /// Maximum retries per request on HTTP 429 (rate limits) or transient 5xx errors.
    #[arg(long, default_value = "5")]
    pub max_retries: usize,

    /// Backoff in seconds before the first retry of a request; it doubles per retry.
    #[arg(long, default_value = "5")]
    pub retry_backoff_secs: u64,

    /// Maximum requests in flight per worker process.
    #[arg(long, default_value = "64")]
    pub max_concurrent_requests: usize,
}

impl PipelineOptionGroup for RemoteLlmArgs {}

impl RemoteLlmArgs {
    /// How requests authenticate, from `--api_key` or `--bearer_token`.
    pub fn auth(&self) -> RemoteAuth {
        self.api_key
            .clone()
            .map(RemoteAuth::ApiKey)
            .or_else(|| self.bearer_token.clone().map(RemoteAuth::BearerToken))
            .unwrap_or_default()
    }

    /// Where requests go: `--endpoint_url`, else Vertex AI in `--cloud_project` and
    /// `--cloud_region`, else the Gemini Developer API.
    pub fn endpoint(&self) -> GeminiEndpoint {
        match (&self.endpoint_url, &self.cloud_project) {
            (Some(url), _) => GeminiEndpoint::Url(url.clone()),
            (None, Some(project)) => GeminiEndpoint::VertexAi {
                project: project.clone(),
                location: self.cloud_region.clone(),
            },
            (None, None) => GeminiEndpoint::DeveloperApi,
        }
    }

    /// The `generationConfig` sent with every request.
    pub fn generation_config(&self) -> GenerationConfig {
        GenerationConfig {
            temperature: self.temperature,
            max_output_tokens: self.max_output_tokens,
            thinking_config: self
                .thinking_budget
                .map(|thinking_budget| ThinkingConfig { thinking_budget }),
        }
    }

    /// The Gemini adapter for these arguments.
    pub fn adapter(&self) -> GeminiAdapter {
        GeminiAdapter::new(&self.model_name, &self.endpoint())
            .with_generation_config(self.generation_config())
    }

    /// The transport settings for these arguments.
    pub fn remote_config(&self) -> RemoteConfig {
        RemoteConfig {
            auth: self.auth(),
            max_retries: self.max_retries,
            initial_retry_backoff: Duration::from_secs(self.retry_backoff_secs),
            max_concurrent_requests: self.max_concurrent_requests,
            batch_bounds: BatchBounds {
                min_batch_size: 1,
                max_batch_size: self.batch_size,
                max_batch_duration: Some(Duration::from_millis(50)),
            },
            ..RemoteConfig::default()
        }
    }
}

/// The request for one input line, or `None` for a blank line. The line is the prompt,
/// verbatim; its id is a hash of the line.
pub fn prompt_request(line: String) -> Option<PromptRequest> {
    if line.trim().is_empty() {
        return None;
    }
    let mut hasher = DefaultHasher::new();
    line.hash(&mut hasher);
    Some(PromptRequest {
        request_id: format!("req_{:016x}", hasher.finish()),
        prompt: line,
    })
}

/// Formats a prediction result as an `Input: ..., Output: ...` line.
pub fn format_prediction(prediction: &PredictionResult<PromptRequest, LLMResponse>) -> String {
    format!(
        "Input: {}, Output: {}",
        prediction.input.prompt, prediction.output.response_text
    )
}

/// A failed request as a `request_id<TAB>error` line.
pub fn format_failure(failure: &Failure<PromptRequest>) -> String {
    format!("{}\t{}", failure.input.request_id, failure.error)
}

/// Builds the remote inference pipeline with Dead-Letter Queue support.
pub fn build_remote_inference_pipeline(
    input: &PCollection<PromptRequest>,
    config: RemoteConfig,
    adapter: GeminiAdapter,
) -> WithFailures<PredictionResult<PromptRequest, LLMResponse>, Failure<PromptRequest>> {
    let model_id = adapter.model_name.clone();
    let handler = RemoteModelHandler::new(config, adapter).with_model_id(model_id);

    input.apply(RunInference::new("RemoteGeminiInference", handler).with_exception_handling())
}

/// Builds the complete Remote LLM Inference pipeline from [`RemoteLlmArgs`], run with
/// `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &RemoteLlmArgs) -> Pipeline {
    let p = Pipeline::create(options);

    let multi_output = build_remote_inference_pipeline(
        &p.apply(textio::Read::new("ReadLines", &args.input))
            .flat_map("ToPromptRequest", prompt_request)
            .reshuffle("ReshufflePrompts"),
        args.remote_config(),
        args.adapter(),
    );

    multi_output
        .output
        .map("FormatPredictions", |pred| format_prediction(&pred))
        .apply(textio::Write::new("WriteLines", &args.output));

    multi_output
        .failures
        .map("FormatDLQ", |fail| format_failure(&fail))
        .apply(textio::Write::new("WriteLines", &args.dlq_output));

    p
}

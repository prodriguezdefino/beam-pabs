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

//! Remote LLM inference with Gemini on the Gemini Developer API or Vertex AI.
//!
//! The pipeline sends each input line as one prompt and writes `Input: ..., Output: ...`
//! lines. Requests that fail after their retries go to a dead-letter file, and the pipeline
//! continues.

use std::time::Duration;

use beam::ml::remote::{
    GeminiAdapter, GeminiEndpoint, GenerationConfig, LLMResponse, PromptRequest, RemoteAuth,
    RemoteConfig, RemoteModelHandler, ThinkingConfig,
};
use beam::ml::{BatchBounds, ModelHandler, RunInference};
use beam::options::Secret;
use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "remote_llm_inference",
    about = "Apache Beam remote LLM inference with Gemini"
)]
pub struct Args {
    /// Text file with one prompt for each line. `scripts/make_prompts.py` builds a set of
    /// 1,000 prompts from the public Shakespeare corpus.
    #[arg(long)]
    pub input: String,

    /// Path of the file to write the predictions to.
    #[arg(long, default_value = "/tmp/gemini_predictions.txt")]
    pub output: String,

    /// Path of the file to write `prompt<TAB>error` lines to, for failed requests.
    #[arg(long, default_value = "/tmp/gemini_dlq.txt")]
    pub dlq_output: String,

    /// Google Cloud project billed for Vertex AI requests. Without it, and without
    /// `--endpoint_url`, requests go to the Gemini Developer API.
    #[arg(long)]
    pub cloud_project: Option<String>,

    /// Vertex AI location of the model, used with `--cloud_project`.
    #[arg(long, default_value = "us-central1")]
    pub cloud_region: String,

    /// A full `generateContent` URL, in place of `--cloud_project` and `--cloud_region`.
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

    /// Sampling temperature. Unset uses the model default.
    #[arg(long)]
    pub temperature: Option<f32>,

    /// Maximum generated tokens, thinking included. Unset uses the model default.
    #[arg(long)]
    pub max_output_tokens: Option<u32>,

    /// Maximum thinking tokens (`0` turns thinking off). Unset uses the model default.
    #[arg(long)]
    pub thinking_budget: Option<u32>,

    /// Maximum prompts for each batch. The requests of a batch are sent concurrently.
    #[arg(long, default_value_t = 1)]
    pub batch_size: usize,

    /// Maximum retries of a request on HTTP 429 (rate limits) or transient 5xx errors.
    #[arg(long, default_value_t = 5)]
    pub max_retries: usize,

    /// Backoff in seconds before the first retry of a request. It doubles for each retry.
    #[arg(long, default_value_t = 5)]
    pub retry_backoff_secs: u64,

    /// Maximum requests in flight for each worker process.
    #[arg(long, default_value_t = 64)]
    pub max_concurrent_requests: usize,
}

impl PipelineOptionGroup for Args {}

impl Args {
    /// How requests authenticate, from `--api_key` or `--bearer_token`.
    pub fn auth(&self) -> RemoteAuth {
        match (&self.api_key, &self.bearer_token) {
            (Some(key), _) => RemoteAuth::ApiKey(key.clone()),
            (None, Some(token)) => RemoteAuth::BearerToken(token.clone()),
            (None, None) => RemoteAuth::default(),
        }
    }

    /// Where requests go: `--endpoint_url`, else Vertex AI in `--cloud_project`, else the
    /// Gemini Developer API.
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

    /// The Gemini adapter, with the `generationConfig` that every request sends.
    pub fn adapter(&self) -> GeminiAdapter {
        GeminiAdapter::new(&self.model_name, &self.endpoint()).with_generation_config(
            GenerationConfig {
                temperature: self.temperature,
                max_output_tokens: self.max_output_tokens,
                thinking_config: self
                    .thinking_budget
                    .map(|thinking_budget| ThinkingConfig { thinking_budget }),
            },
        )
    }

    /// The transport settings: authentication, retries, concurrency and batch size.
    pub fn remote_config(&self) -> RemoteConfig {
        RemoteConfig {
            auth: self.auth(),
            max_retries: self.max_retries,
            initial_retry_backoff: Duration::from_secs(self.retry_backoff_secs),
            max_concurrent_requests: self.max_concurrent_requests,
            batch_bounds: BatchBounds::new(1, self.batch_size)
                .with_duration(Duration::from_millis(50)),
            ..RemoteConfig::default()
        }
    }
}

/// The request for one input line, or `None` for a blank line. The line is the prompt,
/// verbatim. The id is the FNV-1a hash of the line, so it is the same in every run.
pub fn prompt_request(line: String) -> Option<PromptRequest> {
    if line.trim().is_empty() {
        return None;
    }
    let hash = line.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    });
    Some(PromptRequest {
        request_id: format!("req_{hash:016x}"),
        prompt: line,
    })
}

/// Sends each non-blank line to the model as one prompt.
///
/// The output has one `Input: ..., Output: ...` line for each answered prompt. The failures
/// have one record for each request that failed after its retries.
pub struct GenerateResponses<H> {
    handler: H,
}

impl<H: ModelHandler<PromptRequest, LLMResponse>> GenerateResponses<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

impl<H: ModelHandler<PromptRequest, LLMResponse>> PTransform<PCollection<String>>
    for GenerateResponses<H>
{
    type Output = WithFailures<String, Failure<PromptRequest>>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        let results = lines
            .flat_map("ToPromptRequest", prompt_request)
            .reshuffle("Reshuffle")
            .apply(
                "RunInference",
                RunInference::new(self.handler.clone()).with_exception_handling(),
            );
        WithFailures {
            output: results.output.map("FormatPrediction", |res| {
                format!(
                    "Input: {}, Output: {}",
                    res.input.prompt, res.output.response_text
                )
            }),
            failures: results.failures,
        }
    }
}

/// Builds the remote inference pipeline from [`Args`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    let handler = RemoteModelHandler::new(args.remote_config(), args.adapter())
        .with_model_id(&args.model_name);

    let WithFailures { output, failures } = p
        .apply("ReadPrompts", textio::Read::new(&args.input))
        .apply("GenerateResponses", GenerateResponses::new(handler));
    output.apply("WritePredictions", textio::Write::new(&args.output));
    failures
        .map("FormatFailures", |f| {
            format!("{}\t{}", f.input.prompt, f.error)
        })
        .apply("WriteFailures", textio::Write::new(&args.dlq_output));

    p
}

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

//! Adapter for Google Cloud Vertex AI and Gemini `generateContent` endpoints.

use serde::{Deserialize, Serialize};

use super::{
    LLMResponse, PromptRequest, RemoteCall, RemoteClient, RemoteEndpointAdapter,
    RemoteInferenceError,
};

/// Where a [`GeminiAdapter`] sends `generateContent` requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GeminiEndpoint {
    /// Vertex AI in a Google Cloud project and location, e.g. `us-central1` or `global`.
    VertexAi {
        /// Google Cloud project that is billed.
        project: String,
        /// Vertex AI location serving the model.
        location: String,
    },
    /// The Gemini Developer API (Google AI Studio).
    DeveloperApi,
    /// A full `generateContent` URL, for example a proxy or a test server.
    Url(String),
}

impl GeminiEndpoint {
    /// The `generateContent` URL of `model` at this endpoint.
    pub fn generate_content_url(&self, model: &str) -> String {
        match self {
            Self::VertexAi { project, location } => {
                let host = match location.as_str() {
                    "global" => "aiplatform.googleapis.com".to_string(),
                    regional => format!("{regional}-aiplatform.googleapis.com"),
                };
                format!(
                    "https://{host}/v1/projects/{project}/locations/{location}/publishers/google/\
                     models/{model}:generateContent"
                )
            }
            Self::DeveloperApi => format!(
                "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            ),
            Self::Url(url) => url.clone(),
        }
    }
}

/// Sampling settings sent as `generationConfig`; unset fields keep the model's defaults.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationConfig {
    /// Sampling temperature.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    /// Maximum number of generated tokens, including thinking tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    /// Thinking settings of thinking models such as `gemini-2.5-flash`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_config: Option<ThinkingConfig>,
}

impl GenerationConfig {
    /// Whether every setting is left to the model.
    pub fn is_unset(&self) -> bool {
        *self == Self::default()
    }
}

/// Thinking settings of a [`GenerationConfig`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThinkingConfig {
    /// Maximum thinking tokens; `0` turns thinking off.
    pub thinking_budget: u32,
}

/// Adapter for Google Cloud Gemini and Vertex AI `generateContent` endpoints.
///
/// Each prompt is sent verbatim as a single user turn. The response text is the
/// concatenation of all non-thought text parts of the first candidate.
#[derive(Clone, Debug)]
pub struct GeminiAdapter {
    /// Model name (e.g. `"gemini-2.5-flash"`).
    pub model_name: String,
    /// The `generateContent` URL of the model.
    pub endpoint_url: String,
    /// Sampling settings sent with every request.
    pub generation_config: GenerationConfig,
}

impl GeminiAdapter {
    /// Creates a `GeminiAdapter` for `model_name` served at `endpoint`.
    pub fn new(model_name: impl Into<String>, endpoint: &GeminiEndpoint) -> Self {
        let model_name = model_name.into();
        Self {
            endpoint_url: endpoint.generate_content_url(&model_name),
            model_name,
            generation_config: GenerationConfig::default(),
        }
    }

    /// Sets the sampling settings sent with every request.
    pub fn with_generation_config(mut self, generation_config: GenerationConfig) -> Self {
        self.generation_config = generation_config;
        self
    }
}

#[derive(Serialize)]
struct GeminiPart<'a> {
    text: &'a str,
}

#[derive(Serialize)]
struct GeminiContent<'a> {
    role: &'static str,
    parts: Vec<GeminiPart<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiRequestBody<'a> {
    contents: Vec<GeminiContent<'a>>,
    #[serde(skip_serializing_if = "GenerationConfig::is_unset")]
    generation_config: &'a GenerationConfig,
}

#[derive(Deserialize)]
struct GeminiCandidatePart {
    text: Option<String>,
    #[serde(default)]
    thought: bool,
}

#[derive(Deserialize)]
struct GeminiCandidateContent {
    parts: Option<Vec<GeminiCandidatePart>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    content: Option<GeminiCandidateContent>,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct GeminiUsageMetadata {
    #[serde(rename = "totalTokenCount")]
    total_token_count: Option<usize>,
}

#[derive(Deserialize)]
struct GeminiResponseBody {
    candidates: Option<Vec<GeminiCandidate>>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsageMetadata>,
    error: Option<GeminiErrorDetails>,
}

#[derive(Deserialize)]
struct GeminiErrorDetails {
    message: Option<String>,
}

impl GeminiCandidate {
    /// All non-thought text parts, concatenated, or `None` if there are none.
    fn text(self) -> Option<String> {
        let texts: Vec<String> = self
            .content
            .and_then(|content| content.parts)
            .into_iter()
            .flatten()
            .filter(|part| !part.thought)
            .filter_map(|part| part.text)
            .collect();
        (!texts.is_empty()).then(|| texts.concat())
    }
}

impl GeminiResponseBody {
    fn into_response(self, request_id: &str) -> Result<LLMResponse, RemoteInferenceError> {
        let candidate = self
            .candidates
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| RemoteInferenceError::Adapter("Gemini returned no candidates".into()))?;
        let finish_reason = candidate.finish_reason.clone();
        let response_text = candidate.text().ok_or_else(|| {
            RemoteInferenceError::Adapter(format!(
                "Gemini returned no text (finishReason: {})",
                finish_reason.as_deref().unwrap_or("unspecified")
            ))
        })?;
        Ok(LLMResponse {
            request_id: request_id.to_string(),
            response_text,
            token_count: self
                .usage_metadata
                .and_then(|u| u.total_token_count)
                .unwrap_or(0),
        })
    }
}

/// The server's `Retry-After` delay, when given in seconds.
fn retry_after_ms(response: &reqwest::Response) -> Option<u64> {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(|secs| secs * 1000)
}

impl RemoteEndpointAdapter<PromptRequest, LLMResponse> for GeminiAdapter {
    fn execute<'a>(
        &'a self,
        remote: &'a RemoteClient,
        request: &'a PromptRequest,
    ) -> RemoteCall<'a, LLMResponse> {
        Box::pin(async move {
            let body = GeminiRequestBody {
                contents: vec![GeminiContent {
                    role: "user",
                    parts: vec![GeminiPart {
                        text: &request.prompt,
                    }],
                }],
                generation_config: &self.generation_config,
            };

            let response = remote
                .auth
                .authenticate(remote.client.post(&self.endpoint_url))
                .await
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    if e.is_timeout() {
                        RemoteInferenceError::Timeout(e.to_string())
                    } else {
                        RemoteInferenceError::Network(e.to_string())
                    }
                })?;

            let status = response.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(RemoteInferenceError::RateLimitExceeded {
                    retry_after_ms: retry_after_ms(&response),
                });
            }
            if !status.is_success() {
                return Err(RemoteInferenceError::Http {
                    status: status.as_u16(),
                    message: response.text().await.unwrap_or_default(),
                });
            }

            let body: GeminiResponseBody = response
                .json()
                .await
                .map_err(|e| RemoteInferenceError::Serialization(e.to_string()))?;
            if let Some(err) = body.error {
                return Err(RemoteInferenceError::Http {
                    status: status.as_u16(),
                    message: err
                        .message
                        .unwrap_or_else(|| "Unknown Gemini error".to_string()),
                });
            }
            body.into_response(&request.request_id)
        })
    }
}

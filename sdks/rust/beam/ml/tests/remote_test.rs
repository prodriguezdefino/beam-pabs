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

//! Integration tests for [`RemoteModelHandler`] and [`GeminiAdapter`] using `wiremock` and Prism runner.

#![cfg(feature = "remote")]

use std::time::{Duration, Instant};

use beam::options::Secret;
use beam::prelude::*;
use beam_ml::remote::{
    GeminiAdapter, GeminiEndpoint, GenerationConfig, LLMResponse, PromptRequest, RemoteAuth,
    RemoteConfig, RemoteInferenceError, RemoteModelHandler, ThinkingConfig,
};
use beam_ml::{BatchBounds, ModelHandler, PredictionResult, RunInference};
use testing::{TestPipeline, passert};
use wiremock::matchers::{body_json, body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const MODEL: &str = "gemini-2.5-flash";
const GENERATE_PATH: &str = "/generateContent";

fn text_response(parts: serde_json::Value, total_tokens: usize) -> serde_json::Value {
    serde_json::json!({
        "candidates": [
            {
                "content": { "parts": parts, "role": "model" },
                "finishReason": "STOP"
            }
        ],
        "usageMetadata": { "totalTokenCount": total_tokens }
    })
}

fn prompt(request_id: &str, prompt: &str) -> PromptRequest {
    PromptRequest {
        request_id: request_id.to_string(),
        prompt: prompt.to_string(),
    }
}

fn user_turn(prompt: &str) -> serde_json::Value {
    serde_json::json!({ "contents": [ { "role": "user", "parts": [ { "text": prompt } ] } ] })
}

fn mock_adapter(server: &MockServer) -> GeminiAdapter {
    GeminiAdapter::new(
        MODEL,
        &GeminiEndpoint::Url(format!("{}{GENERATE_PATH}", server.uri())),
    )
}

fn fast_config() -> RemoteConfig {
    RemoteConfig::default()
        .with_auth(RemoteAuth::None)
        .with_timeout(Duration::from_secs(5))
        .with_initial_retry_backoff(Duration::from_millis(20))
}

/// Runs `batch` as one `run_inference` call, outside a pipeline, so the batch composition
/// is exact.
fn run_batch(
    handler: &RemoteModelHandler<PromptRequest, LLMResponse, GeminiAdapter>,
    batch: Vec<PromptRequest>,
) -> Result<Vec<LLMResponse>> {
    let model = handler.load_model()?;
    handler.run_inference(&batch, &model, None)
}

#[test]
fn test_gemini_endpoint_urls() {
    let cases = [
        (
            GeminiEndpoint::VertexAi {
                project: "my-project".into(),
                location: "us-central1".into(),
            },
            "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project/locations/\
             us-central1/publishers/google/models/gemini-2.5-flash:generateContent",
        ),
        (
            GeminiEndpoint::VertexAi {
                project: "my-project".into(),
                location: "global".into(),
            },
            "https://aiplatform.googleapis.com/v1/projects/my-project/locations/global/\
             publishers/google/models/gemini-2.5-flash:generateContent",
        ),
        (
            GeminiEndpoint::DeveloperApi,
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:\
             generateContent",
        ),
        (
            GeminiEndpoint::Url("http://localhost:1/x".into()),
            "http://localhost:1/x",
        ),
    ];
    for (endpoint, expected) in cases {
        assert_eq!(
            endpoint.generate_content_url(MODEL),
            expected,
            "{endpoint:?}"
        );
    }
}

#[test]
fn test_retryable_errors() {
    let cases = [
        (
            RemoteInferenceError::RateLimitExceeded {
                retry_after_ms: None,
            },
            true,
        ),
        (RemoteInferenceError::Timeout("t".into()), true),
        (RemoteInferenceError::Network("n".into()), true),
        (
            RemoteInferenceError::Http {
                status: 503,
                message: String::new(),
            },
            true,
        ),
        (
            RemoteInferenceError::Http {
                status: 400,
                message: String::new(),
            },
            false,
        ),
        (RemoteInferenceError::Adapter("a".into()), false),
        (RemoteInferenceError::Serialization("s".into()), false),
    ];
    for (error, retryable) in cases {
        assert_eq!(error.is_retryable(), retryable, "{error:?}");
    }
}

#[tokio::test]
async fn test_remote_gemini_inference_success() {
    let mock_server = MockServer::start().await;

    let response_json = text_response(
        serde_json::json!([
            { "text": "Apache Beam is an advanced unified data processing framework." }
        ]),
        42,
    );

    // The key is referenced as a file secret and must reach the endpoint resolved.
    let key_file =
        std::env::temp_dir().join(format!("beam_remote_test_key_{}", std::process::id()));
    std::fs::write(&key_file, "test-gemini-key\n").expect("key file is writable");
    let api_key: Secret = format!("file:{}", key_file.display())
        .parse()
        .expect("valid secret reference");

    Mock::given(method("POST"))
        .and(path("/v1beta/models/gemini-2.5-flash:generateContent"))
        .and(header("x-goog-api-key", "test-gemini-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response_json))
        .expect(2)
        .mount(&mock_server)
        .await;

    let config = RemoteConfig::default()
        .with_auth(RemoteAuth::ApiKey(api_key))
        .with_timeout(Duration::from_secs(5))
        .with_initial_retry_backoff(Duration::from_millis(20));
    let adapter = GeminiAdapter::new(
        MODEL,
        &GeminiEndpoint::Url(format!(
            "{}/v1beta/models/gemini-2.5-flash:generateContent",
            mock_server.uri()
        )),
    );
    let handler = RemoteModelHandler::new(config, adapter);

    let p = TestPipeline::new();
    let requests = vec![
        prompt("req_1", "What is Apache Beam?"),
        prompt("req_2", "Explain Rust SDK."),
    ];

    let inputs = p.apply(Create::new("Inputs", requests));
    let predictions = inputs.apply(RunInference::new("GeminiInference", handler));

    passert::that("AssertPredictions", &predictions).has_count(2);
    passert::that("AssertPredictions", &predictions).satisfies(
        |results: &[PredictionResult<PromptRequest, LLMResponse>]| {
            if results.len() != 2 {
                return Err(format!("Expected 2 predictions, got {}", results.len()).into());
            }

            for res in results {
                if res.output.token_count != 42 {
                    return Err(
                        format!("Expected 42 tokens, got {}", res.output.token_count).into(),
                    );
                }
                if !res.output.response_text.contains("Apache Beam") {
                    return Err(
                        format!("Unexpected response text: {}", res.output.response_text).into(),
                    );
                }
            }
            Ok(())
        },
    );

    p.run().await.expect("pipeline should succeed");
}

#[tokio::test]
async fn test_remote_gemini_inference_retry_on_rate_limit() {
    let mock_server = MockServer::start().await;

    let success_json = text_response(
        serde_json::json!([{ "text": "Success after backoff retry." }]),
        10,
    );

    // First request returns 429 Too Many Requests
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // The next request succeeds
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(success_json))
        .mount(&mock_server)
        .await;

    let config = fast_config().with_max_retries(3);
    let handler = RemoteModelHandler::new(config, mock_adapter(&mock_server));

    let p = TestPipeline::new();
    let requests = vec![prompt("retry_req", "Will this retry?")];

    let inputs = p.apply(Create::new("Inputs", requests));
    let predictions = inputs.apply(RunInference::new("GeminiRetryInference", handler));

    passert::that("AssertPredictions", &predictions).has_count(1);
    passert::that("AssertPredictions", &predictions).satisfies(
        |results: &[PredictionResult<PromptRequest, LLMResponse>]| {
            let first = results.first().ok_or("Empty results")?;
            if first.output.response_text != "Success after backoff retry." {
                return Err(format!("Unexpected text: {}", first.output.response_text).into());
            }
            Ok(())
        },
    );

    p.run().await.expect("pipeline should succeed with retry");
}

/// A throttled request is retried alone: the other requests of its batch are sent once.
#[tokio::test(flavor = "multi_thread")]
async fn test_retry_resends_only_the_failed_request() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .and(body_partial_json(user_turn("throttled")))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(2)
        .expect(2)
        .with_priority(1)
        .mount(&mock_server)
        .await;

    let texts = ["throttled", "steady_a", "steady_b"];
    for text in texts {
        Mock::given(method("POST"))
            .and(path(GENERATE_PATH))
            .and(body_partial_json(user_turn(text)))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(text_response(serde_json::json!([{ "text": text }]), 1)),
            )
            .expect(1)
            .with_priority(2)
            .mount(&mock_server)
            .await;
    }

    let handler = RemoteModelHandler::new(
        fast_config().with_max_retries(3),
        mock_adapter(&mock_server),
    );
    let batch = texts.iter().map(|text| prompt(text, text)).collect();

    let responses = run_batch(&handler, batch).expect("every request eventually succeeds");
    let answers: Vec<_> = responses.iter().map(|r| r.response_text.as_str()).collect();
    assert_eq!(answers, texts);
    mock_server.verify().await;
}

/// A request that stays throttled fails the batch once its own retries run out.
#[tokio::test(flavor = "multi_thread")]
async fn test_retries_exhausted_per_request() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(503).set_body_string("overloaded"))
        .expect(3)
        .mount(&mock_server)
        .await;

    let handler = RemoteModelHandler::new(
        fast_config().with_max_retries(2),
        mock_adapter(&mock_server),
    );
    let err = run_batch(&handler, vec![prompt("r", "down")]).expect_err("server stays down");
    assert!(err.to_string().contains("503"), "{err}");
    mock_server.verify().await;
}

/// Every request of a batch takes its own concurrency permit.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrency_permit_per_request() {
    const DELAY: Duration = Duration::from_millis(300);
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(DELAY)
                .set_body_json(text_response(serde_json::json!([{ "text": "ok" }]), 1)),
        )
        .mount(&mock_server)
        .await;

    let batch: Vec<_> = (0..3).map(|i| prompt(&i.to_string(), "p")).collect();
    let cases = [(1, DELAY * 3, None), (3, DELAY, Some(DELAY * 2))];
    for (permits, at_least, below) in cases {
        let handler = RemoteModelHandler::new(
            fast_config().with_max_concurrent_requests(permits),
            mock_adapter(&mock_server),
        );
        let start = Instant::now();
        run_batch(&handler, batch.clone()).expect("batch succeeds");
        let elapsed = start.elapsed();
        assert!(
            elapsed >= at_least,
            "{permits} permits: {elapsed:?} < {at_least:?}"
        );
        if let Some(below) = below {
            assert!(
                elapsed < below,
                "{permits} permits: {elapsed:?} >= {below:?}"
            );
        }
    }
}

/// The response text joins every non-thought text part of the first candidate.
#[tokio::test(flavor = "multi_thread")]
async fn test_response_parts_are_joined() {
    let mock_server = MockServer::start().await;
    let parts = serde_json::json!([
        { "text": "internal reasoning", "thought": true },
        { "text": "Hello, " },
        { "inlineData": { "mimeType": "image/png", "data": "" } },
        { "text": "world." }
    ]);
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(text_response(parts, 7)))
        .mount(&mock_server)
        .await;

    let handler = RemoteModelHandler::new(fast_config(), mock_adapter(&mock_server));
    let responses = run_batch(&handler, vec![prompt("r", "greet")]).expect("request succeeds");
    assert_eq!(
        responses,
        [LLMResponse {
            request_id: "r".into(),
            response_text: "Hello, world.".into(),
            token_count: 7,
        }]
    );
}

/// A response without text is an error, not an empty prediction.
#[tokio::test(flavor = "multi_thread")]
async fn test_response_without_text_fails() {
    let mock_server = MockServer::start().await;
    let body = serde_json::json!({
        "candidates": [ { "content": { "role": "model" }, "finishReason": "MAX_TOKENS" } ]
    });
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&mock_server)
        .await;

    let handler = RemoteModelHandler::new(fast_config(), mock_adapter(&mock_server));
    let err = run_batch(&handler, vec![prompt("r", "p")]).expect_err("no text");
    assert!(err.to_string().contains("MAX_TOKENS"), "{err}");
    mock_server.verify().await;
}

/// The request is the prompt as one user turn plus the configured `generationConfig`,
/// which is left out when nothing is set.
#[tokio::test(flavor = "multi_thread")]
async fn test_request_body_shape() {
    let parity = GenerationConfig {
        temperature: Some(0.0),
        max_output_tokens: Some(256),
        thinking_config: Some(ThinkingConfig { thinking_budget: 0 }),
    };
    let cases = [
        (GenerationConfig::default(), user_turn("Say hi")),
        (
            parity,
            serde_json::json!({
                "contents": [ { "role": "user", "parts": [ { "text": "Say hi" } ] } ],
                "generationConfig": {
                    "temperature": 0.0,
                    "maxOutputTokens": 256,
                    "thinkingConfig": { "thinkingBudget": 0 }
                }
            }),
        ),
    ];

    for (generation_config, expected_body) in cases {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(GENERATE_PATH))
            .and(body_json(&expected_body))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(text_response(serde_json::json!([{ "text": "hi" }]), 3)),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let adapter = mock_adapter(&mock_server).with_generation_config(generation_config);
        let handler = RemoteModelHandler::new(fast_config().with_max_retries(0), adapter);
        run_batch(&handler, vec![prompt("r", "Say hi")]).expect("body matches");
        mock_server.verify().await;
    }
}

#[tokio::test]
async fn test_remote_gemini_dead_letter_queue_on_permanent_error() {
    let mock_server = MockServer::start().await;

    let error_json = serde_json::json!({
        "error": {
            "code": 400,
            "message": "Invalid prompt syntax or safety block.",
            "status": "INVALID_ARGUMENT"
        }
    });

    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(400).set_body_json(error_json))
        .mount(&mock_server)
        .await;

    let config = fast_config()
        .with_max_retries(1)
        .with_batch_bounds(BatchBounds::new(1, 1));
    let handler = RemoteModelHandler::new(config, mock_adapter(&mock_server));

    let p = TestPipeline::new();
    let requests = vec![prompt("bad_req", "Blocked content")];

    let inputs = p.apply(Create::new("Inputs", requests));
    let multi_output =
        inputs.apply(RunInference::new("GeminiDlqInference", handler).with_exception_handling());

    let main_output = multi_output.output;
    let dlq_output = multi_output.failures;

    // Main output should be empty because request failed
    passert::that("AssertMainOutput", &main_output).empty();

    // DLQ output should contain the failed request and error message
    passert::that("AssertDlqOutput", &dlq_output).has_count(1);
    passert::that("AssertDlqOutput", &dlq_output).satisfies(
        |failures: &[Failure<PromptRequest>]| {
            let fail = failures.first().ok_or("Empty DLQ")?;
            if fail.input.request_id != "bad_req" {
                return Err(format!("Unexpected request ID: {}", fail.input.request_id).into());
            }
            if !fail.error.contains("Invalid prompt syntax") {
                return Err(format!("Unexpected error message: {}", fail.error).into());
            }
            Ok(())
        },
    );

    p.run().await.expect("pipeline with DLQ should succeed");
}

/// Runs `batch` on its own thread, failing the test if it does not finish within `limit`.
fn run_batch_within(
    handler: &RemoteModelHandler<PromptRequest, LLMResponse, GeminiAdapter>,
    batch: Vec<PromptRequest>,
    limit: Duration,
) -> std::result::Result<Vec<LLMResponse>, String> {
    let handler = handler.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(run_batch(&handler, batch).map_err(|e| e.to_string()));
    });
    rx.recv_timeout(limit)
        .expect("run_inference did not finish in time")
}

fn file_secret(name: &str, value: &str) -> Secret {
    let path = std::env::temp_dir().join(format!("beam_remote_{name}_{}", std::process::id()));
    std::fs::write(&path, value).expect("secret file is writable");
    format!("file:{}", path.display())
        .parse()
        .expect("valid secret reference")
}

#[test]
fn test_remote_config_defaults() {
    let config = RemoteConfig::default();
    assert_eq!(config.auth, RemoteAuth::ApplicationDefault);
    assert_eq!(config.timeout, Duration::from_secs(60));
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.initial_retry_backoff, Duration::from_secs(5));
    assert_eq!(config.max_concurrent_requests, 64);
    assert_eq!(
        config.batch_bounds,
        BatchBounds::new(1, 16).with_duration(Duration::from_millis(50))
    );
}

#[test]
fn test_remote_config_builders() {
    let bounds = BatchBounds::new(2, 3).with_duration(Duration::from_millis(9));
    let config = RemoteConfig::default()
        .with_auth(RemoteAuth::None)
        .with_timeout(Duration::from_millis(1234))
        .with_max_retries(7)
        .with_initial_retry_backoff(Duration::from_millis(11))
        .with_max_concurrent_requests(5)
        .with_batch_bounds(bounds);
    assert_eq!(config.auth, RemoteAuth::None);
    assert_eq!(config.timeout, Duration::from_millis(1234));
    assert_eq!(config.max_retries, 7);
    assert_eq!(config.initial_retry_backoff, Duration::from_millis(11));
    assert_eq!(config.max_concurrent_requests, 5);
    assert_eq!(config.batch_bounds, bounds);
}

#[test]
fn test_handler_reports_config_bounds_and_model_id() {
    let bounds = BatchBounds::new(4, 8);
    let handler: RemoteModelHandler<PromptRequest, LLMResponse, GeminiAdapter> =
        RemoteModelHandler::new(
            RemoteConfig::default().with_batch_bounds(bounds),
            GeminiAdapter::new(MODEL, &GeminiEndpoint::DeveloperApi),
        );
    assert_eq!(handler.get_batch_bounds(), bounds);
    assert_eq!(handler.model_id(), None);
    let named = handler.with_model_id("gemini-prod");
    assert_eq!(named.model_id(), Some("gemini-prod".to_string()));
}

#[test]
fn test_retry_after() {
    let cases = [
        (
            RemoteInferenceError::RateLimitExceeded {
                retry_after_ms: Some(1500),
            },
            Some(Duration::from_millis(1500)),
        ),
        (
            RemoteInferenceError::RateLimitExceeded {
                retry_after_ms: None,
            },
            None,
        ),
        (RemoteInferenceError::Timeout("t".into()), None),
    ];
    for (error, expected) in cases {
        assert_eq!(error.retry_after(), expected, "{error:?}");
    }
}

fn runner_thread() -> std::thread::ThreadId {
    beam_ml::remote::block_on_remote(async { Ok(std::thread::current().id()) })
        .expect("block_on_remote")
}

/// Outside any runtime the future is driven on the calling thread.
#[test]
fn test_block_on_remote_without_runtime_runs_inline() {
    assert_eq!(runner_thread(), std::thread::current().id());
}

/// On a multi-thread runtime the caller blocks in place instead of spawning a thread.
#[tokio::test(flavor = "multi_thread")]
async fn test_block_on_remote_multi_thread_blocks_in_place() {
    assert_eq!(runner_thread(), std::thread::current().id());
}

/// On a current-thread runtime the future moves to a helper thread.
#[tokio::test(flavor = "current_thread")]
async fn test_block_on_remote_current_thread_uses_helper_thread() {
    assert_ne!(runner_thread(), std::thread::current().id());
}

/// Backoff doubles per retry and the attempt counter bounds the retries.
#[tokio::test(flavor = "multi_thread")]
async fn test_backoff_doubles_between_retries() {
    const BACKOFF: Duration = Duration::from_millis(100);
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(503))
        .expect(3)
        .mount(&mock_server)
        .await;

    let handler = RemoteModelHandler::new(
        fast_config()
            .with_max_retries(2)
            .with_initial_retry_backoff(BACKOFF),
        mock_adapter(&mock_server),
    );
    let start = Instant::now();
    let err = run_batch_within(&handler, vec![prompt("r", "p")], Duration::from_secs(10))
        .expect_err("server stays down");
    let elapsed = start.elapsed();
    assert!(err.contains("503"), "{err}");
    // BACKOFF + 2 * BACKOFF
    assert!(elapsed >= BACKOFF * 3, "{elapsed:?}");
    mock_server.verify().await;
}

/// Non-retryable errors are returned on the first attempt even with retries left.
#[tokio::test(flavor = "multi_thread")]
async fn test_non_retryable_error_is_not_retried() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let handler = RemoteModelHandler::new(
        fast_config().with_max_retries(3),
        mock_adapter(&mock_server),
    );
    let err = run_batch_within(&handler, vec![prompt("r", "p")], Duration::from_secs(5))
        .expect_err("400 is permanent");
    assert!(err.contains("400") && err.contains("bad request"), "{err}");
    mock_server.verify().await;
}

/// A numeric `Retry-After` is converted from seconds to milliseconds; other forms are ignored.
#[tokio::test(flavor = "multi_thread")]
async fn test_retry_after_header_is_parsed() {
    let cases = [
        ("2", "Some(2000)"),
        (" 3 ", "Some(3000)"),
        ("Wed, 21 Oct 2015 07:28:00 GMT", "None"),
    ];
    for (header_value, expected) in cases {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(GENERATE_PATH))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", header_value))
            .expect(1)
            .mount(&mock_server)
            .await;

        let handler = RemoteModelHandler::new(
            fast_config().with_max_retries(0),
            mock_adapter(&mock_server),
        );
        let err = run_batch_within(&handler, vec![prompt("r", "p")], Duration::from_secs(5))
            .expect_err("429 without retries fails");
        assert!(
            err.contains("429") && err.contains(&format!("Retry after {expected} ms")),
            "{header_value}: {err}"
        );
        mock_server.verify().await;
    }
}

/// A 429 without `Retry-After` reports no server delay.
#[tokio::test(flavor = "multi_thread")]
async fn test_rate_limit_without_retry_after() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(ResponseTemplate::new(429))
        .mount(&mock_server)
        .await;
    let handler = RemoteModelHandler::new(
        fast_config().with_max_retries(0),
        mock_adapter(&mock_server),
    );
    let err = run_batch_within(&handler, vec![prompt("r", "p")], Duration::from_secs(5))
        .expect_err("429 without retries fails");
    assert!(err.contains("Retry after None ms"), "{err}");
}

/// A 200 carrying an `error` object is surfaced with its message.
#[tokio::test(flavor = "multi_thread")]
async fn test_error_body_with_success_status_fails() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "error": { "message": "quota" } })),
        )
        .mount(&mock_server)
        .await;
    let handler = RemoteModelHandler::new(fast_config(), mock_adapter(&mock_server));
    let err = run_batch_within(&handler, vec![prompt("r", "p")], Duration::from_secs(5))
        .expect_err("error body");
    assert!(err.contains("200") && err.contains("quota"), "{err}");
}

/// Bearer token auth sends the resolved secret in `Authorization`.
#[tokio::test(flavor = "multi_thread")]
async fn test_bearer_token_auth() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(GENERATE_PATH))
        .and(header("authorization", "Bearer tok-123"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(text_response(serde_json::json!([{ "text": "ok" }]), 2)),
        )
        .expect(1)
        .mount(&mock_server)
        .await;
    let config = fast_config()
        .with_max_retries(0)
        .with_auth(RemoteAuth::BearerToken(file_secret("bearer", "tok-123\n")));
    let handler = RemoteModelHandler::new(config, mock_adapter(&mock_server));
    let responses = run_batch_within(&handler, vec![prompt("r", "p")], Duration::from_secs(5))
        .expect("authenticated request succeeds");
    assert_eq!(responses[0].response_text, "ok");
    assert_eq!(responses[0].token_count, 2);
    mock_server.verify().await;
}

#[test]
fn test_prompt_request_coder() {
    use beam::coders::{Coder, Context, DefaultCoder, URN_KV};
    let coder = PromptRequest::coder();
    assert_eq!(coder.urn(), URN_KV);
    let value = prompt("id-1", "hello");
    for context in [Context::Nested, Context::WholeStream] {
        let mut buf = Vec::new();
        coder.encode(&value, &mut buf, context).expect("encode");
        assert!(!buf.is_empty());
        let decoded = coder.decode(&mut buf.as_slice(), context).expect("decode");
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_llm_response_coder() {
    use beam::coders::{Coder, Context, DefaultCoder, URN_KV};
    let coder = LLMResponse::coder();
    assert_eq!(coder.urn(), URN_KV);
    let value = LLMResponse {
        request_id: "id-2".into(),
        response_text: "answer".into(),
        token_count: 99,
    };
    for context in [Context::Nested, Context::WholeStream] {
        let mut buf = Vec::new();
        coder.encode(&value, &mut buf, context).expect("encode");
        assert!(!buf.is_empty());
        let decoded = coder.decode(&mut buf.as_slice(), context).expect("decode");
        assert_eq!(decoded, value);
    }
    let mut buf = Vec::new();
    value.encode_element(&mut buf).expect("encode_element");
    assert_eq!(
        LLMResponse::decode_element(&mut buf.as_slice()).expect("decode_element"),
        value
    );
}

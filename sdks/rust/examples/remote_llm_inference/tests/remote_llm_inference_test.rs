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

//! Integration tests for the Remote LLM Inference example pipeline on Prism runner.

use beam::ml::PredictionResult;
use beam::ml::remote::{
    GeminiEndpoint, GenerationConfig, LLMResponse, PromptRequest, RemoteAuth, ThinkingConfig,
};
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use remote_llm_inference::{
    RemoteLlmArgs, build_remote_inference_pipeline, format_failure, format_prediction,
    prompt_request,
};
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn parse_args(flags: &[&str]) -> RemoteLlmArgs {
    let argv = ["remote_llm_inference", "--input=prompts.txt"]
        .into_iter()
        .chain(flags.iter().copied());
    beam::options::try_parse_from::<RemoteLlmArgs, _, _>(argv)
        .expect("flags parse")
        .1
}

const PROMPT_LINES: [&str; 3] = [
    "What is Apache Beam and what makes its unified model unique?",
    "How does Apache Beam handle out-of-order data with event time and watermarks?",
    "Describe the benefits of RunInference micro-batching in data processing pipelines.",
];

#[test]
fn test_defaults_match_python_baseline() {
    let args = parse_args(&["--cloud_project=my-project"]);
    assert_eq!(args.model_name, "gemini-2.5-flash");
    assert_eq!(
        args.endpoint(),
        GeminiEndpoint::VertexAi {
            project: "my-project".into(),
            location: "us-central1".into(),
        }
    );
    assert_eq!(args.auth(), RemoteAuth::ApplicationDefault);
    assert_eq!(args.generation_config(), GenerationConfig::default());

    let config = args.remote_config();
    assert_eq!(config.batch_bounds.max_batch_size, 1);
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.initial_retry_backoff.as_secs(), 5);
}

#[test]
fn test_endpoint_selection() {
    let cases: [(&[&str], GeminiEndpoint); 3] = [
        (
            &["--cloud_project=p", "--cloud_region=global"],
            GeminiEndpoint::VertexAi {
                project: "p".into(),
                location: "global".into(),
            },
        ),
        (
            &["--endpoint_url=http://localhost:1/generateContent"],
            GeminiEndpoint::Url("http://localhost:1/generateContent".into()),
        ),
        (&[], GeminiEndpoint::DeveloperApi),
    ];
    for (flags, expected) in cases {
        assert_eq!(parse_args(flags).endpoint(), expected, "{flags:?}");
    }
}

#[test]
fn test_generation_config_flags() {
    let args = parse_args(&[
        "--temperature=0",
        "--max_output_tokens=256",
        "--thinking_budget=0",
    ]);
    assert_eq!(
        args.generation_config(),
        GenerationConfig {
            temperature: Some(0.0),
            max_output_tokens: Some(256),
            thinking_config: Some(ThinkingConfig { thinking_budget: 0 }),
        }
    );
}

#[test]
fn test_prompt_construction() {
    let cases = [
        (
            "Explain: to be or not to be",
            Some("Explain: to be or not to be"),
        ),
        (
            "  leading and trailing space  ",
            Some("  leading and trailing space  "),
        ),
        ("", None),
        ("   \t", None),
    ];
    for (line, expected) in cases {
        let request = prompt_request(line.to_string());
        assert_eq!(
            request.as_ref().map(|r| r.prompt.as_str()),
            expected,
            "{line:?}"
        );
    }

    let a = prompt_request("same".into()).expect("non-blank");
    let b = prompt_request("same".into()).expect("non-blank");
    let c = prompt_request("other".into()).expect("non-blank");
    assert_eq!(a.request_id, b.request_id);
    assert_ne!(a.request_id, c.request_id);
}

#[test]
fn test_output_formats() {
    let input = PromptRequest {
        request_id: "req_1".into(),
        prompt: "What is 5+2?".into(),
    };
    let prediction = PredictionResult::new(
        input.clone(),
        LLMResponse {
            request_id: "req_1".into(),
            response_text: "7".into(),
            token_count: 9,
        },
    );
    assert_eq!(
        format_prediction(&prediction),
        "Input: What is 5+2?, Output: 7"
    );
    assert_eq!(
        format_failure(&Failure::new(input, "HTTP error status 400: bad")),
        "req_1\tHTTP error status 400: bad"
    );
}

#[tokio::test]
async fn test_remote_llm_pipeline_success_on_prism() {
    let mock_server = MockServer::start().await;

    // Each line reaches the model verbatim, as a single user turn.
    for line in PROMPT_LINES {
        let response_body = serde_json::json!({
            "candidates": [
                {
                    "content": {
                        "parts": [ { "text": "Beam provides " }, { "text": "unified processing." } ],
                        "role": "model"
                    },
                    "finishReason": "STOP"
                }
            ],
            "usageMetadata": { "totalTokenCount": 55 }
        });
        Mock::given(method("POST"))
            .and(path("/generateContent"))
            .and(body_partial_json(serde_json::json!({
                "contents": [ { "role": "user", "parts": [ { "text": line } ] } ]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body))
            .expect(1)
            .mount(&mock_server)
            .await;
    }

    let endpoint = format!("--endpoint_url={}/generateContent", mock_server.uri());
    let args = parse_args(&[&endpoint, "--batch_size=2"]);
    let config = args.remote_config().with_auth(RemoteAuth::None);

    let p = TestPipeline::new();
    let lines: Vec<String> = PROMPT_LINES.iter().map(|l| l.to_string()).collect();
    let input = p
        .apply(Create::new("CreateLines", lines))
        .flat_map("ToPromptRequest", prompt_request);

    let multi_output = build_remote_inference_pipeline(&input, config, args.adapter());
    let formatted = multi_output
        .output
        .map("Format", |pred| format_prediction(&pred));

    let mut expected: Vec<String> = PROMPT_LINES
        .iter()
        .map(|line| format!("Input: {line}, Output: Beam provides unified processing."))
        .collect();
    expected.sort();
    passert::that("AssertFormatted", &formatted).satisfies(move |lines: &[String]| {
        let mut actual = lines.to_vec();
        actual.sort();
        if actual == expected {
            Ok(())
        } else {
            Err(format!("Expected {expected:?}, got {actual:?}").into())
        }
    });
    passert::that("PAssert", &multi_output.failures).empty();

    p.run().await.expect("pipeline execution should succeed");
}

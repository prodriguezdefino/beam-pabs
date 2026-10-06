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

use beam::ml::remote::{
    GeminiEndpoint, GenerationConfig, RemoteAuth, RemoteModelHandler, ThinkingConfig,
};
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use remote_llm_inference::{Args, GenerateResponses, prompt_request};
use serde_json::json;
use wiremock::matchers::{body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn parse_args(flags: &[&str]) -> Args {
    let argv = ["remote_llm_inference", "--input=prompts.txt"]
        .into_iter()
        .chain(flags.iter().copied());
    beam::options::try_parse_from::<Args, _, _>(argv)
        .expect("flags parse")
        .1
}

/// Answers `prompt` with HTTP `status` and `body`, one time.
async fn answer(server: &MockServer, prompt: &str, status: u16, body: serde_json::Value) {
    Mock::given(method("POST"))
        .and(path("/generateContent"))
        .and(body_partial_json(
            json!({ "contents": [ { "role": "user", "parts": [ { "text": prompt } ] } ] }),
        ))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .expect(1)
        .mount(server)
        .await;
}

#[tokio::test]
async fn test_generate_responses_routes_rejected_prompts_to_failures() {
    let server = MockServer::start().await;
    let reply = json!({
        "candidates": [ { "content": { "role": "model",
            "parts": [ { "text": "Beam " }, { "text": "rocks." } ] } } ],
        "usageMetadata": { "totalTokenCount": 5 }
    });
    answer(&server, "What is Beam?", 200, reply.clone()).await;
    answer(&server, "What is Rust?", 200, reply).await;
    answer(
        &server,
        "bad prompt",
        400,
        json!({ "error": { "message": "bad" } }),
    )
    .await;

    let args = parse_args(&[&format!("--endpoint_url={}/generateContent", server.uri())]);
    let handler = RemoteModelHandler::new(
        args.remote_config().with_auth(RemoteAuth::None),
        args.adapter(),
    );

    let p = TestPipeline::new();
    let lines = ["What is Beam?", "", "What is Rust?", "bad prompt"].map(String::from);
    let WithFailures { output, failures } = p
        .apply("Create", Create::new(lines.to_vec()))
        .apply("GenerateResponses", GenerateResponses::new(handler));

    passert::that("AssertPredictions", &output).contains_in_any_order([
        "Input: What is Beam?, Output: Beam rocks.".to_string(),
        "Input: What is Rust?, Output: Beam rocks.".to_string(),
    ]);
    passert::that(
        "AssertFailedPrompts",
        &failures.map("Prompt", |f| f.input.prompt),
    )
    .contains_in_any_order(["bad prompt".to_string()]);

    p.run().await.expect("pipeline must succeed");
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
fn test_flags_set_generation_and_transport() {
    let defaults = parse_args(&[]);
    assert_eq!(defaults.auth(), RemoteAuth::ApplicationDefault);
    assert_eq!(
        defaults.adapter().generation_config,
        GenerationConfig::default()
    );
    assert_eq!(defaults.remote_config().batch_bounds.max_batch_size, 1);

    let args = parse_args(&[
        "--temperature=0",
        "--max_output_tokens=256",
        "--thinking_budget=0",
        "--batch_size=8",
    ]);
    assert_eq!(
        args.adapter().generation_config,
        GenerationConfig {
            temperature: Some(0.0),
            max_output_tokens: Some(256),
            thinking_config: Some(ThinkingConfig { thinking_budget: 0 }),
        }
    );
    assert_eq!(args.remote_config().batch_bounds.max_batch_size, 8);
}

#[test]
fn test_prompt_request_keeps_line_and_has_stable_id() {
    let request = prompt_request("  to be or not to be ".into()).expect("non-blank line");
    assert_eq!(request.prompt, "  to be or not to be ");
    assert_eq!(request.request_id, "req_12ab3d6fa17b63cf");
    assert!(prompt_request(" \t".into()).is_none());
}

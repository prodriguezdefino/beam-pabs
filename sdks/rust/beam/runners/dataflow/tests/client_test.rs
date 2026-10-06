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

use std::collections::HashMap;

use dataflow::client::{
    ClientError, DataflowApiClient, HttpDataflowClient, JOB_STATE_CANCELLED, JOB_STATE_DONE,
    JOB_STATE_DRAINED, JOB_STATE_FAILED, JOB_STATE_PENDING, JOB_STATE_QUEUED, JOB_STATE_RUNNING,
    JOB_STATE_UNKNOWN, JOB_STATE_UPDATED, JobMessageItem, JobMetricsResponse, MessageCursor,
    is_successful_state, is_terminal_state,
};
use dataflow::translate::{
    DataflowEnvironment, DataflowJob, SdkOptionsPayload, SdkPipelineOptions, UserAgent, Version,
};
use serde_json::json;
use wiremock::matchers::{
    body_partial_json, header, method, path, query_param, query_param_is_missing,
};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn test_job_state_predicates() {
    assert!(is_terminal_state(JOB_STATE_DONE));
    assert!(is_terminal_state(JOB_STATE_FAILED));
    assert!(is_terminal_state(JOB_STATE_CANCELLED));
    assert!(is_terminal_state(JOB_STATE_UPDATED));
    assert!(is_terminal_state(JOB_STATE_DRAINED));

    assert!(!is_terminal_state(JOB_STATE_RUNNING));
    assert!(!is_terminal_state(JOB_STATE_PENDING));
    assert!(!is_terminal_state(JOB_STATE_QUEUED));
    assert!(!is_terminal_state(JOB_STATE_UNKNOWN));

    assert!(is_successful_state(JOB_STATE_DONE));
    assert!(is_successful_state(JOB_STATE_DRAINED));
    assert!(!is_successful_state(JOB_STATE_FAILED));
    assert!(!is_successful_state(JOB_STATE_CANCELLED));
    assert!(!is_successful_state(JOB_STATE_RUNNING));
}

#[tokio::test]
async fn test_http_dataflow_client_live_mock_server() {
    const JOBS: &str = "/v1b3/projects/test-proj/locations/us-central1/jobs";
    const AUTH: &str = "Bearer mock-token-xyz";

    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(JOBS))
        .and(header("authorization", AUTH))
        .and(header("content-type", "application/json"))
        .and(body_partial_json(
            json!({"projectId": "test-proj", "name": "http-job"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "job-http-1", "name": "http-job", "currentState": "JOB_STATE_RUNNING"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{JOBS}/job-http-1")))
        .and(header("authorization", AUTH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "job-http-1", "name": "http-job", "currentState": "JOB_STATE_DONE"
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{JOBS}/job-http-1/messages")))
        .and(query_param("startTime", "2026-09-23T00:00:00Z"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobMessages": [{"id": "msg-1", "messageText": "message from http mock"}]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{JOBS}/job-http-1/messages")))
        .and(query_param_is_missing("startTime"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobMessages": [{"id": "msg-2", "messageText": "message without start time"}]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{JOBS}/not-found")))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "Job not found"})))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{JOBS}/invalid-json")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw("not-a-valid-json-response", "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = HttpDataflowClient::with_endpoint_and_token(server.uri(), Some("mock-token-xyz"));

    let debug_str = format!("{client:?}");
    assert!(debug_str.contains("HttpDataflowClient"));
    assert!(debug_str.contains("has_custom_token: true"));

    let job = DataflowJob {
        project_id: "test-proj".to_string(),
        name: "http-job".to_string(),
        job_type: "JOB_TYPE_BATCH".to_string(),
        steps: Vec::new(),
        labels: HashMap::new(),
        environment: DataflowEnvironment {
            user_agent: UserAgent {
                name: "test".to_string(),
                version: "1.0".to_string(),
            },
            version: Version {
                job_type: "FNAPI_BATCH".to_string(),
                major: "6".to_string(),
            },
            temp_storage_prefix: "gs://test/temp".to_string(),
            experiments: vec!["use_runner_v2".to_string()],
            service_account_email: None,
            service_options: Vec::new(),
            sdk_pipeline_options: SdkPipelineOptions {
                options: SdkOptionsPayload {
                    pipeline_url: "gs://test/staging/model".to_string(),
                    pipeline_proto_hash: "hash".to_string(),
                    region: "us-central1".to_string(),
                    temp_location: "gs://test/temp".to_string(),
                    experiments: vec!["use_runner_v2".to_string()],
                    additional_options: HashMap::new(),
                },
                display_data: Vec::new(),
            },
            worker_pools: Vec::new(),
        },
    };

    let created = client
        .create_job("test-proj", "us-central1", &job)
        .await
        .unwrap();
    assert_eq!(created.id, "job-http-1");
    assert_eq!(created.current_state, "JOB_STATE_RUNNING");

    let fetched = client
        .get_job("test-proj", "us-central1", "job-http-1")
        .await
        .unwrap();
    assert_eq!(fetched.id, "job-http-1");
    assert_eq!(fetched.current_state, "JOB_STATE_DONE");

    let msgs = client
        .list_messages(
            "test-proj",
            "us-central1",
            "job-http-1",
            Some("2026-09-23T00:00:00Z"),
        )
        .await
        .unwrap();
    assert_eq!(msgs.job_messages.len(), 1);
    assert_eq!(
        msgs.job_messages[0].message_text.as_deref(),
        Some("message from http mock")
    );

    let msgs_no_time = client
        .list_messages("test-proj", "us-central1", "job-http-1", None)
        .await
        .unwrap();
    assert_eq!(msgs_no_time.job_messages.len(), 1);
    assert_eq!(
        msgs_no_time.job_messages[0].message_text.as_deref(),
        Some("message without start time")
    );

    let err_404 = client
        .get_job("test-proj", "us-central1", "not-found")
        .await
        .unwrap_err();
    match err_404 {
        ClientError::Api { status, .. } => assert_eq!(status, 404),
        other => panic!("expected ClientError::Api, got {other:?}"),
    }

    let err_json = client
        .get_job("test-proj", "us-central1", "invalid-json")
        .await
        .unwrap_err();
    assert!(
        matches!(err_json, ClientError::Json(_)),
        "expected ClientError::Json, got {err_json:?}"
    );

    let default_client = HttpDataflowClient::default();
    let debug_default = format!("{default_client:?}");
    assert!(debug_default.contains("HttpDataflowClient"));
}

fn message(id: Option<&str>, time: Option<&str>, text: &str) -> JobMessageItem {
    JobMessageItem {
        id: id.map(str::to_string),
        time: time.map(str::to_string),
        message_text: Some(text.to_string()),
        message_importance: None,
    }
}

fn texts(messages: &[JobMessageItem]) -> Vec<&str> {
    messages
        .iter()
        .filter_map(|m| m.message_text.as_deref())
        .collect()
}

#[test]
fn test_message_cursor_skips_messages_repeated_by_inclusive_start_time() {
    let (first, cursor) = MessageCursor::default().advance(vec![
        message(Some("1"), Some("t1"), "a"),
        message(Some("2"), Some("t2"), "b"),
    ]);
    assert_eq!(texts(&first), ["a", "b"]);
    assert_eq!(cursor.start_time(), Some("t2"));

    // The API returns the message at t2 again, since startTime=t2 is inclusive.
    let (second, cursor) = cursor.advance(vec![
        message(Some("2"), Some("t2"), "b"),
        message(Some("3"), Some("t3"), "c"),
    ]);
    assert_eq!(texts(&second), ["c"]);
    assert_eq!(cursor.start_time(), Some("t3"));

    let (third, cursor) = cursor.advance(vec![message(Some("3"), Some("t3"), "c")]);
    assert!(third.is_empty());
    assert_eq!(cursor.start_time(), Some("t3"));
}

#[test]
fn test_message_cursor_keeps_new_messages_sharing_the_boundary_time() {
    let (_, cursor) = MessageCursor::default().advance(vec![message(Some("1"), Some("t1"), "a")]);
    let (new, cursor) = cursor.advance(vec![
        message(Some("1"), Some("t1"), "a"),
        message(Some("2"), Some("t1"), "b"),
    ]);
    assert_eq!(texts(&new), ["b"]);

    // Both ids at t1 are now remembered.
    let (again, _) = cursor.advance(vec![
        message(Some("1"), Some("t1"), "a"),
        message(Some("2"), Some("t1"), "b"),
    ]);
    assert!(again.is_empty());
}

#[test]
fn test_message_cursor_treats_unidentified_messages_as_new() {
    let (_, cursor) = MessageCursor::default().advance(vec![message(None, Some("t1"), "a")]);
    let (again, _) = cursor.advance(vec![message(None, Some("t1"), "a")]);
    assert_eq!(texts(&again), ["a"]);
}

#[test]
fn test_message_cursor_ignores_untimed_messages_for_position() {
    let (new, cursor) = MessageCursor::default().advance(vec![
        message(Some("1"), Some("t1"), "a"),
        message(Some("2"), None, "b"),
    ]);
    assert_eq!(texts(&new), ["a", "b"]);
    assert_eq!(cursor.start_time(), Some("t1"));
}

/// A user metric update named `name`, reported in `context`, carrying one of `scalar` or
/// `distribution`.
fn metric_update(
    name: &str,
    context: &[(&str, &str)],
    scalar: Option<serde_json::Value>,
    distribution: Option<serde_json::Value>,
) -> dataflow::client::MetricUpdateItem {
    dataflow::client::MetricUpdateItem {
        name: Some(dataflow::client::MetricStructuredName {
            origin: Some("user".to_string()),
            name: name.to_string(),
            context: context
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }),
        scalar,
        distribution,
        gauge: None,
        update_time: None,
    }
}

/// A user counter update named `name`, reported in `context`, with value `value`.
fn scalar_metric(
    name: &str,
    context: &[(&str, &str)],
    value: i64,
) -> dataflow::client::MetricUpdateItem {
    metric_update(name, context, Some(serde_json::json!(value)), None)
}

#[test]
fn test_job_metrics_response_to_metric_results() {
    let response = JobMetricsResponse {
        metrics: vec![
            scalar_metric(
                "PAssertSuccess/PAssert",
                &[
                    ("namespace", "PAssert"),
                    ("step", "PAssert/Check"),
                    ("tentative", "true"),
                ],
                1,
            ),
            scalar_metric(
                "PAssertSuccess/PAssert",
                &[("namespace", "PAssert"), ("step", "PAssert/Check")],
                1,
            ),
            metric_update(
                "my_dist",
                &[("namespace", "custom"), ("step", "DoFnStep")],
                None,
                Some(json!({
                    "count": 5,
                    "sum": 150,
                    "min": 10,
                    "max": 50
                })),
            ),
        ],
        metric_time: None,
    };

    let metric_results = response.to_metric_results();
    assert_eq!(
        metric_results.counter("PAssert", "PAssertSuccess/PAssert"),
        Some(1)
    );
    let dist = metric_results.distribution("custom", "my_dist").unwrap();
    assert_eq!(dist.count, 5);
    assert_eq!(dist.sum, 150);
    assert_eq!(dist.min, 10);
    assert_eq!(dist.max, 50);
}

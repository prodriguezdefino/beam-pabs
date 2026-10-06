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

//! Tests of `follow_job` with in-memory message streams.

use beam::runners::{JobState, PipelineResult};
use model::job_management::{
    JobMessage, JobMessagesResponse, JobStateEvent, job_message::MessageImportance,
    job_messages_response::Response, job_state,
};
use prism::{PrismRunnerError, follow_job};
use tonic::{Code, Status};

type Item = Result<JobMessagesResponse, Status>;
type Open = Result<Vec<Item>, Status>;

fn message(text: &str, importance: MessageImportance) -> Item {
    Ok(JobMessagesResponse {
        response: Some(Response::MessageResponse(JobMessage {
            message_text: text.to_string(),
            importance: importance as i32,
            ..Default::default()
        })),
    })
}

fn error(text: &str) -> Item {
    message(text, MessageImportance::JobMessageError)
}

fn state(state: job_state::Enum) -> Item {
    Ok(JobMessagesResponse {
        response: Some(Response::StateResponse(JobStateEvent {
            state: state as i32,
            timestamp: None,
        })),
    })
}

fn broken() -> Item {
    Err(Status::unknown(
        "h2 protocol error: stream closed because of a broken pipe",
    ))
}

/// Follows job `job1`. Each call to `open` takes the next entry of `opens`.
async fn follow(opens: Vec<Open>) -> (Result<PipelineResult, PrismRunnerError>, usize) {
    let mut opens = opens.into_iter();
    let mut count = 0;
    let outcome = follow_job("job1", || {
        count += 1;
        let next = opens
            .next()
            .unwrap_or_else(|| Err(Status::internal("the test has no more streams")));
        async move { next.map(tokio_stream::iter) }
    })
    .await;
    (outcome, count)
}

#[tokio::test(start_paused = true)]
async fn a_broken_stream_opens_again_and_reports_the_failure_cause() {
    let (outcome, opens) = follow(vec![
        Ok(vec![state(job_state::Enum::Running), broken()]),
        Ok(vec![
            state(job_state::Enum::Running),
            message("a retained message", MessageImportance::JobMessageBasic),
            state(job_state::Enum::Failed),
            error("PAssert 'Contains' failed: expected to contain [2, 4]"),
        ]),
    ])
    .await;

    assert_eq!(opens, 2);
    assert!(
        matches!(&outcome, Err(PrismRunnerError::JobFailed(detail))
            if detail == "job1: PAssert 'Contains' failed: expected to contain [2, 4]"),
        "{outcome:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_break_between_the_failed_state_and_its_cause_keeps_the_cause() {
    let (outcome, _) = follow(vec![
        Ok(vec![state(job_state::Enum::Failed), broken()]),
        Ok(vec![state(job_state::Enum::Failed), error("the cause")]),
    ])
    .await;

    assert!(
        matches!(&outcome, Err(PrismRunnerError::JobFailed(detail)) if detail == "job1: the cause"),
        "{outcome:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_job_done_after_a_break_succeeds() {
    let (outcome, opens) = follow(vec![
        Ok(vec![broken()]),
        Ok(vec![state(job_state::Enum::Done)]),
    ])
    .await;

    assert_eq!(opens, 2);
    assert!(
        matches!(&outcome, Ok(result) if result.state == JobState::Done && result.job_id == "job1"),
        "{outcome:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_cannot_open_again_is_lost() {
    let (outcome, _) = follow(vec![
        Ok(vec![broken()]),
        Err(Status::unavailable("connection refused")),
    ])
    .await;

    assert!(
        matches!(&outcome, Err(PrismRunnerError::MessageStreamLost { job_id, breaks: 1, cause })
            if job_id == "job1" && cause.code() == Code::Unavailable),
        "{outcome:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_breaks_every_time_is_lost_after_three_reopens() {
    let (outcome, opens) = follow(vec![Ok(vec![broken()]); 5]).await;

    assert_eq!(opens, 4);
    assert!(
        matches!(&outcome, Err(PrismRunnerError::MessageStreamLost { breaks: 4, cause, .. })
            if cause.code() == Code::Unknown),
        "{outcome:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_ends_before_a_terminal_state_is_premature() {
    let (outcome, opens) = follow(vec![Ok(vec![state(job_state::Enum::Running)])]).await;

    assert_eq!(opens, 1);
    assert!(
        matches!(&outcome, Err(PrismRunnerError::PrematureStreamEnd)),
        "{outcome:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_first_open_that_fails_is_a_grpc_error() {
    let (outcome, opens) = follow(vec![Err(Status::not_found("job with id job1 not found"))]).await;

    assert_eq!(opens, 1);
    assert!(
        matches!(&outcome, Err(PrismRunnerError::Grpc(status)) if status.code() == Code::NotFound),
        "{outcome:?}"
    );
}

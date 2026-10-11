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

use beam::runners::{JobState, PipelineResult};
use model::job_management::job_state;

#[test]
fn test_job_state_parse_and_display() {
    assert_eq!("DONE".parse(), Ok(JobState::Done));
    assert_eq!("JOB_STATE_DONE".parse(), Ok(JobState::Done));
    assert_eq!("failed".parse(), Ok(JobState::Failed));
    assert_eq!("CANCELLED".parse(), Ok(JobState::Cancelled));
    assert_eq!("CANCELED".parse(), Ok(JobState::Cancelled));
    assert_eq!("RUNNING".parse(), Ok(JobState::Running));
    assert_eq!("STOPPED".parse(), Ok(JobState::Stopped));
    assert_eq!("UPDATED".parse(), Ok(JobState::Updated));
    assert_eq!("DRAINING".parse(), Ok(JobState::Draining));
    assert_eq!("DRAINED".parse(), Ok(JobState::Drained));
    assert_eq!("STARTING".parse(), Ok(JobState::Starting));
    assert_eq!("CANCELLING".parse(), Ok(JobState::Cancelling));
    assert_eq!("PENDING".parse(), Ok(JobState::Pending));
    assert_eq!("QUEUED".parse(), Ok(JobState::Queued));
    assert_eq!("UNKNOWN_VALUE".parse(), Ok(JobState::Unknown));

    assert_eq!(JobState::Done.as_str(), "DONE");
    assert_eq!(JobState::Done.to_string(), "DONE");
}

#[test]
fn test_job_state_predicates() {
    assert!(JobState::Done.is_terminal());
    assert!(JobState::Failed.is_terminal());
    assert!(JobState::Cancelled.is_terminal());
    assert!(JobState::Updated.is_terminal());
    assert!(JobState::Drained.is_terminal());
    assert!(!JobState::Running.is_terminal());
    assert!(!JobState::Pending.is_terminal());

    assert!(JobState::Done.is_successful());
    assert!(JobState::Drained.is_successful());
    assert!(!JobState::Failed.is_successful());
    assert!(!JobState::Cancelled.is_successful());
}

#[test]
fn test_pipeline_result_state() {
    let res = PipelineResult::new("job-1", JobState::Done);
    assert_eq!(res.state, JobState::Done);

    let res2 = PipelineResult::new("job-2", JobState::Failed);
    assert_eq!(res2.state, JobState::Failed);
}

#[test]
fn test_proto_job_state_conversions() {
    assert_eq!(JobState::from(job_state::Enum::Done), JobState::Done);
    assert_eq!(JobState::from(job_state::Enum::Failed), JobState::Failed);
    assert_eq!(
        JobState::from(job_state::Enum::Cancelled),
        JobState::Cancelled
    );
    assert_eq!(JobState::from(job_state::Enum::Running), JobState::Running);
    assert_eq!(job_state::Enum::from(JobState::Done), job_state::Enum::Done);
    assert_eq!(
        job_state::Enum::from(JobState::Failed),
        job_state::Enum::Failed
    );
}

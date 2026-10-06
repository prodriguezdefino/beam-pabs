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

//! The job resources of the API: job states, jobs and job messages.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::constants::{
    JOB_STATE_CANCELLED, JOB_STATE_CANCELLING, JOB_STATE_DONE, JOB_STATE_DRAINED,
    JOB_STATE_DRAINING, JOB_STATE_FAILED, JOB_STATE_PENDING, JOB_STATE_QUEUED, JOB_STATE_RUNNING,
    JOB_STATE_STOPPED, JOB_STATE_UNKNOWN, JOB_STATE_UPDATED,
};

/// State of a Dataflow job in the v1b3 API (`JOB_STATE_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobState {
    Unknown,
    Stopped,
    Running,
    Done,
    Failed,
    Cancelled,
    Updated,
    Draining,
    Drained,
    Pending,
    Cancelling,
    Queued,
}

impl JobState {
    const ALL: [Self; 12] = [
        Self::Unknown,
        Self::Stopped,
        Self::Running,
        Self::Done,
        Self::Failed,
        Self::Cancelled,
        Self::Updated,
        Self::Draining,
        Self::Drained,
        Self::Pending,
        Self::Cancelling,
        Self::Queued,
    ];

    /// Returns the string representation of this job state.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => JOB_STATE_UNKNOWN,
            Self::Stopped => JOB_STATE_STOPPED,
            Self::Running => JOB_STATE_RUNNING,
            Self::Done => JOB_STATE_DONE,
            Self::Failed => JOB_STATE_FAILED,
            Self::Cancelled => JOB_STATE_CANCELLED,
            Self::Updated => JOB_STATE_UPDATED,
            Self::Draining => JOB_STATE_DRAINING,
            Self::Drained => JOB_STATE_DRAINED,
            Self::Pending => JOB_STATE_PENDING,
            Self::Cancelling => JOB_STATE_CANCELLING,
            Self::Queued => JOB_STATE_QUEUED,
        }
    }

    /// Returns `Unknown` for a name that this client does not know.
    pub fn parse(name: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|s| s.as_str() == name)
            .unwrap_or(Self::Unknown)
    }

    /// Returns true if a job in this state has stopped for good.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Done | Self::Failed | Self::Cancelled | Self::Updated | Self::Drained
        )
    }

    /// Returns true if the job finished successfully.
    pub fn is_successful(self) -> bool {
        matches!(self, Self::Done | Self::Drained)
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<JobState> for beam::runners::JobState {
    fn from(s: JobState) -> Self {
        match s {
            JobState::Unknown => Self::Unknown,
            JobState::Stopped => Self::Stopped,
            JobState::Running => Self::Running,
            JobState::Done => Self::Done,
            JobState::Failed => Self::Failed,
            JobState::Cancelled => Self::Cancelled,
            JobState::Updated => Self::Updated,
            JobState::Draining => Self::Draining,
            JobState::Drained => Self::Drained,
            JobState::Pending => Self::Pending,
            JobState::Cancelling => Self::Cancelling,
            JobState::Queued => Self::Queued,
        }
    }
}

/// Returns true if the given job state string represents a terminal state.
pub fn is_terminal_state(state: &str) -> bool {
    JobState::parse(state).is_terminal()
}

/// Returns true if the given job state string represents a successful state.
pub fn is_successful_state(state: &str) -> bool {
    JobState::parse(state).is_successful()
}

/// Response returned by `jobs.create` and `jobs.get`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobResponse {
    pub id: String,
    pub project_id: Option<String>,
    pub name: String,
    #[serde(rename = "type")]
    pub job_type: Option<String>,
    #[serde(default)]
    pub current_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_state_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// The `clientRequestId` of the request that created the job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_request_id: Option<String>,
}

/// Response returned by `jobs.messages.list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListJobMessagesResponse {
    #[serde(default)]
    pub job_messages: Vec<JobMessageItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Individual job status message from Dataflow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobMessageItem {
    pub id: Option<String>,
    pub time: Option<String>,
    pub message_text: Option<String>,
    pub message_importance: Option<String>,
}

/// Position in a job message stream, used to fetch each message exactly once.
///
/// The `startTime` filter of `messages.list` is inclusive, so each poll returns the messages
/// at the last seen timestamp again. The cursor filters out the IDs already returned at that
/// timestamp. A message without an ID is always treated as new.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageCursor {
    time: Option<String>,
    ids_at_time: BTreeSet<String>,
}

impl MessageCursor {
    pub fn start_time(&self) -> Option<&str> {
        self.time.as_deref()
    }

    /// Splits a listed page into the messages not returned before and the cursor for the
    /// next poll. Expects messages in ascending time order.
    pub fn advance(self, messages: Vec<JobMessageItem>) -> (Vec<JobMessageItem>, Self) {
        let unseen: Vec<JobMessageItem> = messages
            .into_iter()
            .filter(|msg| !self.has_seen(msg))
            .collect();
        let next = unseen.iter().fold(self, Self::record);
        (unseen, next)
    }

    fn has_seen(&self, msg: &JobMessageItem) -> bool {
        msg.time.is_some()
            && msg.time == self.time
            && msg
                .id
                .as_ref()
                .is_some_and(|id| self.ids_at_time.contains(id))
    }

    fn record(self, msg: &JobMessageItem) -> Self {
        match &msg.time {
            None => self,
            Some(time) if self.time.as_ref() == Some(time) => Self {
                ids_at_time: self.ids_at_time.into_iter().chain(msg.id.clone()).collect(),
                ..self
            },
            Some(time) => Self {
                time: Some(time.clone()),
                ids_at_time: msg.id.iter().cloned().collect(),
            },
        }
    }
}

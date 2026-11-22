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

//! Failure modes of the Nexmark launcher.

use std::fmt;

use beam::external::ExpansionError;
use beam::options::OptionsError;
use beam::runners::{JobState, RunnerError};

use crate::config::SourceType;

/// Failure modes encountered while configuring or running a Nexmark pipeline.
#[derive(Debug)]
pub enum LauncherError {
    /// A flag that the selected source, sink or mode needs is missing or empty.
    MissingFlag {
        flag: &'static str,
        /// What needs the flag, for example `reading events from Pub/Sub`.
        needed_by: &'static str,
    },
    /// The source reads an endless stream, so the run needs `--streaming`.
    NeedsStreaming(SourceType),
    /// The expansion service could not expand a cross-language transform.
    Expansion(ExpansionError),
    /// A transform rejected its configuration when it expanded.
    Transform(beam::Error),
    /// Updating or snapshotting `PipelineOptions` failed.
    Options(OptionsError),
    /// Runner execution failed.
    Runner(RunnerError),
    /// The job ended in a state other than `DONE` or `DRAINED`.
    Unsuccessful(JobState),
    /// A `--assert_correctness` check did not run or did not pass.
    Correctness(String),
    /// The streaming monitor cancelled the job because of fatal errors or no progress.
    Stopped(String),
    /// The rows of the BigQuery summary could not be built.
    Summary(String),
}

impl fmt::Display for LauncherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingFlag { flag, needed_by } => {
                write!(f, "{flag} is required for {needed_by}")
            }
            Self::NeedsStreaming(source) => write!(
                f,
                "--source_type={source:?} reads an endless stream; add --streaming or use \
                 --pubsub_mode=PUBLISH_ONLY"
            ),
            Self::Expansion(err) => write!(f, "cross-language expansion error: {err}"),
            Self::Transform(err) => write!(f, "transform error: {err}"),
            Self::Options(err) => write!(f, "pipeline options error: {err}"),
            Self::Runner(err) => write!(f, "pipeline execution error: {err}"),
            Self::Unsuccessful(state) => write!(f, "job ended in state {state}"),
            Self::Correctness(reason) => write!(f, "correctness check failed: {reason}"),
            Self::Stopped(reason) => write!(f, "streaming job cancelled: {reason}"),
            Self::Summary(reason) => write!(f, "cannot build the BigQuery summary: {reason}"),
        }
    }
}

impl std::error::Error for LauncherError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Expansion(err) => Some(err),
            Self::Transform(err) => err
                .source()
                .map(|source| source as &(dyn std::error::Error + 'static)),
            Self::Options(err) => Some(err),
            Self::Runner(err) => Some(err),
            Self::MissingFlag { .. }
            | Self::NeedsStreaming(_)
            | Self::Unsuccessful(_)
            | Self::Correctness(_)
            | Self::Stopped(_)
            | Self::Summary(_) => None,
        }
    }
}

impl From<ExpansionError> for LauncherError {
    fn from(err: ExpansionError) -> Self {
        Self::Expansion(err)
    }
}

impl From<beam::Error> for LauncherError {
    fn from(err: beam::Error) -> Self {
        Self::Transform(err)
    }
}

impl From<OptionsError> for LauncherError {
    fn from(err: OptionsError) -> Self {
        Self::Options(err)
    }
}

impl From<RunnerError> for LauncherError {
    fn from(err: RunnerError) -> Self {
        Self::Runner(err)
    }
}

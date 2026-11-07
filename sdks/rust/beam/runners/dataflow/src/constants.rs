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

//! Canonical constants for Google Cloud Dataflow runner translation and execution.

use std::time::Duration;

use beam::retry::Backoff;

pub use beam::pipeline::{
    OPTION_NAMESPACE_CORE, OPTION_NAMESPACE_DATAFLOW, OPTION_NAMESPACE_GCP, OPTION_NAMESPACE_RUNNER,
};

/// Current Apache Beam SDK version, matching Beam release stream (>= 2.21.0 required by Dataflow).
pub use beam::pipeline::BEAM_SDK_VERSION;

/// Default Dataflow v1b3 API endpoint.
pub const DEFAULT_DATAFLOW_ENDPOINT: &str = "https://dataflow.googleapis.com";

/// Dataflow Job Types
pub const JOB_TYPE_BATCH: &str = "JOB_TYPE_BATCH";
pub const JOB_TYPE_STREAMING: &str = "JOB_TYPE_STREAMING";

/// Dataflow FnAPI Version Types
pub const FNAPI_BATCH: &str = "FNAPI_BATCH";
pub const FNAPI_STREAMING: &str = "FNAPI_STREAMING";

/// Runner v2 Experiment Flags
pub const EXPERIMENT_USE_RUNNER_V2: &str = "use_runner_v2";
pub const EXPERIMENT_STREAMING_ENGINE_STATE_TAG_ENCODING_V2_SUPPORTED: &str =
    "streaming_engine_state_tag_encoding_v2_supported";
pub const EXPERIMENT_ENABLE_STREAMING_ENGINE_STATE_TAG_ENCODING_V2: &str =
    "enable_streaming_engine_state_tag_encoding_v2";

/// Worker Pool Configuration Constants
pub const WORKER_POOL_KIND_HARNESS: &str = "harness";
pub const WORKER_IP_UNSPECIFIED: &str = "WORKER_IP_UNSPECIFIED";
pub const WORKER_IP_PRIVATE: &str = "WORKER_IP_PRIVATE";

/// The API value of `--autoscaling_algorithm=NONE`: a fixed number of workers.
pub const AUTOSCALING_ALGORITHM_NONE: &str = "AUTOSCALING_ALGORITHM_NONE";
/// The API value of `--autoscaling_algorithm=THROUGHPUT_BASED`: workers scale with throughput.
pub const AUTOSCALING_ALGORITHM_BASIC: &str = "AUTOSCALING_ALGORITHM_BASIC";

/// Staged Artifact Names
pub const STAGED_WORKER_NAME: &str = "worker";
pub const STAGED_MODEL_NAME: &str = "model";

/// Environment & Agent Metadata
pub const DEFAULT_DATAFLOW_MAJOR_VERSION: &str = "6";
pub const DATAFLOW_USER_AGENT_NAME: &str = "Apache Beam Rust SDK";
pub const DATAFLOW_RUNNER_DISPLAY_NAME: &str = "DataflowRunner";
pub const DEFAULT_RUST_ENVIRONMENT_ID: &str = "rust_env";

/// Default interval between polling requests when waiting for a Dataflow job to finish.
pub const DEFAULT_JOB_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Consecutive failed job-status requests tolerated while waiting for a job, before the
/// wait gives up and reports the last error. Transient failures reset the count.
pub const MAX_CONSECUTIVE_JOB_STATUS_ERRORS: u32 = 10;

/// How many times the client sends a request that fails with a transient error.
pub const MAX_REQUEST_ATTEMPTS: u32 = 4;

/// The retries of a request: waits from 1 second, doubled before each next retry.
pub const REQUEST_RETRY: Backoff = Backoff {
    attempts: MAX_REQUEST_ATTEMPTS,
    initial: Duration::from_secs(1),
    max: Duration::from_secs(60),
};

/// Terminal and active Dataflow job states.
pub const JOB_STATE_UNKNOWN: &str = "JOB_STATE_UNKNOWN";
pub const JOB_STATE_STOPPED: &str = "JOB_STATE_STOPPED";
pub const JOB_STATE_RUNNING: &str = "JOB_STATE_RUNNING";
pub const JOB_STATE_DONE: &str = "JOB_STATE_DONE";
pub const JOB_STATE_FAILED: &str = "JOB_STATE_FAILED";
pub const JOB_STATE_CANCELLED: &str = "JOB_STATE_CANCELLED";
pub const JOB_STATE_UPDATED: &str = "JOB_STATE_UPDATED";
pub const JOB_STATE_DRAINING: &str = "JOB_STATE_DRAINING";
pub const JOB_STATE_DRAINED: &str = "JOB_STATE_DRAINED";
pub const JOB_STATE_PENDING: &str = "JOB_STATE_PENDING";
pub const JOB_STATE_CANCELLING: &str = "JOB_STATE_CANCELLING";
pub const JOB_STATE_QUEUED: &str = "JOB_STATE_QUEUED";

/// Error importance level for Dataflow job messages.
pub const JOB_MESSAGE_ERROR: &str = "JOB_MESSAGE_ERROR";

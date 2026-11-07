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

//! CLI argument parsing, pipeline assembly, monitoring, and summary reporting for Nexmark runs.

mod args;
mod avro;
mod bigquery;
mod error;
mod monitor;
mod perf;
mod pipeline;
mod report;
mod run;
mod side_input;
mod stop;
mod transport;
mod watch;

pub use self::args::NexmarkArgs;
pub use self::avro::{AVRO_AUCTION_FILES, AVRO_BID_FILES, AVRO_EVENT_FILES, AVRO_PERSON_FILES};
pub use self::bigquery::{RunInfo, SummaryRow, build_summary_pipeline, summary_rows};
pub use self::error::LauncherError;
pub use self::monitor::{
    BYTES, ELEMENTS, EVENT_TIMESTAMP_MS, EVENTS, METRIC_NAMESPACE, Monitor, PROCESSING_TIME_MS,
    RESULTS, metric_name,
};
pub use self::perf::{NexmarkPerf, ProgressSnapshot, steady_state_events_per_sec};
pub use self::pipeline::{build_nexmark_pipeline, is_prism_runner};
pub use self::report::{NexmarkRunOutcome, QueryRunSummary, format_summary_table, run_nexmark};
pub use self::side_input::build_side_input_pipeline;
pub use self::stop::{
    Decision, MonitorState, MonitorTiming, Observation, StopRules, decide, running_time_limit,
};
pub use self::transport::{
    DECODE_ERRORS, PUBSUB_ID_ATTRIBUTE, PUBSUB_TIMESTAMP_ATTRIBUTE, decode_event, encode_event,
};
pub use self::watch::{
    StopReason, StoppedJobs, StreamingRun, monitor_streaming_job, with_publisher,
};

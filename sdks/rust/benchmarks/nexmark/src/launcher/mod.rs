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
mod monitor;
mod perf;
mod pipeline;
mod report;
mod stop;
mod watch;

pub use self::args::NexmarkArgs;
pub use self::monitor::{
    BYTES, ELEMENTS, EVENT_TIMESTAMP_MS, EVENTS, METRIC_NAMESPACE, Monitor, PROCESSING_TIME_MS,
    RESULTS, metric_name,
};
pub use self::perf::{NexmarkPerf, ProgressSnapshot, steady_state_events_per_sec};
pub use self::pipeline::{LauncherError, build_nexmark_pipeline, is_prism_runner};
pub use self::report::{NexmarkRunOutcome, QueryRunSummary, format_summary_table, run_nexmark};
pub use self::stop::{Decision, MonitorState, MonitorTiming, Observation, StopRules, decide};
pub use self::watch::{StopReason, StreamingRun, monitor_streaming_job};

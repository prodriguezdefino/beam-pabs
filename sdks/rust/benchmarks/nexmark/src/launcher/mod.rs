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

//! CLI argument parsing, pipeline assembly, and summary reporting for Nexmark runs.

mod args;
mod pipeline;
mod report;

pub use self::args::NexmarkArgs;
pub use self::pipeline::{
    COUNTER_SINK_BYTES, COUNTER_SINK_RECORDS, COUNTER_SOURCE_BYTES, COUNTER_SOURCE_EVENTS,
    LauncherError, METRIC_NAMESPACE, build_nexmark_pipeline, is_prism_runner,
};
pub use self::report::{NexmarkRunOutcome, QueryRunSummary, format_summary_table, run_nexmark};
